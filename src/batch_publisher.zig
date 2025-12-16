const std = @import("std");
const nats_publisher = @import("nats_publisher.zig");
const msgpack = @import("msgpack");
const pgoutput = @import("pgoutput.zig");
const Config = @import("config.zig");
const encoder_mod = @import("encoder.zig");

pub const log = std.log.scoped(.batch_publisher);

/// Convert pgoutput.Column value to encoder.Value
fn columnValueToEncoderValue(
    encoder: *encoder_mod.Encoder,
    column_value: pgoutput.DecodedValue,
) !encoder_mod.Value {
    return switch (column_value) {
        .int32 => |v| encoder.createInt(@intCast(v)),
        .int64 => |v| encoder.createInt(v),
        .float64 => |v| encoder.createFloat(v),
        .boolean => |v| encoder.createBool(v),
        .text, .bytea, .array, .numeric => |v| try encoder.createString(v),
        .jsonb => |v| blk: {
            // For JSONB columns in MessagePack mode, parse and convert to native types
            // For JSON mode, just treat as a string (simpler and avoids ownership issues)
            break :blk switch (encoder.format) {
                .msgpack => blk2: {
                    // Parse JSON and convert to MessagePack native types
                    const parsed = std.json.parseFromSlice(
                        std.json.Value,
                        encoder.allocator,
                        v,
                        .{},
                    ) catch {
                        // If parsing fails, fall back to string
                        break :blk2 try encoder.createString(v);
                    };
                    defer parsed.deinit();

                    const mp = try jsonValueToMsgpack(parsed.value, encoder.allocator);
                    break :blk2 encoder_mod.Value{ .msgpack = mp };
                },
                .json => try encoder.createString(v), // Keep JSONB as JSON string
            };
        },
        .null => encoder.createNull(),
    };
}

/// Convert std.json.Value to msgpack.Payload recursively (for JSONB columns in MessagePack mode)
fn jsonValueToMsgpack(value: std.json.Value, allocator: std.mem.Allocator) !msgpack.Payload {
    return switch (value) {
        .null => msgpack.Payload{ .nil = {} },
        .bool => |b| msgpack.Payload{ .bool = b },
        .integer => |i| msgpack.Payload{ .int = i },
        .float => |f| msgpack.Payload{ .float = f },
        .number_string => |s| blk: {
            // Try to parse as number, fallback to string
            if (std.fmt.parseInt(i64, s, 10)) |int_val| {
                break :blk msgpack.Payload{ .int = int_val };
            } else |_| {
                if (std.fmt.parseFloat(f64, s)) |float_val| {
                    break :blk msgpack.Payload{ .float = float_val };
                } else |_| {
                    break :blk try msgpack.Payload.strToPayload(s, allocator);
                }
            }
        },
        .string => |s| try msgpack.Payload.strToPayload(s, allocator),
        .array => |arr| blk: {
            var msgpack_arr = try allocator.alloc(msgpack.Payload, arr.items.len);
            for (arr.items, 0..) |item, i| {
                msgpack_arr[i] = try jsonValueToMsgpack(item, allocator);
            }
            break :blk msgpack.Payload{ .arr = msgpack_arr };
        },
        .object => |obj| blk: {
            var map_payload = msgpack.Payload.mapPayload(allocator);
            var it = obj.iterator();
            while (it.next()) |entry| {
                const val = try jsonValueToMsgpack(entry.value_ptr.*, allocator);
                try map_payload.mapPut(entry.key_ptr.*, val);
            }
            break :blk map_payload;
        },
    };
}

/// Configuration for batch publishing
pub const BatchConfig = struct {
    /// Maximum number of events per batch
    max_events: usize = Config.Batch.max_events,
    /// Maximum time to wait before flushing (milliseconds)
    max_wait_ms: i64 = Config.Batch.max_age_ms,
    /// Maximum payload size in bytes
    max_payload_bytes: usize = 128 * 1024, // 128KB
};

/// A single CDC event to be batched
pub const CDCEvent = struct {
    subject: [:0]const u8,
    table: []const u8,
    operation: []const u8,
    msg_id: []const u8,
    relation_id: u32, // PostgreSQL relation OID (for schema version tracking)
    data: ?std.ArrayList(pgoutput.Column),
    lsn: u64, // WAL LSN for this event

    pub fn deinit(self: *CDCEvent, allocator: std.mem.Allocator) void {
        allocator.free(self.subject);
        allocator.free(self.table);
        allocator.free(self.operation);
        allocator.free(self.msg_id);

        if (self.data) |*columns| {
            // Free slice-based values in columns
            for (columns.items) |column| {
                // Note: column.name is NOT owned (points to RelationMessage.columns)
                // Only free the value if it's a slice type
                switch (column.value) {
                    .text => |txt| allocator.free(txt),
                    .numeric => |num| allocator.free(num),
                    .array => |arr| allocator.free(arr),
                    .jsonb => |jsn| allocator.free(jsn),
                    .bytea => |byt| allocator.free(byt),
                    else => {}, // int32, int64, float64, boolean, null don't need freeing
                }
            }
            columns.deinit(allocator);
        }
    }
};

/// Batches CDC events upon the BatchConfig and publishes them to NATS
pub const BatchPublisher = struct {
    allocator: std.mem.Allocator,
    publisher: *nats_publisher.Publisher,
    config: BatchConfig,
    format: encoder_mod.Format,

    // Batch state
    events: std.ArrayList(CDCEvent),
    current_payload_size: usize,
    last_flush_time: i64,
    last_confirmed_lsn: u64,

    pub fn init(
        allocator: std.mem.Allocator,
        publisher: *nats_publisher.Publisher,
        config: BatchConfig,
        format: encoder_mod.Format,
    ) BatchPublisher {
        return .{
            .allocator = allocator,
            .publisher = publisher,
            .config = config,
            .format = format,
            .events = std.ArrayList(CDCEvent){},
            .current_payload_size = 0,
            .last_flush_time = std.time.milliTimestamp(),
            .last_confirmed_lsn = 0,
        };
    }

    pub fn deinit(self: *BatchPublisher) void {
        // Flush any remaining events
        _ = self.flush() catch |err| {
            log.err("Failed to flush remaining events during deinit: {}", .{err});
        };

        // Clean up any remaining events
        for (self.events.items) |*event| {
            event.deinit(self.allocator);
        }
        self.events.deinit(self.allocator);
    }

    /// Add an event to the batch. Flushes automatically if batch is full.
    pub fn addEvent(
        self: *BatchPublisher,
        subject: []const u8,
        table: []const u8,
        operation: []const u8,
        msg_id: []const u8,
        relation_id: u32,
        data: ?std.ArrayList(pgoutput.Column),
        lsn: u64,
    ) !void {
        // Make owned copies of the metadata strings
        const owned_subject = try self.allocator.dupeZ(u8, subject);
        errdefer self.allocator.free(owned_subject);

        const owned_table = try self.allocator.dupe(u8, table);
        errdefer self.allocator.free(owned_table);

        const owned_operation = try self.allocator.dupe(u8, operation);
        errdefer self.allocator.free(owned_operation);

        const owned_msg_id = try self.allocator.dupe(u8, msg_id);
        errdefer self.allocator.free(owned_msg_id);

        // Transfer ownership of columns ArrayList (zero-copy optimization)
        const event = CDCEvent{
            .subject = owned_subject,
            .table = owned_table,
            .operation = owned_operation,
            .msg_id = owned_msg_id,
            .relation_id = relation_id,
            .data = data,
            .lsn = lsn,
        };

        try self.events.append(self.allocator, event);
        // Approximate payload size (table + operation strings)
        self.current_payload_size += table.len + operation.len;

        // Check if we should flush based on count or size
        if (self.events.items.len >= self.config.max_events or
            self.current_payload_size >= self.config.max_payload_bytes)
        {
            _ = try self.flush();
        }
    }

    /// Check if batch should be flushed based on time
    pub fn shouldFlushByTime(self: *BatchPublisher) bool {
        if (self.events.items.len == 0) return false;

        const now = std.time.milliTimestamp();
        const elapsed = now - self.last_flush_time;
        return elapsed >= self.config.max_wait_ms;
    }

    /// Get the last LSN that was successfully confirmed by NATS
    pub fn getLastConfirmedLsn(self: *BatchPublisher) u64 {
        return self.last_confirmed_lsn;
    }

    /// Flush accumulated events to NATS
    /// Returns the maximum LSN that was successfully flushed, or 0 if no events
    pub fn flush(self: *BatchPublisher) !u64 {
        if (self.events.items.len == 0) return 0;

        // Calculate maximum LSN in this batch
        var max_lsn: u64 = 0;
        for (self.events.items) |event| {
            if (event.lsn > max_lsn) {
                max_lsn = event.lsn;
            }
        }

        const flush_start = std.time.milliTimestamp();
        const event_count = self.events.items.len;

        log.info("📦 Starting flush of {d} events", .{event_count});

        // For single event, encode and publish directly
        if (event_count == 1) {
            const event = self.events.items[0];

            // Use unified encoder
            var encoder = encoder_mod.Encoder.init(self.allocator, self.format);
            defer encoder.deinit();

            var event_map = encoder.createMap();
            defer event_map.free(self.allocator);

            try event_map.put("subject", try encoder.createString(event.subject));
            try event_map.put("table", try encoder.createString(event.table));
            try event_map.put("operation", try encoder.createString(event.operation));
            try event_map.put("msg_id", try encoder.createString(event.msg_id));
            try event_map.put("relation_id", encoder.createInt(@intCast(event.relation_id)));
            try event_map.put("lsn", encoder.createInt(@intCast(event.lsn)));

            // Add column data if present
            if (event.data) |columns| {
                log.debug("Single event has {d} columns", .{columns.items.len});
                var data_map = encoder.createMap();

                for (columns.items) |column| {
                    const value_enc = try columnValueToEncoderValue(&encoder, column.value);
                    try data_map.put(column.name, value_enc);
                }

                try event_map.put("data", data_map);
            }

            const encoded = try encoder.encode(event_map);
            defer self.allocator.free(encoded);

            try self.publisher.publish(event.subject, encoded, event.msg_id);

            // Flush async publishes to actually send them to NATS
            try self.publisher.flushAsync();

            log.debug("Published single event: {s}", .{event.subject});
        } else {
            // Use unified encoder for batch publishing
            var encoder = encoder_mod.Encoder.init(self.allocator, self.format);
            defer encoder.deinit();

            // Create array of events
            var batch_array = try encoder.createArray(event_count);
            defer batch_array.free(self.allocator);

            const encode_start = std.time.milliTimestamp();

            for (self.events.items, 0..) |event, i| {
                // Each event is a map with subject, table, operation, msg_id, and data
                var event_map = encoder.createMap();

                try event_map.put("subject", try encoder.createString(event.subject));
                try event_map.put("table", try encoder.createString(event.table));
                try event_map.put("operation", try encoder.createString(event.operation));
                try event_map.put("msg_id", try encoder.createString(event.msg_id));
                try event_map.put("relation_id", encoder.createInt(@intCast(event.relation_id)));
                try event_map.put("lsn", encoder.createInt(@intCast(event.lsn)));

                // Add column data if present
                if (event.data) |columns| {
                    var data_map = encoder.createMap();

                    for (columns.items) |column| {
                        const value_enc = try columnValueToEncoderValue(&encoder, column.value);
                        try data_map.put(column.name, value_enc);
                    }

                    try event_map.put("data", data_map);
                }

                try batch_array.setIndex(i, event_map);
            }

            const encoded = try encoder.encode(batch_array);
            defer self.allocator.free(encoded);

            const encode_elapsed = std.time.milliTimestamp() - encode_start;

            // Publish the batch with a composite message ID
            const publish_start = std.time.milliTimestamp();
            const first_msg_id = self.events.items[0].msg_id;
            const last_msg_id = self.events.items[event_count - 1].msg_id;
            const batch_msg_id = try std.fmt.allocPrint(
                self.allocator,
                "batch-{s}-to-{s}",
                .{ first_msg_id, last_msg_id },
            );
            defer self.allocator.free(batch_msg_id);

            // Use first event's subject pattern but with .batch suffix
            const batch_subject = try std.fmt.allocPrintSentinel(
                self.allocator,
                "{s}.batch",
                .{self.events.items[0].subject},
                0,
            );
            defer self.allocator.free(batch_subject);

            try self.publisher.publish(batch_subject, encoded, batch_msg_id);
            const publish_elapsed = std.time.milliTimestamp() - publish_start;

            // Flush async publishes to actually send them to NATS
            try self.publisher.flushAsync();

            log.info("Published batch: {d} events, {d} bytes to {s} (encode: {d}ms, publish: {d}ms)", .{
                event_count,
                encoded.len,
                batch_subject,
                encode_elapsed,
                publish_elapsed,
            });
        }

        log.info("Cleaning up {d} events after successful flush", .{event_count});

        // Clean up events
        for (self.events.items) |*event| {
            event.deinit(self.allocator);
        }
        self.events.clearRetainingCapacity();
        self.current_payload_size = 0;
        self.last_flush_time = std.time.milliTimestamp();

        // Log flush timing if it took longer than expected
        const flush_elapsed = std.time.milliTimestamp() - flush_start;
        if (flush_elapsed > 5) {
            log.warn(
                "Slow flush: {d}ms for {d} events",
                .{ flush_elapsed, event_count },
            );
        }

        // Store confirmed LSN after successful flush
        self.last_confirmed_lsn = max_lsn;
        log.debug("Flushed batch with max LSN: {x}", .{max_lsn});
        return max_lsn;
    }
};
