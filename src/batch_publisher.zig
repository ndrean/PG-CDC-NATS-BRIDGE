const std = @import("std");
const nats_publisher = @import("nats_publisher.zig");
const msgpack = @import("msgpack");
const pgoutput = @import("pgoutput.zig");

pub const log = std.log.scoped(.batch_publisher);

/// Configuration for batch publishing
pub const BatchConfig = struct {
    /// Maximum number of events per batch
    max_events: usize = 200,
    /// Maximum time to wait before flushing (milliseconds)
    max_wait_ms: i64 = 100,
    /// Maximum payload size in bytes
    max_payload_bytes: usize = 128 * 1024, // 128KB
};

/// A single CDC event to be batched
pub const CDCEvent = struct {
    subject: [:0]const u8,
    table: []const u8,
    operation: []const u8,
    msg_id: []const u8,
    data: ?std.StringHashMap(pgoutput.DecodedValue),

    pub fn deinit(self: *CDCEvent, allocator: std.mem.Allocator) void {
        allocator.free(self.subject);
        allocator.free(self.table);
        allocator.free(self.operation);
        allocator.free(self.msg_id);

        if (self.data) |*data_map| {
            // Free all keys and text values in the hashmap
            var it = data_map.iterator();
            while (it.next()) |entry| {
                allocator.free(entry.key_ptr.*);
                // Free text values (other types are copied by value, no allocation)
                switch (entry.value_ptr.*) {
                    .text => |txt| allocator.free(txt),
                    else => {},
                }
            }
            data_map.deinit();
        }
    }
};

/// Batches CDC events upon the BatchConfig and publishes them to NATS
pub const BatchPublisher = struct {
    allocator: std.mem.Allocator,
    publisher: *nats_publisher.Publisher,
    config: BatchConfig,

    // Batch state
    events: std.ArrayList(CDCEvent),
    current_payload_size: usize,
    last_flush_time: i64,

    pub fn init(
        allocator: std.mem.Allocator,
        publisher: *nats_publisher.Publisher,
        config: BatchConfig,
    ) BatchPublisher {
        return .{
            .allocator = allocator,
            .publisher = publisher,
            .config = config,
            .events = std.ArrayList(CDCEvent){},
            .current_payload_size = 0,
            .last_flush_time = std.time.milliTimestamp(),
        };
    }

    pub fn deinit(self: *BatchPublisher) void {
        // Flush any remaining events
        self.flush() catch |err| {
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
        data: ?std.StringHashMap(pgoutput.DecodedValue),
    ) !void {
        // Make owned copies of the data
        const owned_subject = try self.allocator.dupeZ(u8, subject);
        errdefer self.allocator.free(owned_subject);

        const owned_table = try self.allocator.dupe(u8, table);
        errdefer self.allocator.free(owned_table);

        const owned_operation = try self.allocator.dupe(u8, operation);
        errdefer self.allocator.free(owned_operation);

        const owned_msg_id = try self.allocator.dupe(u8, msg_id);
        errdefer self.allocator.free(owned_msg_id);

        // Clone the data hashmap with owned keys and owned text values
        var owned_data: ?std.StringHashMap(pgoutput.DecodedValue) = null;
        if (data) |data_map| {
            owned_data = std.StringHashMap(pgoutput.DecodedValue).init(self.allocator);
            var it = data_map.iterator();
            while (it.next()) |entry| {
                const owned_key = try self.allocator.dupe(u8, entry.key_ptr.*);

                // Deep copy the value, especially text fields
                const owned_value = switch (entry.value_ptr.*) {
                    .text => |txt| pgoutput.DecodedValue{ .text = try self.allocator.dupe(u8, txt) },
                    else => entry.value_ptr.*, // int32, int64, float64, boolean are copied by value
                };

                try owned_data.?.put(owned_key, owned_value);
            }
        }

        const event = CDCEvent{
            .subject = owned_subject,
            .table = owned_table,
            .operation = owned_operation,
            .msg_id = owned_msg_id,
            .data = owned_data,
        };

        try self.events.append(self.allocator, event);
        // Approximate payload size (table + operation strings)
        self.current_payload_size += table.len + operation.len;

        // Check if we should flush based on count or size
        if (self.events.items.len >= self.config.max_events or
            self.current_payload_size >= self.config.max_payload_bytes)
        {
            try self.flush();
        }
    }

    /// Check if batch should be flushed based on time
    pub fn shouldFlushByTime(self: *BatchPublisher) bool {
        if (self.events.items.len == 0) return false;

        const now = std.time.milliTimestamp();
        const elapsed = now - self.last_flush_time;
        return elapsed >= self.config.max_wait_ms;
    }

    /// Flush accumulated events to NATS
    pub fn flush(self: *BatchPublisher) !void {
        if (self.events.items.len == 0) return;

        const flush_start = std.time.milliTimestamp();
        const event_count = self.events.items.len;

        // For single event, encode and publish directly
        if (event_count == 1) {
            const event = self.events.items[0];

            // Use ArrayList for dynamic buffer (no fixed size limit)
            var buffer = std.ArrayList(u8){};
            defer buffer.deinit(self.allocator);

            // Create a stream wrapper for ArrayList
            const ArrayListStream = struct {
                list: *std.ArrayList(u8),
                allocator: std.mem.Allocator,
                pos: usize = 0,

                const Stream = @This();
                pub const WriteError = std.mem.Allocator.Error;
                pub const ReadError = error{EndOfStream};

                pub fn write(stream: *Stream, bytes: []const u8) WriteError!usize {
                    try stream.list.appendSlice(stream.allocator, bytes);
                    return bytes.len;
                }

                pub fn read(stream: *Stream, dest: []u8) ReadError!usize {
                    const available = stream.list.items.len - stream.pos;
                    if (available == 0) return 0;
                    const to_read = @min(dest.len, available);
                    @memcpy(dest[0..to_read], stream.list.items[stream.pos..][0..to_read]);
                    stream.pos += to_read;
                    return to_read;
                }
            };

            var write_stream = ArrayListStream{ .list = &buffer, .allocator = self.allocator };
            var read_stream = ArrayListStream{ .list = &buffer, .allocator = self.allocator };

            var packer = msgpack.Pack(
                *ArrayListStream,
                *ArrayListStream,
                ArrayListStream.WriteError,
                ArrayListStream.ReadError,
                ArrayListStream.write,
                ArrayListStream.read,
            ).init(&write_stream, &read_stream);

            var event_map = msgpack.Payload.mapPayload(self.allocator);
            errdefer event_map.free(self.allocator);

            try event_map.mapPut("subject", try msgpack.Payload.strToPayload(event.subject, self.allocator));
            try event_map.mapPut("table", try msgpack.Payload.strToPayload(event.table, self.allocator));
            try event_map.mapPut("operation", try msgpack.Payload.strToPayload(event.operation, self.allocator));
            try event_map.mapPut("msg_id", try msgpack.Payload.strToPayload(event.msg_id, self.allocator));

            // Add column data if present
            if (event.data) |data_map| {
                log.debug("Single event has {d} columns", .{data_map.count()});
                var data_payload = msgpack.Payload.mapPayload(self.allocator);
                // Don't defer - needs to stay alive until packer.write() completes

                var it = data_map.iterator();
                while (it.next()) |entry| {
                    const col_name = entry.key_ptr.*;
                    const value = entry.value_ptr.*;

                    const value_payload = switch (value) {
                        .int32 => |v| msgpack.Payload{ .int = @intCast(v) },
                        .int64 => |v| msgpack.Payload{ .int = v },
                        .float64 => |v| msgpack.Payload{ .float = v },
                        .boolean => |v| msgpack.Payload{ .bool = v },
                        .text => |v| try msgpack.Payload.strToPayload(v, self.allocator),
                    };

                    try data_payload.mapPut(col_name, value_payload);
                }

                try event_map.mapPut("data", data_payload);
            }

            try packer.write(event_map);
            const encoded = buffer.items;

            try self.publisher.publish(event.subject, encoded, event.msg_id);
            log.debug("Published single event: {s}", .{event.subject});

            // Free payload structures after encoding is complete
            event_map.free(self.allocator);
        } else {
            // Use arena allocator for MessagePack encoding to reduce allocations
            // This reduces ~300 allocations per batch to just 1
            var arena = std.heap.ArenaAllocator.init(self.allocator);
            defer arena.deinit();
            const encoding_allocator = arena.allocator();

            // Publish as a batch using MessagePack array
            var buffer: [131072]u8 = undefined; // 128KB buffer
            const compat = msgpack.compat;
            var write_buffer = compat.fixedBufferStream(&buffer);
            var read_buffer = compat.fixedBufferStream(&buffer);

            const BufferType = compat.BufferStream;
            var packer = msgpack.Pack(
                *BufferType,
                *BufferType,
                BufferType.WriteError,
                BufferType.ReadError,
                BufferType.write,
                BufferType.read,
            ).init(&write_buffer, &read_buffer);

            // Create array payload
            var batch_array = try msgpack.Payload.arrPayload(event_count, encoding_allocator);
            // No defer needed - arena.deinit() handles it

            for (self.events.items, 0..) |event, i| {
                // Each event is a map with subject, table, operation, msg_id, and data
                var event_map = msgpack.Payload.mapPayload(encoding_allocator);

                try event_map.mapPut("subject", try msgpack.Payload.strToPayload(event.subject, encoding_allocator));
                try event_map.mapPut("table", try msgpack.Payload.strToPayload(event.table, encoding_allocator));
                try event_map.mapPut("operation", try msgpack.Payload.strToPayload(event.operation, encoding_allocator));
                try event_map.mapPut("msg_id", try msgpack.Payload.strToPayload(event.msg_id, encoding_allocator));

                // Add column data if present
                if (event.data) |data_map| {
                    var data_payload = msgpack.Payload.mapPayload(encoding_allocator);

                    var it = data_map.iterator();
                    while (it.next()) |entry| {
                        const col_name = entry.key_ptr.*;
                        const value = entry.value_ptr.*;

                        const value_payload = switch (value) {
                            .int32 => |v| msgpack.Payload{ .int = @intCast(v) },
                            .int64 => |v| msgpack.Payload{ .int = v },
                            .float64 => |v| msgpack.Payload{ .float = v },
                            .boolean => |v| msgpack.Payload{ .bool = v },
                            .text => |v| try msgpack.Payload.strToPayload(v, encoding_allocator),
                        };

                        try data_payload.mapPut(col_name, value_payload);
                    }

                    try event_map.mapPut("data", data_payload);
                }

                batch_array.arr[i] = event_map;
            }

            // Write batch array
            try packer.write(batch_array);

            // Get the encoded bytes
            const written = write_buffer.pos;
            const encoded = buffer[0..written];

            // Publish the batch with a composite message ID
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

            log.info("Published batch: {d} events, {d} bytes to {s}", .{
                event_count,
                encoded.len,
                batch_subject,
            });
        }

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
            log.warn("Slow flush: {d}ms for {d} events", .{ flush_elapsed, event_count });
        }
    }
};
