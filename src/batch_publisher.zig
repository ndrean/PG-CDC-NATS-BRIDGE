//! Batch Publisher with background flush thread
//!
//! Manages a dedicated thread that dequeues CDC events from an SPSC queue,
//! batches them, encodes to MessagePack/JSON, and publishes to NATS.

const std = @import("std");
const c_imports = @import("c_imports.zig");
const c = c_imports.c;
const nats_publisher = @import("nats_publisher.zig");
const msgpack = @import("msgpack");
const pgoutput = @import("pgoutput.zig");
const SPSCQueue = @import("spsc_queue.zig").SPSCQueue;
const Config = @import("config.zig");
const encoder_mod = @import("encoder.zig");
const Metrics = @import("metrics.zig").Metrics;

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

/// Batch publisher that runs a background flush thread
/// Dequeues events from SPSC queue, batches, encodes, and publishes to NATS
pub const BatchPublisher = struct {
    allocator: std.mem.Allocator,
    publisher: *nats_publisher.Publisher,
    config: BatchConfig,
    format: encoder_mod.Format,
    metrics: ?*Metrics, // Optional metrics reference

    // Lock-free event queue (SPSC: Single Producer Single Consumer)
    // Producer: Main thread (via EventProcessor)
    // Consumer: Flush thread (this thread)
    // Now uses pointers to avoid copying CDCEvent structs
    event_queue: SPSCQueue(*CDCEvent),

    // Reclaim queue for memory ownership cycle
    // Flush thread returns pointers here after publishing
    // Main thread reclaims and frees in idle loops
    reclaim_queue: SPSCQueue(*CDCEvent),

    // Atomic state shared between threads
    last_confirmed_lsn: std.atomic.Value(u64), // Last LSN confirmed by NATS
    fatal_error: std.atomic.Value(bool), // Set when NATS reconnection fails
    flush_complete: std.atomic.Value(bool), // Set when flush thread finishes final flush

    // Flush thread
    flush_thread: ?std.Thread,
    should_stop: std.atomic.Value(bool),

    pub fn init(
        allocator: std.mem.Allocator,
        publisher: *nats_publisher.Publisher,
        config: BatchConfig,
        format: encoder_mod.Format,
        metrics: ?*Metrics,
        runtime_config: *const Config.RuntimeConfig,
    ) !BatchPublisher {
        // Initialize lock-free queues with power-of-2 capacity from runtime config
        const event_queue = try SPSCQueue(*CDCEvent).init(
            allocator,
            runtime_config.batch_ring_buffer_size,
        );
        const reclaim_queue = try SPSCQueue(*CDCEvent).init(
            allocator,
            runtime_config.batch_ring_buffer_size,
        );

        return BatchPublisher{
            .allocator = allocator,
            .publisher = publisher,
            .config = config,
            .format = format,
            .metrics = metrics,
            .event_queue = event_queue,
            .reclaim_queue = reclaim_queue,
            .last_confirmed_lsn = std.atomic.Value(u64).init(0),
            .fatal_error = std.atomic.Value(bool).init(false),
            .flush_complete = std.atomic.Value(bool).init(false),
            .flush_thread = null,
            .should_stop = std.atomic.Value(bool).init(false),
        };
    }

    /// Start the background flush thread. Must be called after init() and after
    /// the BatchPublisher is in its final memory location (not on stack).
    pub fn start(self: *BatchPublisher) !void {
        // Spawn flush thread - self must be at stable address
        self.flush_thread = try std.Thread.spawn(.{}, flushLoop, .{self});
    }

    /// Join the flush thread (waits for completion)
    pub fn join(self: *BatchPublisher) void {
        // Signal flush thread to stop
        self.should_stop.store(true, .seq_cst);

        // Wait for flush thread to finish
        if (self.flush_thread) |thread| {
            thread.join();
            self.flush_thread = null;
        }
    }

    /// Deinit - cleanup resources (call after join)
    pub fn deinit(self: *BatchPublisher) void {
        // Clean up any remaining events in the event queue
        while (self.event_queue.pop()) |event_ptr| {
            event_ptr.deinit(self.allocator);
            self.allocator.destroy(event_ptr);
        }

        // Clean up any remaining events in the reclaim queue
        while (self.reclaim_queue.pop()) |event_ptr| {
            event_ptr.deinit(self.allocator);
            self.allocator.destroy(event_ptr);
        }

        // Deinit the queues themselves
        self.event_queue.deinit();
        self.reclaim_queue.deinit();

        log.info("🥁 Batch publisher stopped", .{});
    }

    /// Add an event to the batch (called from flush thread)
    /// Now works with event pointers
    fn addToBatch(
        batch: *std.ArrayList(*CDCEvent),
        event_ptr: *CDCEvent,
        allocator: std.mem.Allocator,
    ) !void {
        try batch.append(allocator, event_ptr);
    }

    /// Get the last LSN that was successfully confirmed by NATS
    /// Called only when ACK thresholds are met (bytes/time/keepalive)
    pub fn getLastConfirmedLsn(self: *BatchPublisher) u64 {
        return self.last_confirmed_lsn.load(.seq_cst);
    }

    /// Get current queue usage (0.0 = empty, 1.0 = full)
    pub fn getQueueUsage(self: *BatchPublisher) f64 {
        const current_len = self.event_queue.len();
        const capacity = self.event_queue.capacity;
        return @as(f64, @floatFromInt(current_len)) / @as(f64, @floatFromInt(capacity));
    }

    /// Check if both queues are empty (safe point for arena reset)
    pub fn queuesEmpty(self: *BatchPublisher) bool {
        return self.event_queue.isEmpty() and self.reclaim_queue.isEmpty();
    }

    /// Check if a fatal error occurred (e.g., NATS reconnection timeout)
    pub fn hasFatalError(self: *BatchPublisher) bool {
        return self.fatal_error.load(.seq_cst);
    }

    /// Check if the flush thread has completed all pending work
    pub fn isFlushComplete(self: *BatchPublisher) bool {
        return self.flush_complete.load(.seq_cst);
    }

    /// Reclaim published events from the reclaim queue (called from main thread)
    /// Returns number of events reclaimed and freed
    /// Batches reclamation to reduce overhead
    pub fn reclaimEvents(self: *BatchPublisher) usize {
        var count: usize = 0;
        const max_reclaim_per_call: usize = 100; // Limit reclamation per call to avoid blocking

        while (count < max_reclaim_per_call) {
            const event_ptr = self.reclaim_queue.pop() orelse break;
            // Free individual allocations (no-op for ArenaAllocator)
            event_ptr.deinit(self.allocator);
            self.allocator.destroy(event_ptr);
            count += 1;
        }
        return count;
    }

    /// Background thread that continuously drains events from lock-free queue and flushes to NATS
    fn flushLoop(self: *BatchPublisher) void {
        log.info("ℹ️ Lock-free flush thread started", .{});

        var batches_processed: usize = 0;
        var last_flush_time = std.time.milliTimestamp();

        // Persistent batch that accumulates event pointers across iterations
        var batch = std.ArrayList(*CDCEvent){};
        var current_payload_size: usize = 0; // Track approximate payload size
        defer {
            // Clean up on thread exit - return pointers to reclaim queue
            for (batch.items) |event_ptr| {
                self.reclaim_queue.push(event_ptr) catch |err| {
                    log.err("Failed to reclaim event on thread exit: {}", .{err});
                    // Last resort - free directly
                    event_ptr.deinit(self.allocator);
                    self.allocator.destroy(event_ptr);
                };
            }
            batch.deinit(self.allocator);
        }

        while (!self.should_stop.load(.seq_cst)) {
            // Drain events from the queue until we hit a threshold
            while (batch.items.len < self.config.max_events and
                current_payload_size < self.config.max_payload_bytes)
            {
                const event_ptr = self.event_queue.pop() orelse break;

                // Approximate payload size (table + operation + subject strings)
                const event_size = event_ptr.table.len + event_ptr.operation.len + event_ptr.subject.len;

                addToBatch(&batch, event_ptr, self.allocator) catch |err| {
                    log.err("⚠️ Failed to append to batch: {}", .{err});
                    // Return to reclaim queue on error
                    self.reclaim_queue.push(event_ptr) catch {
                        // Last resort - free directly
                        event_ptr.deinit(self.allocator);
                        self.allocator.destroy(event_ptr);
                    };
                    break;
                };

                current_payload_size += event_size;
            }
            const now = std.time.milliTimestamp();
            const time_elapsed = now - last_flush_time;

            // Flush if we have events AND (batch is full OR payload too large OR timeout reached)
            const should_flush = batch.items.len > 0 and
                (batch.items.len >= self.config.max_events or
                    current_payload_size >= self.config.max_payload_bytes or
                    time_elapsed >= self.config.max_wait_ms);

            if (should_flush) {
                batches_processed += 1;
                log.info("Flush thread processing batch #{d} with {d} events", .{ batches_processed, batch.items.len });

                // Log the msg_ids of events being flushed
                if (batch.items.len > 0) {
                    log.debug("Batch #{d} contains msg_ids: {s} ... {s}", .{
                        batches_processed,
                        batch.items[0].msg_id,
                        batch.items[batch.items.len - 1].msg_id,
                    });
                }

                // Flush the current batch - this will free the events but retain capacity
                self.flushBatch(&batch) catch |err| {
                    log.err("⚠️ Failed to flush batch: {}", .{err});
                };

                // Clear the batch (capacity is retained for reuse)
                // Events were already freed by flushBatch
                current_payload_size = 0;

                last_flush_time = now;

                // Update queue usage metrics after flushing batch
                // Queue usage has meaningfully changed - we just drained events
                if (self.metrics) |m| {
                    const queue_usage = self.getQueueUsage();
                    m.updateQueueUsage(queue_usage);
                }
            } else if (batch.items.len == 0) {
                // No events available
                // Check if we should stop immediately (no pending work)
                if (self.should_stop.load(.seq_cst)) {
                    break;
                }
                // Sleep briefly to avoid busy-waiting
                std.Thread.sleep(1 * std.time.ns_per_ms);
            } else {
                // Have events but timeout not reached
                // If shutting down, flush immediately instead of waiting
                if (self.should_stop.load(.seq_cst)) {
                    log.info("Shutdown detected with {d} pending events - flushing now", .{batch.items.len});
                    self.flushBatch(&batch) catch |err| {
                        log.err("⚠️ Failed to flush pending batch on shutdown: {}", .{err});
                    };
                    current_payload_size = 0;
                    break;
                }
                // Keep them and sleep briefly
                std.Thread.sleep(1 * std.time.ns_per_ms);
            }
        }

        // Drain any remaining events on shutdown
        log.info("Flush thread shutting down, draining remaining events...", .{});

        // Drain remaining events into the existing batch
        while (self.event_queue.pop()) |event_ptr| {
            addToBatch(&batch, event_ptr, self.allocator) catch |err| {
                log.err("⚠️ Failed to append final event: {}", .{err});
                // Return to reclaim queue
                self.reclaim_queue.push(event_ptr) catch {
                    event_ptr.deinit(self.allocator);
                    self.allocator.destroy(event_ptr);
                };
                break;
            };
        }

        if (batch.items.len > 0) {
            log.info("Flushing final batch with {d} events", .{batch.items.len});
            self.flushBatch(&batch) catch |err| {
                log.err("⚠️ Failed to flush final batch: {}", .{err});
            };
        }

        // Signal that flush thread has completed all work
        self.flush_complete.store(true, .seq_cst);
        log.info("✅ Flush thread completed - all events published", .{});

        // Release NATS thread-local storage
        // This is required when user-created threads call NATS C library APIs
        c.nats_ReleaseThreadMemory();
    }

    /// Flush a batch to NATS (runs in flush thread)
    /// Takes a pointer to the batch ArrayList of event pointers
    /// Returns event pointers to reclaim queue after flushing
    fn flushBatch(self: *BatchPublisher, batch: *std.ArrayList(*CDCEvent)) !void {
        if (batch.items.len == 0) return;

        // Use allocator directly (flush arena caused segfault due to premature freeing)
        // NATS C library may hold references to strings after publish() returns
        const flush_alloc = self.allocator;

        // Calculate maximum LSN in this batch
        var max_lsn: u64 = 0;
        for (batch.items) |event_ptr| {
            if (event_ptr.lsn > max_lsn) {
                max_lsn = event_ptr.lsn;
            }
        }

        const flush_start = std.time.milliTimestamp();
        const event_count = batch.items.len;

        log.info("📦 Starting flush of {d} events", .{event_count});

        // For single event, encode and publish directly
        if (event_count == 1) {
            const event_ptr = batch.items[0];

            // Use unified encoder with flush arena
            var encoder = encoder_mod.Encoder.init(flush_alloc, self.format);
            defer encoder.deinit();

            var event_map = encoder.createMap();
            defer event_map.free(flush_alloc);

            try event_map.put("subject", try encoder.createString(event_ptr.subject));
            try event_map.put("table", try encoder.createString(event_ptr.table));
            try event_map.put("operation", try encoder.createString(event_ptr.operation));
            try event_map.put("msg_id", try encoder.createString(event_ptr.msg_id));
            try event_map.put("relation_id", encoder.createInt(@intCast(event_ptr.relation_id)));
            try event_map.put("lsn", encoder.createInt(@intCast(event_ptr.lsn)));

            // Add column data if present
            if (event_ptr.data) |columns| {
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

            try self.publisher.publish(event_ptr.subject, encoded, event_ptr.msg_id);

            // Flush async publishes to actually send them to NATS
            try self.publisher.flushAsync();

            log.debug("Published single event: {s}", .{event_ptr.subject});
        } else {
            // Use unified encoder for batch publishing with flush arena
            var encoder = encoder_mod.Encoder.init(flush_alloc, self.format);
            defer encoder.deinit();

            // Create array of events
            var batch_array = try encoder.createArray(event_count);
            defer batch_array.free(flush_alloc);

            const encode_start = std.time.milliTimestamp();

            for (batch.items, 0..) |event_ptr, i| {
                // Each event is a map with subject, table, operation, msg_id, and data
                var event_map = encoder.createMap();

                try event_map.put("subject", try encoder.createString(event_ptr.subject));
                try event_map.put("table", try encoder.createString(event_ptr.table));
                try event_map.put("operation", try encoder.createString(event_ptr.operation));
                try event_map.put("msg_id", try encoder.createString(event_ptr.msg_id));
                try event_map.put("relation_id", encoder.createInt(@intCast(event_ptr.relation_id)));
                try event_map.put("lsn", encoder.createInt(@intCast(event_ptr.lsn)));

                // Add column data if present
                if (event_ptr.data) |columns| {
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
            const first_msg_id = batch.items[0].msg_id;
            const last_msg_id = batch.items[event_count - 1].msg_id;
            const batch_msg_id = try std.fmt.allocPrint(
                flush_alloc,
                "batch-{s}-to-{s}",
                .{ first_msg_id, last_msg_id },
            );
            defer self.allocator.free(batch_msg_id);

            // Use first event's subject pattern but with .batch suffix
            const batch_subject = try std.fmt.allocPrintSentinel(
                flush_alloc,
                "{s}.batch",
                .{batch.items[0].subject},
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

        // Update last confirmed LSN after successful flush
        if (max_lsn > 0) {
            self.last_confirmed_lsn.store(max_lsn, .seq_cst);
            log.debug("Updated last confirmed LSN to {x}", .{max_lsn});
        }

        // Check if publish failed due to NATS timeout - trigger fatal error to shutdown bridge
        // This happens in publisher.flushAsync() which throws error.FlushFailed
        // If we got here without error, the flush succeeded

        log.debug("Returning {d} events to reclaim queue", .{event_count});

        // Return event pointers to reclaim queue instead of freeing
        // Main thread will reclaim and free them
        for (batch.items) |event_ptr| {
            self.reclaim_queue.push(event_ptr) catch |err| {
                log.err("Failed to push to reclaim queue: {}", .{err});
                // Last resort - free directly (should rarely happen)
                event_ptr.deinit(self.allocator);
                self.allocator.destroy(event_ptr);
            };
        }
        batch.clearRetainingCapacity();

        // Log flush timing if it took longer than expected
        const flush_elapsed = std.time.milliTimestamp() - flush_start;
        if (flush_elapsed > 5) {
            log.warn(
                "Slow flush: {d}ms for {d} events",
                .{ flush_elapsed, event_count },
            );
        }
    }
};
