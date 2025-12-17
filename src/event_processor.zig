//! CDC Event Processor
//!
//! Processes CDC events from PostgreSQL pgoutput format and enqueues them
//! for publishing to NATS. Runs in the main thread.

const std = @import("std");
const pgoutput = @import("pgoutput.zig");
const SPSCQueue = @import("spsc_queue.zig").SPSCQueue;
const Config = @import("config.zig");
const Metrics = @import("metrics.zig").Metrics;
const batch_publisher = @import("batch_publisher.zig");

pub const log = std.log.scoped(.event_processor);

/// Event processor that runs in the main thread
/// Decodes pgoutput tuples, creates CDC events, and enqueues them to the SPSC queue
pub const EventProcessor = struct {
    allocator: std.mem.Allocator,
    event_queue: *SPSCQueue(batch_publisher.CDCEvent),
    metrics: ?*Metrics, // Optional metrics reference

    pub fn init(
        allocator: std.mem.Allocator,
        event_queue: *SPSCQueue(batch_publisher.CDCEvent),
        metrics: ?*Metrics,
    ) EventProcessor {
        return .{
            .allocator = allocator,
            .event_queue = event_queue,
            .metrics = metrics,
        };
    }

    /// Process CDC event from pgoutput format and enqueue for publishing
    /// This encapsulates the entire CDC event processing pipeline.
    /// Returns the event's LSN for tracking, or null if the event was filtered out.
    pub fn processCdcEvent(
        self: *EventProcessor,
        rel: pgoutput.RelationMessage,
        tuple_data: pgoutput.TupleData,
        operation: []const u8,
        wal_end: u64,
    ) !void {
        // Decode tuple data to get actual column values
        // Use allocator so decoded values survive and are owned by the event
        var decoded_values = pgoutput.decodeTuple(
            self.allocator,
            tuple_data,
            rel.columns,
        ) catch |err| {
            log.warn("⚠️ Failed to decode tuple: {}", .{err});
            return;
        };
        // NOTE: addEvent() takes ownership of decoded_values.
        // The flush thread will free them after publishing.
        errdefer {
            // Only free on error - if addEvent() fails
            for (decoded_values.items) |column| {
                switch (column.value) {
                    .text => |txt| self.allocator.free(txt),
                    .numeric => |num| self.allocator.free(num),
                    .array => |arr| self.allocator.free(arr),
                    .jsonb => |jsn| self.allocator.free(jsn),
                    .bytea => |byt| self.allocator.free(byt),
                    else => {}, // int32, int64, float64, boolean, null don't need freeing
                }
            }
            decoded_values.deinit(self.allocator);
        }

        // Extract ID value for logging (if present)
        // Optimize: check length first before memcmp (most column names aren't "id")
        var id_buf: [64]u8 = undefined;
        const id_str = blk: {
            for (decoded_values.items) |column| {
                // Quick rejection: check length first (avoid memcmp for wrong-length names)
                if (column.name.len == 2 and column.name[0] == 'i' and column.name[1] == 'd') {
                    break :blk switch (column.value) {
                        .int32 => |v| std.fmt.bufPrint(&id_buf, "{d}", .{v}) catch "?",
                        .int64 => |v| std.fmt.bufPrint(&id_buf, "{d}", .{v}) catch "?",
                        .text => |v| if (v.len <= id_buf.len) v else "?",
                        else => "?",
                    };
                }
            }
            break :blk null;
        };

        // Convert operation to lowercase for NATS subject
        const operation_lower = switch (operation[0]) {
            'I' => "insert", // INSERT
            'U' => "update", // UPDATE
            'D' => "delete", // DELETE
            else => unreachable, // Only these 3 operations exist in CDC
        };

        // Create NATS subject
        var subject_buf: [Config.Buffers.subject_buffer_size]u8 = undefined;
        const subject = try std.fmt.bufPrintZ(
            &subject_buf,
            "{s}.{s}.{s}",
            .{ Config.Nats.subject_cdc_prefix, rel.name, operation_lower },
        );

        // Generate message ID from WAL LSN for idempotent delivery
        var msg_id_buf: [Config.Buffers.msg_id_buffer_size]u8 = undefined;
        const msg_id = try std.fmt.bufPrint(
            &msg_id_buf,
            "{x}-{s}-{s}",
            .{ wal_end, rel.name, operation_lower },
            // or rather .{wal_end, relation_id, row_index} in case of multiple rwo changes with same wal_end, or multiple ops on the same table in same record? PG WAL records can contain multiple row changes!
        );

        // Enqueue event to SPSC queue
        try self.addEvent(
            subject,
            rel.name,
            operation,
            msg_id,
            rel.relation_id,
            decoded_values,
            wal_end,
        );

        // Update metrics if available
        if (self.metrics) |m| {
            m.incrementCdcEvents();
        }

        // Log single line with table, operation, and ID
        if (id_str) |id| {
            log.info("{s} {s}.{s} id={s} → {s}", .{ operation, rel.namespace, rel.name, id, subject });
        } else {
            log.info("{s} {s}.{s} → {s}", .{ operation, rel.namespace, rel.name, subject });
        }
    }

    /// Add an event to the lock-free queue. Wait-free operation (no locks, no blocking).
    /// Takes ownership of the `data` ArrayList - caller must not free it.
    fn addEvent(
        self: *EventProcessor,
        subject: []const u8,
        table: []const u8,
        operation: []const u8,
        msg_id: []const u8,
        relation_id: u32,
        data: ?std.ArrayList(pgoutput.Column),
        lsn: u64,
    ) !void {
        log.debug("📥 Adding event to queue: {s} {s}", .{ operation, table });

        // Only copy the strings (subject, table, operation, msg_id)
        // Take ownership of the columns ArrayList directly (zero-copy for column data)
        const owned_subject = try self.allocator.dupeZ(u8, subject);
        errdefer self.allocator.free(owned_subject);

        const owned_table = try self.allocator.dupe(u8, table);
        errdefer self.allocator.free(owned_table);

        const owned_operation = try self.allocator.dupe(u8, operation);
        errdefer self.allocator.free(owned_operation);

        const owned_msg_id = try self.allocator.dupe(u8, msg_id);
        errdefer self.allocator.free(owned_msg_id);

        const event = batch_publisher.CDCEvent{
            .subject = owned_subject,
            .table = owned_table,
            .operation = owned_operation,
            .msg_id = owned_msg_id,
            .relation_id = relation_id,
            .data = data, // Transfer ownership - no copy!
            .lsn = lsn,
        };
        // If push fails with unexpected error, clean up the event (including the hashmap we now own)
        errdefer {
            var mut_event = event;
            mut_event.deinit(self.allocator);
        }

        // Push to lock-free queue with backpressure retry
        var retry_count: usize = 0;
        while (true) {
            self.event_queue.push(event) catch |err| {
                if (err == error.QueueFull) {
                    retry_count += 1;

                    // Log warning on first retry, then periodically
                    if (retry_count == 1 or retry_count % 100 == 0) {
                        log.warn(
                            "⚠️ Event queue full! Applying backpressure (retry #{d}). Queue capacity: {d}",
                            .{ retry_count, Config.Batch.ring_buffer_size },
                        );
                    }

                    // Yield CPU to flush thread
                    std.Thread.yield() catch {};
                    continue; // Retry
                }

                // Unexpected error - propagate (errdefer will clean up)
                return err;
            };

            // Success!
            if (retry_count > 0) {
                log.info("Queue space available after {d} retries, resuming", .{retry_count});
            }
            break;
        }

        log.debug("Event added to lock-free queue", .{});
    }
};
