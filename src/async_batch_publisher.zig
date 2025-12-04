const std = @import("std");
const batch_publisher = @import("batch_publisher.zig");
const nats_publisher = @import("nats_publisher.zig");
const pgoutput = @import("pgoutput.zig");

pub const log = std.log.scoped(.async_batch_publisher);

/// Async batch publisher that offloads flushing to a dedicated thread
pub const AsyncBatchPublisher = struct {
    allocator: std.mem.Allocator,
    publisher: *nats_publisher.Publisher,
    config: batch_publisher.BatchConfig,

    // Main thread batch (being built)
    current_batch: std.ArrayList(batch_publisher.CDCEvent),
    current_payload_size: usize,
    last_flush_time: i64,

    // Shared state between main thread and flush thread
    mutex: std.Thread.Mutex,
    pending_batch: ?std.ArrayList(batch_publisher.CDCEvent), // Batch waiting to be flushed
    last_confirmed_lsn: std.atomic.Value(u64), // Last LSN confirmed by NATS

    // Flush thread
    flush_thread: ?std.Thread,
    should_stop: std.atomic.Value(bool),

    pub fn init(
        allocator: std.mem.Allocator,
        publisher: *nats_publisher.Publisher,
        config: batch_publisher.BatchConfig,
    ) !AsyncBatchPublisher {
        return AsyncBatchPublisher{
            .allocator = allocator,
            .publisher = publisher,
            .config = config,
            .current_batch = std.ArrayList(batch_publisher.CDCEvent){},
            .current_payload_size = 0,
            .last_flush_time = std.time.milliTimestamp(),
            .mutex = .{},
            .pending_batch = null,
            .last_confirmed_lsn = std.atomic.Value(u64).init(0),
            .flush_thread = null,
            .should_stop = std.atomic.Value(bool).init(false),
        };
    }

    /// Start the background flush thread. Must be called after init() and after
    /// the AsyncBatchPublisher is in its final memory location (not on stack).
    pub fn start(self: *AsyncBatchPublisher) !void {
        // Spawn flush thread - self must be at stable address
        self.flush_thread = try std.Thread.spawn(.{}, flushLoop, .{self});
    }

    pub fn deinit(self: *AsyncBatchPublisher) void {
        // Signal flush thread to stop
        self.should_stop.store(true, .seq_cst);

        // Wait for flush thread to finish
        if (self.flush_thread) |thread| {
            thread.join();
        }

        // Clean up current batch
        for (self.current_batch.items) |*event| {
            event.deinit(self.allocator);
        }
        self.current_batch.deinit(self.allocator);

        // Clean up pending batch if any
        if (self.pending_batch) |*batch| {
            for (batch.items) |*event| {
                event.deinit(self.allocator);
            }
            batch.deinit(self.allocator);
        }

        log.info("🥁 Async batch publisher stopped", .{});
    }

    /// Add an event to the batch. Non-blocking - only locks briefly to swap batches.
    pub fn addEvent(
        self: *AsyncBatchPublisher,
        subject: []const u8,
        table: []const u8,
        operation: []const u8,
        msg_id: []const u8,
        data: ?std.StringHashMap(pgoutput.DecodedValue),
        lsn: u64,
    ) !void {
        log.debug("📥 Adding event to batch: {s} {s} (current batch size: {d})", .{ operation, table, self.current_batch.items.len });

        // Make owned copies of the data
        const owned_subject = try self.allocator.dupeZ(u8, subject);
        errdefer self.allocator.free(owned_subject);

        const owned_table = try self.allocator.dupe(u8, table);
        errdefer self.allocator.free(owned_table);

        const owned_operation = try self.allocator.dupe(u8, operation);
        errdefer self.allocator.free(owned_operation);

        const owned_msg_id = try self.allocator.dupe(u8, msg_id);
        errdefer self.allocator.free(owned_msg_id);

        // Clone the data hashmap
        var owned_data: ?std.StringHashMap(pgoutput.DecodedValue) = null;
        if (data) |data_map| {
            owned_data = std.StringHashMap(pgoutput.DecodedValue).init(self.allocator);
            var it = data_map.iterator();
            while (it.next()) |entry| {
                const owned_key = try self.allocator.dupe(u8, entry.key_ptr.*);

                // Deep copy the value, especially text-based fields (slices)
                const owned_value = switch (entry.value_ptr.*) {
                    .text => |txt| pgoutput.DecodedValue{ .text = try self.allocator.dupe(u8, txt) },
                    .numeric => |num| pgoutput.DecodedValue{ .numeric = try self.allocator.dupe(u8, num) },
                    .array => |arr| pgoutput.DecodedValue{ .array = try self.allocator.dupe(u8, arr) },
                    .jsonb => |jsn| pgoutput.DecodedValue{ .jsonb = try self.allocator.dupe(u8, jsn) },
                    .bytea => |byt| pgoutput.DecodedValue{ .bytea = try self.allocator.dupe(u8, byt) },
                    else => entry.value_ptr.*, // int32, int64, float64, boolean are copied by value
                };

                try owned_data.?.put(owned_key, owned_value);
            }
        }

        const event = batch_publisher.CDCEvent{
            .subject = owned_subject,
            .table = owned_table,
            .operation = owned_operation,
            .msg_id = owned_msg_id,
            .data = owned_data,
            .lsn = lsn,
        };

        try self.current_batch.append(self.allocator, event);
        self.current_payload_size += table.len + operation.len;

        log.debug("✅ Event added successfully. Batch now has {d} events, {d} bytes", .{ self.current_batch.items.len, self.current_payload_size });

        // Check if we should flush based on count or size
        const should_flush_count = self.current_batch.items.len >= self.config.max_events;
        const should_flush_size = self.current_payload_size >= self.config.max_payload_bytes;

        if (should_flush_count or should_flush_size) {
            if (should_flush_count) {
                log.debug("Batch full by count: {d}/{d} events", .{ self.current_batch.items.len, self.config.max_events });
            }
            if (should_flush_size) {
                log.debug("Batch full by size: {d}/{d} bytes", .{ self.current_payload_size, self.config.max_payload_bytes });
            }
            try self.triggerFlush();
        }
    }

    /// Check if batch should be flushed based on time
    pub fn shouldFlushByTime(self: *AsyncBatchPublisher) bool {
        if (self.current_batch.items.len == 0) return false;

        const now = std.time.milliTimestamp();
        const elapsed = now - self.last_flush_time;
        return elapsed >= self.config.max_wait_ms;
    }

    /// Get the last LSN that was successfully confirmed by NATS
    pub fn getLastConfirmedLsn(self: *AsyncBatchPublisher) u64 {
        return self.last_confirmed_lsn.load(.seq_cst);
    }

    /// Trigger async flush by swapping current batch with pending
    pub fn triggerFlush(self: *AsyncBatchPublisher) !void {
        if (self.current_batch.items.len == 0) return;

        log.debug("Triggering flush for {d} events", .{self.current_batch.items.len});

        // Lock and swap batches
        self.mutex.lock();
        defer self.mutex.unlock();

        // If there's still a pending batch, merge current batch into it
        // This prevents blocking the main thread while maintaining event order
        if (self.pending_batch) |*pending| {
            log.warn("Flush thread is falling behind - merging batches ({d} + {d} events)", .{
                pending.items.len,
                self.current_batch.items.len,
            });

            // Append current batch to pending
            try pending.appendSlice(self.allocator, self.current_batch.items);

            // Clear current batch (events are now owned by pending)
            self.current_batch.clearRetainingCapacity();
            self.current_payload_size = 0;
            self.last_flush_time = std.time.milliTimestamp();
        } else {
            // Move current batch to pending
            log.info("Moving {d} events to pending batch for flush thread", .{self.current_batch.items.len});
            self.pending_batch = self.current_batch;
            self.current_batch = std.ArrayList(batch_publisher.CDCEvent){};
            self.current_payload_size = 0;
            self.last_flush_time = std.time.milliTimestamp();
        }
    }

    /// Background thread that continuously flushes pending batches
    fn flushLoop(self: *AsyncBatchPublisher) void {
        log.info("Async flush thread started", .{});
        var batches_processed: usize = 0;

        while (!self.should_stop.load(.seq_cst)) {
            // Check if there's a pending batch
            self.mutex.lock();
            const maybe_batch = self.pending_batch;
            self.pending_batch = null;
            self.mutex.unlock();

            if (maybe_batch) |batch| {
                batches_processed += 1;
                log.info("Flush thread processing batch #{d} with {d} events", .{ batches_processed, batch.items.len });
                // Flush this batch (outside the lock)
                self.flushBatch(batch) catch |err| {
                    log.err("Failed to flush batch: {}", .{err});
                };
            } else {
                // No pending batch, sleep briefly
                std.Thread.sleep(1 * std.time.ns_per_ms);
            }
        }

        // Drain any final pending batch
        self.mutex.lock();
        const final_batch = self.pending_batch;
        self.pending_batch = null;
        self.mutex.unlock();

        if (final_batch) |batch| {
            log.info("Flushing final batch with {d} events", .{batch.items.len});
            self.flushBatch(batch) catch |err| {
                log.err("Failed to flush final batch: {}", .{err});
            };
        }
    }

    /// Flush a batch to NATS (runs in flush thread)
    fn flushBatch(self: *AsyncBatchPublisher, batch: std.ArrayList(batch_publisher.CDCEvent)) !void {
        if (batch.items.len == 0) {
            @constCast(&batch).deinit(self.allocator);
            return;
        }

        const flush_start = std.time.milliTimestamp();
        const event_count = batch.items.len;

        log.info("⚡ Flushing batch of {d} events", .{event_count});

        // Use temporary BatchPublisher for actual encoding/publishing logic
        var temp_publisher = batch_publisher.BatchPublisher{
            .allocator = self.allocator,
            .publisher = self.publisher,
            .config = self.config,
            .events = batch, // Transfer ownership to temp_publisher
            .current_payload_size = 0,
            .last_flush_time = 0,
            .last_confirmed_lsn = 0,
        };

        // Call the existing flush implementation (it will clean up events)
        const confirmed_lsn = temp_publisher.flush() catch |err| {
            log.err("Flush failed: {}", .{err});
            // Clean up on error
            for (temp_publisher.events.items) |*event| {
                event.deinit(self.allocator);
            }
            temp_publisher.events.deinit(self.allocator);
            return err;
        };

        // Update last confirmed LSN after successful flush
        if (confirmed_lsn > 0) {
            self.last_confirmed_lsn.store(confirmed_lsn, .seq_cst);
            log.debug("Updated last confirmed LSN to {x}", .{confirmed_lsn});
        }

        // temp_publisher.flush() already cleaned up events but retained capacity
        // We need to deinit the ArrayList to free the capacity buffer
        temp_publisher.events.deinit(self.allocator);

        // Log flush timing
        const flush_elapsed = std.time.milliTimestamp() - flush_start;
        log.info("✓ Flushed {d} events in {d}ms", .{ event_count, flush_elapsed });
        if (flush_elapsed > 5) {
            log.warn("Slow flush: {d}ms for {d} events", .{ flush_elapsed, event_count });
        }
    }
};
