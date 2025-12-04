const std = @import("std");
const batch_publisher = @import("batch_publisher.zig");
const nats_publisher = @import("nats_publisher.zig");
const pgoutput = @import("pgoutput.zig");
const SPSCQueue = @import("spsc_queue.zig").SPSCQueue;

pub const log = std.log.scoped(.async_batch_publisher);

/// Async batch publisher that offloads flushing to a dedicated thread
pub const AsyncBatchPublisher = struct {
    allocator: std.mem.Allocator,
    publisher: *nats_publisher.Publisher,
    config: batch_publisher.BatchConfig,

    // Lock-free event queue (SPSC: Single Producer Single Consumer)
    // Producer: Main thread adding WAL events
    // Consumer: Flush thread publishing to NATS
    event_queue: SPSCQueue(batch_publisher.CDCEvent),

    // Atomic state shared between threads
    last_confirmed_lsn: std.atomic.Value(u64), // Last LSN confirmed by NATS

    // Flush thread
    flush_thread: ?std.Thread,
    should_stop: std.atomic.Value(bool),

    pub fn init(
        allocator: std.mem.Allocator,
        publisher: *nats_publisher.Publisher,
        config: batch_publisher.BatchConfig,
    ) !AsyncBatchPublisher {
        // Initialize lock-free queue with power-of-2 capacity
        // 4096 events = can buffer ~8 batches worth of events at max_events=500
        const event_queue = try SPSCQueue(batch_publisher.CDCEvent).init(allocator, 4096);

        return AsyncBatchPublisher{
            .allocator = allocator,
            .publisher = publisher,
            .config = config,
            .event_queue = event_queue,
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

        // Clean up any remaining events in the queue
        while (self.event_queue.pop()) |event| {
            var mut_event = event;
            mut_event.deinit(self.allocator);
        }

        // Deinit the queue itself
        self.event_queue.deinit();

        log.info("🥁 Async batch publisher stopped", .{});
    }

    /// Add an event to the lock-free queue. Wait-free operation (no locks, no blocking).
    /// Takes ownership of the `data` ArrayList - caller must not free it.
    pub fn addEvent(
        self: *AsyncBatchPublisher,
        subject: []const u8,
        table: []const u8,
        operation: []const u8,
        msg_id: []const u8,
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
                        log.warn("Event queue full! Applying backpressure (retry #{d}). Queue capacity: 4096", .{retry_count});
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

        log.debug("✅ Event added to lock-free queue", .{});
    }

    /// Get the last LSN that was successfully confirmed by NATS
    pub fn getLastConfirmedLsn(self: *AsyncBatchPublisher) u64 {
        return self.last_confirmed_lsn.load(.seq_cst);
    }

    /// Get current queue usage (0.0 = empty, 1.0 = full)
    pub fn getQueueUsage(self: *AsyncBatchPublisher) f64 {
        const current_len = self.event_queue.len();
        const capacity = self.event_queue.capacity;
        return @as(f64, @floatFromInt(current_len)) / @as(f64, @floatFromInt(capacity));
    }

    /// Background thread that continuously drains events from lock-free queue and flushes to NATS
    fn flushLoop(self: *AsyncBatchPublisher) void {
        log.info("Lock-free flush thread started", .{});

        var batches_processed: usize = 0;
        var last_flush_time = std.time.milliTimestamp();

        while (!self.should_stop.load(.seq_cst)) {
            // Create a fresh batch for each flush cycle
            var batch = std.ArrayList(batch_publisher.CDCEvent){};

            // Drain up to max_events from the queue
            while (batch.items.len < self.config.max_events) {
                const event = self.event_queue.pop() orelse break;
                batch.append(self.allocator, event) catch |err| {
                    log.err("Failed to append to batch: {}", .{err});
                    // Clean up event on error
                    var mut_event = event;
                    mut_event.deinit(self.allocator);
                    break;
                };
            }

            const now = std.time.milliTimestamp();
            const time_elapsed = now - last_flush_time;

            // Flush if we have events AND (batch is full OR timeout reached)
            const should_flush = batch.items.len > 0 and
                (batch.items.len >= self.config.max_events or
                    time_elapsed >= self.config.max_wait_ms);

            if (should_flush) {
                batches_processed += 1;
                log.info("Flush thread processing batch #{d} with {d} events", .{ batches_processed, batch.items.len });

                // flushBatch takes ownership and cleans up
                self.flushBatch(batch) catch |err| {
                    log.err("Failed to flush batch: {}", .{err});
                };

                last_flush_time = now;
            } else if (batch.items.len == 0) {
                // No events available, sleep briefly to avoid busy-waiting
                std.Thread.sleep(1 * std.time.ns_per_ms);
            } else {
                // Have events but timeout not reached, clean up and retry
                for (batch.items) |*event| {
                    var mut_event = event.*;
                    mut_event.deinit(self.allocator);
                }
                batch.deinit(self.allocator);
            }
        }

        // Drain any remaining events on shutdown
        log.info("Flush thread shutting down, draining remaining events...", .{});
        var final_batch = std.ArrayList(batch_publisher.CDCEvent){};

        while (self.event_queue.pop()) |event| {
            final_batch.append(self.allocator, event) catch |err| {
                log.err("Failed to append final event: {}", .{err});
                var mut_event = event;
                mut_event.deinit(self.allocator);
                break;
            };
        }

        if (final_batch.items.len > 0) {
            log.info("Flushing final batch with {d} events", .{final_batch.items.len});
            self.flushBatch(final_batch) catch |err| {
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
