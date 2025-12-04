const std = @import("std");
const nats_publisher = @import("nats_publisher.zig");
const msgpack = @import("msgpack");
const pgoutput = @import("pgoutput.zig");

pub const log = std.log.scoped(.async_direct_publisher);

/// A single CDC event to be published
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
            var it = data_map.iterator();
            while (it.next()) |entry| {
                allocator.free(entry.key_ptr.*);
                switch (entry.value_ptr.*) {
                    .text => |txt| allocator.free(txt),
                    else => {},
                }
            }
            data_map.deinit();
        }
    }
};

/// Direct async publisher - no batching, just a simple queue
pub const AsyncDirectPublisher = struct {
    allocator: std.mem.Allocator,
    publisher: *nats_publisher.Publisher,

    // Event queue (mutex-protected)
    mutex: std.Thread.Mutex,
    event_queue: std.ArrayList(CDCEvent),

    // Publisher thread
    publish_thread: ?std.Thread,
    should_stop: std.atomic.Value(bool),

    // Metrics
    events_published: std.atomic.Value(usize),
    last_flush_time: std.atomic.Value(i64),

    pub fn init(
        allocator: std.mem.Allocator,
        publisher: *nats_publisher.Publisher,
    ) !AsyncDirectPublisher {
        return AsyncDirectPublisher{
            .allocator = allocator,
            .publisher = publisher,
            .mutex = .{},
            .event_queue = std.ArrayList(CDCEvent){},
            .publish_thread = null,
            .should_stop = std.atomic.Value(bool).init(false),
            .events_published = std.atomic.Value(usize).init(0),
            .last_flush_time = std.atomic.Value(i64).init(std.time.milliTimestamp()),
        };
    }

    /// Start the background publisher thread
    pub fn start(self: *AsyncDirectPublisher) !void {
        self.publish_thread = try std.Thread.spawn(.{}, publishLoop, .{self});
    }

    pub fn deinit(self: *AsyncDirectPublisher) void {
        // Signal publisher thread to stop
        self.should_stop.store(true, .seq_cst);

        // Wait for publisher thread to finish
        if (self.publish_thread) |thread| {
            thread.join();
        }

        // Clean up remaining events in queue
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.event_queue.items) |*event| {
            event.deinit(self.allocator);
        }
        self.event_queue.deinit(self.allocator);

        const published = self.events_published.load(.seq_cst);
        log.info("🥁 Async direct publisher stopped ({d} events published)", .{published});
    }

    /// Add an event to the queue (non-blocking)
    pub fn addEvent(
        self: *AsyncDirectPublisher,
        subject: []const u8,
        table: []const u8,
        operation: []const u8,
        msg_id: []const u8,
        data: ?std.StringHashMap(pgoutput.DecodedValue),
    ) !void {
        // Make owned copies
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
                const owned_value = switch (entry.value_ptr.*) {
                    .text => |txt| pgoutput.DecodedValue{ .text = try self.allocator.dupe(u8, txt) },
                    else => entry.value_ptr.*,
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

        // Add to queue
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.event_queue.append(self.allocator, event);
    }

    /// Background thread that continuously publishes events
    fn publishLoop(self: *AsyncDirectPublisher) void {
        log.info("Async direct publisher started", .{});

        var events_since_flush: usize = 0;
        const flush_interval_events = 100; // Flush NATS every N events
        const flush_interval_ms = 100; // Or every N milliseconds

        while (!self.should_stop.load(.seq_cst)) {
            // Try to get an event from the queue
            self.mutex.lock();
            const maybe_event = if (self.event_queue.items.len > 0)
                self.event_queue.orderedRemove(0)
            else
                null;
            self.mutex.unlock();

            if (maybe_event) |event| {
                // Publish the event
                self.publishEvent(event) catch |err| {
                    log.err("Failed to publish event: {}", .{err});
                };
                events_since_flush += 1;

                // Periodic NATS flush
                if (events_since_flush >= flush_interval_events) {
                    self.publisher.flushAsync() catch |err| {
                        log.err("Failed to flush NATS: {}", .{err});
                    };
                    events_since_flush = 0;
                    self.last_flush_time.store(std.time.milliTimestamp(), .seq_cst);
                }
            } else {
                // No events, check if we should flush based on time
                const now = std.time.milliTimestamp();
                const last_flush = self.last_flush_time.load(.seq_cst);
                if (events_since_flush > 0 and (now - last_flush) >= flush_interval_ms) {
                    self.publisher.flushAsync() catch |err| {
                        log.err("Failed to flush NATS: {}", .{err});
                    };
                    events_since_flush = 0;
                    self.last_flush_time.store(now, .seq_cst);
                }

                // Sleep briefly if queue is empty
                std.Thread.sleep(1 * std.time.ns_per_ms);
            }
        }

        // Drain remaining events
        self.mutex.lock();
        const remaining = self.event_queue.items.len;
        self.mutex.unlock();

        if (remaining > 0) {
            log.info("Draining {d} remaining events...", .{remaining});
            while (true) {
                self.mutex.lock();
                const maybe_event = if (self.event_queue.items.len > 0)
                    self.event_queue.orderedRemove(0)
                else
                    null;
                self.mutex.unlock();

                if (maybe_event) |event| {
                    self.publishEvent(event) catch |err| {
                        log.err("Failed to publish event during drain: {}", .{err});
                    };
                } else {
                    break;
                }
            }

            // Final flush
            self.publisher.flushAsync() catch |err| {
                log.err("Failed to flush NATS during shutdown: {}", .{err});
            };
        }

        log.info("Async direct publisher thread stopped", .{});
    }

    /// Encode and publish a single event (runs in publisher thread)
    fn publishEvent(self: *AsyncDirectPublisher, event: CDCEvent) !void {
        defer {
            var evt = event;
            evt.deinit(self.allocator);
        }

        const encode_start = std.time.nanoTimestamp();

        // Use ArrayList for dynamic buffer
        var buffer = std.ArrayList(u8){};
        defer buffer.deinit(self.allocator);

        // Create stream wrapper for ArrayList
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

        // Build event map
        var event_map = msgpack.Payload.mapPayload(self.allocator);
        defer event_map.free(self.allocator);

        try event_map.mapPut("subject", try msgpack.Payload.strToPayload(event.subject, self.allocator));
        try event_map.mapPut("table", try msgpack.Payload.strToPayload(event.table, self.allocator));
        try event_map.mapPut("operation", try msgpack.Payload.strToPayload(event.operation, self.allocator));
        try event_map.mapPut("msg_id", try msgpack.Payload.strToPayload(event.msg_id, self.allocator));

        // Add column data if present
        if (event.data) |data_map| {
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

        // Encode
        try packer.write(event_map);
        const encoded = buffer.items;

        const encode_elapsed = @as(f64, @floatFromInt(std.time.nanoTimestamp() - encode_start)) / 1_000_000.0;

        // Publish to NATS
        const publish_start = std.time.nanoTimestamp();
        try self.publisher.publish(event.subject, encoded, event.msg_id);
        const publish_elapsed = @as(f64, @floatFromInt(std.time.nanoTimestamp() - publish_start)) / 1_000_000.0;

        // Increment counter
        _ = self.events_published.fetchAdd(1, .seq_cst);

        // Log if slow
        const total_elapsed = encode_elapsed + publish_elapsed;
        if (total_elapsed > 5.0) {
            log.warn("Slow publish: {d:.2}ms total (encode: {d:.2}ms, publish: {d:.2}ms) for {s}", .{
                total_elapsed,
                encode_elapsed,
                publish_elapsed,
                event.subject,
            });
        }
    }

    /// Get queue size (for monitoring)
    pub fn getQueueSize(self: *AsyncDirectPublisher) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.event_queue.items.len;
    }
};
