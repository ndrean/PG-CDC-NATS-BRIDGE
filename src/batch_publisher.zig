const std = @import("std");
const nats_publisher = @import("nats_publisher.zig");
const msgpack = @import("msgpack");

pub const log = std.log.scoped(.batch_publisher);

/// Configuration for batch publishing
pub const BatchConfig = struct {
    /// Maximum number of events per batch
    max_events: usize = 100,
    /// Maximum time to wait before flushing (milliseconds)
    max_wait_ms: i64 = 50,
    /// Maximum payload size in bytes
    max_payload_bytes: usize = 64 * 1024, // 64KB
};

/// A single CDC event to be batched
pub const CDCEvent = struct {
    subject: [:0]const u8,
    payload: []const u8,
    msg_id: []const u8,

    pub fn deinit(self: *CDCEvent, allocator: std.mem.Allocator) void {
        allocator.free(self.subject);
        allocator.free(self.payload);
        allocator.free(self.msg_id);
    }
};

/// Batches CDC events and publishes them to NATS
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
            .events = std.ArrayList(CDCEvent).init(allocator),
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
        self.events.deinit();
    }

    /// Add an event to the batch. Flushes automatically if batch is full.
    pub fn addEvent(
        self: *BatchPublisher,
        subject: []const u8,
        payload: []const u8,
        msg_id: []const u8,
    ) !void {
        // Make owned copies of the data
        const owned_subject = try self.allocator.dupeZ(u8, subject);
        errdefer self.allocator.free(owned_subject);

        const owned_payload = try self.allocator.dupe(u8, payload);
        errdefer self.allocator.free(owned_payload);

        const owned_msg_id = try self.allocator.dupe(u8, msg_id);
        errdefer self.allocator.free(owned_msg_id);

        const event = CDCEvent{
            .subject = owned_subject,
            .payload = owned_payload,
            .msg_id = owned_msg_id,
        };

        try self.events.append(event);
        self.current_payload_size += payload.len;

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

        const event_count = self.events.items.len;

        // For single event, publish directly
        if (event_count == 1) {
            const event = self.events.items[0];
            try self.publisher.publish(event.subject, event.payload, event.msg_id);
            log.debug("Published single event: {s}", .{event.subject});
        } else {
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
            var batch_array = msgpack.Payload.arrayPayload(self.allocator);
            defer batch_array.free(self.allocator);

            for (self.events.items) |event| {
                // Each event is a map with subject, payload_base64, and msg_id
                var event_map = msgpack.Payload.mapPayload(self.allocator);

                try event_map.mapPut("subject", try msgpack.Payload.strToPayload(event.subject, self.allocator));

                // Base64 encode the payload since it's MessagePack binary
                const b64_encoder = std.base64.standard.Encoder;
                const b64_len = b64_encoder.calcSize(event.payload.len);
                const b64_payload = try self.allocator.alloc(u8, b64_len);
                defer self.allocator.free(b64_payload);
                _ = b64_encoder.encode(b64_payload, event.payload);

                try event_map.mapPut("payload", try msgpack.Payload.strToPayload(b64_payload, self.allocator));
                try event_map.mapPut("msg_id", try msgpack.Payload.strToPayload(event.msg_id, self.allocator));

                try batch_array.arrayAppend(event_map);
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
    }
};
