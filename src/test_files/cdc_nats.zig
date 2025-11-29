const std = @import("std");
const nats = @import("nats_publisher.zig");
const cdc = @import("cdc.zig");
const msgpack_encoder = @import("msgpack_encoder.zig");

pub const log = std.log.scoped(.cdc_nats);

/// Configuration for CDC NATS Publisher
pub const CDCPublisherConfig = struct {
    nats_url: [:0]const u8 = "nats://localhost:4222",
    stream_name: [:0]const u8 = "CDC_EVENTS",
    subject_prefix: []const u8 = "cdc",
    use_msgpack: bool = true,
};

/// CDC-specific NATS Publisher
///
/// Wraps the core NATS publisher with CDC domain logic.
/// Handles CDC event encoding, subject naming, and stream setup.
///
/// Usage:
/// ```zig
/// var cdc_pub = try CDCPublisher.init(allocator, .{
///     .stream_name = "CDC_BRIDGE",
///     .subject_prefix = "cdc",
/// });
/// defer cdc_pub.deinit();
/// try cdc_pub.connect();
///
/// // Publish CDC Change
/// try cdc_pub.publishChange(change);
///
/// // Or publish raw CDC event
/// try cdc_pub.publishCDCEvent("users", "insert", payload_data, msg_id);
/// ```
pub const CDCPublisher = struct {
    allocator: std.mem.Allocator,
    config: CDCPublisherConfig,
    publisher: nats.Publisher,

    pub fn init(allocator: std.mem.Allocator, config: CDCPublisherConfig) !CDCPublisher {
        const nats_config = nats.PublisherConfig{
            .url = config.nats_url,
        };

        return CDCPublisher{
            .allocator = allocator,
            .config = config,
            .publisher = try nats.Publisher.init(allocator, nats_config),
        };
    }

    pub fn connect(self: *CDCPublisher) !void {
        try self.publisher.connect();

        // Create CDC stream
        const subjects_pattern = try std.fmt.allocPrint(
            self.allocator,
            "{s}.>",
            .{self.config.subject_prefix},
        );
        defer self.allocator.free(subjects_pattern);

        const stream_config = nats.StreamConfig{
            .name = self.config.stream_name,
            .subjects = &.{subjects_pattern},
            .retention = .limits,
            .max_msgs = 1_000_000,
            .max_bytes = 1024 * 1024 * 1024, // 1GB
            .max_age_ns = 60 * 1_000_000_000, // 1 min
            .storage = .file,
            .replicas = 1,
        };

        try nats.createStream(self.publisher.js.?, self.allocator, stream_config);
    }

    pub fn deinit(self: *CDCPublisher) void {
        self.publisher.deinit();
    }

    /// Publish a CDC event with optional message ID for deduplication
    ///
    /// Parameters:
    /// - table: Table name
    /// - operation: Operation type (e.g., "insert", "update", "delete")
    /// - data: Pre-encoded message payload
    /// - msg_id: Optional unique message ID for idempotent delivery
    pub fn publishCDCEvent(
        self: *CDCPublisher,
        table: []const u8,
        operation: []const u8,
        data: []const u8,
        msg_id: ?[]const u8,
    ) !void {
        // Build subject: prefix.table.operation
        const subject = try std.fmt.allocPrint(
            self.allocator,
            "{s}.{s}.{s}",
            .{ self.config.subject_prefix, table, operation },
        );
        defer self.allocator.free(subject);

        try self.publisher.publish(subject, data, msg_id);
    }

    /// Publish a CDC Change event with automatic encoding
    ///
    /// Message ID is automatically generated from the LSN for idempotent delivery.
    /// Encoding format (MessagePack or JSON) is determined by config.use_msgpack.
    pub fn publishChange(self: *CDCPublisher, change: cdc.Change) !void {
        // Build subject: prefix.table.operation
        const operation_str = @tagName(change.operation);
        const subject = try std.fmt.allocPrint(
            self.allocator,
            "{s}.{s}.{s}",
            .{ self.config.subject_prefix, change.table, operation_str },
        );
        defer self.allocator.free(subject);

        // Use LSN as message ID for deduplication
        const msg_id = change.lsn;

        if (self.config.use_msgpack) {
            // Encode to MessagePack binary format
            const encoded = try msgpack_encoder.encodeChange(self.allocator, change);
            defer self.allocator.free(encoded);
            try self.publisher.publish(subject, encoded, msg_id);
        } else {
            // Publish as JSON manually formatted
            const json = try std.fmt.allocPrint(
                self.allocator,
                \\{{"lsn":"{s}","table":"{s}","operation":"{s}","timestamp":{d},"tx_id":{?}}}
            ,
                .{
                    change.lsn,
                    change.table,
                    operation_str,
                    change.timestamp,
                    change.tx_id,
                },
            );
            defer self.allocator.free(json);

            try self.publisher.publish(subject, json, msg_id);
        }
    }
};
