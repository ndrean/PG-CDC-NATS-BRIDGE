//! Parser for decoderbufs protobuf messages
const std = @import("std");
const pb = @import("proto/decoderbufs.pb.zig");

pub const log = std.log.scoped(.decoderbufs_parser);

/// Parses a decoderbufs protobuf message from WAL payload
pub const Parser = struct {
    allocator: std.mem.Allocator,
    payload: []const u8,

    pub fn init(allocator: std.mem.Allocator, payload: []const u8) Parser {
        return .{
            .allocator = allocator,
            .payload = payload,
        };
    }

    /// Parse the protobuf WAL message
    pub fn parse(self: *Parser) !pb.RowMessage {
        // Import protobuf module
        const protobuf = @import("protobuf");

        // Create a Reader from the byte slice
        var reader = std.Io.Reader.fixed(self.payload);

        // Decode from the reader
        const row_msg = try protobuf.decode(pb.RowMessage, &reader, self.allocator);

        return row_msg;
    }
};

/// Helper to format operation as string
pub fn opToString(op: ?pb.Op) []const u8 {
    if (op) |operation| {
        return switch (operation) {
            .INSERT => "INSERT",
            .UPDATE => "UPDATE",
            .DELETE => "DELETE",
            .BEGIN => "BEGIN",
            .COMMIT => "COMMIT",
            else => "UNKNOWN",
        };
    }
    return "UNKNOWN";
}

/// Helper to extract datum value as string (for logging/debugging)
pub fn datumToString(allocator: std.mem.Allocator, datum: pb.DatumMessage) ![]const u8 {
    if (datum.datum) |d| {
        return switch (d) {
            .datum_int32 => |v| try std.fmt.allocPrint(allocator, "{d}", .{v}),
            .datum_int64 => |v| try std.fmt.allocPrint(allocator, "{d}", .{v}),
            .datum_float => |v| try std.fmt.allocPrint(allocator, "{d}", .{v}),
            .datum_double => |v| try std.fmt.allocPrint(allocator, "{d}", .{v}),
            .datum_bool => |v| try std.fmt.allocPrint(allocator, "{}", .{v}),
            .datum_string => |v| try allocator.dupe(u8, v),
            .datum_bytes => |v| try std.fmt.allocPrint(allocator, "bytes({d})", .{v.len}),
            .datum_point => |v| try std.fmt.allocPrint(allocator, "({d},{d})", .{ v.x, v.y }),
            .datum_missing => try allocator.dupe(u8, "NULL"),
        };
    }
    return try allocator.dupe(u8, "NULL");
}
