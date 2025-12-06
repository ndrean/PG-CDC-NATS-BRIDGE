//! Command-line arguments parsing for CDC Bridge application
const std = @import("std");

const log = std.log.scoped(.args);

/// Command-line arguments structure
pub const Args = struct {
    streams: []const []const u8, // Stream names to verify (e.g., CDC, SCHEMA)
    http_port: u16,
    slot_name: []const u8,
    publication_name: []const u8,
    tables: []const []const u8, // Empty slice = all tables

    /// Parse command-line arguments
    pub fn parseArgs(allocator: std.mem.Allocator) !Args {
        // Parse command-line arguments
        var args = try std.process.argsWithAllocator(allocator);
        _ = args.skip(); // Skip program name
        var streams: []const []const u8 = &.{"CDC"}; // default: CDC stream only
        var http_port: u16 = 8080; // default
        var slot_name: []const u8 = "bridge_slot"; // default
        var publication_name: []const u8 = "bridge_pub"; // default
        var tables: []const []const u8 = &.{}; // default: all tables

        while (args.next()) |arg| {
            if (std.mem.eql(u8, arg, "--stream")) {
                if (args.next()) |value| {
                    // Parse comma-separated stream names
                    var stream_list = std.ArrayList([]const u8){};
                    defer stream_list.deinit(allocator);

                    var iter = std.mem.splitScalar(u8, value, ',');
                    while (iter.next()) |stream_name| {
                        const trimmed = std.mem.trim(
                            u8,
                            stream_name,
                            &std.ascii.whitespace,
                        );

                        if (trimmed.len > 0) {
                            // Heap-allocate the string to ensure it outlives parseArgs()
                            const owned = try allocator.dupe(u8, trimmed);
                            try stream_list.append(allocator, owned);
                        }
                    }

                    streams = try stream_list.toOwnedSlice(allocator);
                }
            } else if (std.mem.eql(u8, arg, "--port")) {
                if (args.next()) |value| {
                    http_port = std.fmt.parseInt(u16, value, 10) catch {
                        log.err("--port requires a valid port number (1-65535)", .{});
                        return error.InvalidArguments;
                    };
                }
            } else if (std.mem.eql(u8, arg, "--slot")) {
                if (args.next()) |value| {
                    slot_name = value;
                }
            } else if (std.mem.eql(u8, arg, "--publication")) {
                if (args.next()) |value| {
                    publication_name = value;
                }
            } else if (std.mem.eql(u8, arg, "--table")) {
                if (args.next()) |value| {
                    // Check if "all" (case-insensitive)
                    if (std.ascii.eqlIgnoreCase(value, "all")) {
                        tables = &.{}; // Empty = all tables
                    } else {
                        // Parse comma-separated table names
                        var table_list = std.ArrayList([]const u8){};
                        defer table_list.deinit(allocator);

                        var iter = std.mem.splitScalar(u8, value, ',');
                        while (iter.next()) |table_name| {
                            const trimmed = std.mem.trim(
                                u8,
                                table_name,
                                &std.ascii.whitespace,
                            );

                            if (trimmed.len > 0) {
                                // Heap-allocate the string to ensure it outlives parseArgs()
                                const owned = try allocator.dupe(u8, trimmed);
                                try table_list.append(allocator, owned);
                            }
                        }

                        tables = try table_list.toOwnedSlice(allocator);
                    }
                }
            }
        }

        return Args{
            .streams = streams,
            .http_port = http_port,
            .slot_name = slot_name,
            .publication_name = publication_name,
            .tables = tables,
        };
    }
};
