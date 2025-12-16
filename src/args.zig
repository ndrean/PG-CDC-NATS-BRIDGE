//! Command-line arguments parsing for CDC Bridge application
const std = @import("std");
const config = @import("config.zig");
const encoder = @import("encoder.zig");
const log = std.log.scoped(.args);

/// Command-line arguments structure
pub const Args = struct {
    http_port: u16,
    slot_name: []const u8,
    publication_name: []const u8,
    encoding_format: encoder.Format,
    enable_compression: bool,

    /// Parse command-line arguments and create RuntimeConfig
    /// Returns both the CLI args and the merged runtime configuration
    pub fn parseArgs(allocator: std.mem.Allocator) !struct { args: Args, runtime_config: config.RuntimeConfig } {
        // Parse command-line arguments
        var args = try std.process.argsWithAllocator(allocator);
        _ = args.skip(); // Skip program name
        var http_port: u16 = 6543; // default
        var slot_name: []const u8 = config.Postgres.default_slot_name; // default
        var publication_name: []const u8 = config.Postgres.default_publication_name; // default
        var encoding_format: encoder.Format = .msgpack; // default
        var enable_compression: bool = false; // default: disabled

        while (args.next()) |arg| {
            if (std.mem.eql(u8, arg, "--port")) {
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
            } else if (std.mem.eql(u8, arg, "--pub")) {
                if (args.next()) |value| {
                    publication_name = value;
                }
            } else if (std.mem.eql(u8, arg, "--json")) {
                encoding_format = .json;
            } else if (std.mem.eql(u8, arg, "--zstd")) {
                enable_compression = true;
            }
        }

        const cli_args = Args{
            .http_port = http_port,
            .slot_name = slot_name,
            .publication_name = publication_name,
            .encoding_format = encoding_format,
            .enable_compression = enable_compression,
        };

        // Build runtime configuration by merging CLI args with compile-time defaults
        var runtime_config = config.RuntimeConfig.defaults();
        runtime_config.http_port = http_port;
        runtime_config.slot_name = slot_name;
        runtime_config.publication_name = publication_name;
        runtime_config.enable_compression = enable_compression;

        // Read PostgreSQL configuration from environment variables
        // Priority: POSTGRES_BRIDGE_* > PG_* > defaults
        runtime_config.pg_host = std.process.getEnvVarOwned(allocator, "PG_HOST") catch |err| blk: {
            log.info("PG_HOST not set ({any}), using default: {s}", .{ err, runtime_config.pg_host });
            break :blk runtime_config.pg_host;
        };

        if (std.process.getEnvVarOwned(allocator, "PG_PORT")) |port_str| {
            defer allocator.free(port_str);
            runtime_config.pg_port = std.fmt.parseInt(u16, port_str, 10) catch |err| blk: {
                log.warn("Invalid PG_PORT value '{s}' ({any}), using default: {d}", .{ port_str, err, runtime_config.pg_port });
                break :blk runtime_config.pg_port;
            };
        } else |err| {
            log.info("PG_PORT not set ({any}), using default: {d}", .{ err, runtime_config.pg_port });
        }

        // Try POSTGRES_BRIDGE_USER first, fallback to PG_USER
        runtime_config.pg_user = std.process.getEnvVarOwned(allocator, "POSTGRES_BRIDGE_USER") catch blk: {
            const generic_user = std.process.getEnvVarOwned(allocator, "PG_USER") catch |err| blk2: {
                log.info("POSTGRES_BRIDGE_USER and PG_USER not set ({any}), using default: {s}", .{ err, runtime_config.pg_user });
                break :blk2 runtime_config.pg_user;
            };
            break :blk generic_user;
        };

        // Try POSTGRES_BRIDGE_PASSWORD first, fallback to PG_PASSWORD
        runtime_config.pg_password = std.process.getEnvVarOwned(allocator, "POSTGRES_BRIDGE_PASSWORD") catch blk: {
            const generic_password = std.process.getEnvVarOwned(allocator, "PG_PASSWORD") catch |err| blk2: {
                log.info("POSTGRES_BRIDGE_PASSWORD and PG_PASSWORD not set ({any}), using default", .{err});
                break :blk2 runtime_config.pg_password;
            };
            break :blk generic_password;
        };

        runtime_config.pg_database = std.process.getEnvVarOwned(allocator, "PG_DB") catch |err| blk: {
            log.info("PG_DB not set ({any}), using default: {s}", .{ err, runtime_config.pg_database });
            break :blk runtime_config.pg_database;
        };

        return .{
            .args = cli_args,
            .runtime_config = runtime_config,
        };
    }
};
