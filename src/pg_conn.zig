const std = @import("std");

const c = @cImport({
    @cInclude("libpq-fe.h");
});

pub const log = std.log.scoped(.pg_conn);

/// PostgreSQL connection configuration
pub const Config = struct {
    host: []const u8 = "127.0.0.1",
    port: u16 = 5432,
    user: []const u8 = "postgres",
    password: []const u8 = "postgres",
    database: []const u8 = "postgres",
    /// Enable replication mode (adds replication=database to connection string)
    replication: bool = false,
};

/// Connect to PostgreSQL with the given configuration
///
/// Returns a PGconn pointer that must be closed with PQfinish()
/// Caller is responsible for calling c.PQfinish(conn) when done
pub fn connect(allocator: std.mem.Allocator, config: Config) !*c.PGconn {
    // Build connection string
    const conninfo = if (config.replication)
        try std.fmt.allocPrint(
            allocator,
            "host={s} port={d} user={s} password={s} dbname={s} replication=database",
            .{ config.host, config.port, config.user, config.password, config.database },
        )
    else
        try std.fmt.allocPrint(
            allocator,
            "host={s} port={d} user={s} password={s} dbname={s}",
            .{ config.host, config.port, config.user, config.password, config.database },
        );
    defer allocator.free(conninfo);

    const conn = c.PQconnectdb(conninfo.ptr) orelse {
        log.err("Connection failed: PQconnectdb returned null", .{});
        return error.ConnectionFailed;
    };

    if (c.PQstatus(conn) != c.CONNECTION_OK) {
        const err_msg = c.PQerrorMessage(conn);
        log.err("Connection failed: {s}", .{err_msg});
        c.PQfinish(conn);
        return error.ConnectionFailed;
    }

    return conn;
}
