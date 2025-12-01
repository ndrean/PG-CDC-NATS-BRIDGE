//! WAL monitoring functionality.
//!
//! Includes getting current WAL LSN and monitoring WAL lag in a background thread.
const std = @import("std");
const c = @cImport({
    @cInclude("libpq-fe.h");
});
const pg_conn = @import("pg_conn.zig");
const metrics_mod = @import("metrics.zig");

pub const log = std.log.scoped(.wal_monitor);

/// Configuration for WAL monitoring
pub const Config = struct {
    pg_config: *const pg_conn.PgConf,
    slot_name: []const u8,
    check_interval_seconds: u32 = 30,
};

/// Get current WAL LSN position
///
/// Caller is responsible for freeing the returned LSN string
pub fn getCurrentLSN(allocator: std.mem.Allocator, pg_conf: *const pg_conn.PgConf) ![]const u8 {
    const conninfo = try pg_conf.connInfo(allocator, false);
    defer allocator.free(conninfo);

    const conn = c.PQconnectdb(conninfo.ptr) orelse {
        log.err("🔴 Connection failed: PQconnectdb returned null", .{});
        return error.ConnectionFailed;
    };

    if (c.PQstatus(conn) != c.CONNECTION_OK) {
        const err_msg = c.PQerrorMessage(conn);
        log.err("🔴 Connection failed: {s}", .{err_msg});
        c.PQfinish(conn);
        return error.ConnectionFailed;
    }

    defer c.PQfinish(conn);

    const query = "SELECT pg_current_wal_lsn()::text";

    const result = c.PQexec(conn, query.ptr) orelse {
        log.err("🔴 Query execution failed: PQexec returned null", .{});
        return error.QueryFailed;
    };

    if (c.PQresultStatus(result) != c.PGRES_TUPLES_OK and c.PQresultStatus(result) != c.PGRES_COMMAND_OK) {
        const err_msg = c.PQerrorMessage(conn);
        log.err("🔴 Query failed: {s}", .{err_msg});
        c.PQclear(result);
        return error.QueryFailed;
    }
    defer c.PQclear(result);

    if (c.PQntuples(result) == 0) {
        return error.NoLSNReturned;
    }

    const lsn_cstr = c.PQgetvalue(result, 0, 0);
    // potential error if lsn_cstr is null, but PQgetvalue should not return null if there is at least one tuple, the check above
    const lsn = std.mem.span(lsn_cstr);
    return try allocator.dupe(u8, lsn);
}

/// Background thread that periodically checks WAL lag
pub fn monitorWalLag(
    metrics: *metrics_mod.Metrics,
    config: Config,
    should_stop: *std.atomic.Value(bool),
    allocator: std.mem.Allocator,
) !void {
    log.info("ℹ️ WAL lag monitor started (checking every {d}s)\n", .{config.check_interval_seconds});

    while (!should_stop.load(.seq_cst)) {
        // Check WAL lag
        checkWalLag(metrics, config, allocator) catch |err| {
            log.warn("⚠️ Failed to check WAL lag: {}", .{err});
        };

        // Sleep for check interval
        var remaining_seconds = config.check_interval_seconds;
        while (remaining_seconds > 0 and !should_stop.load(.seq_cst)) {
            std.Thread.sleep(1 * std.time.ns_per_s);
            remaining_seconds -= 1;
        }
    }

    log.info("🥁 WAL lag monitor stopped\n", .{});
}

fn checkWalLag(
    metrics: *metrics_mod.Metrics,
    config: Config,
    allocator: std.mem.Allocator,
) !void {
    // Build connection string
    const conninfo = try config.pg_config.connInfo(allocator, false);
    defer allocator.free(conninfo);

    const conn = c.PQconnectdb(conninfo.ptr);

    defer c.PQfinish(conn);

    if (c.PQstatus(conn) != c.CONNECTION_OK) {
        log.warn("⚠️ WAL monitor connection failed: {s}", .{c.PQerrorMessage(conn)});
        return error.ConnectionFailed;
    }

    // Query replication slot status
    const query_str = try std.fmt.allocPrint(
        allocator,
        \\SELECT
        \\  active,
        \\  COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0) as lag_bytes
        \\FROM pg_replication_slots
        \\WHERE slot_name = '{s}'
    ,
        .{config.slot_name},
    );
    defer allocator.free(query_str);
    const query = try allocator.dupeZ(u8, query_str);
    defer allocator.free(query);

    const result = c.PQexec(conn, query.ptr);
    defer c.PQclear(result);

    if (c.PQresultStatus(result) != c.PGRES_TUPLES_OK) {
        log.warn("🔴 WAL lag query failed: {s}", .{c.PQerrorMessage(conn)});
        return error.QueryFailed;
    }

    const nrows = c.PQntuples(result);
    if (nrows == 0) {
        log.warn("🔴 Replication slot '{s}' not found", .{config.slot_name});
        metrics.updateWalLag(false, 0);
        return;
    }

    // Parse result
    const active_str = c.PQgetvalue(result, 0, 0);
    const lag_bytes_str = c.PQgetvalue(result, 0, 1);

    const slot_active = std.mem.eql(u8, std.mem.span(active_str), "t");
    const lag_bytes = try std.fmt.parseInt(u64, std.mem.span(lag_bytes_str), 10);

    // Update metrics
    metrics.updateWalLag(slot_active, lag_bytes);

    // Log warnings for concerning states
    const one_gb: u64 = 1024 * 1024 * 1024;
    if (!slot_active) {
        log.warn("⚠️  Replication slot '{s}' is INACTIVE (lag: {d} MB)", .{
            config.slot_name,
            lag_bytes / (1024 * 1024),
        });
    } else if (lag_bytes > one_gb) {
        log.warn("🔴 CRITICAL: WAL lag exceeds 1GB! ({d} MB) - disk may fill up!", .{
            lag_bytes / (1024 * 1024),
        });
    } else if (lag_bytes > (512 * 1024 * 1024)) { // 512MB warning threshold
        log.warn("⚠️  WAL lag exceeds 512MB ({d} MB)", .{
            lag_bytes / (1024 * 1024),
        });
    } else {
        log.debug("ℹ️ WAL lag: {d} MB (slot active: {s})", .{
            lag_bytes / (1024 * 1024),
            if (slot_active) "yes" else "no",
        });
    }
}
