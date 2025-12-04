//! Replication setup tasks, including creating/dropping replication slots and publications,
const std = @import("std");
const pg_conn = @import("pg_conn.zig");

pub const log = std.log.scoped(.replication_setup);

const c = @cImport({
    @cInclude("libpq-fe.h");
});

/// Struct to handle replication setup tasks and includes get current WAL LSN.
///
/// Includes creating|droping replication slots and publications.
pub const ReplicationSetup = struct {
    allocator: std.mem.Allocator,
    pg_config: pg_conn.PgConf,

    /// Create a replication slot if it doesn't exist
    pub fn createSlot(
        self: *const ReplicationSetup,
        slot_name: []const u8,
    ) !void {

        // Check if slot exists
        const check_query = try std.fmt.allocPrintSentinel(
            self.allocator,
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name = '{s}'",
            .{slot_name},
            0,
        );
        defer self.allocator.free(check_query);

        const conn = try self.connect();
        defer c.PQfinish(conn);

        const check_result = try runQuery(conn, check_query);
        defer c.PQclear(check_result);
        log.info("Checking for replication slot '{s}'...", .{slot_name});

        const exists = c.PQntuples(check_result) > 0;

        if (exists) {
            log.info("✅ Replication slot '{s}' already exists", .{slot_name});
            return;
        }

        // Create the slot
        const create_query = try std.fmt.allocPrintSentinel(
            self.allocator,
            "SELECT pg_create_logical_replication_slot('{s}', 'pgoutput')",
            .{slot_name},
            0,
        );
        defer self.allocator.free(create_query);

        const create_result = try runQuery(conn, create_query);
        defer c.PQclear(create_result);

        log.info("✅ Replication slot '{s}' created", .{slot_name});
    }

    /// Create a publication if it doesn't exist on given tables.
    /// ALL TABLES if tables is empty or list of table names.
    pub fn createPublication(
        self: *const ReplicationSetup,
        pub_name: []const u8,
        tables: []const []const u8,
    ) !void {
        const conn = try self.connect();
        defer c.PQfinish(conn);

        // Check if publication exists
        const check_query = try std.fmt.allocPrintSentinel(
            self.allocator,
            "SELECT pubname FROM pg_publication WHERE pubname = '{s}'",
            .{pub_name},
            0,
        );
        defer self.allocator.free(check_query);

        const check_result = try runQuery(conn, check_query);
        defer c.PQclear(check_result);

        const exists = c.PQntuples(check_result) > 0;

        if (exists) {
            if (tables.len == 0) {
                log.info("✅  Publication '{s}' already exists, listening on ALL TABLES", .{pub_name});
            } else {
                const table_list = try std.mem.join(self.allocator, ", ", tables);
                defer self.allocator.free(table_list);
                log.info("✅ Publication '{s}' already exists, listening on tables: {s}", .{ pub_name, table_list });
            }
            return;
        }

        // Create publication
        const create_query = if (tables.len == 0)
            try std.fmt.allocPrintSentinel(
                self.allocator,
                "CREATE PUBLICATION {s} FOR ALL TABLES",
                .{pub_name},
                0,
            )
        else blk: {
            // Join tables with ", "
            const table_list = try std.mem.join(self.allocator, ", ", tables);
            defer self.allocator.free(table_list);

            const query = try std.fmt.allocPrint(
                self.allocator,
                "CREATE PUBLICATION {s} FOR TABLE {s}",
                .{ pub_name, table_list },
            );
            defer self.allocator.free(query);

            break :blk try self.allocator.dupeZ(u8, query);
        };
        defer self.allocator.free(create_query);

        const create_result = try runQuery(conn, create_query);
        defer c.PQclear(create_result);

        if (tables.len == 0) {
            log.info("✅ Publication '{s}' created for ALL TABLES", .{pub_name});
        } else {
            const table_list = try std.mem.join(self.allocator, ", ", tables);
            defer self.allocator.free(table_list);
            log.info("✅ Publication '{s}' created for tables: {s}", .{ pub_name, table_list });
        }
    }

    /// Drop a replication slot
    pub fn dropSlot(self: *const ReplicationSetup, slot_name: []const u8) !void {
        log.info("Dropping replication slot '{s}'...", .{slot_name});

        const query = try std.fmt.allocPrintSentinel(
            self.allocator,
            "SELECT pg_drop_replication_slot('{s}')",
            .{slot_name},
            0,
        );
        defer self.allocator.free(query);

        const conn = try self.connect();
        defer c.PQfinish(conn);

        const result = try runQuery(conn, query);
        defer c.PQclear(result);

        log.info("✓ Replication slot '{s}' dropped", .{slot_name});
    }

    /// Drop a publication
    pub fn dropPublication(self: *const ReplicationSetup, pub_name: []const u8) !void {
        log.info("Dropping publication '{s}'...", .{pub_name});

        const query = try std.fmt.allocPrintSentinel(
            self.allocator,
            "DROP PUBLICATION IF EXISTS {s}",
            .{pub_name},
            0,
        );
        defer self.allocator.free(query);

        const conn = try self.connect();
        defer c.PQfinish(conn);

        const result = try runQuery(conn, query);
        defer c.PQclear(result);

        log.info("✓ Publication '{s}' dropped", .{pub_name});
    }

    fn runQuery(conn: *c.PGconn, query: []const u8) !*c.PGresult {
        const result = c.PQexec(conn, query.ptr) orelse {
            log.err("Query execution failed: PQexec returned null", .{});
            return error.QueryFailed;
        };

        if (c.PQresultStatus(result) != c.PGRES_TUPLES_OK and c.PQresultStatus(result) != c.PGRES_COMMAND_OK) {
            const err_msg = c.PQerrorMessage(conn);
            log.err("Query failed: {s}", .{err_msg});
            c.PQclear(result);
            return error.QueryFailed;
        }

        return result;
    }

    pub fn connect(self: *const ReplicationSetup) !*c.PGconn {
        // Build connection string
        const conninfo = try self.pg_config.connInfo(
            self.allocator,
            self.pg_config.replication,
        );
        defer self.allocator.free(conninfo);

        if (conninfo.len == 0) {
            log.err("Invalid configuration: connection string is empty", .{});
            return error.InvalidConfig;
        }

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

        return conn;
    }
};
