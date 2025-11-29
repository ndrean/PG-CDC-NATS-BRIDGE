const std = @import("std");
const pg_setup = @import("pg_setup.zig");

pub const log = std.log.scoped(.replication_setup);

const c = @cImport({
    @cInclude("libpq-fe.h");
});

pub const ReplicationSetup = struct {
    allocator: std.mem.Allocator,
    pg_config: *const pg_setup.PgSetup,

    /// Create a replication slot if it doesn't exist
    pub fn createSlot(self: *const ReplicationSetup, slot_name: []const u8) !void {
        log.info("Creating replication slot '{s}'...", .{slot_name});

        const conn = try self.connect();
        defer c.PQfinish(conn);

        // Check if slot exists
        const check_query = try std.fmt.allocPrintSentinel(
            self.allocator,
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name = '{s}'",
            .{slot_name},
            0,
        );
        defer self.allocator.free(check_query);

        const check_result = c.PQexec(conn, check_query.ptr);
        defer c.PQclear(check_result);

        if (c.PQresultStatus(check_result) != c.PGRES_TUPLES_OK) {
            const err_msg = c.PQerrorMessage(conn);
            log.err("Failed to check slot existence: {s}", .{err_msg});
            return error.SlotCheckFailed;
        }

        const exists = c.PQntuples(check_result) > 0;

        if (exists) {
            log.info("Replication slot '{s}' already exists", .{slot_name});
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

        const create_result = c.PQexec(conn, create_query.ptr);
        defer c.PQclear(create_result);

        if (c.PQresultStatus(create_result) != c.PGRES_TUPLES_OK) {
            const err_msg = c.PQerrorMessage(conn);
            log.err("Failed to create replication slot: {s}", .{err_msg});
            return error.SlotCreationFailed;
        }

        log.info("✓ Replication slot '{s}' created", .{slot_name});
    }

    /// Create a publication if it doesn't exist
    pub fn createPublication(self: *const ReplicationSetup, pub_name: []const u8, tables: []const []const u8) !void {
        log.info("Creating publication '{s}'...", .{pub_name});

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

        const check_result = c.PQexec(conn, check_query.ptr);
        defer c.PQclear(check_result);

        if (c.PQresultStatus(check_result) != c.PGRES_TUPLES_OK) {
            const err_msg = c.PQerrorMessage(conn);
            log.err("Failed to check publication existence: {s}", .{err_msg});
            return error.PublicationCheckFailed;
        }

        const exists = c.PQntuples(check_result) > 0;

        if (exists) {
            log.info("Publication '{s}' already exists", .{pub_name});
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

        const create_result = c.PQexec(conn, create_query.ptr);
        defer c.PQclear(create_result);

        if (c.PQresultStatus(create_result) != c.PGRES_COMMAND_OK) {
            const err_msg = c.PQerrorMessage(conn);
            log.err("Failed to create publication: {s}", .{err_msg});
            return error.PublicationCreationFailed;
        }

        log.info("✓ Publication '{s}' created", .{pub_name});
    }

    /// Get current WAL LSN
    ///
    /// Caller is responsible for freeing the returned LSN string
    pub fn getCurrentLSN(self: *const ReplicationSetup) ![]const u8 {
        const query = "SELECT pg_current_wal_lsn()::text";
        const result = try self.runQuery(query);
        defer c.PQclear(result);

        if (c.PQntuples(result) == 0) {
            return error.NoLSNReturned;
        }

        const lsn_cstr = c.PQgetvalue(result, 0, 0);
        // potential error if lsn_cstr is null, but PQgetvalue should not return null if there is at least one tuple, the check above
        const lsn = std.mem.span(lsn_cstr);
        return try self.allocator.dupe(u8, lsn);
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

        const result = try self.runQuery(query);
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

        const result = try self.runQuery(query);
        defer c.PQclear(result);

        log.info("✓ Publication '{s}' dropped", .{pub_name});
    }

    // Private helper to connect
    fn connect(self: *const ReplicationSetup) !*c.PGconn {
        const conninfo = try self.pg_config.connInfo(self.allocator, false);
        defer self.allocator.free(conninfo);

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

    fn runQuery(self: *const ReplicationSetup, query: []const u8) !*c.PGresult {
        const conn = try self.connect();
        defer c.PQfinish(conn);

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
};
