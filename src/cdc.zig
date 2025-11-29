const std = @import("std");
const c = @cImport({
    @cInclude("libpq-fe.h");
});
const pg_conn = @import("pg_conn.zig");

pub const log = std.log.scoped(.cdc);

/// CDC Change represents a single change event from PostgreSQL
pub const Change = struct {
    /// Transaction ID
    tx_id: ?u64 = null,

    /// Log Sequence Number
    lsn: []const u8,

    /// Table name
    table: []const u8,

    /// Operation type
    operation: Operation,

    /// Old values (for UPDATE/DELETE)
    before: ?std.json.Value = null,

    /// New values (for INSERT/UPDATE)
    after: ?std.json.Value = null,

    /// Timestamp
    timestamp: i64,

    pub const Operation = enum {
        INSERT,
        UPDATE,
        DELETE,
        BEGIN,
        COMMIT,
    };
};

/// Replication slot configuration
pub const ReplicationConfig = struct {
    slot_name: []const u8 = "bridge_slot",
    publication_name: []const u8 = "bridge_pub",
    /// Tables to replicate (empty = all tables)
    tables: []const []const u8 = &.{},
    /// PostgreSQL connection config
    pg_config: pg_conn.Config = .{},
};

/// CDC Consumer manages PostgreSQL logical replication
///
/// Exposes methods to create replication slots, publications,
/// and to stream changes.
/// - `init`: Initialize the consumer with a connection pool and config
/// - `deinit`: Clean up resources
/// - `createSlot`: Create replication slot if it doesn't exist
/// - `createPublication`: Create publication for tables
/// - `streamChanges`: Start streaming replication changes
/// - `getCurrentLSN`: Get current WAL LSN position
pub const Consumer = struct {
    allocator: std.mem.Allocator,
    conn: ?*c.PGconn = null,
    config: ReplicationConfig,

    pub fn init(
        allocator: std.mem.Allocator,
        config: ReplicationConfig,
    ) !Consumer {
        const conn = try pg_conn.connect(
            allocator,
            config.pg_config,
        );

        return Consumer{
            .allocator = allocator,
            .conn = conn,
            .config = config,
        };
    }

    pub fn deinit(self: *Consumer) void {
        if (self.conn) |conn| {
            c.PQfinish(conn);
            self.conn = null;
        }
    }

    /// Create replication slot if it doesn't exist
    pub fn createSlot(self: *Consumer) !void {
        log.info("Creating replication slot '{s}'...", .{self.config.slot_name});

        if (self.conn == null) return error.NotConnected;
        const conn = self.conn.?;

        // Check if slot already exists
        const check_query = try std.fmt.allocPrintZ(
            self.allocator,
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name = '{s}'",
            .{self.config.slot_name},
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
            log.info("Replication slot '{s}' already exists", .{self.config.slot_name});
            return;
        }

        // Create the slot using pgoutput plugin
        const create_query = try std.fmt.allocPrintZ(
            self.allocator,
            "SELECT pg_create_logical_replication_slot('{s}', 'pgoutput')",
            .{self.config.slot_name},
        );
        defer self.allocator.free(create_query);

        const create_result = c.PQexec(conn, create_query.ptr);
        defer c.PQclear(create_result);

        if (c.PQresultStatus(create_result) != c.PGRES_TUPLES_OK) {
            const err_msg = c.PQerrorMessage(conn);
            log.err("Failed to create replication slot: {s}", .{err_msg});
            return error.SlotCreationFailed;
        }

        log.info("✓ Replication slot '{s}' created", .{self.config.slot_name});
    }

    /// Create publication for tables
    pub fn createPublication(self: *Consumer) !void {
        log.info("Creating publication '{s}'...", .{self.config.publication_name});

        if (self.conn == null) return error.NotConnected;
        const conn = self.conn.?;

        // Check if publication exists
        const check_query = try std.fmt.allocPrintZ(
            self.allocator,
            "SELECT pubname FROM pg_publication WHERE pubname = '{s}'",
            .{self.config.publication_name},
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
            log.info("Publication '{s}' already exists", .{self.config.publication_name});
            return;
        }

        // Build CREATE PUBLICATION query
        const create_query = if (self.config.tables.len == 0)
            try std.fmt.allocPrintZ(
                self.allocator,
                "CREATE PUBLICATION {s} FOR ALL TABLES",
                .{self.config.publication_name},
            )
        else blk: {
            // Join tables with ", "
            const table_list = try std.mem.join(self.allocator, ", ", self.config.tables);
            defer self.allocator.free(table_list);

            const query = try std.fmt.allocPrint(
                self.allocator,
                "CREATE PUBLICATION {s} FOR TABLE {s}",
                .{ self.config.publication_name, table_list },
            );
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

        log.info("✓ Publication '{s}' created", .{self.config.publication_name});
    }

    /// Get current WAL LSN position
    pub fn getCurrentLSN(self: *Consumer) ![]const u8 {
        if (self.conn == null) return error.NotConnected;
        const conn = self.conn.?;

        const query = "SELECT pg_current_wal_lsn()::text";
        const result = c.PQexec(conn, query);
        defer c.PQclear(result);

        if (c.PQresultStatus(result) != c.PGRES_TUPLES_OK) {
            const err_msg = c.PQerrorMessage(conn);
            log.err("Failed to get current LSN: {s}", .{err_msg});
            return error.GetLSNFailed;
        }

        if (c.PQntuples(result) == 0) {
            return error.NoLSN;
        }

        // Get the LSN value from the first row, first column
        const lsn_ptr = c.PQgetvalue(result, 0, 0);
        const lsn = std.mem.span(lsn_ptr);

        // Dupe the LSN since it will be invalid after PQclear()
        return try self.allocator.dupe(u8, lsn);
    }
};
