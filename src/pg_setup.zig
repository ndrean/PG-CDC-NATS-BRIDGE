const std = @import("std");

pub const log = std.log.scoped(.pg_setup);

/// PostgreSQL connection configuration
/// Centralized storage for database credentials used across the application
pub const PgSetup = struct {
    host: []const u8,
    port: u16,
    user: []const u8,
    password: []const u8,
    database: []const u8,

    /// Build a PostgreSQL connection string
    ///
    /// Caller is responsible for freeing the returned string
    pub fn connInfo(self: *const PgSetup, allocator: std.mem.Allocator, replication: bool) ![:0]const u8 {
        const conninfo_str = if (replication)
            try std.fmt.allocPrint(
                allocator,
                "host={s} port={d} user={s} password={s} dbname={s} replication=database",
                .{ self.host, self.port, self.user, self.password, self.database },
            )
        else
            try std.fmt.allocPrint(
                allocator,
                "host={s} port={d} user={s} password={s} dbname={s}",
                .{ self.host, self.port, self.user, self.password, self.database },
            );
        defer allocator.free(conninfo_str);

        // Return null-terminated string for C APIs
        return try allocator.dupeZ(u8, conninfo_str);
    }
};
