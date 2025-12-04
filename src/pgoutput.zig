//! Parser for PostgreSQL logical replication protocol
const std = @import("std");
const array_mod = @import("array.zig");
const numeric_mod = @import("numeric.zig");

pub const log = std.log.scoped(.pgoutput);

/// Helper: Check if a year is a leap year
inline fn isLeapYear(year: i32) bool {
    return (@rem(year, 4) == 0 and @rem(year, 100) != 0) or (@rem(year, 400) == 0);
}

/// pgoutput protocol parser
///
/// Parses PostgreSQL logical replication binary messages
///
/// Spec: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
pub const PgOutputMessage = union(enum) {
    begin: BeginMessage,
    commit: CommitMessage,
    relation: RelationMessage,
    insert: InsertMessage,
    update: UpdateMessage,
    delete: DeleteMessage,
    origin: OriginMessage,
    type: TypeMessage,
    truncate: TruncateMessage,
};

/// pgoutput message BEGIN structures
pub const BeginMessage = struct {
    final_lsn: u64,
    timestamp: i64,
    xid: u32,
};

/// pgoutput message COMMIT structures
pub const CommitMessage = struct {
    flags: u8,
    commit_lsn: u64,
    end_lsn: u64,
    timestamp: i64,
};

/// PostgreSQL built-in Type OIDs (Object IDs).
///
/// These values are standard and obtained from PostgreSQL with: `docker exec postgres psql -U postgres -d postgres -c "SELECT oid, typname FROM pg_type WHERE typname IN ('bool', 'int2', 'int4', 'int8', 'float4', 'float8', 'text', 'varchar', 'bpchar', 'date', 'timestamptz', 'uuid', 'bytea', 'jsonb', 'numeric', '_int4', '_text', '_jsonb') ORDER BY oid;"`
pub const PgOid = enum(u32) {
    // Numeric Types
    BOOL = 16,
    INT2 = 21, // smallint
    INT4 = 23, // integer
    INT8 = 20, // bigint
    FLOAT4 = 700,
    FLOAT8 = 701,

    // Character Types
    TEXT = 25,
    VARCHAR = 1043,
    BPCHAR = 1042, // char(n)

    // Date/Time Types
    DATE = 1082,
    TIMESTAMP = 1114, // timestamp without time zone (8 bytes)
    TIMESTAMPTZ = 1184, // timestamp with time zone (8 bytes)

    // Others
    UUID = 2950,
    BYTEA = 17,
    JSON = 114, // json (text format, before jsonb)
    JSONB = 3802,
    NUMERIC = 1700,

    // Array Types (prefixed with underscore in PostgreSQL)
    ARRAY_INT2 = 1005, // _int2 (smallint[])
    ARRAY_INT4 = 1007, // _int4 (integer[])
    ARRAY_INT8 = 1016, // _int8 (bigint[])
    ARRAY_TEXT = 1009, // _text (text[])
    ARRAY_VARCHAR = 1015, // _varchar (varchar[])
    ARRAY_FLOAT4 = 1021, // _float4 (float4[])
    ARRAY_FLOAT8 = 1022, // _float8 (float8[])
    ARRAY_BOOL = 1000, // _bool (boolean[])
    ARRAY_JSONB = 3807, // _jsonb (jsonb[])
    ARRAY_NUMERIC = 1231, // _numeric (numeric[])
    ARRAY_UUID = 2951, // _uuid (uuid[])
    ARRAY_TIMESTAMPTZ = 1185, // _timestamptz (timestamptz[])

    _, // Placeholder for unsupported types
};

const NUMERIC_POS: u16 = 0x0000;
const NUMERIC_NEG: u16 = 0x4000;
const NUMERIC_NAN: u16 = 0xC000;
const NUMERIC_PINF: u16 = 0xD000;
const NUMERIC_NINF: u16 = 0xF000;

const NBASE: u16 = 10000; // Each digit represents 4 decimal digits

/// Decoded column value representation
pub const DecodedValue = union(enum) {
    null,
    boolean: bool,
    int32: i32,
    int64: i64,
    float64: f64,
    text: []const u8, // TEXT, VARCHAR, CHAR, UUID, DATE, TIMESTAMPTZ
    numeric: []const u8, // NUMERIC - keep as decimal string
    jsonb: []const u8, // JSONB - as JSON string
    array: []const u8, // ARRAY - as JSON string
    bytea: []const u8, // BYTEA - raw bytes or hex/base64 encoded
};

/// Decoded column (name + value pair)
/// Used instead of HashMap for better performance - we only iterate, never lookup by key
pub const Column = struct {
    name: []const u8,
    value: DecodedValue,
};

/// Decodes the raw bytes based on the PostgreSQL type OID (BINARY format).
///
/// This version handles binary-encoded data from PostgreSQL when using
/// START_REPLICATION with binary 'true'.
/// Returns the same DecodedValue types as decodeColumnData() for compatibility.
pub fn decodeBinColumnData(
    allocator: std.mem.Allocator,
    type_id: u32,
    raw_bytes: []const u8,
) !DecodedValue {

    // _ = @import("jsonb.zig"); // TODO: Use when implementing JSONB binary parsing

    const oid: PgOid = @enumFromInt(type_id);

    return switch (oid) {
        // --- Fixed-Width Numeric Types (Binary format) ---
        .BOOL => {
            if (raw_bytes.len != 1) return error.InvalidDataLength;
            return .{ .boolean = raw_bytes[0] != 0 };
        },

        .INT2 => {
            if (raw_bytes.len != 2) return error.InvalidDataLength;
            const val = std.mem.readInt(i16, raw_bytes[0..2], .big);
            return .{ .int32 = @intCast(val) };
        },

        .INT4 => {
            if (raw_bytes.len != 4) return error.InvalidDataLength;
            const val = std.mem.readInt(i32, raw_bytes[0..4], .big);
            return .{ .int32 = val };
        },

        .INT8 => {
            if (raw_bytes.len != 8) return error.InvalidDataLength;
            const val = std.mem.readInt(i64, raw_bytes[0..8], .big);
            return .{ .int64 = val };
        },

        .FLOAT4 => {
            if (raw_bytes.len != 4) return error.InvalidDataLength;
            const bits = std.mem.readInt(u32, raw_bytes[0..4], .big);
            const val: f32 = @bitCast(bits);
            return .{ .float64 = @floatCast(val) };
        },

        .FLOAT8 => {
            if (raw_bytes.len != 8) return error.InvalidDataLength;
            const bits = std.mem.readInt(u64, raw_bytes[0..8], .big);
            const val: f64 = @bitCast(bits);
            return .{ .float64 = val };
        },

        // --- Date/Time Types (Binary format) ---
        .DATE => {
            if (raw_bytes.len != 4) return error.InvalidDataLength;
            const pg_days = std.mem.readInt(i32, raw_bytes[0..4], .big);

            // PostgreSQL epoch: 2000-01-01
            // Convert to Unix timestamp then to date components
            const PG_EPOCH_DAYS: i64 = 10957; // Days between 1970-01-01 and 2000-01-01
            const total_unix_days = @as(i64, @intCast(pg_days)) + PG_EPOCH_DAYS;

            // Simple date calculation (accounts for leap years)
            var days = total_unix_days;
            var year: i32 = 1970;

            // Count years
            while (true) {
                const days_in_year: i64 = if (isLeapYear(year)) 366 else 365;
                if (days < days_in_year) break;
                days -= days_in_year;
                year += 1;
            }

            // Count months
            const is_leap = isLeapYear(year);
            const month_days = [12]i32{ 31, if (is_leap) 29 else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
            var month: i32 = 1;
            for (month_days) |month_len| {
                if (days < month_len) break;
                days -= month_len;
                month += 1;
            }
            const day: i32 = @intCast(days + 1);

            // Format as YYYY-MM-DD (always 10 chars)
            var buf: [16]u8 = undefined;
            _ = try std.fmt.bufPrint(
                &buf,
                "{d:0>4}-{d:0>2}-{d:0>2}",
                .{ @as(u32, @intCast(year)), @as(u32, @intCast(month)), @as(u32, @intCast(day)) },
            );

            return .{ .text = try allocator.dupe(u8, buf[0..10]) };
        },

        // ISO 8601 format: 2025-10-26T10:00:00.000000Z
        .TIMESTAMP, .TIMESTAMPTZ => {
            // Both use identical binary format: 8 bytes of microseconds since PG epoch
            // TIMESTAMPTZ is timezone-aware, TIMESTAMP is not, but encoding is the same
            if (raw_bytes.len != 8) return error.InvalidDataLength;
            const microseconds = std.mem.readInt(i64, raw_bytes[0..8], .big);

            // PostgreSQL epoch: 2000-01-01 00:00:00 UTC
            const PG_EPOCH_SECONDS: i64 = 946684800;

            // Convert PG microsecs to Unix seconds
            const total_seconds = PG_EPOCH_SECONDS + @divFloor(microseconds, 1_000_000);
            const remaining_micros: u32 = @intCast(@abs(@mod(microseconds, 1_000_000)));

            // Manual datetime calculation
            var seconds = total_seconds;
            var year: i32 = 1970;

            // Count years
            while (true) {
                const seconds_in_year: i64 = if (isLeapYear(year)) 366 * 86400 else 365 * 86400;
                if (seconds < seconds_in_year) break;
                seconds -= seconds_in_year;
                year += 1;
            }

            // Count months
            const is_leap = isLeapYear(year);
            const month_days = [12]i32{ 31, if (is_leap) 29 else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
            var month: i32 = 1;
            for (month_days) |month_len| {
                const seconds_in_month: i64 = @as(i64, month_len) * 86400;
                if (seconds < seconds_in_month) break;
                seconds -= seconds_in_month;
                month += 1;
            }

            const day: i32 = @intCast(@divFloor(seconds, 86400) + 1);
            seconds = @mod(seconds, 86400);
            const hour: i32 = @intCast(@divFloor(seconds, 3600));
            seconds = @mod(seconds, 3600);
            const minute: i32 = @intCast(@divFloor(seconds, 60));
            const second: i32 = @intCast(@mod(seconds, 60));

            // Format as ISO 8601: 2025-10-26T10:00:00.000000Z (always 27 chars)
            var buf: [32]u8 = undefined;
            _ = try std.fmt.bufPrint(
                &buf,
                "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>6}Z",
                .{
                    @as(u32, @intCast(year)),
                    @as(u32, @intCast(month)),
                    @as(u32, @intCast(day)),
                    @as(u32, @intCast(hour)),
                    @as(u32, @intCast(minute)),
                    @as(u32, @intCast(second)),
                    remaining_micros,
                },
            );

            return .{ .text = try allocator.dupe(u8, buf[0..27]) };
        },

        // --- UUID (Binary format - 16 bytes) ---
        // use `bufPrint` to avoid heap allocation
        .UUID => {
            if (raw_bytes.len != 16) return error.InvalidDataLength;
            // Format as xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (always 36 chars)
            var buf: [36]u8 = undefined;
            _ = try std.fmt.bufPrint(
                &buf,
                "{x:0>2}{x:0>2}{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}",
                .{
                    raw_bytes[0],  raw_bytes[1],  raw_bytes[2],  raw_bytes[3],
                    raw_bytes[4],  raw_bytes[5],  raw_bytes[6],  raw_bytes[7],
                    raw_bytes[8],  raw_bytes[9],  raw_bytes[10], raw_bytes[11],
                    raw_bytes[12], raw_bytes[13], raw_bytes[14], raw_bytes[15],
                },
            );
            // UUID is always exactly 36 characters, allocate and return
            return .{ .text = try allocator.dupe(u8, &buf) };
        },

        // --- Variable-Length Text Types (Binary format is still text) ---
        .TEXT,
        .VARCHAR,
        .BPCHAR,
        .BYTEA,
        .JSON,
        => {
            // raw_bytes is from readBytes() which uses arena_allocator
            // We need to dupe with main_allocator so it survives arena cleanup
            const owned = try allocator.dupe(u8, raw_bytes);
            return .{ .text = owned };
        },

        // --- NUMERIC (Binary format - use numeric.zig) ---
        .NUMERIC => {
            const result = try numeric_mod.parseNumeric(allocator, raw_bytes);
            // parseNumeric returns NumericResult { .slice, .allocated }
            // where .slice is a sub-slice of .allocated pointing to the actual data
            //
            // The issue with dupe+free was that we were freeing result.allocated
            // immediately after duping result.slice, which could cause issues.
            //
            // Better approach: Use realloc to shrink the buffer to exact size.
            // This avoids the dupe+free dance and is more efficient.
            const exact_size = result.slice.len;
            if (allocator.resize(result.allocated, exact_size)) {
                // Resize succeeded in-place
                return .{ .numeric = result.allocated[0..exact_size] };
            } else {
                // Need to allocate new buffer
                const owned = try allocator.dupe(u8, result.slice);
                allocator.free(result.allocated);
                return .{ .numeric = owned };
            }
        },

        // // --- JSON (text format even in binary mode) ---
        // .JSON => {
        //     // PostgreSQL sends JSON as plain text even when binary format is requested
        //     const owned = try allocator.dupe(u8, raw_bytes);
        //     return .{ .jsonb = owned }; // Use jsonb field for compatibility
        // },

        // --- JSONB (binary format is version byte + JSON text) ---
        .JSONB => {
            // PostgreSQL JSONB v1 format: version byte (0x01) + plain JSON text
            // No complex parsing needed - just skip the version byte
            if (raw_bytes.len < 1) return error.InvalidDataLength;
            const version = raw_bytes[0];
            if (version != 1) return error.UnsupportedJsonbVersion;

            // Skip version byte, return JSON text
            const owned = try allocator.dupe(u8, raw_bytes[1..]);
            return .{ .jsonb = owned };
        },

        // --- Array Types (Binary format - use array.zig) ---
        .ARRAY_INT2, .ARRAY_INT4, .ARRAY_INT8, .ARRAY_TEXT, .ARRAY_VARCHAR, .ARRAY_FLOAT4, .ARRAY_FLOAT8, .ARRAY_BOOL, .ARRAY_JSONB, .ARRAY_NUMERIC, .ARRAY_UUID, .ARRAY_TIMESTAMPTZ => {
            const parsed = try array_mod.parseArray(allocator, raw_bytes);
            defer parsed.deinit(allocator);

            // Convert ArrayResult to PostgreSQL text format: {1,2,3}
            const text_repr = try arrayToText(allocator, parsed);
            return .{ .array = text_repr };
        },

        // --- Handle ENUMs and unknown types gracefully ---
        else => {
            // ENUMs and other user-defined types are sent as text in binary format
            // Treat them as TEXT and log a warning for visibility
            log.warn("Unknown type OID {d}, treating as text", .{type_id});
            const owned = try allocator.dupe(u8, raw_bytes);
            return .{ .text = owned };
        },
    };
}

/// Helper: Convert ArrayResult to PostgreSQL text format
fn arrayToText(allocator: std.mem.Allocator, arr: @import("array.zig").ArrayResult) ![]const u8 {
    var buffer: std.ArrayList(u8) = .empty;
    errdefer buffer.deinit(allocator);

    const writer = buffer.writer(allocator);

    // Handle multi-dimensional arrays
    if (arr.ndim > 1) {
        try writer.writeByte('{');
        // For simplicity, flatten to 1D for now
        // TODO: Handle proper multi-dimensional formatting
    } else {
        try writer.writeByte('{');
    }

    for (arr.elements, 0..) |elem, i| {
        if (i > 0) try writer.writeByte(',');

        switch (elem) {
            .null_value => try writer.writeAll("NULL"),
            .data => |data| {
                // Decode based on element OID
                const oid: PgOid = @enumFromInt(arr.element_oid);
                switch (oid) {
                    .INT2 => {
                        const val = std.mem.readInt(i16, data[0..2], .big);
                        try writer.print("{d}", .{val});
                    },
                    .INT4 => {
                        const val = std.mem.readInt(i32, data[0..4], .big);
                        try writer.print("{d}", .{val});
                    },
                    .INT8 => {
                        const val = std.mem.readInt(i64, data[0..8], .big);
                        try writer.print("{d}", .{val});
                    },
                    .TEXT, .VARCHAR => {
                        try writer.writeByte('"');
                        try writer.writeAll(data);
                        try writer.writeByte('"');
                    },
                    .FLOAT8 => {
                        const bits = std.mem.readInt(u64, data[0..8], .big);
                        const val: f64 = @bitCast(bits);
                        try writer.print("{d}", .{val});
                    },
                    .BOOL => {
                        const val = data[0] != 0;
                        try writer.writeAll(if (val) "t" else "f");
                    },
                    else => {
                        // Fallback: hex encode
                        try writer.writeAll("\\x");
                        for (data) |byte| {
                            try writer.print("{x:0>2}", .{byte});
                        }
                    },
                }
            },
        }
    }

    try writer.writeByte('}');

    return buffer.toOwnedSlice(allocator);
}

/// Decodes the raw bytes based on the PostgreSQL type OID (TEXT format).
///
/// Note: pgoutput sends data in TEXT format by default, not binary!
/// raw_bytes contains the UTF-8 text representation of the value.
pub fn decodeColumnData(
    _: std.mem.Allocator,
    type_id: u32,
    raw_bytes: []const u8,
) !DecodedValue {
    // What if type_id is not in PgOid?
    const oid: PgOid = @enumFromInt(type_id);
    // catch {
    //     // Fallback for unsupported OIDs (e.g., arrays, custom types, numeric)
    //     return error.UnsupportedType;
    // };

    return switch (oid) {
        // --- 1. Fixed-Width Types (Simple reads) ----------------------

        // BOOL - text format is "t" or "f"
        .BOOL => {
            if (raw_bytes.len == 0) return error.InvalidDataLength;
            return .{ .boolean = raw_bytes[0] == 't' };
        },

        // INT2 - parse text as integer
        .INT2 => {
            const val = try std.fmt.parseInt(i16, raw_bytes, 10);
            return .{ .int32 = @intCast(val) };
        },

        // INT4 - parse text as integer
        .INT4 => {
            const val = try std.fmt.parseInt(i32, raw_bytes, 10);
            return .{ .int32 = val };
        },

        // INT8 - parse text as integer
        .INT8 => {
            const val = try std.fmt.parseInt(i64, raw_bytes, 10);
            return .{ .int64 = val };
        },

        // TIMESTAMPTZ - parse text timestamp
        .TIMESTAMPTZ => {
            // PostgreSQL text format for timestamptz: "2025-11-30 20:13:29.377405+00"
            // For now, text representation
            // TODO: Parse into actual timestamp
            return .{ .text = raw_bytes };
        },

        // FLOAT8 - parse text as float
        .FLOAT8 => {
            const val = try std.fmt.parseFloat(f64, raw_bytes);
            return .{ .float64 = val };
        },

        // DATE - parse text date "YYYY-MM-DD"
        .DATE => {
            // For now, text representation
            // TODO: Parse into days since 2000-01-01
            return .{ .text = raw_bytes };
        },

        // UUID - text format is "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
        .UUID => {
            // pgoutput sends UUID in text format, already formatted
            return .{ .text = raw_bytes };
        },

        // --- 2. Variable-Length Types (The raw bytes are the value) ---

        // TEXT, VARCHAR, BPCHAR. The bytes are already the UTF-8 text string.
        .TEXT, .VARCHAR, .BPCHAR => {
            // Note: The bytes retrieved by parseTupleData are already a dynamically allocated slice
            // of the text content (assuming it's not TOASTed/unchanged, which current code handles).
            return .{ .text = raw_bytes };
        },

        // BYTEA. The bytes are the raw binary content.
        .BYTEA => {
            return .{ .text = raw_bytes }; // Representing as raw byte slice
        },

        // --- 3. Complex Types (TEXT format) ---------------------------------

        // JSONB - text format is already JSON string
        .JSONB => {
            return .{ .jsonb = raw_bytes };
        },

        // NUMERIC - text format is decimal string like "123.456"
        .NUMERIC => {
            return .{ .numeric = raw_bytes };
        },

        // --- 4. Array Types (TEXT format) -----------------------------------
        // PostgreSQL text array format: {val1,val2,val3} or {{1,2},{3,4}}

        .ARRAY_INT2,
        .ARRAY_INT4,
        .ARRAY_INT8,
        .ARRAY_TEXT,
        .ARRAY_VARCHAR,
        .ARRAY_FLOAT4,
        .ARRAY_FLOAT8,
        .ARRAY_BOOL,
        .ARRAY_JSONB,
        .ARRAY_NUMERIC,
        .ARRAY_UUID,
        .ARRAY_TIMESTAMPTZ,
        => {
            // Arrays in TEXT format come as PostgreSQL text representation
            // Examples: {1,2,3}, {"a","b","c"}, {{1,2},{3,4}}
            return .{ .array = raw_bytes };
        },

        // --- 5. Unsupported Types -------------------------------------------

        else => return error.UnsupportedType,
    };
}

pub const RelationMessage = struct {
    relation_id: u32,
    namespace: []const u8,
    name: []const u8,
    replica_identity: u8,
    columns: []ColumnInfo,

    pub const ColumnInfo = struct {
        flags: u8,
        name: []const u8,
        type_id: u32,
        type_modifier: i32,
    };

    pub fn deinit(self: *RelationMessage, allocator: std.mem.Allocator) void {
        allocator.free(self.namespace);
        allocator.free(self.name);
        for (self.columns) |col| {
            allocator.free(col.name);
        }
        allocator.free(self.columns);
    }

    pub fn clone(self: RelationMessage, allocator: std.mem.Allocator) !RelationMessage {
        const cloned_namespace = try allocator.dupe(u8, self.namespace);
        errdefer allocator.free(cloned_namespace);

        const cloned_name = try allocator.dupe(u8, self.name);
        errdefer allocator.free(cloned_name);

        const cloned_columns = try allocator.alloc(ColumnInfo, self.columns.len);
        errdefer allocator.free(cloned_columns);

        for (self.columns, 0..) |col, i| {
            const cloned_col_name = try allocator.dupe(u8, col.name);
            errdefer {
                // Free previously cloned column names on error
                for (cloned_columns[0..i]) |c| {
                    allocator.free(c.name);
                }
            }
            cloned_columns[i] = ColumnInfo{
                .flags = col.flags,
                .name = cloned_col_name,
                .type_id = col.type_id,
                .type_modifier = col.type_modifier,
            };
        }

        return RelationMessage{
            .relation_id = self.relation_id,
            .namespace = cloned_namespace,
            .name = cloned_name,
            .replica_identity = self.replica_identity,
            .columns = cloned_columns,
        };
    }
};

pub const InsertMessage = struct {
    relation_id: u32,
    tuple_data: TupleData,

    pub fn deinit(self: *InsertMessage, allocator: std.mem.Allocator) void {
        self.tuple_data.deinit(allocator);
    }
};

pub const UpdateMessage = struct {
    relation_id: u32,
    old_tuple: ?TupleData,
    new_tuple: TupleData,

    pub fn deinit(self: *UpdateMessage, allocator: std.mem.Allocator) void {
        if (self.old_tuple) |*old| {
            old.deinit(allocator);
        }
        self.new_tuple.deinit(allocator);
    }
};

pub const DeleteMessage = struct {
    relation_id: u32,
    old_tuple: TupleData,

    pub fn deinit(self: *DeleteMessage, allocator: std.mem.Allocator) void {
        self.old_tuple.deinit(allocator);
    }
};

pub const OriginMessage = struct {
    commit_lsn: u64,
    name: []const u8,
};

pub const TypeMessage = struct {
    type_id: u32,
    namespace: []const u8,
    name: []const u8,
};

pub const TruncateMessage = struct {
    options: u8,
    relation_ids: []u32,
};

pub const TupleData = struct {
    columns: []?[]const u8,

    pub fn deinit(self: *TupleData, allocator: std.mem.Allocator) void {
        for (self.columns) |col| {
            if (col) |data| {
                allocator.free(data);
            }
        }
        allocator.free(self.columns);
    }
};

pub fn decodeTuple(
    allocator: std.mem.Allocator,
    tuple: TupleData,
    columns: []const RelationMessage.ColumnInfo,
) !std.ArrayList(Column) {
    var decoded_columns = std.ArrayList(Column){};
    errdefer decoded_columns.deinit(allocator);

    if (tuple.columns.len != columns.len) return error.ColumnMismatch;

    // Pre-allocate capacity for all columns
    try decoded_columns.ensureTotalCapacity(allocator, columns.len);

    for (columns, tuple.columns) |col_info, col_data| {
        if (col_data) |raw_bytes| {
            // Log raw text value for debugging
            log.debug("Column '{s}' (OID={d}): '{s}'", .{ col_info.name, col_info.type_id, raw_bytes });

            const decoded_value = decodeBinColumnData(allocator, col_info.type_id, raw_bytes) catch |err| {
                log.err("Failed to decode column '{s}' (type_id={d}, bytes={d}): {}", .{ col_info.name, col_info.type_id, raw_bytes.len, err });
                return err;
            };
            // Column name points to RelationMessage which has stable lifetime
            // No need to duplicate - it lives for the entire relation cache
            decoded_columns.appendAssumeCapacity(Column{
                .name = col_info.name,
                .value = decoded_value,
            });
        } else {
            // Handle NULL values
            log.debug("Column '{s}' is NULL", .{col_info.name});
            decoded_columns.appendAssumeCapacity(Column{
                .name = col_info.name,
                .value = .null,
            });
        }
    }
    return decoded_columns;
}

/// Parser state for decoding pgoutput messages
///
/// Handles reading from a byte slice and parsing messages sequentially
pub const Parser = struct {
    allocator: std.mem.Allocator,
    data: []const u8,
    pos: usize,

    pub fn init(allocator: std.mem.Allocator, data: []const u8) Parser {
        return .{
            .allocator = allocator,
            .data = data,
            .pos = 0,
        };
    }

    pub fn parse(self: *Parser) !PgOutputMessage {
        if (self.pos >= self.data.len) {
            return error.EndOfData;
        }

        const msg_type = self.data[self.pos];
        self.pos += 1;

        return switch (msg_type) {
            'B' => .{ .begin = try self.parseBegin() },
            'C' => .{ .commit = try self.parseCommit() },
            'R' => .{ .relation = try self.parseRelation() },
            'I' => .{ .insert = try self.parseInsert() },
            'U' => .{ .update = try self.parseUpdate() },
            'D' => .{ .delete = try self.parseDelete() },
            'O' => .{ .origin = try self.parseOrigin() },
            'Y' => .{ .type = try self.parseType() },
            'T' => .{ .truncate = try self.parseTruncate() },
            else => {
                log.warn("Unknown pgoutput message type: {c} (0x{x})", .{ msg_type, msg_type });
                return error.UnknownMessageType;
            },
        };
    }

    fn parseBegin(self: *Parser) !BeginMessage {
        const final_lsn = try self.readU64();
        const timestamp = try self.readI64();
        const xid = try self.readU32();

        return BeginMessage{
            .final_lsn = final_lsn,
            .timestamp = timestamp,
            .xid = xid,
        };
    }

    fn parseCommit(self: *Parser) !CommitMessage {
        const flags = try self.readU8();
        const commit_lsn = try self.readU64();
        const end_lsn = try self.readU64();
        const timestamp = try self.readI64();

        return CommitMessage{
            .flags = flags,
            .commit_lsn = commit_lsn,
            .end_lsn = end_lsn,
            .timestamp = timestamp,
        };
    }

    fn parseRelation(self: *Parser) !RelationMessage {
        const relation_id = try self.readU32();
        const namespace = try self.readString();
        const name = try self.readString();
        const replica_identity = try self.readU8();
        const num_columns = try self.readU16();

        var columns = try self.allocator.alloc(RelationMessage.ColumnInfo, num_columns);
        errdefer self.allocator.free(columns);

        var i: usize = 0;
        while (i < num_columns) : (i += 1) {
            const flags = try self.readU8();
            const col_name = try self.readString();
            const type_id = try self.readU32();
            const type_modifier = try self.readI32();

            columns[i] = .{
                .flags = flags,
                .name = col_name,
                .type_id = type_id,
                .type_modifier = type_modifier,
            };
        }

        return RelationMessage{
            .relation_id = relation_id,
            .namespace = namespace,
            .name = name,
            .replica_identity = replica_identity,
            .columns = columns,
        };
    }

    fn parseInsert(self: *Parser) !InsertMessage {
        const relation_id = try self.readU32();
        const tuple_type = try self.readU8();

        if (tuple_type != 'N') {
            log.warn("Expected 'N' (new tuple), got: {c}", .{tuple_type});
            return error.InvalidTupleType;
        }

        const tuple_data = try self.parseTupleData();

        return InsertMessage{
            .relation_id = relation_id,
            .tuple_data = tuple_data,
        };
    }

    fn parseUpdate(self: *Parser) !UpdateMessage {
        const relation_id = try self.readU32();
        const key_or_old = try self.readU8();

        var old_tuple: ?TupleData = null;
        if (key_or_old == 'K' or key_or_old == 'O') {
            old_tuple = try self.parseTupleData();
            _ = try self.readU8(); // Read 'N' for new tuple
        }

        const new_tuple = try self.parseTupleData();

        return UpdateMessage{
            .relation_id = relation_id,
            .old_tuple = old_tuple,
            .new_tuple = new_tuple,
        };
    }

    fn parseDelete(self: *Parser) !DeleteMessage {
        const relation_id = try self.readU32();
        const key_or_old = try self.readU8();

        if (key_or_old != 'K' and key_or_old != 'O') {
            log.warn("Expected 'K' or 'O', got: {c}", .{key_or_old});
            return error.InvalidTupleType;
        }

        const old_tuple = try self.parseTupleData();

        return DeleteMessage{
            .relation_id = relation_id,
            .old_tuple = old_tuple,
        };
    }

    fn parseOrigin(self: *Parser) !OriginMessage {
        const commit_lsn = try self.readU64();
        const name = try self.readString();

        return OriginMessage{
            .commit_lsn = commit_lsn,
            .name = name,
        };
    }

    fn parseType(self: *Parser) !TypeMessage {
        const type_id = try self.readU32();
        const namespace = try self.readString();
        const name = try self.readString();

        return TypeMessage{
            .type_id = type_id,
            .namespace = namespace,
            .name = name,
        };
    }

    fn parseTruncate(self: *Parser) !TruncateMessage {
        const options = try self.readU8();
        const num_relations = try self.readU32();

        var relation_ids = try self.allocator.alloc(u32, num_relations);
        errdefer self.allocator.free(relation_ids);

        var i: usize = 0;
        while (i < num_relations) : (i += 1) {
            relation_ids[i] = try self.readU32();
        }

        return TruncateMessage{
            .options = options,
            .relation_ids = relation_ids,
        };
    }

    /// Parse tuple data
    fn parseTupleData(self: *Parser) !TupleData {
        const num_columns = try self.readU16();
        var columns = try self.allocator.alloc(?[]const u8, num_columns);
        errdefer self.allocator.free(columns);

        var i: usize = 0;
        while (i < num_columns) : (i += 1) {
            const col_type = try self.readU8();

            columns[i] = switch (col_type) {
                'n' => null, // NULL value
                'u' => null, // Unchanged TOASTed value
                't' => blk: {
                    // Text format column data
                    const len = try self.readU32();
                    const data = try self.readBytes(len);
                    log.debug("Column {d}: TEXT format, len={d}", .{ i, len });
                    break :blk data;
                },
                'b' => blk: {
                    // Binary format column data
                    const len = try self.readU32();
                    const data = try self.readBytes(len);
                    log.debug("Column {d}: BINARY format, len={d}", .{ i, len });
                    break :blk data;
                },
                else => {
                    log.warn("Unknown column type: {c}", .{col_type});
                    return error.UnknownColumnType;
                },
            };
        }

        return TupleData{ .columns = columns };
    }

    // Low-level read functions

    /// Read a single byte
    fn readU8(self: *Parser) !u8 {
        if (self.pos >= self.data.len) {
            return error.UnexpectedEndOfData;
        }
        const val = self.data[self.pos];
        self.pos += 1;
        return val;
    }

    /// Read a 16-bit unsigned integer in big-endian order
    fn readU16(self: *Parser) !u16 {
        if (self.pos + 2 > self.data.len) {
            return error.UnexpectedEndOfData;
        }
        const val = std.mem.readInt(u16, self.data[self.pos..][0..2], .big);
        self.pos += 2;
        return val;
    }

    /// Read a 32-bit unsigned integer in big-endian order
    fn readU32(self: *Parser) !u32 {
        if (self.pos + 4 > self.data.len) {
            return error.UnexpectedEndOfData;
        }
        const val = std.mem.readInt(u32, self.data[self.pos..][0..4], .big);
        self.pos += 4;
        return val;
    }

    /// Read a 32-bit signed integer in big-endian order
    fn readI32(self: *Parser) !i32 {
        if (self.pos + 4 > self.data.len) {
            return error.UnexpectedEndOfData;
        }
        const val = std.mem.readInt(i32, self.data[self.pos..][0..4], .big);
        self.pos += 4;
        return val;
    }

    /// Read a 64-bit unsigned integer in big-endian order
    fn readU64(self: *Parser) !u64 {
        if (self.pos + 8 > self.data.len) {
            return error.UnexpectedEndOfData;
        }
        const val = std.mem.readInt(u64, self.data[self.pos..][0..8], .big);
        self.pos += 8;
        return val;
    }

    /// Read a 64-bit signed integer in big-endian order
    fn readI64(self: *Parser) !i64 {
        if (self.pos + 8 > self.data.len) {
            return error.UnexpectedEndOfData;
        }
        const val = std.mem.readInt(i64, self.data[self.pos..][0..8], .big);
        self.pos += 8;
        return val;
    }

    /// Read a null-terminated string
    ///
    /// Caller is responsible for freeing the returned string
    fn readString(self: *Parser) ![]const u8 {
        const start = self.pos;
        while (self.pos < self.data.len and self.data[self.pos] != 0) {
            self.pos += 1;
        }

        if (self.pos >= self.data.len) {
            return error.UnexpectedEndOfData;
        }

        const str = self.data[start..self.pos];
        self.pos += 1; // Skip null terminator

        return try self.allocator.dupe(u8, str);
    }

    /// Read a sequence of bytes of given length
    ///
    /// Caller is responsible for freeing the returned byte slice
    fn readBytes(self: *Parser, len: u32) ![]const u8 {
        const end = self.pos + len;
        if (end > self.data.len) {
            return error.UnexpectedEndOfData;
        }

        const bytes = self.data[self.pos..end];
        self.pos = end;

        return try self.allocator.dupe(u8, bytes);
    }
};
