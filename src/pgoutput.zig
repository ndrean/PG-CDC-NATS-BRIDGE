//! Parser for PostgreSQL logical replication protocol
const std = @import("std");

pub const log = std.log.scoped(.pgoutput);

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
/// These values are standard.
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
    TIMESTAMPTZ = 1184, // timestamp with time zone (8 bytes)

    // Others
    UUID = 2950,
    BYTEA = 17,
    JSONB = 3802,
    NUMERIC = 1700,
    _, // Placeholder for unsupported types
};

/// Decoded column value representation
pub const DecodedValue = union(enum) {
    int32: i32,
    int64: i64,
    float64: f64,
    boolean: bool,
    text: []const u8,
    // Add more types as you implement them (e.g., date, timestamp, bytea)
};

/// Decodes the raw bytes based on the PostgreSQL type OID.
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

        // --- 3. TODO Complex/Other Types ------------------------------------

        // specific decoding logic: JSONB, NUMERIC, Arrays, UUID (16 bytes)
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
) !std.StringHashMap(DecodedValue) {
    var decoded_map = std.StringHashMap(DecodedValue).init(allocator);
    errdefer decoded_map.deinit();

    if (tuple.columns.len != columns.len) return error.ColumnMismatch;

    for (columns, tuple.columns) |col_info, col_data| {
        if (col_data) |raw_bytes| {
            const decoded_value = decodeColumnData(allocator, col_info.type_id, raw_bytes) catch |err| {
                log.err("Failed to decode column '{s}' (type_id={d}, bytes={d}): {}", .{ col_info.name, col_info.type_id, raw_bytes.len, err });
                return err;
            };
            try decoded_map.put(col_info.name, decoded_value);
        } else {
            // Handle NULL values
            // skip? or use a custom DecodedValue union option for NULL?)
        }
    }
    return decoded_map;
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
                    const len = try self.readU32();
                    break :blk try self.readBytes(len);
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
