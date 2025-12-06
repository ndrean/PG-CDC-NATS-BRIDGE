const std = @import("std");
const nats_publisher = @import("nats_publisher.zig");
const pgoutput = @import("pgoutput.zig");
const msgpack = @import("msgpack");

pub const log = std.log.scoped(.schema_publisher);

/// Schema version tracker - uses relation_id as natural version number
pub const SchemaCache = struct {
    // Map: table_name -> relation_id
    cache: std.StringHashMap(u32),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) SchemaCache {
        return .{
            .cache = std.StringHashMap(u32).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *SchemaCache) void {
        // Free all keys (table names) we own
        var it = self.cache.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.cache.deinit();
    }

    /// Check if schema has changed. Returns true if schema changed or is new.
    pub fn hasChanged(self: *SchemaCache, table_name: []const u8, relation_id: u32) !bool {
        const cached_id = self.cache.get(table_name);

        if (cached_id == null or cached_id.? != relation_id) {
            // Schema changed or new table - update cache
            // Need to own the key
            const owned_name = try self.allocator.dupe(u8, table_name);
            errdefer self.allocator.free(owned_name);

            // Free old key if exists
            const old_entry = try self.cache.fetchPut(owned_name, relation_id);
            if (old_entry) |old| {
                self.allocator.free(old.key);
            }

            return true;
        }

        return false;
    }
};

/// Publish schema information to NATS schema stream
pub fn publishSchema(
    publisher: *nats_publisher.Publisher,
    relation: *const pgoutput.RelationMessage,
    allocator: std.mem.Allocator,
) !void {
    const schema_version = std.time.timestamp();

    log.info("📋 Publishing schema for table '{s}' (relation_id={d}, version={d})", .{
        relation.name,
        relation.relation_id,
        schema_version,
    });

    // Build subject: schema.{table_name}
    const subject = try std.fmt.allocPrint(
        allocator,
        "schema.{s}\x00",
        .{relation.name},
    );
    defer allocator.free(subject);
    const subject_z: [:0]const u8 = subject[0 .. subject.len - 1 :0];

    // Build MessagePack payload with schema info
    var buffer = std.ArrayList(u8).empty;
    defer buffer.deinit(allocator);

    const ArrayListStream = struct {
        list: *std.ArrayList(u8),
        allocator: std.mem.Allocator,

        const WriteError = std.mem.Allocator.Error;
        const ReadError = error{};

        pub fn write(self: *@This(), bytes: []const u8) WriteError!usize {
            try self.list.appendSlice(self.allocator, bytes);
            return bytes.len;
        }

        pub fn read(self: *@This(), out: []u8) ReadError!usize {
            _ = self;
            _ = out;
            return 0;
        }
    };

    var write_stream = ArrayListStream{ .list = &buffer, .allocator = allocator };
    var read_stream = ArrayListStream{ .list = &buffer, .allocator = allocator };

    var packer = msgpack.Pack(
        *ArrayListStream,
        *ArrayListStream,
        ArrayListStream.WriteError,
        ArrayListStream.ReadError,
        ArrayListStream.write,
        ArrayListStream.read,
    ).init(&write_stream, &read_stream);

    var schema_map = msgpack.Payload.mapPayload(allocator);
    defer schema_map.free(allocator);

    // Add schema fields
    try schema_map.mapPut("table", try msgpack.Payload.strToPayload(relation.name, allocator));
    try schema_map.mapPut("namespace", try msgpack.Payload.strToPayload(relation.namespace, allocator));
    try schema_map.mapPut("relation_id", msgpack.Payload{ .int = @intCast(relation.relation_id) });
    try schema_map.mapPut("schema_version", msgpack.Payload{ .int = schema_version });
    try schema_map.mapPut("replica_identity", msgpack.Payload{ .int = @intCast(relation.replica_identity) });

    // Add columns array
    var columns_array = try msgpack.Payload.arrPayload(relation.columns.len, allocator);
    for (relation.columns, 0..) |col, i| {
        var col_map = msgpack.Payload.mapPayload(allocator);
        try col_map.mapPut("name", try msgpack.Payload.strToPayload(col.name, allocator));
        try col_map.mapPut("type_id", msgpack.Payload{ .int = @intCast(col.type_id) });
        try col_map.mapPut("type_modifier", msgpack.Payload{ .int = @intCast(col.type_modifier) });
        try col_map.mapPut("flags", msgpack.Payload{ .int = @intCast(col.flags) });
        columns_array.arr[i] = col_map;
    }
    try schema_map.mapPut("columns", columns_array);

    try packer.write(schema_map);
    const encoded = buffer.items;

    // Publish to schema stream with relation_id as message ID (for deduplication)
    var msg_id_buf: [64]u8 = undefined;
    const msg_id = try std.fmt.bufPrint(&msg_id_buf, "schema-{s}-{d}", .{ relation.name, relation.relation_id });

    try publisher.publish(subject_z, encoded, msg_id);
    try publisher.flushAsync();

    log.info("✅ Schema published: {s} ({d} columns)", .{ relation.name, relation.columns.len });
}
