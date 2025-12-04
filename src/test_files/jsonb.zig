const std = @import("std");
const num = @import("numeric.zig");

pub const JsonbDecodeError = error{
    Truncated,
    InvalidJsonb,
    UnsupportedVersion,
    OutOfMemory,
};

pub const JsonbValue = union(enum) {
    null,
    bool: bool,
    number: []const u8, // keep as string for precision
    string: []const u8,
    array: []JsonbValue,
    object: []JsonbKeyValue,

    pub fn deinit(self: JsonbValue, allocator: std.mem.Allocator) void {
        switch (self) {
            .array => |arr| {
                for (arr) |item| item.deinit(allocator);
                allocator.free(arr);
            },
            .object => |obj| {
                for (obj) |kv| {
                    allocator.free(kv.key);
                    kv.value.deinit(allocator);
                }
                allocator.free(obj);
            },
            .number => |n| allocator.free(n),
            .string => |s| allocator.free(s),
            .null, .bool => {},
        }
    }
};

pub const JsonbKeyValue = struct {
    key: []const u8,
    value: JsonbValue,
};

// JEntry type flags (top 4 bits)
const JENTRY_NULL: u32 = 0x00000000;
const JENTRY_STRING: u32 = 0x10000000;
const JENTRY_NUMERIC: u32 = 0x20000000;
const JENTRY_BOOL_FALSE: u32 = 0x30000000;
const JENTRY_BOOL_TRUE: u32 = 0x40000000;
const JENTRY_CONTAINER: u32 = 0x50000000;

const JENTRY_TYPE_MASK: u32 = 0x70000000;
const JENTRY_LEN_MASK: u32 = 0x0FFFFFFF;
const JENTRY_HAS_OFF: u32 = 0x80000000; // offset vs length flag

// Container header flags
const JB_ARRAY: u32 = 0x80000000;
const JB_OBJECT: u32 = 0x40000000;
const JB_COUNT_MASK: u32 = 0x0FFFFFFF;

pub fn parseJsonb(allocator: std.mem.Allocator, buf: []const u8) JsonbDecodeError!JsonbValue {
    if (buf.len < 1) return JsonbDecodeError.Truncated;

    // Version byte
    const version = buf[0];
    if (version != 1) return JsonbDecodeError.UnsupportedVersion;

    if (buf.len < 5) return JsonbDecodeError.Truncated;

    return parseContainer(allocator, buf[1..]);
}

fn parseContainer(allocator: std.mem.Allocator, buf: []const u8) JsonbDecodeError!JsonbValue {
    if (buf.len < 4) return JsonbDecodeError.Truncated;

    const header = std.mem.readInt(u32, buf[0..4], .big);
    const count = header & JB_COUNT_MASK;
    const is_array = (header & JB_ARRAY) != 0;
    const is_object = (header & JB_OBJECT) != 0;

    if (!is_array and !is_object) {
        // Scalar wrapped in container - shouldn't happen at root normally
        return JsonbDecodeError.InvalidJsonb;
    }

    const pos: usize = 4;

    if (is_array) {
        return parseArray(allocator, buf, pos, count);
    } else {
        return parseObject(allocator, buf, pos, count);
    }
}

fn parseArray(allocator: std.mem.Allocator, buf: []const u8, start_pos: usize, count: u32) JsonbDecodeError!JsonbValue {
    const n: usize = @intCast(count);

    // Read JEntries
    var jentries = try allocator.alloc(u32, n);
    defer allocator.free(jentries);

    var pos = start_pos;
    for (0..n) |i| {
        if (pos + 4 > buf.len) return JsonbDecodeError.Truncated;
        jentries[i] = std.mem.readInt(u32, buf[pos..][0..4], .big);
        pos += 4;
    }

    // Data starts after all JEntries
    const data_start = pos;

    var elements = try allocator.alloc(JsonbValue, n);
    errdefer {
        for (elements[0..]) |e| e.deinit(allocator);
        allocator.free(elements);
    }

    var data_offset: usize = 0;
    for (0..n) |i| {
        const jentry = jentries[i];
        const result = try parseValue(allocator, buf, data_start, jentry, data_offset);
        elements[i] = result.value;
        data_offset += result.len;
    }

    return .{ .array = elements };
}

fn parseObject(allocator: std.mem.Allocator, buf: []const u8, start_pos: usize, count: u32) JsonbDecodeError!JsonbValue {
    const n: usize = @intCast(count);

    // Objects have 2*n JEntries: keys first, then values
    var jentries = try allocator.alloc(u32, n * 2);
    defer allocator.free(jentries);

    var pos = start_pos;
    for (0..n * 2) |i| {
        if (pos + 4 > buf.len) return JsonbDecodeError.Truncated;
        jentries[i] = std.mem.readInt(u32, buf[pos..][0..4], .big);
        pos += 4;
    }

    const data_start = pos;

    var kvs = try allocator.alloc(JsonbKeyValue, n);
    errdefer {
        for (kvs[0..]) |kv| {
            allocator.free(kv.key);
            kv.value.deinit(allocator);
        }
        allocator.free(kvs);
    }

    var data_offset: usize = 0;

    // Parse keys first (all keys come before all values in data)
    var key_ends = try allocator.alloc(usize, n);
    defer allocator.free(key_ends);

    for (0..n) |i| {
        const jentry = jentries[i];
        const len = jentry & JENTRY_LEN_MASK;

        if (data_start + data_offset + len > buf.len) return JsonbDecodeError.Truncated;

        kvs[i].key = try allocator.dupe(u8, buf[data_start + data_offset .. data_start + data_offset + len]);
        data_offset += len;
        key_ends[i] = data_offset;
    }

    // Parse values
    for (0..n) |i| {
        const jentry = jentries[n + i];
        const result = try parseValue(allocator, buf, data_start, jentry, data_offset);
        kvs[i].value = result.value;
        data_offset += result.len;
    }

    return .{ .object = kvs };
}

const ParseResult = struct {
    value: JsonbValue,
    len: usize,
};

fn parseValue(allocator: std.mem.Allocator, buf: []const u8, data_start: usize, jentry: u32, offset: usize) JsonbDecodeError!ParseResult {
    const type_tag = jentry & JENTRY_TYPE_MASK;
    const len: usize = @intCast(jentry & JENTRY_LEN_MASK);

    const data_pos = data_start + offset;

    switch (type_tag) {
        JENTRY_NULL => return .{ .value = .null, .len = 0 },

        JENTRY_BOOL_FALSE => return .{ .value = .{ .bool = false }, .len = 0 },

        JENTRY_BOOL_TRUE => return .{ .value = .{ .bool = true }, .len = 0 },

        JENTRY_STRING => {
            if (data_pos + len > buf.len) return JsonbDecodeError.Truncated;
            const str = try allocator.dupe(u8, buf[data_pos .. data_pos + len]);
            return .{ .value = .{ .string = str }, .len = len };
        },

        JENTRY_NUMERIC => {
            if (data_pos + len > buf.len) return JsonbDecodeError.Truncated;
            // NUMERIC in JSONB uses PostgreSQL NUMERIC binary format
            const num_result = num.parseNumeric(
                allocator,
                buf[data_pos .. data_pos + len],
            ) catch {
                return JsonbDecodeError.InvalidJsonb;
            };
            return .{
                .value = .{ .number = num_result.slice },
                .len = len,
            };
        },

        JENTRY_CONTAINER => {
            if (data_pos + 4 > buf.len) return JsonbDecodeError.Truncated;
            const nested = try parseContainer(allocator, buf[data_pos .. data_pos + len]);
            return .{ .value = nested, .len = len };
        },

        else => return JsonbDecodeError.InvalidJsonb,
    }
}

/// Convert JsonbValue to JSON string for SQLite
pub fn jsonbToString(allocator: std.mem.Allocator, value: JsonbValue) ![]u8 {
    var list: std.ArrayList(u8) = .empty;
    errdefer list.deinit(allocator);

    try writeJson(allocator, &list, value);

    return list.toOwnedSlice(allocator);
}

fn writeJson(allocator: std.mem.Allocator, list: *std.ArrayList(u8), value: JsonbValue) !void {
    switch (value) {
        .null => try list.appendSlice(allocator, "null"),
        .bool => |b| try list.appendSlice(allocator, if (b) "true" else "false"),
        .number => |n| try list.appendSlice(allocator, n),
        .string => |s| {
            try list.append(allocator, '"');
            for (s) |c| {
                switch (c) {
                    '"' => try list.appendSlice(allocator, "\\\""),
                    '\\' => try list.appendSlice(allocator, "\\\\"),
                    0x08 => try list.appendSlice(allocator, "\\b"),
                    0x09 => try list.appendSlice(allocator, "\\t"),
                    0x0A => try list.appendSlice(allocator, "\\n"),
                    0x0C => try list.appendSlice(allocator, "\\f"),
                    0x0D => try list.appendSlice(allocator, "\\r"),
                    0x00...0x07, 0x0B, 0x0E...0x1F => |ctrl| {
                        const hex = "0123456789abcdef";
                        try list.appendSlice(allocator, "\\u00");
                        try list.append(allocator, hex[ctrl >> 4]);
                        try list.append(allocator, hex[ctrl & 0xF]);
                    },
                    else => try list.append(allocator, c),
                }
            }
            try list.append(allocator, '"');
        },
        .array => |arr| {
            try list.append(allocator, '[');
            for (arr, 0..) |item, i| {
                if (i > 0) try list.append(allocator, ',');
                try writeJson(allocator, list, item);
            }
            try list.append(allocator, ']');
        },
        .object => |obj| {
            try list.append(allocator, '{');
            for (obj, 0..) |kv, i| {
                if (i > 0) try list.append(allocator, ',');
                try list.append(allocator, '"');
                try list.appendSlice(allocator, kv.key);
                try list.append(allocator, '"');
                try list.append(allocator, ':');
                try writeJson(allocator, list, kv.value);
            }
            try list.append(allocator, '}');
        },
    }
}

test "jsonbToString simple object" {
    const alloc = std.testing.allocator;

    // Manually construct a JsonbValue for testing the serializer
    var kvs = try alloc.alloc(JsonbKeyValue, 2);
    kvs[0] = .{ .key = try alloc.dupe(u8, "name"), .value = .{ .string = try alloc.dupe(u8, "Alice") } };
    kvs[1] = .{ .key = try alloc.dupe(u8, "age"), .value = .{ .number = try alloc.dupe(u8, "30") } };

    const value = JsonbValue{ .object = kvs };
    defer value.deinit(alloc);

    const json = try jsonbToString(alloc, value);
    defer alloc.free(json);

    try std.testing.expectEqualStrings("{\"name\":\"Alice\",\"age\":30}", json);
}

test "parseJsonb - simple object" {
    const alloc = std.testing.allocator;

    // {"a": 1}
    // Let's build this manually:
    //
    // [version: 1]
    // [container header: 4 bytes] - object with 1 key
    // [key JEntry: 4 bytes] - string "a" length 1
    // [value JEntry: 4 bytes] - numeric
    // [key data: "a"]
    // [value data: numeric 1]

    // Actually, let's get real bytes from PostgreSQL first.
    // Run this in psql:
    //   SELECT encode('{"a":1}'::jsonb::bytea, 'hex');
    //
    // Or simpler - test the JSON serializer first with constructed values:

    var kvs = try alloc.alloc(JsonbKeyValue, 1);
    kvs[0] = .{
        .key = try alloc.dupe(u8, "a"),
        .value = .{ .number = try alloc.dupe(u8, "1") },
    };

    const value = JsonbValue{ .object = kvs };
    defer value.deinit(alloc);

    const json = try jsonbToString(alloc, value);
    defer alloc.free(json);

    try std.testing.expectEqualStrings("{\"a\":1}", json);
}

test "parseJsonb - array" {
    const alloc = std.testing.allocator;

    var elements = try alloc.alloc(JsonbValue, 3);
    elements[0] = .{ .number = try alloc.dupe(u8, "1") };
    elements[1] = .{ .number = try alloc.dupe(u8, "2") };
    elements[2] = .{ .number = try alloc.dupe(u8, "3") };

    const value = JsonbValue{ .array = elements };
    defer value.deinit(alloc);

    const json = try jsonbToString(alloc, value);
    defer alloc.free(json);

    try std.testing.expectEqualStrings("[1,2,3]", json);
}

test "parseJsonb - nested" {
    const alloc = std.testing.allocator;

    // {"name": "Alice", "scores": [10, 20], "active": true}

    // Build scores array
    var scores = try alloc.alloc(JsonbValue, 2);
    scores[0] = .{ .number = try alloc.dupe(u8, "10") };
    scores[1] = .{ .number = try alloc.dupe(u8, "20") };

    // Build object
    var kvs = try alloc.alloc(JsonbKeyValue, 3);
    kvs[0] = .{ .key = try alloc.dupe(u8, "name"), .value = .{ .string = try alloc.dupe(u8, "Alice") } };
    kvs[1] = .{ .key = try alloc.dupe(u8, "scores"), .value = .{ .array = scores } };
    kvs[2] = .{ .key = try alloc.dupe(u8, "active"), .value = .{ .bool = true } };

    const value = JsonbValue{ .object = kvs };
    defer value.deinit(alloc);

    const json = try jsonbToString(alloc, value);
    defer alloc.free(json);

    try std.testing.expectEqualStrings("{\"name\":\"Alice\",\"scores\":[10,20],\"active\":true}", json);
}

test "jsonbToString - all types" {
    const alloc = std.testing.allocator;

    // Test null
    {
        const json = try jsonbToString(alloc, .null);
        defer alloc.free(json);
        try std.testing.expectEqualStrings("null", json);
    }

    // Test bool
    {
        const json = try jsonbToString(alloc, .{ .bool = true });
        defer alloc.free(json);
        try std.testing.expectEqualStrings("true", json);
    }

    // Test number
    {
        const nb = try alloc.dupe(u8, "42.5");
        const value = JsonbValue{ .number = nb };
        defer value.deinit(alloc);

        const json = try jsonbToString(alloc, value);
        defer alloc.free(json);
        try std.testing.expectEqualStrings("42.5", json);
    }

    // Test string with escapes
    {
        const str = try alloc.dupe(u8, "hello\nworld");
        const value = JsonbValue{ .string = str };
        defer value.deinit(alloc);

        const json = try jsonbToString(alloc, value);
        defer alloc.free(json);
        try std.testing.expectEqualStrings("\"hello\\nworld\"", json);
    }
}
