const std = @import("std");

pub const ArrayDecodeError = error{
    Truncated,
    InvalidArray,
    OutOfMemory,
    UnsupportedDimensions,
};

pub const ArrayElement = union(enum) {
    null_value,
    data: []const u8,
};

pub const ArrayResult = struct {
    element_oid: u32,
    ndim: usize,
    dimensions: []i32, // length of each dimension
    elements: []ArrayElement, // flattened elements
    allocated_bytes: []u8,

    pub fn deinit(self: ArrayResult, allocator: std.mem.Allocator) void {
        allocator.free(self.dimensions);
        allocator.free(self.elements);
        allocator.free(self.allocated_bytes);
    }

    /// Get total number of elements (product of all dimensions)
    pub fn totalElements(self: ArrayResult) usize {
        if (self.ndim == 0) return 0;
        var total: usize = 1;
        for (self.dimensions) |d| {
            total *= @intCast(d);
        }
        return total;
    }
};

pub fn parseArray(allocator: std.mem.Allocator, buf: []const u8) ArrayDecodeError!ArrayResult {
    if (buf.len < 12) return ArrayDecodeError.Truncated;

    var pos: usize = 0;

    const ndim = std.mem.readInt(i32, buf[pos..][0..4], .big);
    pos += 4;

    if (ndim < 0) return ArrayDecodeError.InvalidArray;

    // flags - skip
    pos += 4;

    const element_oid = std.mem.readInt(u32, buf[pos..][0..4], .big);
    pos += 4;

    const ndim_usize: usize = @intCast(ndim);

    // Handle empty array
    if (ndim == 0) {
        return .{
            .element_oid = element_oid,
            .ndim = 0,
            .dimensions = try allocator.alloc(i32, 0),
            .elements = try allocator.alloc(ArrayElement, 0),
            .allocated_bytes = try allocator.dupe(u8, buf),
        };
    }

    // Read dimensions
    if (buf.len < pos + ndim_usize * 8) return ArrayDecodeError.Truncated;

    var dimensions = try allocator.alloc(i32, ndim_usize);
    errdefer allocator.free(dimensions);

    var num_elements: usize = 1;
    for (0..ndim_usize) |i| {
        const dim_len = std.mem.readInt(i32, buf[pos..][0..4], .big);
        pos += 4;
        pos += 4; // skip lower bound

        if (dim_len < 0) return ArrayDecodeError.InvalidArray;
        dimensions[i] = dim_len;
        num_elements *= @intCast(dim_len);
    }

    var elements = try allocator.alloc(ArrayElement, num_elements);
    errdefer allocator.free(elements);

    const allocated = try allocator.dupe(u8, buf);
    errdefer allocator.free(allocated);

    for (0..num_elements) |i| {
        if (pos + 4 > buf.len) return ArrayDecodeError.Truncated;

        const elem_len = std.mem.readInt(i32, buf[pos..][0..4], .big);
        pos += 4;

        if (elem_len == -1) {
            elements[i] = .null_value;
        } else {
            const len: usize = @intCast(elem_len);
            if (pos + len > buf.len) return ArrayDecodeError.Truncated;

            elements[i] = .{ .data = allocated[pos .. pos + len] };
            pos += len;
        }
    }

    return .{
        .element_oid = element_oid,
        .ndim = ndim_usize,
        .dimensions = dimensions,
        .elements = elements,
        .allocated_bytes = allocated,
    };
}

fn makeArrayBuf(comptime T: type, element_oid: u32, values: []const ?T) ![]u8 {
    // Helper for tests - calculates size and builds buffer
    var size: usize = 20; // header: ndim(4) + flags(4) + oid(4) + dim_len(4) + lower(4)
    for (values) |v| {
        size += 4; // length field
        if (v != null) size += @sizeOf(T);
    }

    var buf = try std.testing.allocator.alloc(u8, size);
    var pos: usize = 0;

    // ndim = 1
    std.mem.writeInt(i32, buf[pos..][0..4], 1, .big);
    pos += 4;
    // flags = 0
    std.mem.writeInt(i32, buf[pos..][0..4], 0, .big);
    pos += 4;
    // element_oid
    std.mem.writeInt(u32, buf[pos..][0..4], element_oid, .big);
    pos += 4;
    // dim_len
    std.mem.writeInt(i32, buf[pos..][0..4], @intCast(values.len), .big);
    pos += 4;
    // lower bound = 1
    std.mem.writeInt(i32, buf[pos..][0..4], 1, .big);
    pos += 4;

    // elements
    for (values) |v| {
        if (v) |val| {
            std.mem.writeInt(i32, buf[pos..][0..4], @sizeOf(T), .big);
            pos += 4;
            std.mem.writeInt(T, buf[pos..][0..@sizeOf(T)], val, .big);
            pos += @sizeOf(T);
        } else {
            std.mem.writeInt(i32, buf[pos..][0..4], -1, .big); // NULL
            pos += 4;
        }
    }

    return buf;
}

test "parseArray - int4 array" {
    const alloc = std.testing.allocator;

    // {10, 20, NULL, 40}
    const buf = try makeArrayBuf(i32, 23, &[_]?i32{ 10, 20, null, 40 });
    defer alloc.free(buf);

    const result = try parseArray(alloc, buf);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 23), result.element_oid);
    try std.testing.expectEqual(@as(usize, 4), result.elements.len);

    // Element 0: 10
    try std.testing.expectEqual(@as(i32, 10), std.mem.readInt(i32, result.elements[0].data[0..4], .big));
    // Element 1: 20
    try std.testing.expectEqual(@as(i32, 20), std.mem.readInt(i32, result.elements[1].data[0..4], .big));
    // Element 2: NULL
    try std.testing.expectEqual(ArrayElement.null_value, result.elements[2]);
    // Element 3: 40
    try std.testing.expectEqual(@as(i32, 40), std.mem.readInt(i32, result.elements[3].data[0..4], .big));
}

test "parseArray - empty array" {
    const alloc = std.testing.allocator;

    // Empty array: ndim=0
    const buf = [_]u8{
        0, 0, 0, 0, // ndim = 0
        0, 0, 0, 0, // flags
        0, 0, 0, 23, // element_oid = 23 (int4)
    };

    const result = try parseArray(alloc, &buf);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 0), result.elements.len);
}

// {"a", "bc", "cde f"}
test "parseArray - text array" {
    const alloc = std.testing.allocator;

    const buf = [_]u8{
        0, 0, 0, 1, // ndim = 1
        0, 0, 0, 0, // flags = 0
        0, 0, 0, 25, // element_oid = TEXT
        0, 0, 0, 3, // dim1_len = 3
        0, 0, 0, 1, // dim1_lower = 1
        // elements:
        0, 0, 0, 1, 'a', // len=1, "a"
        0, 0, 0, 2, 'b', 'c', // len=2, "bc"
        0, 0, 0, 5, 'c', 'd', 'e', ' ', 'f', // len=5, "cde f"
    };

    const result = try parseArray(alloc, &buf);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 25), result.element_oid);
    try std.testing.expectEqual(@as(usize, 3), result.elements.len);

    try std.testing.expectEqualStrings("a", result.elements[0].data);
    try std.testing.expectEqualStrings("bc", result.elements[1].data);
    try std.testing.expectEqualStrings("cde f", result.elements[2].data);
}

// [[0,1], [2,3], [4,5]]
test "parseArray - 2D int array" {
    const alloc = std.testing.allocator;

    // [[0,1], [2,3], [4,5]]
    const buf = [_]u8{
        0, 0, 0, 2, // ndim = 2
        0, 0, 0, 0, // flags = 0
        0, 0, 0, 23, // element_oid = INT4
        0, 0, 0, 3, // dim1_len = 3
        0, 0, 0, 1, // dim1_lower = 1
        0, 0, 0, 2, // dim2_len = 2
        0, 0, 0, 1, // dim2_lower = 1
        // elements (6 total, flattened):
        0, 0, 0, 4, 0, 0, 0, 0, // len=4, value=0
        0, 0, 0, 4, 0, 0, 0, 1, // len=4, value=1
        0, 0, 0, 4, 0, 0, 0, 2, // len=4, value=2
        0, 0, 0, 4, 0, 0, 0, 3, // len=4, value=3
        0, 0, 0, 4, 0, 0, 0, 4, // len=4, value=4
        0, 0, 0, 4, 0, 0, 0, 5, // len=4, value=5
    };

    const result = try parseArray(alloc, &buf);
    defer result.deinit(alloc);

    // Verify structure
    try std.testing.expectEqual(@as(u32, 23), result.element_oid);
    try std.testing.expectEqual(@as(usize, 2), result.ndim);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 3, 2 }, result.dimensions);
    try std.testing.expectEqual(@as(usize, 6), result.elements.len);

    // Verify values [0, 1, 2, 3, 4, 5]
    for (0..6) |i| {
        const val = std.mem.readInt(i32, result.elements[i].data[0..4], .big);
        try std.testing.expectEqual(@as(i32, @intCast(i)), val);
    }
}
