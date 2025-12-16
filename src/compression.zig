//! Per-table zstd dictionary compression for snapshots
//!
//! This module manages per-table compression dictionaries for optimal
//! snapshot compression. Uses the zstd library's built-in dictionary support.
//!
//! ## Usage:
//!
//! ```zig
//! var dict_mgr = DictionaryManager.init(allocator);
//! defer dict_mgr.deinit();
//!
//! // Compress snapshot chunk
//! const compressed = try dict_mgr.compressSnapshot(
//!     "users",
//!     msgpack_data,
//!     allocator,
//! );
//! defer allocator.free(compressed);
//!
//! // Decompress snapshot chunk
//! const decompressed = try dict_mgr.decompressSnapshot(
//!     "users",
//!     compressed,
//!     allocator,
//! );
//! defer allocator.free(decompressed);
//! ```

const std = @import("std");
const zstd = @import("zstd");
const config = @import("config.zig");

pub const log = std.log.scoped(.compression);

/// Dictionary metadata for tracking and logging
pub const DictMetadata = struct {
    table_name: []const u8,
    size_bytes: usize,
    loaded_at: i64, // unix timestamp
};

/// Per-table dictionary compression manager
pub const DictionaryManager = struct {
    allocator: std.mem.Allocator,

    // Compression contexts (one per table for thread safety)
    compression_contexts: std.StringHashMap(*zstd.ZSTD_CCtx),
    decompression_contexts: std.StringHashMap(*zstd.ZSTD_DCtx),

    // Dictionary data (loaded from embedded files or disk)
    dictionaries: std.StringHashMap([]const u8),

    // Metadata for observability
    metadata: std.StringHashMap(DictMetadata),

    // Mutex for thread-safe dictionary loading
    mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator) DictionaryManager {
        return .{
            .allocator = allocator,
            .compression_contexts = std.StringHashMap(*zstd.ZSTD_CCtx).init(allocator),
            .decompression_contexts = std.StringHashMap(*zstd.ZSTD_DCtx).init(allocator),
            .dictionaries = std.StringHashMap([]const u8).init(allocator),
            .metadata = std.StringHashMap(DictMetadata).init(allocator),
            .mutex = .{},
        };
    }

    pub fn deinit(self: *DictionaryManager) void {
        // Free compression contexts
        var cctx_it = self.compression_contexts.iterator();
        while (cctx_it.next()) |entry| {
            _ = zstd.free_compressor(entry.value_ptr.*);
        }
        self.compression_contexts.deinit();

        // Free decompression contexts
        var dctx_it = self.decompression_contexts.iterator();
        while (dctx_it.next()) |entry| {
            _ = zstd.free_decompressor(entry.value_ptr.*);
        }
        self.decompression_contexts.deinit();

        // Free dictionary data
        var dict_it = self.dictionaries.iterator();
        while (dict_it.next()) |entry| {
            self.allocator.free(entry.value_ptr.*);
        }
        self.dictionaries.deinit();

        // Free metadata
        self.metadata.deinit();
    }

    /// Load dictionary for table (from disk or embedded)
    /// Thread-safe with mutex protection
    fn ensureDictionaryLoaded(self: *DictionaryManager, table_name: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Check if already loaded
        if (self.dictionaries.contains(table_name)) {
            return;
        }

        // Try to load dictionary from disk
        // Path: ./dictionaries/{table_name}.zstd.dict
        const dict_path = try std.fmt.allocPrint(
            self.allocator,
            "dictionaries/{s}.zstd.dict",
            .{table_name},
        );
        defer self.allocator.free(dict_path);

        const dict_bytes = std.fs.cwd().readFileAlloc(
            self.allocator,
            dict_path,
            1024 * 1024, // 1MB max dictionary size
        ) catch |err| {
            log.warn("Failed to load dictionary for table '{s}': {any}, using no dictionary", .{ table_name, err });
            // Store empty dictionary to mark as "attempted"
            try self.dictionaries.put(table_name, &[_]u8{});
            return;
        };

        try self.dictionaries.put(table_name, dict_bytes);

        // Store metadata
        const metadata = DictMetadata{
            .table_name = table_name,
            .size_bytes = dict_bytes.len,
            .loaded_at = std.time.timestamp(),
        };
        try self.metadata.put(table_name, metadata);

        log.info("Loaded zstd dictionary for table '{s}' ({d} bytes)", .{ table_name, dict_bytes.len });
    }

    /// Get or create compression context for table
    fn getCompressionContext(self: *DictionaryManager, table_name: []const u8) !*zstd.ZSTD_CCtx {
        // Check if context exists
        if (self.compression_contexts.get(table_name)) |ctx| {
            // Reset session for reuse
            try zstd.reset_compressor_session(ctx);
            return ctx;
        }

        // Create new context with optimized settings for MessagePack data
        // MessagePack is structured binary data, similar to JSON
        const ctx = try zstd.init_compressor(.{
            .recipe = .structured_data, // level 9, btultra strategy
        });

        // Load dictionary into context if available
        try self.ensureDictionaryLoaded(table_name);
        const dict = self.dictionaries.get(table_name).?;
        if (dict.len > 0) {
            try zstd.load_compression_dictionary(ctx, dict);
        }

        try self.compression_contexts.put(table_name, ctx);
        return ctx;
    }

    /// Get or create decompression context for table
    fn getDecompressionContext(self: *DictionaryManager, table_name: []const u8) !*zstd.ZSTD_DCtx {
        // Check if context exists
        if (self.decompression_contexts.get(table_name)) |ctx| {
            // Reset session for reuse
            try zstd.reset_decompressor_session(ctx);
            return ctx;
        }

        // Create new context
        const ctx = try zstd.init_decompressor(.{});

        // Load dictionary into context if available
        try self.ensureDictionaryLoaded(table_name);
        const dict = self.dictionaries.get(table_name).?;
        if (dict.len > 0) {
            try zstd.load_decompression_dictionary(ctx, dict);
        }

        try self.decompression_contexts.put(table_name, ctx);
        return ctx;
    }

    /// Compress snapshot chunk with per-table dictionary
    ///
    /// Automatically loads and caches dictionary for the table.
    /// Returns compressed data that caller must free.
    pub fn compressSnapshot(
        self: *DictionaryManager,
        table_name: []const u8,
        data: []const u8,
        allocator: std.mem.Allocator,
    ) ![]const u8 {
        const ctx = try self.getCompressionContext(table_name);
        return try zstd.compress(allocator, ctx, data);
    }

    /// Decompress snapshot chunk with per-table dictionary
    ///
    /// Automatically loads and caches dictionary for the table.
    /// Returns decompressed data that caller must free.
    pub fn decompressSnapshot(
        self: *DictionaryManager,
        table_name: []const u8,
        compressed: []const u8,
        allocator: std.mem.Allocator,
    ) ![]const u8 {
        const ctx = try self.getDecompressionContext(table_name);
        return try zstd.decompress(allocator, ctx, compressed);
    }

    /// Get compression statistics for observability
    pub fn getStats(self: *DictionaryManager) CompressionStats {
        return .{
            .dictionaries_loaded = self.dictionaries.count(),
            .compression_contexts = self.compression_contexts.count(),
            .decompression_contexts = self.decompression_contexts.count(),
        };
    }
};

pub const CompressionStats = struct {
    dictionaries_loaded: usize,
    compression_contexts: usize,
    decompression_contexts: usize,
};

// Tests
test "compression: compress and decompress without dictionary" {
    const allocator = std.testing.allocator;

    var dict_mgr = DictionaryManager.init(allocator);
    defer dict_mgr.deinit();

    const original = "Hello, World! This is test data.";

    const compressed = try dict_mgr.compressSnapshot("test_table", original, allocator);
    defer allocator.free(compressed);

    try std.testing.expect(compressed.len < original.len);

    const decompressed = try dict_mgr.decompressSnapshot("test_table", compressed, allocator);
    defer allocator.free(decompressed);

    try std.testing.expectEqualStrings(original, decompressed);
}

test "compression: context reuse" {
    const allocator = std.testing.allocator;

    var dict_mgr = DictionaryManager.init(allocator);
    defer dict_mgr.deinit();

    const data1 = "First chunk of data";
    const data2 = "Second chunk of data";

    // First compression creates context
    const compressed1 = try dict_mgr.compressSnapshot("users", data1, allocator);
    defer allocator.free(compressed1);

    // Second compression reuses context
    const compressed2 = try dict_mgr.compressSnapshot("users", data2, allocator);
    defer allocator.free(compressed2);

    // Verify both decompress correctly
    const decompressed1 = try dict_mgr.decompressSnapshot("users", compressed1, allocator);
    defer allocator.free(decompressed1);
    try std.testing.expectEqualStrings(data1, decompressed1);

    const decompressed2 = try dict_mgr.decompressSnapshot("users", compressed2, allocator);
    defer allocator.free(decompressed2);
    try std.testing.expectEqualStrings(data2, decompressed2);
}

test "compression: stats" {
    const allocator = std.testing.allocator;

    var dict_mgr = DictionaryManager.init(allocator);
    defer dict_mgr.deinit();

    const data = "test";

    _ = try dict_mgr.compressSnapshot("table1", data, allocator);
    _ = try dict_mgr.compressSnapshot("table2", data, allocator);

    const stats = dict_mgr.getStats();
    try std.testing.expectEqual(@as(usize, 2), stats.compression_contexts);
}
