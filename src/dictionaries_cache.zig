//! Dictionary caching for zstd compression
//!
//! This module manages in-memory caching of zstd dictionaries fetched from NATS KV.
//! Each table has a versioned dictionary identifier (e.g., dict_users_001) that allows
//! retraining dictionaries without breaking existing snapshots in flight.
//!
//! Thread safety: Not thread-safe. Caller must synchronize access if used from multiple threads.

const std = @import("std");

pub const log = std.log.scoped(.dictionaries_cache);

/// Dictionary data with metadata
pub const DictionaryEntry = struct {
    /// Dictionary identifier (e.g., "dict_users_001")
    dict_id: []const u8,
    /// Binary dictionary data for zstd compression
    data: []const u8,

    pub fn deinit(self: *DictionaryEntry, allocator: std.mem.Allocator) void {
        allocator.free(self.dict_id);
        allocator.free(self.data);
    }
};

/// Dictionary cache for in-memory storage
///
/// Maps table_name -> DictionaryEntry
/// Dictionaries are fetched from NATS KV on bridge startup and cached for the lifetime
/// of the bridge process.
pub const DictionariesCache = struct {
    allocator: std.mem.Allocator,
    cache: std.StringHashMap(DictionaryEntry),

    /// Initialize an empty dictionaries cache
    pub fn init(allocator: std.mem.Allocator) DictionariesCache {
        return .{
            .allocator = allocator,
            .cache = std.StringHashMap(DictionaryEntry).init(allocator),
        };
    }

    /// Free all resources
    pub fn deinit(self: *DictionariesCache) void {
        // Free all entries (keys and values)
        var it = self.cache.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*); // Free table_name key
            var dict_entry = entry.value_ptr;
            dict_entry.deinit(self.allocator); // Free dict_id and data
        }
        self.cache.deinit();
    }

    /// Store a dictionary in the cache
    ///
    /// Args:
    ///   table_name: Name of the table this dictionary is for
    ///   dict_id: Dictionary identifier (e.g., "dict_users_001")
    ///   dict_data: Binary dictionary data
    ///
    /// Returns:
    ///   Error if allocation fails
    pub fn put(
        self: *DictionariesCache,
        table_name: []const u8,
        dict_id: []const u8,
        dict_data: []const u8,
    ) !void {
        // Check if entry already exists
        const existing = self.cache.getPtr(table_name);

        if (existing) |old_entry_ptr| {
            // Update existing entry in place
            var old_entry = old_entry_ptr.*;
            old_entry.deinit(self.allocator); // Free old dict_id and data

            // Create new owned copies
            const owned_dict_id = try self.allocator.dupe(u8, dict_id);
            errdefer self.allocator.free(owned_dict_id);

            const owned_dict_data = try self.allocator.dupe(u8, dict_data);
            errdefer self.allocator.free(owned_dict_data);

            // Update the entry in place (keep the old key)
            old_entry_ptr.* = DictionaryEntry{
                .dict_id = owned_dict_id,
                .data = owned_dict_data,
            };
        } else {
            // Create new entry
            const owned_table_name = try self.allocator.dupe(u8, table_name);
            errdefer self.allocator.free(owned_table_name);

            const owned_dict_id = try self.allocator.dupe(u8, dict_id);
            errdefer self.allocator.free(owned_dict_id);

            const owned_dict_data = try self.allocator.dupe(u8, dict_data);
            errdefer self.allocator.free(owned_dict_data);

            const entry = DictionaryEntry{
                .dict_id = owned_dict_id,
                .data = owned_dict_data,
            };

            try self.cache.put(owned_table_name, entry);
        }

        log.info("📚 Cached dictionary for table '{s}' (id={s}, size={d} bytes)", .{
            table_name,
            dict_id,
            dict_data.len,
        });
    }

    /// Get a dictionary entry for a table
    ///
    /// Returns:
    ///   Dictionary entry if found, null otherwise
    pub fn get(self: *const DictionariesCache, table_name: []const u8) ?DictionaryEntry {
        return self.cache.get(table_name);
    }

    /// Get just the dictionary ID for a table (without the data)
    ///
    /// Returns:
    ///   Dictionary ID if found, null otherwise
    pub fn getDictId(self: *const DictionariesCache, table_name: []const u8) ?[]const u8 {
        if (self.cache.get(table_name)) |entry| {
            return entry.dict_id;
        }
        return null;
    }

    /// Get just the dictionary data for a table (without the ID)
    ///
    /// Returns:
    ///   Dictionary data if found, null otherwise
    pub fn getDictData(self: *const DictionariesCache, table_name: []const u8) ?[]const u8 {
        if (self.cache.get(table_name)) |entry| {
            return entry.data;
        }
        return null;
    }

    /// Check if a dictionary exists for a table
    pub fn has(self: *const DictionariesCache, table_name: []const u8) bool {
        return self.cache.contains(table_name);
    }

    /// Get the number of cached dictionaries
    pub fn count(self: *const DictionariesCache) usize {
        return self.cache.count();
    }

    /// Clear all cached dictionaries
    pub fn clear(self: *DictionariesCache) void {
        var it = self.cache.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            var dict_entry = entry.value_ptr;
            dict_entry.deinit(self.allocator);
        }
        self.cache.clearRetainingCapacity();
    }
};

// Tests
test "DictionariesCache - basic operations" {
    const allocator = std.testing.allocator;
    var cache = DictionariesCache.init(allocator);
    defer cache.deinit();

    // Add a dictionary
    const dict_data = "fake_dict_data_binary_blob";
    try cache.put("users", "dict_users_001", dict_data);

    try std.testing.expect(cache.count() == 1);
    try std.testing.expect(cache.has("users") == true);

    const entry = cache.get("users").?;
    try std.testing.expectEqualStrings("dict_users_001", entry.dict_id);
    try std.testing.expectEqualStrings(dict_data, entry.data);

    // Test getDictId and getDictData
    try std.testing.expectEqualStrings("dict_users_001", cache.getDictId("users").?);
    try std.testing.expectEqualStrings(dict_data, cache.getDictData("users").?);

    // Non-existent table
    try std.testing.expect(cache.has("orders") == false);
    try std.testing.expect(cache.get("orders") == null);
    try std.testing.expect(cache.getDictId("orders") == null);
}

test "DictionariesCache - update dictionary" {
    const allocator = std.testing.allocator;
    var cache = DictionariesCache.init(allocator);
    defer cache.deinit();

    // Add initial dictionary
    try cache.put("users", "dict_users_001", "data_v1");
    try std.testing.expectEqualStrings("dict_users_001", cache.getDictId("users").?);

    // Update with new version
    try cache.put("users", "dict_users_002", "data_v2");
    try std.testing.expect(cache.count() == 1); // Still only one entry
    try std.testing.expectEqualStrings("dict_users_002", cache.getDictId("users").?);
    try std.testing.expectEqualStrings("data_v2", cache.getDictData("users").?);
}

test "DictionariesCache - multiple tables" {
    const allocator = std.testing.allocator;
    var cache = DictionariesCache.init(allocator);
    defer cache.deinit();

    try cache.put("users", "dict_users_001", "users_dict_data");
    try cache.put("orders", "dict_orders_001", "orders_dict_data");
    try cache.put("products", "dict_products_001", "products_dict_data");

    try std.testing.expect(cache.count() == 3);
    try std.testing.expectEqualStrings("dict_users_001", cache.getDictId("users").?);
    try std.testing.expectEqualStrings("dict_orders_001", cache.getDictId("orders").?);
    try std.testing.expectEqualStrings("dict_products_001", cache.getDictId("products").?);
}

test "DictionariesCache - clear" {
    const allocator = std.testing.allocator;
    var cache = DictionariesCache.init(allocator);
    defer cache.deinit();

    try cache.put("users", "dict_users_001", "data1");
    try cache.put("orders", "dict_orders_001", "data2");
    try std.testing.expect(cache.count() == 2);

    cache.clear();
    try std.testing.expect(cache.count() == 0);
    try std.testing.expect(cache.has("users") == false);
    try std.testing.expect(cache.has("orders") == false);
}
