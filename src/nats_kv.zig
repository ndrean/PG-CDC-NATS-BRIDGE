//! NATS Key-Value Store wrapper
//!
//! Provides a simple interface to NATS JetStream-based KV buckets.
//!
//! It is used for storing schema definitions.

const std = @import("std");
const c_imports = @import("c_imports.zig");
const c = c_imports.c;

pub const log = std.log.scoped(.nats_kv);

/// Basic KV Store handler with operations:
/// - `open`: returns a handler on a bucket,
/// - `put` k/v into a bucket,
/// - `get` k/v from a bucket,
/// - `delete` from a bucket.
/// - `deinit` to clean up the handler.
pub const KVStore = struct {
    handle: ?*anyopaque,
    allocator: std.mem.Allocator,

    /// Get a KV store handle by bucket name
    pub fn open(
        js_ctx: anytype,
        bucket_name: [:0]const u8,
        allocator: std.mem.Allocator,
    ) !KVStore {
        var kv: ?*anyopaque = null;
        const status = c.js_KeyValue(@ptrCast(&kv), @ptrCast(js_ctx), bucket_name.ptr);

        if (status != c.NATS_OK) {
            log.err("🔴 Failed to open KV bucket '{s}': {s}", .{
                bucket_name,
                std.mem.span(c.natsStatus_GetText(status)),
            });
            return error.KVOpenFailed;
        }

        return .{
            .handle = kv,
            .allocator = allocator,
        };
    }

    /// Put binary data into KV store
    pub fn put(self: *KVStore, key: [:0]const u8, value: []const u8) !u64 {
        var revision: u64 = 0;
        const status = c.kvStore_Put(
            &revision,
            @ptrCast(self.handle),
            key.ptr,
            value.ptr,
            @intCast(value.len),
        );

        if (status != c.NATS_OK) {
            log.err("🔴 Failed to put key '{s}': {s}", .{
                key,
                std.mem.span(c.natsStatus_GetText(status)),
            });
            return error.KVPutFailed;
        }

        log.debug("KV PUT: {s} → {d} bytes (revision={d})", .{ key, value.len, revision });
        return revision;
    }

    /// Get binary data from KV store
    ///
    /// Caller is responsible for freeing the returned memory.
    pub fn get(self: *KVStore, key: [:0]const u8) !?[]const u8 {
        var entry: ?*anyopaque = null;
        const status = c.kvStore_Get(
            @ptrCast(&entry),
            @ptrCast(self.handle),
            key.ptr,
        );

        if (status == c.NATS_NOT_FOUND) {
            return null;
        }

        if (status != c.NATS_OK) {
            log.err("🔴 Failed to get key '{s}': {s}", .{
                key,
                std.mem.span(c.natsStatus_GetText(status)),
            });
            return error.KVGetFailed;
        }

        defer c.kvEntry_Destroy(@ptrCast(entry));

        // Get value data (kvEntry_Value returns pointer directly)
        const value_ptr: [*c]const u8 = @ptrCast(c.kvEntry_Value(@ptrCast(entry)));
        if (value_ptr == null) {
            return error.KVValueFailed;
        }

        // Get value length
        const value_len = c.kvEntry_ValueLen(@ptrCast(entry));
        if (value_len <= 0) {
            return null; // Empty value
        }

        // Copy the value (caller owns the memory)
        const value_slice = value_ptr[0..@intCast(value_len)];
        return try self.allocator.dupe(u8, value_slice);
    }

    /// Delete a key from KV store
    pub fn delete(self: *KVStore, key: [:0]const u8) !void {
        const status = c.kvStore_Delete(
            @ptrCast(self.handle),
            key.ptr,
        );

        if (status != c.NATS_OK and status != c.NATS_NOT_FOUND) {
            log.err("🔴 Failed to delete key '{s}': {s}", .{
                key,
                std.mem.span(c.natsStatus_GetText(status)),
            });
            return error.KVDeleteFailed;
        }
    }

    /// Destroy the KV store handle
    pub fn deinit(self: *KVStore) void {
        c.kvStore_Destroy(@ptrCast(self.handle));
    }
};
