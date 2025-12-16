//! Bindings and wrappers for Zstandard (zstd) compression library.
//!
//! Zstandard is a fast lossless compression algorithm, targeting real-time compression scenarios
//! at zlib-level and better compression ratios.
//!
//! This module provides a lightweight wrapper around system libzstd.
//!
//! ## Quick Start
//!
//! ```zig
//! // Initialize contexts
//! const cctx = try init_compressor(.{ .recipe = .structured_data });
//! defer _ = free_compressor(cctx);
//!
//! const dctx = try init_decompressor(.{});
//! defer _ = free_decompressor(dctx);
//!
//! // Compress and decompress
//! const compressed = try compress(allocator, cctx, data);
//! defer allocator.free(compressed);
//!
//! const decompressed = try decompress(allocator, dctx, compressed);
//! defer allocator.free(decompressed);
//! ```
const std = @import("std");

pub const log = std.log.scoped(.zstd);

// C API bindings
extern "c" fn ZSTD_isError(code: usize) c_uint;
extern "c" fn ZSTD_getErrorName(code: usize) [*c]const u8;
extern "c" fn ZSTD_versionString() [*c]const u8;
extern "c" fn ZSTD_getFrameContentSize(src: *const anyopaque, srcSize: usize) u64;
extern "c" fn ZSTD_compressBound(srcSize: usize) usize;

pub const ZSTD_CCtx = opaque {};
pub const ZSTD_DCtx = opaque {};

extern "c" fn ZSTD_createCCtx() ?*ZSTD_CCtx;
extern "c" fn ZSTD_freeCCtx(cctx: *ZSTD_CCtx) usize;
extern "c" fn ZSTD_createDCtx() ?*ZSTD_DCtx;
extern "c" fn ZSTD_freeDCtx(dctx: *ZSTD_DCtx) usize;

extern "c" fn ZSTD_compress2(
    cctx: *ZSTD_CCtx,
    dst: [*]u8,
    dstCapacity: usize,
    src: [*]const u8,
    srcSize: usize,
) usize;

extern "c" fn ZSTD_decompressDCtx(
    dctx: *ZSTD_DCtx,
    dst: [*]u8,
    dstCapacity: usize,
    src: [*]const u8,
    srcSize: usize,
) usize;

extern "c" fn ZSTD_minCLevel() c_int;
extern "c" fn ZSTD_maxCLevel() c_int;

pub fn version() []const u8 {
    return std.mem.span(ZSTD_versionString());
}

const ZSTD_CONTENTSIZE_UNKNOWN: u64 = std.math.maxInt(u64);
const ZSTD_CONTENTSIZE_ERROR: u64 = std.math.maxInt(u64) - 1;

pub fn get_decompressed_size(compressed: []const u8) !usize {
    const size = ZSTD_getFrameContentSize(compressed.ptr, compressed.len);
    if (size == ZSTD_CONTENTSIZE_ERROR) {
        return error.NotZstdFormat;
    }
    if (size == ZSTD_CONTENTSIZE_UNKNOWN) {
        return error.SizeUnknown;
    }
    return @intCast(size);
}

// Context parameters
const ZSTD_cParameter = enum(i16) {
    ZSTD_c_compressionLevel = 100,
    ZSTD_c_windowLog = 101,
    ZSTD_c_hashLog = 102,
    ZSTD_c_chainLog = 103,
    ZSTD_c_searchLog = 104,
    ZSTD_c_minMatch = 105,
    ZSTD_c_targetLength = 106,
    ZSTD_c_strategy = 107,
};

pub const ZSTD_strategy = enum(i16) {
    ZSTD_fast = 1,
    ZSTD_dfast = 2,
    ZSTD_greedy = 3,
    ZSTD_lazy = 4,
    ZSTD_lazy2 = 5,
    ZSTD_btlazy2 = 6,
    ZSTD_btopt = 7,
    ZSTD_btultra = 8,
    ZSTD_btultra2 = 9,
};

const ZSTD_ResetDirective = enum(i8) {
    ZSTD_reset_session_only = 0,
    ZSTD_reset_parameters = 1,
    ZSTD_reset_session_and_parameters = 2,
};

extern "c" fn ZSTD_CCtx_setParameter(cctx: *ZSTD_CCtx, param: ZSTD_cParameter, value: i16) usize;
extern "c" fn ZSTD_CCtx_reset(cctx: *ZSTD_CCtx, reset: ZSTD_ResetDirective) usize;
extern "c" fn ZSTD_DCtx_reset(dctx: *ZSTD_DCtx, reset: ZSTD_ResetDirective) usize;
extern "c" fn ZSTD_CCtx_loadDictionary(cctx: *ZSTD_CCtx, dict: [*]const u8, dictSize: usize) usize;
extern "c" fn ZSTD_DCtx_loadDictionary(dctx: *ZSTD_DCtx, dict: [*]const u8, dictSize: usize) usize;

/// Compression recipes for different data types
/// NOTE: This enum is also defined in config.zig for RuntimeConfig
/// The two definitions must be kept in sync manually
pub const CompressionRecipe = enum {
    fast,            // level 1, fast strategy
    balanced,        // level 3, dfast strategy
    binary,          // level 6, lazy2 strategy (default for snapshots)
    text,            // level 9, btopt strategy
    structured_data, // level 9, btultra strategy (MessagePack/JSON)
    maximum,         // level 22, btultra2 strategy

    pub fn getLevel(self: CompressionRecipe) i16 {
        return switch (self) {
            .fast => 1,
            .balanced => 3,
            .maximum => 22,
            .text => 9,
            .structured_data => 9,
            .binary => 6,
        };
    }

    pub fn getStrategy(self: CompressionRecipe) ZSTD_strategy {
        return switch (self) {
            .fast => .ZSTD_fast,
            .balanced => .ZSTD_dfast,
            .maximum => .ZSTD_btultra2,
            .text => .ZSTD_btopt,
            .structured_data => .ZSTD_btultra,
            .binary => .ZSTD_lazy2,
        };
    }
};

pub const CompressionConfig = struct {
    compression_level: ?i16 = null,
    recipe: ?CompressionRecipe = null,
};

pub const DecompressionConfig = struct {
    max_window_log: ?i16 = null,
};

pub fn init_compressor(config: CompressionConfig) !*ZSTD_CCtx {
    const cctx = ZSTD_createCCtx() orelse return error.ZstdError;
    errdefer _ = ZSTD_freeCCtx(cctx);

    const level: i16 = if (config.compression_level) |lvl|
        lvl
    else if (config.recipe) |recipe|
        recipe.getLevel()
    else
        3;

    if (level < ZSTD_minCLevel() or level > ZSTD_maxCLevel()) {
        return error.InvalidCompressionLevel;
    }

    var result = ZSTD_CCtx_setParameter(cctx, ZSTD_cParameter.ZSTD_c_compressionLevel, level);
    if (ZSTD_isError(result) == 1) {
        std.log.err("Zstd error: {s}", .{ZSTD_getErrorName(result)});
        return error.ZstdError;
    }

    if (config.recipe) |recipe| {
        result = ZSTD_CCtx_setParameter(cctx, ZSTD_cParameter.ZSTD_c_strategy, @intFromEnum(recipe.getStrategy()));
        if (ZSTD_isError(result) == 1) {
            std.log.err("Zstd error: {s}", .{ZSTD_getErrorName(result)});
            return error.ZstdError;
        }
    }

    return cctx;
}

pub fn free_compressor(ctx: *ZSTD_CCtx) usize {
    return ZSTD_freeCCtx(ctx);
}

pub fn reset_compressor_session(ctx: *ZSTD_CCtx) !void {
    const reset_result = ZSTD_CCtx_reset(ctx, ZSTD_ResetDirective.ZSTD_reset_session_only);
    if (ZSTD_isError(reset_result) == 1) {
        std.log.err("Zstd error: {s}", .{ZSTD_getErrorName(reset_result)});
        return error.ZstdError;
    }
}

pub fn compress(allocator: std.mem.Allocator, ctx: *ZSTD_CCtx, input: []const u8) ![]u8 {
    const bound = ZSTD_compressBound(input.len);
    if (ZSTD_isError(bound) == 1) {
        log.err("Zstd error: {s}", .{ZSTD_getErrorName(bound)});
        return error.ZstdError;
    }

    const out = try allocator.alloc(u8, bound);
    errdefer allocator.free(out);

    const written_size = ZSTD_compress2(ctx, out.ptr, bound, input.ptr, input.len);
    if (ZSTD_isError(written_size) == 1) {
        log.err("Zstd error: {s}", .{ZSTD_getErrorName(written_size)});
        return error.ZstdError;
    }
    return allocator.realloc(out, written_size);
}

pub fn init_decompressor(config: DecompressionConfig) !*ZSTD_DCtx {
    _ = config;
    const dctx = ZSTD_createDCtx() orelse return error.ZstdError;
    return dctx;
}

pub fn free_decompressor(ctx: *ZSTD_DCtx) usize {
    return ZSTD_freeDCtx(ctx);
}

pub fn reset_decompressor_session(ctx: *ZSTD_DCtx) !void {
    const reset_result = ZSTD_DCtx_reset(ctx, ZSTD_ResetDirective.ZSTD_reset_session_only);
    if (ZSTD_isError(reset_result) == 1) {
        log.err("Zstd error: {s}", .{ZSTD_getErrorName(reset_result)});
        return error.ZstdError;
    }
}

pub fn decompress(allocator: std.mem.Allocator, ctx: *ZSTD_DCtx, input: []const u8) ![]u8 {
    const output_size = try get_decompressed_size(input);
    const out = try allocator.alloc(u8, output_size);
    errdefer allocator.free(out);

    const written = ZSTD_decompressDCtx(ctx, out.ptr, output_size, input.ptr, input.len);
    if (ZSTD_isError(written) == 1) {
        log.err("Zstd error: {s}", .{ZSTD_getErrorName(written)});
        return error.ZstdError;
    }
    return allocator.realloc(out, written);
}

pub fn load_compression_dictionary(ctx: *ZSTD_CCtx, dictionary: []const u8) !void {
    const result = ZSTD_CCtx_loadDictionary(ctx, dictionary.ptr, dictionary.len);
    if (ZSTD_isError(result) == 1) {
        log.err("Zstd load compression dictionary error: {s}", .{ZSTD_getErrorName(result)});
        return error.ZstdError;
    }
}

pub fn load_decompression_dictionary(ctx: *ZSTD_DCtx, dictionary: []const u8) !void {
    const result = ZSTD_DCtx_loadDictionary(ctx, dictionary.ptr, dictionary.len);
    if (ZSTD_isError(result) == 1) {
        std.log.err("Zstd load decompression dictionary error: {s}", .{ZSTD_getErrorName(result)});
        return error.ZstdError;
    }
}

// -----------------------------------
// === Dictionary Training ===
// -----------------------------------

extern "c" fn ZSTD_trainFromBuffer(
    dictBuffer: [*]u8,
    dictBufferCapacity: usize,
    samplesBuffer: [*]const u8,
    samplesSizes: [*]const usize,
    nbSamples: c_uint,
) usize;

/// Train a dictionary from sample data for better compression of small similar files.
/// The samples should be representative of the data you'll compress.
/// dict_size is the target dictionary size (typical: 64-128KB for snapshots).
/// Returns the dictionary data. Caller owns the memory and is responsible for freeing it.
pub fn train_dictionary(
    allocator: std.mem.Allocator,
    samples: []const []const u8,
    dict_size: usize,
) ![]u8 {
    if (samples.len == 0) {
        return error.NoSamples;
    }

    // Build samples buffer and sizes array
    var total_size: usize = 0;
    for (samples) |sample| {
        total_size += sample.len;
    }

    const samples_buffer = try allocator.alloc(u8, total_size);
    defer allocator.free(samples_buffer);

    const sample_sizes = try allocator.alloc(usize, samples.len);
    defer allocator.free(sample_sizes);

    var offset: usize = 0;
    for (samples, 0..) |sample, i| {
        @memcpy(samples_buffer[offset .. offset + sample.len], sample);
        sample_sizes[i] = sample.len;
        offset += sample.len;
    }

    // Train dictionary
    const dict_buffer = try allocator.alloc(u8, dict_size);
    errdefer allocator.free(dict_buffer);

    const result = ZSTD_trainFromBuffer(
        dict_buffer.ptr,
        dict_size,
        samples_buffer.ptr,
        sample_sizes.ptr,
        @intCast(samples.len),
    );

    if (ZSTD_isError(result) == 1) {
        std.log.err("Zstd dictionary training error: {s}", .{ZSTD_getErrorName(result)});
        return error.ZstdError;
    }

    return allocator.realloc(dict_buffer, result);
}

// -----------------------------------
// === Dictionary Support (One-time Use) ===
// -----------------------------------

extern "c" fn ZSTD_compress_usingDict(
    ctx: *ZSTD_CCtx,
    dst: [*]u8,
    dstCapacity: usize,
    src: [*]const u8,
    srcSize: usize,
    dict: ?[*]const u8,
    dictSize: usize,
    compressionLevel: c_int,
) usize;

extern "c" fn ZSTD_decompress_usingDict(
    dctx: *ZSTD_DCtx,
    dst: [*]u8,
    dstCapacity: usize,
    src: [*]const u8,
    srcSize: usize,
    dict: ?[*]const u8,
    dictSize: usize,
) usize;

/// Compress data using a dictionary for better compression of small similar files.
/// The dictionary should be trained on representative sample data.
/// Caller owns the memory and is responsible for freeing it.
///
/// NOTE: For production use with many chunks, prefer using load_compression_dictionary()
/// once, then compress() multiple times. This function is useful for one-off compressions.
pub fn compress_with_dict(
    allocator: std.mem.Allocator,
    ctx: *ZSTD_CCtx,
    input: []const u8,
    dictionary: []const u8,
    level: i32,
) ![]u8 {
    if (level < ZSTD_minCLevel() or level > ZSTD_maxCLevel()) {
        return error.InvalidCompressionLevel;
    }

    const bound = ZSTD_compressBound(input.len);
    if (ZSTD_isError(bound) == 1) {
        log.err("Zstd error: {s}", .{ZSTD_getErrorName(bound)});
        return error.ZstdError;
    }

    const out = try allocator.alloc(u8, bound);
    errdefer allocator.free(out);

    const written_size = ZSTD_compress_usingDict(
        ctx,
        out.ptr,
        bound,
        input.ptr,
        input.len,
        dictionary.ptr,
        dictionary.len,
        level,
    );
    if (ZSTD_isError(written_size) == 1) {
        log.err("Zstd error: {s}", .{ZSTD_getErrorName(written_size)});
        return error.ZstdError;
    }
    return allocator.realloc(out, written_size);
}

/// Decompress data that was compressed using a dictionary.
/// Caller owns the memory and is responsible for freeing it.
///
/// NOTE: For production use with many chunks, prefer using load_decompression_dictionary()
/// once, then decompress() multiple times. This function is useful for one-off decompressions.
pub fn decompress_with_dict(
    allocator: std.mem.Allocator,
    ctx: *ZSTD_DCtx,
    input: []const u8,
    dictionary: []const u8,
    output_size: usize,
) ![]u8 {
    const out = try allocator.alloc(u8, output_size);
    errdefer allocator.free(out);

    const written = ZSTD_decompress_usingDict(
        ctx,
        out.ptr,
        output_size,
        input.ptr,
        input.len,
        dictionary.ptr,
        dictionary.len,
    );
    if (ZSTD_isError(written) == 1) {
        log.err("Zstd error: {s}", .{ZSTD_getErrorName(written)});
        return error.ZstdError;
    }
    return allocator.realloc(out, written);
}
