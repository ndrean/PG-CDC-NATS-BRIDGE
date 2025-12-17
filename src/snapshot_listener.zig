//! Snapshot request listener and generator
//!
//! Runs in a dedicated thread to:
//! 1. Subscribe to NATS 'snapshot.request.>' subject
//! 2. Generate incremental snapshots in chunks using COPY CSV
//! 3. Publish snapshot chunks to NATS INIT stream

const std = @import("std");
const c_imports = @import("c_imports.zig");
const c = c_imports.c;
const pg_conn = @import("pg_conn.zig");
const nats_publisher = @import("nats_publisher.zig");
const publication_mod = @import("publication.zig");
const config = @import("config.zig");
const msgpack = @import("msgpack");
const pg_copy_csv = @import("pg_copy_csv.zig");
const encoder_mod = @import("encoder.zig");
const zstd = @import("zstd");
const nats_kv = @import("nats_kv.zig");
const RuntimeConfig = @import("config.zig").RuntimeConfig;

pub const log = std.log.scoped(.snapshot_listener);

/// Snapshot request context passed to NATS callback
const SnapshotContext = struct {
    allocator: std.mem.Allocator,
    pg_config: *const pg_conn.PgConf,
    publisher: *nats_publisher.Publisher,
    monitored_tables: []const []const u8,
    format: encoder_mod.Format,
    chunk_size: usize,
    enable_compression: bool,
    recipe: config.CompressionRecipe,
    js_ctx: ?*anyopaque, // JetStream context for KV access
};

/// Snapshot listener with thread management
pub const SnapshotListener = struct {
    allocator: std.mem.Allocator,
    pg_config: *const pg_conn.PgConf,
    publisher: *nats_publisher.Publisher,
    should_stop: *std.atomic.Value(bool),
    monitored_tables: []const []const u8,
    thread: ?std.Thread = null,
    format: encoder_mod.Format,
    chunk_size: usize,
    enable_compression: bool,
    recipe: config.CompressionRecipe,
    js_ctx: ?*anyopaque, // JetStream context for dictionary fetching

    /// Initialize snapshot listener (does not start the thread)
    pub fn init(
        allocator: std.mem.Allocator,
        pg_config: *const pg_conn.PgConf,
        publisher: *nats_publisher.Publisher,
        should_stop: *std.atomic.Value(bool),
        monitored_tables: []const []const u8,
        format: encoder_mod.Format,
        runtime_config: *const config.RuntimeConfig,
        js_ctx: ?*anyopaque,
    ) SnapshotListener {
        return .{
            .allocator = allocator,
            .pg_config = pg_config,
            .publisher = publisher,
            .should_stop = should_stop,
            .monitored_tables = monitored_tables,
            .thread = null,
            .format = format,
            .chunk_size = runtime_config.snapshot_chunk_size,
            .enable_compression = runtime_config.enable_compression,
            .recipe = runtime_config.recipe,
            .js_ctx = js_ctx,
        };
    }

    /// Start the snapshot listener thread
    pub fn start(self: *SnapshotListener) !void {
        if (self.thread != null) {
            return error.AlreadyStarted;
        }
        self.thread = try std.Thread.spawn(.{}, listenLoop, .{self});
    }

    /// Join the snapshot listener thread (waits for completion)
    pub fn join(self: *SnapshotListener) void {
        if (self.thread) |thread| {
            thread.join();
            self.thread = null;
        }
    }

    /// Deinit - cleanup resources (call after join)
    pub fn deinit(self: *SnapshotListener) void {
        // No resources to clean up currently
        _ = self;
    }

    /// Background listening loop (internal)
    fn listenLoop(self: *SnapshotListener) !void {
        try listenForSnapshotRequests(
            self.allocator,
            self.pg_config,
            self.publisher,
            self.should_stop,
            self.monitored_tables,
            self.format,
            self.chunk_size,
            self.enable_compression,
            self.recipe,
            self.js_ctx,
        );
    }
};

/// Publish snapshot error to NATS for consumer feedback
fn publishSnapshotError(
    allocator: std.mem.Allocator,
    publisher: *nats_publisher.Publisher,
    table_name: []const u8,
    error_type: []const u8,
    available_tables: []const []const u8,
    snapshot_id: ?[]const u8,
    error_message: ?[]const u8,
    format: encoder_mod.Format,
) !void {
    const subject = try std.fmt.allocPrintSentinel(
        allocator,
        config.Snapshot.error_subject_pattern,
        .{table_name},
        0,
    );
    defer allocator.free(subject);

    var encoder = encoder_mod.Encoder.init(allocator, format);
    defer encoder.deinit();

    var map = encoder.createMap();
    defer map.free(allocator);

    try map.put("error_type", try encoder.createString(error_type));
    try map.put("table", try encoder.createString(table_name));
    try map.put("timestamp", encoder.createInt(std.time.timestamp()));
    try map.put("status", try encoder.createString("failed"));

    // Optional fields
    if (snapshot_id) |sid| {
        try map.put("snapshot_id", try encoder.createString(sid));
    }
    if (error_message) |msg| {
        try map.put("error_message", try encoder.createString(msg));
    }

    // Create array for available_tables
    var tables_array = try encoder.createArray(available_tables.len);
    for (available_tables, 0..) |table, i| {
        try tables_array.setIndex(i, try encoder.createString(table));
    }
    try map.put("available_tables", tables_array);

    const payload = try encoder.encode(map);
    defer allocator.free(payload);

    try publisher.publish(subject, payload, null);
    try publisher.flushAsync();

    log.err("❌ Published snapshot error → {s}: {s}", .{ subject, error_type });
}

/// NATS message callback for snapshot requests
fn onSnapshotRequest(
    _: ?*c.natsConnection,
    sub: ?*c.natsSubscription,
    msg: ?*c.natsMsg,
    closure: ?*anyopaque,
) callconv(.c) void {
    _ = sub;

    const ctx: *SnapshotContext = @ptrCast(@alignCast(closure));

    defer c.natsMsg_Destroy(msg);

    // Extract table name from subject: snapshot.request.<table>
    const subject_ptr = c.natsMsg_GetSubject(msg);
    const subject = std.mem.span(subject_ptr);

    const table_name = blk: {
        if (std.mem.startsWith(u8, subject, config.Snapshot.request_subject_prefix)) {
            break :blk subject[config.Snapshot.request_subject_prefix.len..];
        }
        log.err("⚠️ Invalid snapshot request subject: {s}", .{subject});
        return;
    };

    log.info("📩 Snapshot request via NATS: table='{s}'", .{table_name});

    // Validate table is in monitored tables list
    const is_monitored = publication_mod.isTableMonitored(table_name, ctx.monitored_tables);

    if (!is_monitored) {
        log.warn("⚠️ Snapshot requested for non-monitored table '{s}' (not in publication)", .{table_name});

        // Publish error to NATS so consumer gets feedback
        publishSnapshotError(
            ctx.allocator,
            ctx.publisher,
            table_name,
            "table_not_in_publication",
            ctx.monitored_tables,
            null, // no snapshot_id yet
            null, // no error_message
            ctx.format,
        ) catch |err| {
            log.err("Failed to publish snapshot error: {}", .{err});
        };

        return;
    }

    // Get request metadata from message payload (MessagePack: requested_by, etc.)
    const data_ptr = c.natsMsg_GetData(msg);
    const data_len: usize = @intCast(c.natsMsg_GetDataLength(msg));

    const requested_by = if (data_len > 0) blk: {
        const payload = data_ptr[0..data_len];
        // Try to parse MessagePack for requested_by field
        // For now, just use "nats-consumer"
        _ = payload;
        break :blk "nats-consumer";
    } else "unknown";

    log.info("🔄 Processing snapshot request for table '{s}' (requested_by: {s})", .{
        table_name,
        requested_by,
    });

    // Generate snapshot ID
    const snapshot_id = generateSnapshotId(ctx.allocator) catch |err| {
        log.err("Failed to generate snapshot ID: {}", .{err});
        return;
    };
    defer ctx.allocator.free(snapshot_id);

    // Generate snapshot
    generateIncrementalSnapshot(
        ctx.allocator,
        ctx.pg_config,
        ctx.publisher,
        null, // No PostgreSQL connection needed (we create our own)
        table_name,
        snapshot_id,
        ctx.format,
        ctx.chunk_size,
        ctx.enable_compression,
        ctx.recipe,
        ctx.js_ctx, // Pass JetStream context for dictionary fetching
    ) catch |err| {
        const error_message = std.fmt.allocPrint(
            ctx.allocator,
            "Snapshot generation failed: {}",
            .{err},
        ) catch "Unknown error";
        defer if (!std.mem.eql(u8, error_message, "Unknown error")) {
            ctx.allocator.free(error_message);
        };

        log.err("Snapshot generation failed for table '{s}': {}", .{ table_name, err });

        // Publish error notification to NATS for consumer feedback
        publishSnapshotError(
            ctx.allocator,
            ctx.publisher,
            table_name,
            @errorName(err),
            ctx.monitored_tables,
            snapshot_id,
            error_message,
            ctx.format,
        ) catch |pub_err| {
            log.err("Failed to publish snapshot error notification: {}", .{pub_err});
        };

        return;
    };

    log.info("✅ Snapshot request for '{s}' completed successfully", .{table_name});
}

/// Main entry point: Subscribe to NATS for snapshot requests
pub fn listenForSnapshotRequests(
    allocator: std.mem.Allocator,
    pg_config: *const pg_conn.PgConf,
    publisher: *nats_publisher.Publisher,
    should_stop: *std.atomic.Value(bool),
    monitored_tables: []const []const u8,
    format: encoder_mod.Format,
    chunk_size: usize,
    enable_compression: bool,
    recipe: config.CompressionRecipe,
    js_ctx: ?*anyopaque, // JetStream context for dictionary fetching
) !void {
    log.info("🔔 Starting NATS snapshot listener thread", .{});

    // Create context for NATS callback
    var ctx = SnapshotContext{
        .allocator = allocator,
        .pg_config = pg_config,
        .publisher = publisher,
        .monitored_tables = monitored_tables,
        .format = format,
        .chunk_size = chunk_size,
        .enable_compression = enable_compression,
        .recipe = recipe,
        .js_ctx = js_ctx,
    };

    // Subscribe to snapshot.request.> (wildcard for all tables)
    var sub: ?*c.natsSubscription = null;
    const status = c.natsConnection_Subscribe(
        &sub,
        publisher.nc,
        config.Snapshot.request_subject_wildcard,
        onSnapshotRequest,
        &ctx,
    );

    if (status != c.NATS_OK) {
        log.err("Failed to subscribe to {s}: {s}", .{
            config.Snapshot.request_subject_wildcard,
            std.mem.span(c.natsStatus_GetText(status)),
        });
        return error.SubscribeFailed;
    }
    defer c.natsSubscription_Destroy(sub);

    // Flush to ensure server processed the subscription
    // This sends PING and waits for PONG to verify subscription was registered
    const flush_status = c.natsConnection_Flush(publisher.nc);
    if (flush_status != c.NATS_OK) {
        log.err("⚠️ Failed to flush after subscription: {s}", .{
            std.mem.span(c.natsStatus_GetText(flush_status)),
        });
        return error.FlushFailed;
    }

    // Check if server had any errors processing the subscription
    var last_err_text: [*c]const u8 = null;
    const last_err = c.natsConnection_GetLastError(publisher.nc, &last_err_text);
    if (last_err != c.NATS_OK) {
        const err_msg = if (last_err_text != null) std.mem.span(last_err_text) else "unknown";
        log.err("Server error after subscription: {s}", .{err_msg});
        return error.SubscriptionError;
    }

    log.info("🔔 Subscribed to NATS subject 'snapshot.request.>' for snapshot requests", .{});

    // Keep thread alive until stop signal
    while (!should_stop.load(.seq_cst)) {
        std.Thread.sleep(100 * std.time.ns_per_ms);
    }

    log.info("🥁 Snapshot listener thread stopped", .{});

    // Release NATS thread-local storage
    // This is required when user-created threads call NATS C library APIs
    c.nats_ReleaseThreadMemory();
}

/// Generate incremental snapshot in chunks and publish to NATS
fn generateIncrementalSnapshot(
    allocator: std.mem.Allocator,
    pg_config: *const pg_conn.PgConf,
    publisher: *nats_publisher.Publisher,
    _: ?*c.PGconn, // Original connection (not used, we create a new one for snapshot query)
    table_name: []const u8,
    snapshot_id: []const u8,
    format: encoder_mod.Format,
    chunk_size: usize,
    enable_compression: bool,
    recipe: config.CompressionRecipe,
    js_ctx: ?*anyopaque, // JetStream context for KV access (optional)
) !void {
    log.info("🔄 Generating incremental snapshot for table '{s}' (snapshot_id={s})", .{
        table_name,
        snapshot_id,
    });

    // Fetch dictionary from NATS KV if compression is enabled
    const dict_entry = if (enable_compression and js_ctx != null) blk: {
        break :blk try fetchDictionary(allocator, js_ctx.?, table_name);
    } else null;
    defer if (dict_entry) |entry| {
        allocator.free(entry.dict_id);
        allocator.free(entry.data);
    };

    // Create a separate connection for snapshot query
    const conninfo = try pg_config.connInfo(allocator, false);
    defer allocator.free(conninfo);

    const conn = c.PQconnectdb(conninfo.ptr);
    if (conn == null) return error.ConnectionFailed;
    defer c.PQfinish(conn);

    if (c.PQstatus(conn) != c.CONNECTION_OK) {
        return error.ConnectionFailed;
    }

    // Begin transaction with REPEATABLE READ isolation for snapshot consistency
    // This ensures all COPY queries see the same database state
    const begin_result = c.PQexec(conn, "BEGIN ISOLATION LEVEL REPEATABLE READ");
    defer c.PQclear(begin_result);

    if (c.PQresultStatus(begin_result) != c.PGRES_COMMAND_OK) {
        log.err("BEGIN REPEATABLE READ failed: {s}", .{c.PQerrorMessage(conn)});
        return error.TransactionFailed;
    }

    // Get snapshot LSN AFTER beginning transaction
    // This LSN represents the consistent point in WAL for this snapshot
    const lsn_query = "SELECT pg_current_wal_lsn()::text";
    const lsn_result = c.PQexec(conn, lsn_query.ptr);
    defer c.PQclear(lsn_result);

    if (c.PQresultStatus(lsn_result) != c.PGRES_TUPLES_OK) {
        return error.QueryFailed;
    }

    const lsn_str: []const u8 = std.mem.span(c.PQgetvalue(lsn_result, 0, 0));

    log.info("📸 Snapshot transaction started at LSN: {s}", .{lsn_str});

    // Publish snapshot start notification with LSN watermark
    // Consumers use this to filter CDC events with LSN < snapshot_lsn
    const dict_id_opt = if (dict_entry) |entry| entry.dict_id else null;
    publishSnapshotStart(
        allocator,
        publisher,
        table_name,
        snapshot_id,
        lsn_str,
        format,
        dict_id_opt,
        enable_compression,
    ) catch |err| {
        log.warn("Failed to publish snapshot start notification: {}", .{err});
    };

    // Create arena allocator for snapshot processing (reused across all chunks)
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    // Use COPY CSV format to fetch rows in chunks
    // Use WHERE id > last_id instead of OFFSET for better performance on large tables
    var batch: u32 = 0;
    var total_rows: u64 = 0;
    var last_id: i64 = 0; // Track last ID instead of offset

    while (true) {
        // Reset arena for this chunk (retains capacity for efficiency)
        _ = arena.reset(.retain_capacity);
        const chunk_alloc = arena.allocator();

        // Build COPY CSV query with WHERE id > last_id for efficient chunking
        // This is much faster than OFFSET for large tables
        const copy_query = try std.fmt.allocPrintSentinel(
            chunk_alloc,
            "COPY (SELECT * FROM {s} WHERE id > {d} ORDER BY id LIMIT {d}) TO STDOUT WITH (FORMAT csv, HEADER true)",
            .{ table_name, last_id, chunk_size },
            0,
        );

        // Parse CSV COPY data using arena allocator
        var parser = pg_copy_csv.CopyCsvParser.init(
            chunk_alloc,
            @ptrCast(conn),
        );
        defer parser.deinit();

        parser.executeCopy(copy_query) catch |err| {
            log.err("COPY CSV command failed: {}", .{err});
            _ = c.PQexec(conn, "ROLLBACK");
            return error.CopyFailed;
        };

        // Collect rows into array using arena allocator
        var rows_list: std.ArrayList(pg_copy_csv.CsvRow) = .{};
        defer {
            for (rows_list.items) |*row| {
                row.deinit();
            }
            rows_list.deinit(chunk_alloc);
        }

        var row_iterator = parser.rows();
        while (try row_iterator.next()) |row| {
            try rows_list.append(chunk_alloc, row);
        }

        const num_rows = rows_list.items.len;
        if (num_rows == 0) break;

        total_rows += num_rows;

        // Get column names from parser header
        const col_names = parser.columnNames() orelse return error.NoHeader;

        // Encode chunk as MessagePack with metadata wrapper using arena allocator
        const encoded = try encodeCsvRows(
            chunk_alloc,
            rows_list.items,
            col_names,
            table_name,
            snapshot_id,
            lsn_str,
            batch,
            format,
        );

        // Compress if enabled
        const payload = if (enable_compression) blk: {
            // Convert config.CompressionRecipe to zstd.CompressionRecipe
            const zstd_recipe: zstd.CompressionRecipe = @enumFromInt(@intFromEnum(recipe));
            const cctx = try zstd.init_compressor(.{ .recipe = zstd_recipe });
            defer _ = zstd.free_compressor(cctx);

            // Load dictionary if available (improves compression ratio significantly)
            if (dict_entry) |entry| {
                _ = try zstd.load_compression_dictionary(cctx, entry.data);
            }

            const compressed = try zstd.compress(allocator, cctx, encoded);
            const compression_note = if (dict_entry != null) "with dictionary" else "without dictionary";
            log.info("🗜️  Compressed chunk {d}: {d} → {d} bytes ({d:.1}% reduction, {s})", .{
                batch,
                encoded.len,
                compressed.len,
                @as(f64, @floatFromInt(encoded.len - compressed.len)) / @as(f64, @floatFromInt(encoded.len)) * 100.0,
                compression_note,
            });
            break :blk compressed;
        } else encoded;
        defer if (enable_compression) allocator.free(payload);

        // Publish chunk to NATS: init.snap.users.snap-1733507200.0
        const subject = try std.fmt.allocPrintSentinel(
            allocator,
            config.Snapshot.data_subject_pattern,
            .{ table_name, snapshot_id, batch },
            0,
        );
        defer allocator.free(subject);

        // Message ID for deduplication
        const msg_id_buf = try std.fmt.allocPrint(
            allocator,
            config.Snapshot.data_msg_id_pattern,
            .{ table_name, snapshot_id, batch },
        );
        defer allocator.free(msg_id_buf);

        try publisher.publish(subject, payload, msg_id_buf);
        try publisher.flushAsync();

        log.info("📦 Published snapshot chunk {d} ({d} rows, {d} bytes {s}) → {s}", .{
            batch,
            num_rows,
            payload.len,
            if (enable_compression) "compressed" else "uncompressed",
            subject,
        });

        batch += 1;

        // Update last_id from the last row's id field (first column)
        // This assumes 'id' is the first column in ORDER BY id
        if (num_rows > 0) {
            const last_row = rows_list.items[num_rows - 1];
            if (last_row.fields.len > 0) {
                if (last_row.fields[0].value) |id_str| {
                    last_id = std.fmt.parseInt(i64, id_str, 10) catch last_id;
                }
            }
        }

        // If we got fewer rows than chunk_size, we're done
        if (num_rows < chunk_size) {
            break;
        }
    }

    // Commit transaction to release snapshot isolation
    const commit_result = c.PQexec(conn, "COMMIT");
    defer c.PQclear(commit_result);

    if (c.PQresultStatus(commit_result) != c.PGRES_COMMAND_OK) {
        log.err("COMMIT failed: {s}", .{c.PQerrorMessage(conn)});
        return error.TransactionFailed;
    }

    log.info("✅ Snapshot transaction committed", .{});

    // Publish metadata: init.users.meta
    try publishSnapshotMetadata(
        allocator,
        publisher,
        table_name,
        snapshot_id,
        lsn_str,
        batch,
        total_rows,
        format,
        dict_id_opt,
        enable_compression,
    );

    log.info("✅ Snapshot complete: {s} ({d} batches, {d} rows)", .{
        snapshot_id,
        batch,
        total_rows,
    });
}

/// Parse PostgreSQL LSN format (e.g., "0/17FBE78") to u64
/// PostgreSQL LSN format: "segment/offset" where both are hex numbers
fn parsePgLsn(lsn_str: []const u8) !u64 {
    // Find the '/' separator
    const slash_pos = std.mem.indexOfScalar(u8, lsn_str, '/') orelse return error.InvalidLsnFormat;

    const segment_str = lsn_str[0..slash_pos];
    const offset_str = lsn_str[slash_pos + 1 ..];

    // Parse both parts as hex
    const segment = try std.fmt.parseInt(u32, segment_str, 16);
    const offset = try std.fmt.parseInt(u32, offset_str, 16);

    // Combine: segment is upper 32 bits, offset is lower 32 bits
    return (@as(u64, segment) << 32) | @as(u64, offset);
}

/// Encode CSV rows to MessagePack with metadata wrapper
/// Wraps snapshot data with table name, operation type, LSN, and chunk info
fn encodeCsvRows(
    allocator: std.mem.Allocator,
    rows: []const pg_copy_csv.CsvRow,
    col_names: [][]const u8,
    table_name: []const u8,
    snapshot_id: []const u8,
    lsn: []const u8,
    chunk: u32,
    format: encoder_mod.Format,
) ![]const u8 {
    // Use unified encoder (always MessagePack for snapshots)
    var encoder = encoder_mod.Encoder.init(
        allocator,
        format, // changed to use passed format
    );
    defer encoder.deinit();

    // Build data array (array of row maps)
    var data_array = try encoder.createArray(rows.len);

    for (rows, 0..) |row, row_idx| {
        var row_map = encoder.createMap();

        for (row.fields, 0..) |csv_field, col_idx| {
            if (col_idx >= col_names.len) continue;

            const col_name = col_names[col_idx];

            if (csv_field.isNull()) {
                try row_map.put(col_name, encoder.createNull());
            } else if (csv_field.value) |text_val| {
                // CSV values are already text, just encode them
                try row_map.put(col_name, try encoder.createString(text_val));
            }
        }

        try data_array.setIndex(row_idx, row_map);
    }

    // Build metadata wrapper map
    var wrapper_map = encoder.createMap();
    defer wrapper_map.free(allocator);

    // Parse PostgreSQL LSN string to u64 integer (same format as CDC events)
    const lsn_int = try parsePgLsn(lsn);

    try wrapper_map.put("table", try encoder.createString(table_name));
    try wrapper_map.put("operation", try encoder.createString("snapshot"));
    try wrapper_map.put("snapshot_id", try encoder.createString(snapshot_id));
    try wrapper_map.put("chunk", encoder.createInt(@intCast(chunk)));
    try wrapper_map.put("lsn", encoder.createInt(@intCast(lsn_int)));
    try wrapper_map.put("data", data_array);

    return try encoder.encode(wrapper_map);
}

/// Publish snapshot metadata to NATS
fn publishSnapshotMetadata(
    allocator: std.mem.Allocator,
    publisher: *nats_publisher.Publisher,
    table_name: []const u8,
    snapshot_id: []const u8,
    lsn: []const u8,
    batch_count: u32,
    row_count: u64,
    format: encoder_mod.Format,
    dictionary_id: ?[]const u8,
    compression_enabled: bool,
) !void {
    // Use unified encoder (always MessagePack for snapshots)
    var encoder = encoder_mod.Encoder.init(
        allocator,
        format, // changed to use passed format
    );
    defer encoder.deinit();

    var meta_map = encoder.createMap();
    defer meta_map.free(allocator);

    // Parse PostgreSQL LSN string to u64 integer (same format as CDC events)
    const lsn_int = try parsePgLsn(lsn);

    try meta_map.put("snapshot_id", try encoder.createString(snapshot_id));
    try meta_map.put("lsn", encoder.createInt(@intCast(lsn_int)));
    try meta_map.put("timestamp", encoder.createInt(std.time.timestamp()));
    try meta_map.put("batch_count", encoder.createInt(@intCast(batch_count)));
    try meta_map.put("row_count", encoder.createInt(@intCast(row_count)));
    try meta_map.put("table", try encoder.createString(table_name));
    try meta_map.put("compression_enabled", encoder.createBool(compression_enabled));

    // Add dictionary_id if compression is enabled
    if (dictionary_id) |dict_id| {
        try meta_map.put("dictionary_id", try encoder.createString(dict_id));
    }

    const encoded = try encoder.encode(meta_map);
    defer allocator.free(encoded);

    // sentinel for the C-API
    const subject = try std.fmt.allocPrintSentinel(
        allocator,
        config.Snapshot.meta_subject_pattern,
        .{table_name},
        0,
    );
    defer allocator.free(subject);

    try publisher.publish(subject, encoded, null);
    try publisher.flushAsync();

    log.info("📋 Published snapshot metadata → {s}", .{subject});
}

/// Publish snapshot start notification to NATS
/// This notifies consumers that a snapshot is starting with the LSN watermark
/// Consumers should filter CDC events with LSN < snapshot_lsn to avoid duplicates
fn publishSnapshotStart(
    allocator: std.mem.Allocator,
    publisher: *nats_publisher.Publisher,
    table_name: []const u8,
    snapshot_id: []const u8,
    lsn: []const u8,
    format: encoder_mod.Format,
    dictionary_id: ?[]const u8,
    compression_enabled: bool,
) !void {
    var encoder = encoder_mod.Encoder.init(allocator, format);
    defer encoder.deinit();

    var start_map = encoder.createMap();
    defer start_map.free(allocator);

    // Parse PostgreSQL LSN string to u64 integer
    const lsn_int = try parsePgLsn(lsn);

    try start_map.put("snapshot_id", try encoder.createString(snapshot_id));
    try start_map.put("table", try encoder.createString(table_name));
    try start_map.put("lsn", encoder.createInt(@intCast(lsn_int)));
    try start_map.put("timestamp", encoder.createInt(std.time.timestamp()));
    try start_map.put("status", try encoder.createString("starting"));
    try start_map.put("format", try encoder.createString(@tagName(format)));
    try start_map.put("compression_enabled", encoder.createBool(compression_enabled));

    // Add dictionary_id if compression is enabled
    if (dictionary_id) |dict_id| {
        try start_map.put("dictionary_id", try encoder.createString(dict_id));
    }

    const encoded = try encoder.encode(start_map);
    defer allocator.free(encoded);

    const subject = try std.fmt.allocPrintSentinel(
        allocator,
        config.Snapshot.start_subject_pattern,
        .{table_name},
        0,
    );
    defer allocator.free(subject);

    try publisher.publish(subject, encoded, null);
    try publisher.flushAsync();

    log.info("🚀 Published snapshot start → {s} (LSN watermark: {s})", .{ subject, lsn });
}

/// Generate snapshot ID based on current timestamp with random entropy
/// Format: snap-{timestamp}-{random_u16}
/// Prevents collisions when multiple snapshots are requested in the same second
fn generateSnapshotId(allocator: std.mem.Allocator) ![]const u8 {
    var prng = std.Random.DefaultPrng.init(@intCast(std.time.microTimestamp()));
    const random_suffix = prng.random().int(u16);

    return try std.fmt.allocPrint(
        allocator,
        "snap-{d}-{x:0>4}",
        .{ std.time.timestamp(), random_suffix },
    );
}

/// Initialize dictionaries by checking existence in NATS KV store
/// Called once at bridge startup to verify dictionaries are available
/// Does not cache dictionaries - they are fetched on-demand per snapshot
pub fn initializeDictionaries(
    allocator: std.mem.Allocator,
    js_ctx: ?*anyopaque,
    monitored_tables: []const []const u8,
) !void {
    if (js_ctx == null) {
        log.warn("⚠️  JetStream context not available, skipping dictionary initialization", .{});
        return;
    }

    log.info("🔍 Checking dictionaries in NATS KV store for {d} tables", .{monitored_tables.len});

    // Open dictionaries KV bucket
    const bucket_name = try allocator.dupeZ(u8, config.Snapshot.kv_bucket_dictionaries);
    defer allocator.free(bucket_name);

    var kv_store = nats_kv.KVStore.open(js_ctx.?, bucket_name, allocator) catch |err| {
        log.warn("⚠️  Failed to open dictionaries KV bucket: {} (compression will work without dictionaries)", .{err});
        return; // Graceful degradation
    };
    defer kv_store.deinit();

    var found_count: usize = 0;
    var missing_count: usize = 0;

    for (monitored_tables) |table_name| {
        // Generate dictionary ID for this table
        const dict_id = try std.fmt.allocPrint(allocator, "dict_{s}_001", .{table_name});
        defer allocator.free(dict_id);

        const dict_key = try allocator.dupeZ(u8, dict_id);
        defer allocator.free(dict_key);

        // Check if dictionary exists
        const dict_data = kv_store.get(dict_key) catch |err| {
            log.warn("⚠️  Dictionary check failed for table '{s}' (key={s}): {}", .{
                table_name,
                dict_id,
                err,
            });
            missing_count += 1;
            continue;
        };

        if (dict_data) |data| {
            defer allocator.free(data);
            log.info("✅ Dictionary found for table '{s}' (id={s}, size={d} bytes)", .{
                table_name,
                dict_id,
                data.len,
            });
            found_count += 1;
        } else {
            log.warn("⚠️  No dictionary found for table '{s}' (key={s})", .{
                table_name,
                dict_id,
            });
            missing_count += 1;
        }
    }

    if (found_count == monitored_tables.len) {
        log.info("✅ All {d} dictionaries available in NATS KV", .{found_count});
    } else if (found_count > 0) {
        log.warn("⚠️  {d}/{d} dictionaries available ({d} missing)", .{
            found_count,
            monitored_tables.len,
            missing_count,
        });
    } else {
        log.warn("⚠️  No dictionaries found for any tables (compression will work without dictionaries)", .{});
    }
}

/// Generate dictionary ID from table name
/// Format: {table}_dict
fn generateDictionaryId(allocator: std.mem.Allocator, table_name: []const u8) ![]const u8 {
    return try std.fmt.allocPrint(allocator, "{s}_dict", .{table_name});
}

/// Fetch zstd dictionary from NATS KV store
/// Returns dictionary entry with ID and binary data, or null if not found (graceful degradation)
fn fetchDictionary(
    allocator: std.mem.Allocator,
    js_ctx: *anyopaque,
    table_name: []const u8,
) !?struct { dict_id: []const u8, data: []const u8 } {
    // Generate dictionary ID for this table (e.g., "dict_users_001")
    // The version number is managed externally when training dictionaries
    const dict_id = try std.fmt.allocPrint(allocator, "dict_{s}_001", .{table_name});
    errdefer allocator.free(dict_id);

    // Open dictionaries KV bucket
    const bucket_name = try allocator.dupeZ(u8, config.Snapshot.kv_bucket_dictionaries);
    defer allocator.free(bucket_name);

    var kv_store = nats_kv.KVStore.open(js_ctx, bucket_name, allocator) catch |err| {
        log.warn("Failed to open dictionaries KV bucket: {} (compression will work without dictionary)", .{err});
        return null; // Graceful degradation
    };
    defer kv_store.deinit();

    // Fetch dictionary from KV
    const dict_key = try allocator.dupeZ(u8, dict_id);
    defer allocator.free(dict_key);

    const dict_data = kv_store.get(dict_key) catch |err| {
        log.warn("Failed to fetch dictionary for table '{s}' (key={s}): {} (compression will work without dictionary)", .{
            table_name,
            dict_id,
            err,
        });
        allocator.free(dict_id);
        return null; // Graceful degradation
    };

    if (dict_data == null) {
        log.warn("No dictionary found for table '{s}' (key={s}) (compression will work without dictionary)", .{
            table_name,
            dict_id,
        });
        allocator.free(dict_id);
        return null;
    }

    log.info("📚 Fetched dictionary for table '{s}' from KV (id={s}, size={d} bytes)", .{
        table_name,
        dict_id,
        dict_data.?.len,
    });

    return .{
        .dict_id = dict_id, // Caller owns
        .data = dict_data.?, // Caller owns
    };
}
