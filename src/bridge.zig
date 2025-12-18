//! Bridge application that streams PostgreSQL CDC events to NATS JetStream using pgoutput format
const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const Config = @import("config.zig");
const wal_stream = @import("wal_stream.zig");
const pgoutput = @import("pgoutput.zig");
const nats_publisher = @import("nats_publisher.zig");
const batch_publisher = @import("batch_publisher.zig");
const event_processor = @import("event_processor.zig");
const replication_setup = @import("replication_setup.zig");
const msgpack = @import("msgpack");
const http_server = @import("http_server.zig");
const metrics_mod = @import("metrics.zig");
const wal_monitor = @import("wal_monitor.zig");
const pg_conn = @import("pg_conn.zig");
const args = @import("args.zig");
const schema_publisher = @import("schema_publisher.zig");
const schema_cache_mod = @import("schema_cache.zig");
const publication_mod = @import("publication.zig");
const snapshot_listener = @import("snapshot_listener.zig");
const encoder_mod = @import("encoder.zig");

// Force test discovery for imported modules
comptime {
    _ = @import("pg_copy_csv.zig");
    _ = @import("dictionaries_cache.zig");
}

pub const log = std.log.scoped(.bridge);

// Global flag for graceful shutdown (shared with HTTP server)
var should_stop = std.atomic.Value(bool).init(false);

// Signal handler for graceful shutdown
fn handleShutdown(sig: c_int) callconv(.c) void {
    _ = sig;
    should_stop.store(true, .seq_cst);
}

/// Initialize and verify PostgreSQL replication setup
/// Returns the monitored tables from the publication
fn initReplication(
    allocator: std.mem.Allocator,
    pg_config: *pg_conn.PgConf,
    slot_name: [:0]const u8,
    pub_name: [:0]const u8,
) !replication_setup.ReplicationContext {
    log.info("Initializing PostgreSQL replication...", .{});
    return try replication_setup.init(
        allocator,
        pg_config,
        slot_name,
        pub_name,
    );
}

/// Initialize and connect NATS publisher with metrics tracking
fn initNatsPublisher(
    allocator: std.mem.Allocator,
    metrics: *metrics_mod.Metrics,
) !nats_publisher.Publisher {
    log.debug("Connecting to NATS JetStream...", .{});
    var publisher = try nats_publisher.Publisher.init(
        allocator,
        .{ .url = Config.Nats.default_url },
    );
    errdefer publisher.deinit();

    // Set metrics pointer for NATS reconnection tracking
    publisher.metrics = metrics;

    // Connect and verify required streams
    try publisher.connect();

    return publisher;
}

/// Initialize async batch publisher (does NOT start the thread - caller must call start())
/// The thread MUST be started after the publisher is at its final memory location
fn initBatchPublisher(
    allocator: std.mem.Allocator,
    publisher: *nats_publisher.Publisher,
    format: encoder_mod.Format,
    metrics: *metrics_mod.Metrics,
    runtime_config: *const Config.RuntimeConfig,
) !batch_publisher.BatchPublisher {
    const batch_config = batch_publisher.BatchConfig{
        .max_events = runtime_config.batch_max_events,
        .max_wait_ms = runtime_config.batch_max_wait_ms,
        .max_payload_bytes = runtime_config.batch_max_payload_bytes,
    };

    return try batch_publisher.BatchPublisher.init(
        allocator,
        publisher,
        batch_config,
        format,
        metrics,
        runtime_config,
    );
}

/// Initialize HTTP server for health checks and metrics (does NOT start the thread)
/// The thread MUST be started after the server is at its final memory location
fn initHttpServer(
    allocator: std.mem.Allocator,
    port: u16,
    should_stop_flag: *std.atomic.Value(bool),
    metrics: *metrics_mod.Metrics,
    publisher: ?*nats_publisher.Publisher,
) !http_server.Server {
    return try http_server.Server.init(
        allocator,
        port,
        should_stop_flag,
        metrics,
        publisher,
    );
}

/// Connect to PostgreSQL replication stream and start streaming from current LSN
fn initReplicationStream(
    allocator: std.mem.Allocator,
    pg_config: *pg_conn.PgConf,
    slot_name: [:0]const u8,
    pub_name: [:0]const u8,
) !wal_stream.ReplicationStream {
    log.debug("Connecting to WAL replication stream...", .{});

    const current_lsn = try wal_monitor.getCurrentLSN(allocator, pg_config);
    defer allocator.free(current_lsn);

    log.debug("▶️ Current LSN: {s}\n", .{current_lsn});

    var pg_stream = wal_stream.ReplicationStream.init(
        allocator,
        .{
            .pg_config = pg_config,
            .slot_name = slot_name,
            .publication_name = pub_name,
        },
    );
    errdefer pg_stream.deinit();

    try pg_stream.connect();
    try pg_stream.startStreaming(current_lsn);

    log.info("✅ WAL replication stream started from LSN {s}\n", .{current_lsn});

    return pg_stream;
}

pub fn main() !void {
    const IS_DEBUG = builtin.mode == .Debug;

    var gpa: std.heap.DebugAllocator(.{}) = .init;
    const allocator = if (IS_DEBUG) gpa.allocator() else std.heap.c_allocator;

    defer if (IS_DEBUG) {
        _ = gpa.detectLeaks();
    };

    var tsa = std.heap.ThreadSafeAllocator{ .child_allocator = gpa.allocator() };

    // CDC event allocator
    // Debug: Use ThreadSafeAllocator for leak detection
    // Release: Use c_allocator directly (arena without reset was growing infinitely)
    const event_alloc = if (IS_DEBUG)
        tsa.allocator()
    else
        std.heap.c_allocator;

    // Parse command-line arguments and build runtime config
    const parsed = try args.Args.parseArgs(allocator);
    const parsed_args = parsed.args;
    var runtime_config = parsed.runtime_config;
    defer runtime_config.deinit(allocator);

    // Create null-terminated versions for C APIs (kept alive for entire program)
    const slot_name_z = try allocator.dupeZ(u8, parsed_args.slot_name);
    defer allocator.free(slot_name_z);
    const pub_name_z = try allocator.dupeZ(u8, parsed_args.publication_name);
    defer allocator.free(pub_name_z);

    log.info("▶️ Starting CDC Bridge with parameters:\n", .{});
    log.info("Publication name: \x1b[1m {s} \x1b[0m", .{parsed_args.publication_name});
    log.info("Slot name: \x1b[1m {s} \x1b[0m", .{parsed_args.slot_name});
    log.info("HTTP port: \x1b[1m {d} \x1b[0m", .{parsed_args.http_port});
    log.info("Encoding format: \x1b[1m {s} \x1b[0m", .{@tagName(parsed_args.encoding_format)});
    log.info("Streams: \x1b[1m CDC, INIT \x1b[0m (hardcoded)", .{});

    // Register signal handlers for graceful shutdown
    const empty_mask = std.mem.zeroes(posix.sigset_t);
    const sigaction = posix.Sigaction{
        .handler = .{ .handler = handleShutdown },
        .mask = empty_mask,
        .flags = 0,
    };
    posix.sigaction(posix.SIG.INT, &sigaction, null); // Ctrl+C
    posix.sigaction(posix.SIG.TERM, &sigaction, null); // kill command
    log.info("👋 Press \x1b[1m Ctrl+C \x1b[0m to stop gracefully\n", .{});

    // === Initialize metrics
    var metrics = metrics_mod.Metrics.init();

    // === Start thread: HTTP server (at final memory location)
    var http_srv = try initHttpServer(
        allocator,
        parsed_args.http_port,
        &should_stop,
        &metrics,
        null,
    );
    // Start HTTP server thread AFTER http_srv is at its final memory location
    try http_srv.start();
    defer http_srv.join();
    defer http_srv.deinit();

    // PostgreSQL connection configuration from RuntimeConfig
    var pg_config = pg_conn.PgConf.from_runtime_config(&runtime_config);

    // Initialize replication: create slot + verify publication
    var replication_ctx = try initReplication(
        allocator,
        &pg_config,
        slot_name_z,
        pub_name_z,
    );
    defer replication_ctx.deinit();

    // === Start thread: WAL lag monitor
    const wal_monitor_config = wal_monitor.WalConfig{
        .pg_config = &pg_config,
        .slot_name = parsed_args.slot_name,
        .check_interval_seconds = 30,
    };
    var wal_mon = wal_monitor.WalMonitor.init(
        allocator,
        &metrics,
        wal_monitor_config,
        &should_stop,
    );
    try wal_mon.start();
    defer wal_mon.join();
    defer wal_mon.deinit();

    // === Connect to NATS JetStream
    var publisher = try initNatsPublisher(allocator, &metrics);
    defer publisher.deinit();

    // Make publisher available to HTTP server for stream management
    http_srv.nats_publisher = &publisher;

    // Initialize schema cache for tracking relation_id changes
    var schema_cache = schema_cache_mod.SchemaCache.init(allocator);
    defer schema_cache.deinit();
    log.debug("Schema cache initialized\n", .{});

    // Publish initial schemas to INIT stream (only for monitored tables)
    // Store monitored tables for validation (used by schema changes and snapshot requests)
    const monitored_tables = replication_ctx.tables;

    try schema_publisher.publishInitialSchemas(
        allocator,
        &pg_config,
        &publisher,
        monitored_tables,
        parsed_args.encoding_format,
    );

    // Initialize dictionaries from NATS KV if compression is enabled
    if (runtime_config.enable_compression) {
        snapshot_listener.initializeDictionaries(
            allocator,
            publisher.js, // JetStream context
            monitored_tables,
        ) catch |err| {
            log.warn("⚠️  Dictionary initialization failed: {} (compression will work without dictionaries)", .{err});
            // Continue without dictionaries - graceful degradation
        };
    }

    // === Start thread: snapshot listener
    log.info("Starting snapshot listener thread...", .{});
    var snap_listener = snapshot_listener.SnapshotListener.init(
        allocator,
        &pg_config,
        &publisher,
        &should_stop,
        monitored_tables,
        parsed_args.encoding_format,
        &runtime_config,
        publisher.js, // Pass JetStream context for dictionary fetching
    );
    try snap_listener.start();
    defer snap_listener.join();
    defer snap_listener.deinit();
    log.info("✅ Snapshot listener thread started\n", .{});

    // === Start thread: CDC async publisher (at final memory location)
    // Use c_allocator (thread-safe) for cross-thread allocations:
    // Main thread allocates CDC event data, flush thread deallocates it
    var batch_pub = try initBatchPublisher(
        event_alloc,
        &publisher,
        parsed_args.encoding_format,
        &metrics,
        &runtime_config,
    );
    // Start flush thread AFTER batch_pub is at its final memory location
    try batch_pub.start();
    defer batch_pub.join();
    defer batch_pub.deinit();

    // === Initialize EventProcessor (main thread CDC processor)
    // EventProcessor enqueues to the SPSC queue that BatchPublisher consumes
    var event_proc = event_processor.EventProcessor.init(
        event_alloc, // Use thread-safe allocator for cross-thread data
        &batch_pub.event_queue, // Reference to the SPSC queue
        &metrics,
    );

    const batch_config = batch_publisher.BatchConfig{
        .max_events = Config.Batch.max_events,
        .max_wait_ms = Config.Batch.max_age_ms,
        .max_payload_bytes = Config.Batch.max_payload_bytes,
    };
    log.info("✅ Async batch publishing enabled (max {d} events or {d}ms or {d}KB)\n", .{
        batch_config.max_events,
        batch_config.max_wait_ms,
        batch_config.max_payload_bytes / 1024,
    });

    // Connect to replication stream
    var pg_stream = try initReplicationStream(
        allocator,
        &pg_config,
        slot_name_z,
        pub_name_z,
    );
    defer pg_stream.deinit();

    // Mark as connected in metrics
    metrics.setConnected(true);

    // CDC events are published to subjects like "cdc.table.operation"
    log.info("ℹ️ Subject pattern: \x1b[1m {s} \x1b[0m", .{Config.Nats.cdc_subject_wildcard});

    // <--- Metrics setup
    const present = std.time.timestamp();
    var msg_count: u32 = 0;
    var cdc_events: u32 = 0;
    var last_lsn: u64 = 0;
    var last_ack_lsn: u64 = 0; // Track last acknowledged LSN for keepalives
    var last_keepalive_time = present; // Track last keepalive sent
    const keepalive_interval_seconds: i64 = Config.Bridge.keepalive_interval_seconds; // Send keepalive every 30 seconds

    // Status update batching to reduce PostgreSQL round trips
    var bytes_since_ack: u64 = 0; // Track bytes processed since last ack
    var last_status_update_time = present;
    const status_update_interval_seconds: i64 = Config.Nats.status_update_interval_seconds; // Send status update every 1 second (reduced for visibility)
    const status_update_byte_threshold: u64 = Config.Nats.status_update_byte_threshold; // Or after 1MB of data (better than message count)

    // Periodic structured metric logging for Grafana Alloy/Loki
    var last_metric_log_time = present;
    const metric_log_interval_seconds: i64 = Config.Metrics.metric_log_interval_seconds; // Log metrics every 15 seconds

    // Idle loop optimization - avoid syscalls on every iteration
    var idle_iterations: u32 = 0;
    const idle_check_interval: u32 = 10; // Check time every 10 iterations (~100ms)
    // --->

    // Track relation metadata (table info)
    var relation_map = std.AutoHashMap(u32, pgoutput.RelationMessage).init(allocator);
    defer {
        var it = relation_map.valueIterator();
        while (it.next()) |rel| {
            var r = rel.*;
            r.deinit(allocator);
        }
        relation_map.deinit();
    }

    // Create arena allocator for messages parsing
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    // Run until graceful shutdown signal received
    while (!should_stop.load(.seq_cst)) {
        // Check for fatal NATS errors (e.g., reconnection timeout exceeded)
        if (batch_pub.hasFatalError()) {
            log.err("🔴 FATAL ERROR: NATS reconnection failed - shutting down bridge to prevent WAL overflow", .{});
            break;
        }

        // Reclaim published events from flush thread (memory ownership cycle)
        // Now actually frees memory since we're using c_allocator directly
        const reclaimed = batch_pub.reclaimEvents();
        if (reclaimed > 0) {
            log.debug("Reclaimed and freed {d} published events", .{reclaimed});
        }

        if (pg_stream.receiveMessage()) |maybe_msg| {
            if (maybe_msg) |wal_msg_val| {
                var wal_msg = wal_msg_val;
                defer wal_msg.deinit(allocator);

                msg_count += 1;
                metrics.incrementWalMessages();

                // Handle keepalive messages - reply immediately if requested
                if (wal_msg.type == .keepalive) {
                    if (wal_msg.reply_requested) {
                        // PostgreSQL is requesting a reply - send status update immediately
                        const reply_lsn = if (last_ack_lsn > 0) last_ack_lsn else wal_msg.wal_end;
                        try pg_stream.sendStatusUpdate(reply_lsn);
                        last_keepalive_time = std.time.timestamp();
                        log.debug("Replied to keepalive request (LSN: {x})", .{reply_lsn});
                    }
                    continue; // Don't process keepalives further
                }

                // Parse and publish pgoutput messages
                if (wal_msg.type == .xlogdata and wal_msg.payload.len > 0) {
                    // Reset arena for this message (retains capacity for efficiency)
                    // reset allocations from previous message while keeping the memory buffer
                    _ = arena.reset(.retain_capacity);
                    const arena_allocator = arena.allocator();

                    // Parse messages with arena allocator
                    // Relations use main allocator since they persist in the map
                    var parser = pgoutput.Parser.init(arena_allocator, wal_msg.payload);
                    if (parser.parse()) |pg_msg| {
                        // arena.deinit() handles everything

                        switch (pg_msg) {
                            .relation => |rel| {
                                // "what the columns are"
                                // Relations persist in the map, so clone with main allocator
                                // (arena will be destroyed at end of scope)
                                const cloned_rel_ptr = try rel.clone(allocator);
                                defer allocator.destroy(cloned_rel_ptr);

                                // Check if schema changed for a monitored table
                                const schema_changed = try schema_cache.hasChanged(rel.name, rel.relation_id);

                                if (schema_changed) {
                                    log.info("🔔 Schema change detected for table '{s}' (relation_id={d})", .{ rel.name, rel.relation_id });
                                    try schema_publisher.publishSchema(&publisher, &rel, allocator, parsed_args.encoding_format);
                                }

                                // Update the current relation map HashMap
                                const result = try relation_map.fetchPut(
                                    cloned_rel_ptr.relation_id,
                                    cloned_rel_ptr.*,
                                );
                                // If relation already exists, free the returned old one
                                if (result) |old_entry| {
                                    var old_rel = old_entry.value;
                                    old_rel.deinit(allocator);
                                }
                                log.debug("RELATION: {s}.{s} (id={d}, {d} columns)", .{ rel.namespace, rel.name, rel.relation_id, rel.columns.len });
                            },
                            .begin => |b| {
                                log.info("BEGIN: xid={d} lsn={x}", .{ b.xid, b.final_lsn });
                            },
                            .insert => |ins| {
                                // tupleData contains the new row values
                                if (relation_map.get(ins.relation_id)) |rel| {
                                    try event_proc.processCdcEvent(
                                        rel,
                                        ins.tuple_data,
                                        "INSERT",
                                        wal_msg.wal_end,
                                    );
                                    cdc_events += 1;
                                }
                            },
                            .update => |upd| {
                                if (relation_map.get(upd.relation_id)) |rel| {
                                    try event_proc.processCdcEvent(
                                        rel,
                                        upd.new_tuple,
                                        "UPDATE",
                                        wal_msg.wal_end,
                                    );
                                    cdc_events += 1;
                                }
                            },
                            .delete => |del| {
                                if (relation_map.get(del.relation_id)) |rel| {
                                    try event_proc.processCdcEvent(
                                        rel,
                                        del.old_tuple,
                                        "DELETE",
                                        wal_msg.wal_end,
                                    );
                                    cdc_events += 1;
                                }
                            },
                            .commit => |c| {
                                // Track LSN progression
                                if (c.commit_lsn != last_lsn) {
                                    const lsn_diff = c.commit_lsn - last_lsn;
                                    log.info("COMMIT: lsn={x} (delta: +{d})", .{ c.commit_lsn, lsn_diff });
                                    last_lsn = c.commit_lsn;
                                } else {
                                    log.info("COMMIT: lsn={x}", .{c.commit_lsn});
                                }
                            },
                            else => {},
                        }
                    } else |err| {
                        log.warn("Failed to parse pgoutput message: {}", .{err});
                    }
                }

                // Track the latest WAL position we've received and update metrics
                if (wal_msg.wal_end > 0) {
                    bytes_since_ack += wal_msg.payload.len;
                    metrics.updateLsn(wal_msg.wal_end);
                }

                // Send buffered status update if we hit byte threshold
                // Time-based ACKs are handled in the idle path to avoid syscalls in hot path
                if (bytes_since_ack >= status_update_byte_threshold) {
                    // Only read atomic LSN when we're about to ACK
                    const confirmed_lsn = batch_pub.getLastConfirmedLsn();
                    if (confirmed_lsn > last_ack_lsn) {
                        const now = std.time.timestamp(); // Get timestamp for update
                        try pg_stream.sendStatusUpdate(confirmed_lsn);
                        log.info("✓ ACKed to PostgreSQL: LSN {x} (NATS confirmed, {d} bytes)", .{ confirmed_lsn, bytes_since_ack });
                        last_ack_lsn = confirmed_lsn;
                        bytes_since_ack = 0;
                        last_status_update_time = now;
                        last_keepalive_time = now; // Reset keepalive timer
                    }
                }
            } else {
                // No message available - idle path
                // Sleep 10 ms first to avoid busy-waiting
                std.Thread.sleep(10 * std.time.ns_per_ms);

                // Only check time-based conditions periodically
                idle_iterations += 1;
                if (idle_iterations >= idle_check_interval) {
                    idle_iterations = 0;

                    const now = std.time.timestamp();

                    // Flush pending status updates if time threshold reached
                    if (now - last_status_update_time >= status_update_interval_seconds) {
                        const confirmed_lsn = batch_pub.getLastConfirmedLsn();
                        if (confirmed_lsn > last_ack_lsn) {
                            try pg_stream.sendStatusUpdate(confirmed_lsn);
                            log.info("✓ ACKed to PostgreSQL: LSN {x} (NATS confirmed)", .{confirmed_lsn});
                            last_ack_lsn = confirmed_lsn;
                            bytes_since_ack = 0;
                            last_keepalive_time = now;
                        }
                        // Always update last_status_update_time to avoid checking continuously
                        last_status_update_time = now;
                    }

                    if (now - last_keepalive_time >= keepalive_interval_seconds) {
                        // Send keepalive status update to prevent timeout
                        if (last_ack_lsn > 0) {
                            try pg_stream.sendStatusUpdate(last_ack_lsn);
                            last_keepalive_time = now;
                            log.debug("Sent keepalive (LSN: {x})", .{last_ack_lsn});
                        }
                    }

                    // Periodic structured metric logging for Alloy/Loki
                    if (now - last_metric_log_time >= metric_log_interval_seconds) {
                        const snap = try metrics.snapshot(allocator);
                        defer allocator.free(snap.current_lsn_str);

                        // Structured log format parseable by Grafana Alloy
                        log.info("METRICS uptime={d} wal_messages={d} cdc_events={d} lsn={s} connected={d} pg_reconnects={d} nats_reconnects={d} lag_bytes={d} slot_active={d}", .{
                            snap.uptime_seconds,
                            snap.wal_messages_received,
                            snap.cdc_events_published,
                            snap.current_lsn_str,
                            if (snap.is_connected) @as(u8, 1) else @as(u8, 0),
                            snap.reconnect_count,
                            snap.nats_reconnect_count,
                            snap.wal_lag_bytes,
                            if (snap.slot_active) @as(u8, 1) else @as(u8, 0),
                        });

                        last_metric_log_time = now;
                    }
                }
            }
        } else |err| {
            if (err == error.StreamEnded) {
                log.info("Stream ended gracefully", .{});
                break;
            }

            // Handle connection errors by reconnecting
            log.warn("Connection lost: {}", .{err});
            metrics.setConnected(false);
            log.info("Attempting to reconnect in 2 seconds...", .{});

            // Reclaim events during idle time
            _ = batch_pub.reclaimEvents();

            std.Thread.sleep(2000 * std.time.ns_per_ms); // 2 seconds

            // Get latest LSN and reconnect
            const reconnect_lsn = wal_monitor.getCurrentLSN(allocator, &pg_config) catch |lsn_err| {
                log.err("Failed to get LSN for reconnect: {}", .{lsn_err});
                std.Thread.sleep(Config.Retry.pg_reconnect_delay_seconds * std.time.ns_per_s);
                continue;
            };
            defer allocator.free(reconnect_lsn);

            // Clean up old connection before reconnecting
            pg_stream.deinit();

            // Reconnect to replication stream
            pg_stream.connect() catch |conn_err| {
                log.err("Failed to reconnect: {}", .{conn_err});
                std.Thread.sleep(Config.Retry.pg_reconnect_delay_seconds * std.time.ns_per_s);
                continue;
            };

            pg_stream.startStreaming(reconnect_lsn) catch |stream_err| {
                log.err("Failed to restart streaming: {}", .{stream_err});
                std.Thread.sleep(Config.Retry.pg_reconnect_delay_seconds * std.time.ns_per_s);
                continue;
            };

            log.info("✓ Reconnected to WAL stream at LSN {s}", .{reconnect_lsn});
            metrics.recordReconnect();
            metrics.setConnected(true);
        }
    }

    // Graceful shutdown: signal flush thread to stop and wait for completion
    log.info("\n🛑 Shutdown initiated - signaling flush thread to stop...", .{});

    // CRITICAL: Signal should_stop BEFORE waiting for completion
    batch_pub.should_stop.store(true, .seq_cst);

    const initial_queue_len = batch_pub.event_queue.len();
    if (initial_queue_len > 0) {
        log.info("📤 Queue has {d} events waiting to be published...", .{initial_queue_len});
    }

    const shutdown_timeout_seconds = 30;
    const start_time = std.time.timestamp();
    var last_log_time: i64 = 0;

    // Wait for flush thread to complete (both queue empty AND final flush done)
    while (!batch_pub.isFlushComplete()) {
        const elapsed = std.time.timestamp() - start_time;
        if (elapsed > shutdown_timeout_seconds) {
            const remaining = batch_pub.event_queue.len();
            log.warn("⚠️ Shutdown timeout reached - {d} events may not have been published", .{remaining});
            break;
        }

        // Log progress every second
        if (elapsed - last_log_time >= 1) {
            const remaining = batch_pub.event_queue.len();
            if (remaining > 0 or !batch_pub.isFlushComplete()) {
                log.info("📊 Draining: {d} events in queue, flush thread working...", .{remaining});
            }
            last_log_time = elapsed;
        }

        std.Thread.sleep(100 * std.time.ns_per_ms); // 100ms
    }

    if (batch_pub.isFlushComplete()) {
        log.info("✅ Flush thread completed - all events published to NATS", .{});
    }

    // Send final ACK to PostgreSQL with last confirmed LSN
    const final_lsn = batch_pub.getLastConfirmedLsn();
    if (final_lsn > last_ack_lsn) {
        pg_stream.sendStatusUpdate(final_lsn) catch |err| {
            log.warn("Failed to send final ACK: {}", .{err});
        };
        log.info("📨 Final ACK sent to PostgreSQL: LSN {x}", .{final_lsn});
    }

    log.info("\n=== Bridge Session Summary ------------------------------", .{});
    log.info("Total WAL messages received: {d}", .{msg_count});
    log.info("CDC events published to NATS: {d}", .{cdc_events});
    log.info("Bridge stopped gracefully\n", .{});
}
