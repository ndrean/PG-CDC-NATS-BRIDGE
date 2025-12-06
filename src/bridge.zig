//! Bridge application that streams PostgreSQL CDC events to NATS JetStream using pgoutput format
const std = @import("std");
const posix = std.posix;
const wal_stream = @import("wal_stream.zig");
const pgoutput = @import("pgoutput.zig");
const nats_publisher = @import("nats_publisher.zig");
const batch_publisher = @import("batch_publisher.zig");
const async_batch_publisher = @import("async_batch_publisher.zig");
const replication_setup = @import("replication_setup.zig");
const msgpack = @import("msgpack");
const http_server = @import("http_server.zig");
const metrics_mod = @import("metrics.zig");
const wal_monitor = @import("wal_monitor.zig");
const pg_conn = @import("pg_conn.zig");
const args = @import("args.zig");

pub const log = std.log.scoped(.bridge);

// Global flag for graceful shutdown (shared with HTTP server)
var should_stop = std.atomic.Value(bool).init(false);

/// Helper to process and publish a CDC event (INSERT/UPDATE/DELETE)
fn processCdcEvent(
    main_allocator: std.mem.Allocator,
    rel: pgoutput.RelationMessage,
    tuple_data: pgoutput.TupleData,
    operation: []const u8,
    subject_prefix: []const u8,
    wal_end: u64,
    batch_pub: *async_batch_publisher.AsyncBatchPublisher,
    metrics: *metrics_mod.Metrics,
) !void {
    // Decode tuple data to get actual column values
    // Use main_allocator so decoded values survive arena.deinit()
    var decoded_values = pgoutput.decodeTuple(
        main_allocator,
        tuple_data,
        rel.columns,
    ) catch |err| {
        log.warn("Failed to decode tuple: {}", .{err});
        return;
    };
    // NOTE: addEvent() takes ownership of decoded_values.
    // The flush thread will free them after publishing.
    errdefer {
        // Only free on error - if addEvent() fails
        for (decoded_values.items) |column| {
            switch (column.value) {
                .text => |txt| main_allocator.free(txt),
                .numeric => |num| main_allocator.free(num),
                .array => |arr| main_allocator.free(arr),
                .jsonb => |jsn| main_allocator.free(jsn),
                .bytea => |byt| main_allocator.free(byt),
                else => {}, // int32, int64, float64, boolean, null don't need freeing
            }
        }
        decoded_values.deinit(main_allocator);
    }

    // Extract ID value for logging (if present) - use stack buffer
    // Optimize: check length first before memcmp (most column names aren't "id")
    var id_buf: [64]u8 = undefined;
    const id_str = blk: {
        for (decoded_values.items) |column| {
            // Quick rejection: check length first (avoid memcmp for wrong-length names)
            if (column.name.len == 2 and column.name[0] == 'i' and column.name[1] == 'd') {
                break :blk switch (column.value) {
                    .int32 => |v| std.fmt.bufPrint(&id_buf, "{d}", .{v}) catch "?",
                    .int64 => |v| std.fmt.bufPrint(&id_buf, "{d}", .{v}) catch "?",
                    .text => |v| if (v.len <= id_buf.len) v else "?",
                    else => "?",
                };
            }
        }
        break :blk null;
    };

    // Convert operation to lowercase for NATS subject
    // Optimize: operation is always INSERT/UPDATE/DELETE, use lookup table
    const operation_lower = switch (operation[0]) {
        'I' => "insert", // INSERT
        'U' => "update", // UPDATE
        'D' => "delete", // DELETE
        else => unreachable, // Only these 3 operations exist in CDC
    };

    // Create NATS subject - use stack buffer
    var subject_buf: [128]u8 = undefined;
    const subject = try std.fmt.bufPrintZ(
        &subject_buf,
        "{s}.{s}.{s}",
        .{ subject_prefix, rel.name, operation_lower },
    );

    // Generate message ID from WAL LSN for idempotent delivery - use stack buffer
    var msg_id_buf: [128]u8 = undefined;
    const msg_id = try std.fmt.bufPrint(
        &msg_id_buf,
        "{x}-{s}-{s}",
        .{ wal_end, rel.name, operation_lower },
    );

    // Add to batch publisher with column data and LSN
    try batch_pub.addEvent(subject, rel.name, operation, msg_id, decoded_values, wal_end);
    metrics.incrementCdcEvents();

    // Log single line with table, operation, and ID
    if (id_str) |id| {
        log.info("{s} {s}.{s} id={s} → {s}", .{ operation, rel.namespace, rel.name, id, subject });
    } else {
        log.info("{s} {s}.{s} → {s}", .{ operation, rel.namespace, rel.name, subject });
    }
}

// Signal handler for graceful shutdown
fn handleShutdown(sig: c_int) callconv(.c) void {
    _ = sig;
    should_stop.store(true, .seq_cst);
}

pub fn main() !void {
    const IS_DEBUG = @import("builtin").mode == .Debug;

    var gpa: std.heap.DebugAllocator(.{}) = .init;
    const allocator = if (IS_DEBUG) gpa.allocator() else std.heap.c_allocator;

    defer if (IS_DEBUG) {
        _ = gpa.detectLeaks();
    };

    const parsed_args = try args.Args.parseArgs(allocator);
    defer {
        for (parsed_args.tables) |table| {
            allocator.free(table);
        }
        allocator.free(parsed_args.tables);
    }

    log.info("▶️ Starting CDC Bridge with parameters:\n", .{});
    log.info("Publication name: \x1b[1m {s} \x1b[0m", .{parsed_args.publication_name});
    log.info("Slot name: \x1b[1m {s} \x1b[0m", .{parsed_args.slot_name});
    log.info("Stream name: \x1b[1m {s} \x1b[0m", .{parsed_args.stream_name});
    log.info("HTTP port: \x1b[1m {d} \x1b[0m", .{parsed_args.http_port});

    if (parsed_args.tables.len == 0) {
        log.info("Tables: \x1b[1m ALL\x1b[0m", .{});
    } else {
        const parsed_tables = try std.mem.join(allocator, ", ", parsed_args.tables);
        defer allocator.free(parsed_tables);
        log.info("Tables: \x1b[1m {s} \x1b[0m", .{parsed_tables});
    }

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

    // Initialize metrics
    var metrics = metrics_mod.Metrics.init();

    // Start HTTP server in background thread (publisher will be set later)
    var http_srv = try http_server.Server.init(
        allocator,
        parsed_args.http_port,
        &should_stop,
        &metrics,
        null,
    );
    const http_thread = try std.Thread.spawn(
        .{},
        http_server.Server.run,
        .{&http_srv},
    );
    defer http_thread.join();

    // PostgreSQL connection configuration
    var pg_config = try pg_conn.PgConf.init_from_env(allocator);
    defer pg_config.deinit(allocator);

    const replication = replication_setup.ReplicationSetup{
        .allocator = allocator,
        .pg_config = pg_config,
    };

    // Create null-terminated versions for C APIs (kept alive for entire program)
    const slot_name_z = try allocator.dupeZ(u8, parsed_args.slot_name);
    defer allocator.free(slot_name_z);
    const pub_name_z = try allocator.dupeZ(u8, parsed_args.publication_name);
    defer allocator.free(pub_name_z);

    log.info("\nStarting PostgreSQL replication_slot and publication...", .{});
    try replication.createSlot(slot_name_z);
    try replication.createPublication(pub_name_z, parsed_args.tables);

    // 2. Start WAL lag monitor in background thread
    const monitor_config = wal_monitor.Config{
        .pg_config = &pg_config,
        .slot_name = parsed_args.slot_name,
        .check_interval_seconds = 30,
    };
    const monitor_thread = try std.Thread.spawn(
        .{},
        wal_monitor.monitorWalLag,
        .{ &metrics, monitor_config, &should_stop, allocator },
    );
    defer monitor_thread.join();

    // 3. Connect to NATS JetStream
    log.info("\nConnecting to NATS JetStream...", .{});
    var publisher = try nats_publisher.Publisher.init(allocator, .{
        .url = "nats://localhost:4222",
    });
    defer publisher.deinit();

    // Set metrics pointer for NATS reconnection tracking
    publisher.metrics = &metrics;

    try publisher.connect();

    // Ensure CDC stream exists (created by infrastructure)
    const stream_name_z = try allocator.dupeZ(u8, parsed_args.stream_name);
    defer allocator.free(stream_name_z);

    // Verify stream exists and is accessible (fail-fast if not)
    try nats_publisher.ensureStream(
        publisher.js.?,
        allocator,
        stream_name_z,
    );
    log.info("✅ NATS JetStream stream verified\n", .{});

    // Generate subject pattern from stream name for publishing
    // Convert stream name to lowercase and use as subject prefix
    // Example: CDC -> "cdc.>" subjects
    var subject_buf: [128]u8 = undefined;
    const subject_str = try std.fmt.bufPrint(&subject_buf, "{s}.>", .{parsed_args.stream_name});

    // Convert to lowercase for subject pattern
    var lower_subject_buf: [128]u8 = undefined;
    const lower_subject = blk: {
        if (subject_str.len > lower_subject_buf.len) return error.SubjectTooLong;
        for (subject_str, 0..) |c, i| {
            lower_subject_buf[i] = std.ascii.toLower(c);
        }
        break :blk lower_subject_buf[0..subject_str.len];
    };

    // Make publisher available to HTTP server for stream management
    http_srv.nats_publisher = &publisher;

    // Initialize async batch publisher (with dedicated flush thread)
    const batch_config = batch_publisher.BatchConfig{
        .max_events = 500, // Larger batches = fewer flushes = higher throughput
        .max_wait_ms = 100,
        .max_payload_bytes = 128 * 1024, // Increase payload limit for larger batches
    };
    var batch_pub = try async_batch_publisher.AsyncBatchPublisher.init(allocator, &publisher, batch_config);
    defer batch_pub.deinit();
    // Start flush thread after batch_pub is at its final memory location
    try batch_pub.start();
    log.info("✅ Async batch publishing enabled (max {d} events or {d}ms or {d}KB)\n", .{ batch_config.max_events, batch_config.max_wait_ms, batch_config.max_payload_bytes / 1024 });

    // Get current LSN to skip historical data
    log.info(" \nGetting current LSN position...", .{});
    const current_lsn = try wal_monitor.getCurrentLSN(allocator, &pg_config);
    defer allocator.free(current_lsn);
    log.info("▶️ Current LSN: {s}\n", .{current_lsn});

    // Connect to replication stream starting from current LSN
    log.info(" 4. Connecting to WAL replication stream...", .{});
    var pg_stream = wal_stream.ReplicationStream.init(
        allocator,
        .{
            .pg_config = &pg_config,
            .slot_name = slot_name_z,
            .publication_name = pub_name_z,
        },
    );
    defer pg_stream.deinit();

    try pg_stream.connect();
    try pg_stream.startStreaming(current_lsn);
    log.info(" ✅ WAL replication stream started from LSN {s}\n", .{current_lsn});

    // Mark as connected in metrics
    metrics.setConnected(true);

    // 5. Stream CDC events to NATS
    // This bridge waits for PostgreSQL events generated by the producer
    log.info("ℹ️ Subject pattern: \x1b[1m {s} \x1b[0m", .{lower_subject});

    // Compute subject prefix (without the wildcard suffix)
    // Example: "cdc_bridge.>" -> "cdc_bridge"
    const subject_prefix = blk: {
        if (std.mem.endsWith(u8, lower_subject, ".>")) {
            break :blk lower_subject[0 .. lower_subject.len - 2];
        }
        break :blk lower_subject;
    };

    var msg_count: u32 = 0;
    var cdc_events: u32 = 0;
    var last_lsn: u64 = 0;
    var last_ack_lsn: u64 = 0; // Track last acknowledged LSN for keepalives
    var last_keepalive_time = std.time.timestamp(); // Track last keepalive sent
    const keepalive_interval_seconds: i64 = 30; // Send keepalive every 30 seconds

    // Status update batching to reduce PostgreSQL round trips
    var bytes_since_ack: u64 = 0; // Track bytes processed since last ack
    var last_status_update_time = std.time.timestamp();
    const status_update_interval_seconds: i64 = 1; // Send status update every 1 second (reduced for visibility)
    const status_update_byte_threshold: u64 = 1024 * 1024; // Or after 1MB of data (better than message count)

    // NATS async publish flushing
    var last_nats_flush_time = std.time.timestamp();
    const nats_flush_interval_seconds: i64 = 5; // Flush NATS async publishes every 5 seconds

    // Periodic structured metric logging for Grafana Alloy/Loki
    var last_metric_log_time = std.time.timestamp();
    const metric_log_interval_seconds: i64 = 15; // Log metrics every 15 seconds

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

    // Create arena allocator once and reuse it for all message parsing
    // This avoids creating/destroying arena 70k times per second at high throughput
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    // Run until graceful shutdown signal received
    while (!should_stop.load(.seq_cst)) {
        // Check for fatal NATS errors (e.g., reconnection timeout exceeded)
        if (batch_pub.hasFatalError()) {
            log.err("🔴 FATAL ERROR: NATS reconnection failed - shutting down bridge to prevent WAL overflow", .{});
            break;
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
                    // This frees all allocations from previous message while keeping the memory buffer
                    defer _ = arena.reset(.retain_capacity);
                    const arena_allocator = arena.allocator();

                    // Parse messages with arena allocator
                    // Relations use main allocator since they persist in the map
                    var parser = pgoutput.Parser.init(arena_allocator, wal_msg.payload);
                    if (parser.parse()) |pg_msg| {
                        // arena.deinit() handles everything

                        switch (pg_msg) {
                            .relation => |rel| {
                                // Relations persist in the map, so clone with main allocator
                                // (arena will be destroyed at end of scope)
                                const cloned_rel_ptr = try rel.clone(allocator);
                                defer allocator.destroy(cloned_rel_ptr);

                                // If relation already exists, free the old one first
                                const result = try relation_map.fetchPut(cloned_rel_ptr.relation_id, cloned_rel_ptr.*);
                                if (result) |old_entry| {
                                    var old_rel = old_entry.value;
                                    old_rel.deinit(allocator);
                                }
                                // log.info("RELATION: {s}.{s} (id={d}, {d} columns)", .{ rel.namespace, rel.name, rel.relation_id, rel.columns.len });
                            },
                            .begin => |b| {
                                log.info("BEGIN: xid={d} lsn={x}", .{ b.xid, b.final_lsn });
                            },
                            .insert => |ins| {
                                if (relation_map.get(ins.relation_id)) |rel| {
                                    try processCdcEvent(
                                        allocator,
                                        rel,
                                        ins.tuple_data,
                                        "INSERT",
                                        subject_prefix,
                                        wal_msg.wal_end,
                                        &batch_pub,
                                        &metrics,
                                    );
                                    cdc_events += 1;
                                }
                            },
                            .update => |upd| {
                                if (relation_map.get(upd.relation_id)) |rel| {
                                    try processCdcEvent(
                                        allocator,
                                        rel,
                                        upd.new_tuple,
                                        "UPDATE",
                                        subject_prefix,
                                        wal_msg.wal_end,
                                        &batch_pub,
                                        &metrics,
                                    );
                                    cdc_events += 1;
                                }
                            },
                            .delete => |del| {
                                if (relation_map.get(del.relation_id)) |rel| {
                                    try processCdcEvent(
                                        allocator,
                                        rel,
                                        del.old_tuple,
                                        "DELETE",
                                        subject_prefix,
                                        wal_msg.wal_end,
                                        &batch_pub,
                                        &metrics,
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

                // Get the last LSN confirmed by NATS (after successful flush)
                const confirmed_lsn = batch_pub.getLastConfirmedLsn();

                // Debug: Log confirmed_lsn vs last_ack_lsn every 128KB (power of 2 for efficient bitwise check)
                // Check if lower 17 bits are zero: 128KB = 2^17 = 0x20000
                // Use comptime constant to avoid runtime calculation
                const log_interval = comptime 128 * 1024;
                if (bytes_since_ack > 0 and (bytes_since_ack & (log_interval - 1)) == 0) {
                    log.debug("Checking ACK: confirmed_lsn={x}, last_ack_lsn={x}, bytes={d}", .{ confirmed_lsn, last_ack_lsn, bytes_since_ack });
                }

                // Send buffered status update if we hit time or byte threshold
                // Only ACK up to the LSN that NATS has confirmed
                // Optimize: check byte threshold first (cheaper than timestamp syscall)
                if (confirmed_lsn > last_ack_lsn) {
                    const should_ack_bytes = bytes_since_ack >= status_update_byte_threshold;
                    // Only get timestamp if byte threshold not met (avoid syscall in hot path)
                    const should_ack_time = if (!should_ack_bytes) blk: {
                        const now = std.time.timestamp();
                        break :blk now - last_status_update_time >= status_update_interval_seconds;
                    } else false;

                    if (should_ack_bytes or should_ack_time) {
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
                // No message available - check if we need to send pending acks or keepalive
                const now = std.time.timestamp();

                // Get the last LSN confirmed by NATS
                const confirmed_lsn = batch_pub.getLastConfirmedLsn();

                // Flush pending status updates if time threshold reached
                if (confirmed_lsn > last_ack_lsn and now - last_status_update_time >= status_update_interval_seconds) {
                    try pg_stream.sendStatusUpdate(confirmed_lsn);
                    log.info("✓ ACKed to PostgreSQL: LSN {x} (NATS confirmed)", .{confirmed_lsn});
                    last_ack_lsn = confirmed_lsn;
                    bytes_since_ack = 0;
                    last_status_update_time = now;
                    last_keepalive_time = now;
                } else if (now - last_keepalive_time >= keepalive_interval_seconds) {
                    // Send keepalive status update to prevent timeout
                    if (last_ack_lsn > 0) {
                        try pg_stream.sendStatusUpdate(last_ack_lsn);
                        last_keepalive_time = now;
                        log.debug("Sent keepalive (LSN: {x})", .{last_ack_lsn});
                    }
                }

                // Lock-free queue handles batching automatically in flush thread
                // No manual time-based flush needed

                // Direct publisher handles its own flushing
                // Flush NATS async publishes periodically (backup flush)
                if (now - last_nats_flush_time >= nats_flush_interval_seconds) {
                    try publisher.flushAsync();
                    last_nats_flush_time = now;
                    log.debug("Flushed NATS async publishes", .{});
                }

                // Sleep briefly to avoid busy-waiting and reduce CPU usage
                std.Thread.sleep(1 * std.time.ns_per_ms); // 1ms sleep

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

                // Short sleep to avoid busy waiting
                std.Thread.sleep(10 * std.time.ns_per_ms);
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
            std.Thread.sleep(2000 * std.time.ns_per_ms); // 2 seconds

            // Get latest LSN and reconnect
            const reconnect_lsn = wal_monitor.getCurrentLSN(allocator, &pg_config) catch |lsn_err| {
                log.err("Failed to get LSN for reconnect: {}", .{lsn_err});
                std.Thread.sleep(5000 * std.time.ns_per_ms); // Wait longer before retry
                continue;
            };
            defer allocator.free(reconnect_lsn);

            // Clean up old connection before reconnecting
            pg_stream.deinit();

            // Reconnect to replication stream
            pg_stream.connect() catch |conn_err| {
                log.err("Failed to reconnect: {}", .{conn_err});
                std.Thread.sleep(5000 * std.time.ns_per_ms);
                continue;
            };

            pg_stream.startStreaming(reconnect_lsn) catch |stream_err| {
                log.err("Failed to restart streaming: {}", .{stream_err});
                std.Thread.sleep(5000 * std.time.ns_per_ms);
                continue;
            };

            log.info("✓ Reconnected to WAL stream at LSN {s}", .{reconnect_lsn});
            metrics.recordReconnect();
            metrics.setConnected(true);
        }
    }

    log.info("\n=== Bridge Session Summary ------------------------------", .{});
    log.info("Total WAL messages received: {d}", .{msg_count});
    log.info("CDC events published to NATS: {d}", .{cdc_events});
    log.info("Bridge stopped gracefully\n", .{});
}
