//! Bridge application that streams PostgreSQL CDC events to NATS JetStream using decoderbufs format
const std = @import("std");
const posix = std.posix;
const wal_stream = @import("wal_stream_decoderbufs.zig");
const decoderbufs_parser = @import("decoderbufs_parser.zig");
const nats_publisher = @import("nats_publisher.zig");
const batch_publisher = @import("batch_publisher.zig");
const replication_setup = @import("replication_setup.zig");
const msgpack = @import("msgpack");
const http_server = @import("http_server.zig");
const metrics_mod = @import("metrics.zig");
const wal_monitor = @import("wal_monitor.zig");
const pg_conn = @import("pg_conn.zig");

pub const log = std.log.scoped(.bridge);

// Global flag for graceful shutdown (shared with HTTP server)
var should_stop = std.atomic.Value(bool).init(false);

// Signal handler for graceful shutdown
fn handleShutdown(sig: c_int) callconv(.c) void {
    _ = sig;
    should_stop.store(true, .seq_cst);
}

const Args = struct {
    stream_name: []const u8,
    http_port: u16,
    slot_name: []const u8,
    publication_name: []const u8,
    tables: []const []const u8, // Empty slice = all tables

    pub fn parseArgs(allocator: std.mem.Allocator) !Args {
        // Parse command-line arguments
        var args = try std.process.argsWithAllocator(allocator);
        _ = args.skip(); // Skip program name
        var stream_name: []const u8 = "CDC_BRIDGE"; // default
        var http_port: u16 = 8080; // default
        var slot_name: []const u8 = "bridge_slot"; // default
        var publication_name: []const u8 = "bridge_pub"; // default
        var tables: []const []const u8 = &.{}; // default: all tables

        while (args.next()) |arg| {
            if (std.mem.eql(u8, arg, "--stream")) {
                if (args.next()) |value| {
                    stream_name = value;
                }
            } else if (std.mem.eql(u8, arg, "--port")) {
                if (args.next()) |value| {
                    http_port = std.fmt.parseInt(u16, value, 10) catch {
                        log.err("--port requires a valid port number (1-65535)", .{});
                        return error.InvalidArguments;
                    };
                }
            } else if (std.mem.eql(u8, arg, "--slot")) {
                if (args.next()) |value| {
                    slot_name = value;
                }
            } else if (std.mem.eql(u8, arg, "--publication")) {
                if (args.next()) |value| {
                    publication_name = value;
                }
            } else if (std.mem.eql(u8, arg, "--table")) {
                if (args.next()) |value| {
                    // Check if "all" (case-insensitive)
                    if (std.ascii.eqlIgnoreCase(value, "all")) {
                        tables = &.{}; // Empty = all tables
                    } else {
                        // Parse comma-separated table names
                        var table_list = std.ArrayList([]const u8){};
                        defer table_list.deinit(allocator);

                        var iter = std.mem.splitScalar(u8, value, ',');
                        while (iter.next()) |table_name| {
                            const trimmed = std.mem.trim(
                                u8,
                                table_name,
                                &std.ascii.whitespace,
                            );

                            if (trimmed.len > 0) {
                                // Heap-allocate the string to ensure it outlives parseArgs()
                                const owned = try allocator.dupe(u8, trimmed);
                                try table_list.append(allocator, owned);
                            }
                        }

                        tables = try table_list.toOwnedSlice(allocator);
                    }
                }
            }
        }

        return Args{
            .stream_name = stream_name,
            .http_port = http_port,
            .slot_name = slot_name,
            .publication_name = publication_name,
            .tables = tables,
        };
    }
};

pub fn main() !void {
    const IS_DEBUG = @import("builtin").mode == .Debug;

    var gpa: std.heap.DebugAllocator(.{}) = .init;
    const allocator = if (IS_DEBUG) gpa.allocator() else std.heap.c_allocator;

    defer if (IS_DEBUG) {
        _ = gpa.detectLeaks();
    };

    const parsed_args = try Args.parseArgs(allocator);
    defer {
        for (parsed_args.tables) |table| {
            allocator.free(table);
        }
        allocator.free(parsed_args.tables);
    }

    log.info("Starting CDC Bridge with parameters:\n", .{});
    log.info("Publication name: {s}", .{parsed_args.publication_name});
    log.info("Slot name: {s}", .{parsed_args.slot_name});
    log.info("Stream name: {s}", .{parsed_args.stream_name});
    log.info("HTTP port: {d}", .{parsed_args.http_port});

    if (parsed_args.tables.len == 0) {
        log.info("Tables: ALL", .{});
    } else {
        const parsed_tables = try std.mem.join(allocator, ", ", parsed_args.tables);
        defer allocator.free(parsed_tables);
        log.info("Tables: {s}", .{parsed_tables});
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
    log.info("Press Ctrl+C to stop gracefully\n", .{});

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

    // 1. Create replication slot and publication using libpq
    log.info("1. Setting up replication infrastructure...", .{});

    // PostgreSQL connection configuration
    const pg_host = std.process.getEnvVarOwned(allocator, "POSTGRES_HOST") catch |err| blk: {
        log.info("POSTGRES_HOST env var not set ({}), using default 127.0.0.1", .{err});
        break :blk try allocator.dupe(u8, "127.0.0.1");
    };
    defer allocator.free(pg_host);

    var pg_config = try pg_conn.PgConf.init_from_env(allocator);
    defer pg_config.deinit(allocator);

    const setup = replication_setup.ReplicationSetup{
        .allocator = allocator,
        .pg_config = pg_config,
    };

    // Create null-terminated versions for C APIs (kept alive for entire program)
    const slot_name_z = try allocator.dupeZ(u8, parsed_args.slot_name);
    defer allocator.free(slot_name_z);
    const pub_name_z = try allocator.dupeZ(u8, parsed_args.publication_name);
    defer allocator.free(pub_name_z);

    try setup.createSlot(slot_name_z);
    try setup.createPublication(pub_name_z, parsed_args.tables);
    log.info(" ✓ Replication infrastructure ready\n", .{});

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
    log.info("2. Connecting to NATS JetStream...", .{});
    var publisher = try nats_publisher.Publisher.init(allocator, .{
        .url = "nats://localhost:4222",
    });
    defer publisher.deinit();
    try publisher.connect();

    // Create CDC stream with configurable name
    const stream_name_z = try allocator.dupeZ(u8, parsed_args.stream_name);
    defer allocator.free(stream_name_z);

    // Generate subject pattern from stream name to avoid conflicts
    // Convert stream name to lowercase and use as subject prefix
    // Example: CDC_BRIDGE -> "cdc_bridge.>" or CDC_RT -> "cdc_rt.>"
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

    const subject_z = try allocator.dupeZ(u8, lower_subject);
    defer allocator.free(subject_z);

    const stream_config = nats_publisher.StreamConfig{
        .name = stream_name_z,
        .subjects = &.{subject_z},
    };
    try nats_publisher.createStream(
        publisher.js.?,
        allocator,
        stream_config,
    );
    log.info("✓ NATS JetStream connected\n", .{});

    // Make publisher available to HTTP server for stream management
    http_srv.nats_publisher = &publisher;

    // Initialize batch publisher (always enabled)
    const batch_config = batch_publisher.BatchConfig{
        .max_events = 100,
        .max_wait_ms = 50,
        .max_payload_bytes = 64 * 1024,
    };
    var batch_pub = batch_publisher.BatchPublisher.init(allocator, &publisher, batch_config);
    defer batch_pub.deinit();
    log.info("✓ Batch publishing enabled (max 100 events or 50ms or 64KB)\n", .{});

    // 4. Get current LSN to skip historical data
    log.info(" 3. Getting current LSN position...", .{});
    const current_lsn = try wal_monitor.getCurrentLSN(allocator, &pg_config);
    defer allocator.free(current_lsn);
    log.info("Current LSN: {s}\n", .{current_lsn});

    // 5. Connect to replication stream starting from current LSN
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
    log.info(" ✓ WAL replication stream started from LSN {s}\n", .{current_lsn});

    // Mark as connected in metrics
    metrics.setConnected(true);

    // 5. Stream CDC events to NATS
    // This bridge waits for PostgreSQL events generated by the Elixir producer
    // Run the Elixir producer to generate INSERT/UPDATE/DELETE events
    log.info("5. Streaming events from PostgreSQL to NATS...\n", .{});
    log.info("   Subject pattern: {s}\n", .{lower_subject});

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
    var last_ack_lsn: u64 = 0; // Track last acknowledged LSN for keepalives
    var last_keepalive_time = std.time.timestamp(); // Track last keepalive sent
    const keepalive_interval_seconds: i64 = 30; // Send keepalive every 30 seconds

    // Status update batching to reduce PostgreSQL round trips
    var messages_since_ack: u32 = 0; // Count messages since last ack
    var last_status_update_time = std.time.timestamp();
    const status_update_interval_seconds: i64 = 10; // Send status update every 10 seconds
    const status_update_message_count: u32 = 1000; // Or after 1000 messages

    // NATS async publish flushing
    var last_nats_flush_time = std.time.timestamp();
    const nats_flush_interval_seconds: i64 = 5; // Flush NATS async publishes every 5 seconds

    // Periodic structured metric logging for Grafana Alloy/Loki
    var last_metric_log_time = std.time.timestamp();
    const metric_log_interval_seconds: i64 = 15; // Log metrics every 15 seconds

    // decoderbufs doesn't need relation_map - table names are included in each message

    // Run until graceful shutdown signal received
    while (!should_stop.load(.seq_cst)) {
        if (pg_stream.receiveMessage()) |maybe_msg| {
            if (maybe_msg) |wal_msg_val| {
                var wal_msg = wal_msg_val;
                defer wal_msg.deinit(allocator);

                // Track processing time
                const start_time = std.time.microTimestamp();

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
                    // Record processing time for keepalive
                    const end_time = std.time.microTimestamp();
                    const processing_time = @as(u64, @intCast(end_time - start_time));
                    metrics.recordProcessingTime(processing_time);
                    continue; // Don't process keepalives further
                }

                // Parse and publish pgoutput messages
                if (wal_msg.type == .xlogdata and wal_msg.payload.len > 0) {
                    // Use arena allocator for all parsing and temporary allocations
                    // This reduces 10-15 allocations per event to just 1
                    var arena = std.heap.ArenaAllocator.init(allocator);
                    defer arena.deinit(); // Frees all parsing + temp allocations at once
                    const arena_allocator = arena.allocator();

                    // Parse messages with arena allocator
                    // Relations use main allocator since they persist in the map
                    // Parse decoderbufs protobuf message
                    var parser = decoderbufs_parser.Parser.init(arena_allocator, wal_msg.payload);
                    if (parser.parse()) |row_msg| {
                        var row = row_msg;
                        defer row.deinit(arena_allocator);

                        // Get table name and operation
                        const table_name = row.table orelse continue;
                        const op = row.op orelse continue;
                        const op_str = decoderbufs_parser.opToString(op);

                        // Skip BEGIN/COMMIT operations
                        if (op == .BEGIN or op == .COMMIT) {
                            if (op == .BEGIN) {
                                log.info("BEGIN: xid={?d}", .{row.transaction_id});
                            } else {
                                log.info("COMMIT: time={?d}", .{row.commit_time});
                            }
                            continue;
                        }

                        log.info("{s}: {s}", .{ op_str, table_name });

                        // Log column values for debugging
                        if (row.new_tuple.items.len > 0) {
                            log.debug("  Columns:", .{});
                            for (row.new_tuple.items) |datum| {
                                if (datum.column_name) |col_name| {
                                    const val_str = try decoderbufs_parser.datumToString(arena_allocator, datum);
                                    log.debug("    {s} = {s}", .{ col_name, val_str });
                                }
                            }
                        }

                        // Build subject
                        const op_lower = switch (op) {
                            .INSERT => "insert",
                            .UPDATE => "update",
                            .DELETE => "delete",
                            else => "unknown",
                        };

                        const subject = try std.fmt.allocPrintSentinel(
                            arena_allocator,
                            "{s}.{s}.{s}",
                            .{ subject_prefix, table_name, op_lower },
                            0,
                        );

                        // Generate message ID from WAL LSN
                        const msg_id = try std.fmt.allocPrint(
                            arena_allocator,
                            "{x}-{s}-{s}",
                            .{ wal_msg.wal_end, table_name, op_lower },
                        );

                        // Add to batch publisher (TODO: add column data decoding for protobuf)
                        try batch_pub.addEvent(subject, table_name, op_str, msg_id, null, wal_msg.wal_end);
                        cdc_events += 1;
                        metrics.incrementCdcEvents();
                        log.info("  → Published to NATS: {s} (msg_id: {s})", .{ subject, msg_id });
                    } else |err| {
                        log.warn("Failed to parse decoderbufs message: {}", .{err});
                    }
                }

                // Track the latest WAL position we've received
                if (wal_msg.wal_end > 0) {
                    messages_since_ack += 1;
                    metrics.updateLsn(wal_msg.wal_end);
                }

                // Get the last LSN confirmed by NATS (after successful flush)
                const confirmed_lsn = batch_pub.getLastConfirmedLsn();

                // Send buffered status update if we hit time or message threshold
                // Only ACK up to the LSN that NATS has confirmed
                const now = std.time.timestamp();
                if (confirmed_lsn > last_ack_lsn and
                    (messages_since_ack >= status_update_message_count or
                        now - last_status_update_time >= status_update_interval_seconds))
                {
                    try pg_stream.sendStatusUpdate(confirmed_lsn);
                    log.debug("ACKed to PostgreSQL: LSN {x} (NATS confirmed)", .{confirmed_lsn});
                    last_ack_lsn = confirmed_lsn;
                    messages_since_ack = 0;
                    last_status_update_time = now;
                    last_keepalive_time = now; // Reset keepalive timer
                }

                // Record processing time
                const end_time = std.time.microTimestamp();
                const processing_time = @as(u64, @intCast(end_time - start_time));
                metrics.recordProcessingTime(processing_time);
            } else {
                // No message available - check if we need to send pending acks or keepalive
                const now = std.time.timestamp();

                // Get the last LSN confirmed by NATS
                const confirmed_lsn = batch_pub.getLastConfirmedLsn();

                // Flush pending status updates if time threshold reached
                if (confirmed_lsn > last_ack_lsn and now - last_status_update_time >= status_update_interval_seconds) {
                    try pg_stream.sendStatusUpdate(confirmed_lsn);
                    log.debug("ACKed to PostgreSQL on idle: LSN {x} (NATS confirmed)", .{confirmed_lsn});
                    last_ack_lsn = confirmed_lsn;
                    messages_since_ack = 0;
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

                // Check if batch needs time-based flush
                if (batch_pub.shouldFlushByTime()) {
                    _ = try batch_pub.flush();
                }

                // Flush NATS async publishes periodically
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
                    log.info("METRICS uptime={d} wal_messages={d} cdc_events={d} lsn={s} connected={d} reconnects={d} lag_bytes={d} slot_active={d} processing_time_us={d}", .{
                        snap.uptime_seconds,
                        snap.wal_messages_received,
                        snap.cdc_events_published,
                        snap.current_lsn_str,
                        if (snap.is_connected) @as(u8, 1) else @as(u8, 0),
                        snap.reconnect_count,
                        snap.wal_lag_bytes,
                        if (snap.slot_active) @as(u8, 1) else @as(u8, 0),
                        snap.last_processing_time_us,
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
