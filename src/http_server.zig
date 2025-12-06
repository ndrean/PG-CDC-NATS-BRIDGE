const std = @import("std");
const metrics_mod = @import("metrics.zig");
const nats_publisher = @import("nats_publisher.zig");

pub const log = std.log.scoped(.http_server);

/// Simple HTTP server for health checks and basic control
pub const Server = struct {
    allocator: std.mem.Allocator,
    address: std.net.Address,
    should_stop: *std.atomic.Value(bool),
    metrics: ?*metrics_mod.Metrics,
    nats_publisher: ?*nats_publisher.Publisher,

    pub fn init(
        allocator: std.mem.Allocator,
        port: u16,
        should_stop: *std.atomic.Value(bool),
        metrics: ?*metrics_mod.Metrics,
        nats_pub: ?*nats_publisher.Publisher,
    ) !Server {
        return Server{
            .allocator = allocator,
            .address = try std.net.Address.parseIp4("0.0.0.0", port),
            .should_stop = should_stop,
            .metrics = metrics,
            .nats_publisher = nats_pub,
        };
    }

    /// Run the HTTP server (in a separate thread)
    pub fn run(self: *Server) !void {
        var server = try self.address.listen(.{
            .reuse_address = true,
        });
        defer server.deinit();

        log.info("✅ HTTP server listening on http://0.0.0.0:{d}", .{self.address.getPort()});
        log.info("ℹ️ Available endpoints:", .{});
        log.info("  GET  /health         - Health check", .{});
        log.info("  GET  /status         - Bridge status (JSON)", .{});
        log.info("  GET  /metrics        - Prometheus metrics", .{});
        log.info("  POST /shutdown       - Graceful shutdown", .{});
        log.info("  GET  /streams/info   - Get NATS stream info", .{});

        while (!self.should_stop.load(.seq_cst)) {
            // Poll with timeout to allow checking shutdown flag
            var poll_fds = [_]std.posix.pollfd{
                .{
                    .fd = server.stream.handle,
                    .events = std.posix.POLL.IN,
                    .revents = 0,
                },
            };

            // Poll with 100ms timeout
            const ready = std.posix.poll(&poll_fds, 100) catch |err| {
                log.err("⚠️ Poll error: {}", .{err});
                std.Thread.sleep(100 * std.time.ns_per_ms);
                continue;
            };

            if (ready == 0) {
                // Timeout - check shutdown flag again
                continue;
            }

            const conn = server.accept() catch |err| {
                log.err("🔴 Failed to accept connection: {}", .{err});
                continue;
            };
            defer conn.stream.close();

            self.handleRequest(conn.stream) catch |err| {
                log.warn("⚠️ Error handling request: {}", .{err});
            };
        }

        log.info("👋 HTTP server stopped", .{});
    }

    fn handleRequest(self: *Server, stream: std.net.Stream) !void {
        var buffer: [2048]u8 = undefined;

        // Read request line
        const bytes_read = try stream.read(&buffer);
        if (bytes_read == 0) return;

        const request = buffer[0..bytes_read];

        // Parse method and path (simple HTTP parser)
        var lines = std.mem.splitScalar(u8, request, '\n');
        const first_line = lines.next() orelse return;

        var parts = std.mem.splitScalar(u8, first_line, ' ');
        const method = std.mem.trim(u8, parts.next() orelse return, &std.ascii.whitespace);
        const path = std.mem.trim(u8, parts.next() orelse return, &std.ascii.whitespace);

        log.debug("{s} {s}", .{ method, path });

        // Route
        if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/health")) {
            try self.handleHealth(stream);
        } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/status")) {
            try self.handleStatus(stream);
        } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/metrics")) {
            try self.handleMetrics(stream);
        } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/shutdown")) {
            try self.handleShutdown(stream);
        } else if (std.mem.eql(u8, method, "GET") and std.mem.startsWith(u8, path, "/streams/info")) {
            try self.handleStreamInfo(stream, path);
            // } else if (std.mem.eql(u8, method, "POST") and std.mem.startsWith(u8, path, "/streams/create")) {
            // try self.handleStreamCreate(stream, path);
            // } else if (std.mem.eql(u8, method, "POST") and std.mem.startsWith(u8, path, "/streams/delete")) {
            // try self.handleStreamDelete(stream, path);
            // } else if (std.mem.eql(u8, method, "POST") and std.mem.startsWith(u8, path, "/streams/purge")) {
            // try self.handleStreamPurge(stream, path);
        } else {
            try self.handleNotFound(stream);
        }
    }

    fn sendResponse(stream: std.net.Stream, status: []const u8, content_type: []const u8, body: []const u8) !void {
        var response_buffer: [8192]u8 = undefined;
        const response = try std.fmt.bufPrint(&response_buffer,
            \\HTTP/1.1 {s}
            \\Content-Type: {s}
            \\Content-Length: {d}
            \\Connection: close
            \\
            \\{s}
        , .{ status, content_type, body.len, body });

        _ = try stream.writeAll(response);
    }

    fn handleHealth(self: *Server, stream: std.net.Stream) !void {
        _ = self;
        try sendResponse(stream, "200 OK", "application/json", "{\"status\":\"ok\"}\n");
    }

    fn handleStatus(self: *Server, stream: std.net.Stream) !void {
        if (self.metrics) |m| {
            // Get metrics snapshot
            const snap = try m.snapshot(self.allocator);
            defer self.allocator.free(snap.current_lsn_str);

            // Format JSON with real metrics
            var buffer: [4096]u8 = undefined;
            const status_json = try std.fmt.bufPrint(&buffer,
                \\{{
                \\  "status": "{s}",
                \\  "uptime_seconds": {d},
                \\  "wal_messages_received": {d},
                \\  "cdc_events_published": {d},
                \\  "current_lsn": "{s}",
                \\  "is_connected": {s},
                \\  "pg_reconnect_count": {d},
                \\  "nats_reconnect_count": {d},
                \\  "slot_active": {s},
                \\  "wal_lag_bytes": {d},
                \\  "wal_lag_mb": {d}
                \\}}
                \\
            , .{
                if (snap.is_connected) "connected" else "disconnected",
                snap.uptime_seconds,
                snap.wal_messages_received,
                snap.cdc_events_published,
                snap.current_lsn_str,
                if (snap.is_connected) "true" else "false",
                snap.reconnect_count,
                snap.nats_reconnect_count,
                if (snap.slot_active) "true" else "false",
                snap.wal_lag_bytes,
                snap.wal_lag_bytes / (1024 * 1024), // Convert to MB
            });

            try sendResponse(stream, "200 OK", "application/json", status_json);
        } else {
            // Fallback if no metrics available
            try sendResponse(stream, "200 OK", "application/json", "{\"status\":\"no_metrics\"}\n");
        }
    }

    fn handleMetrics(self: *Server, stream: std.net.Stream) !void {
        if (self.metrics) |m| {
            // Get metrics snapshot
            const snap = try m.snapshot(self.allocator);
            defer self.allocator.free(snap.current_lsn_str);

            // Format Prometheus text format
            var buffer: [8192]u8 = undefined;
            const prom_metrics = try std.fmt.bufPrint(&buffer,
                \\# HELP bridge_uptime_seconds Time since bridge started
                \\# TYPE bridge_uptime_seconds gauge
                \\bridge_uptime_seconds {d}
                \\
                \\# HELP bridge_wal_messages_received_total Total WAL messages received from PostgreSQL
                \\# TYPE bridge_wal_messages_received_total counter
                \\bridge_wal_messages_received_total {d}
                \\
                \\# HELP bridge_cdc_events_published_total Total CDC events published to NATS
                \\# TYPE bridge_cdc_events_published_total counter
                \\bridge_cdc_events_published_total {d}
                \\
                \\# HELP bridge_last_ack_lsn Last acknowledged LSN position
                \\# TYPE bridge_last_ack_lsn gauge
                \\bridge_last_ack_lsn {d}
                \\
                \\# HELP bridge_connected Connection status (1=connected, 0=disconnected)
                \\# TYPE bridge_connected gauge
                \\bridge_connected {d}
                \\
                \\# HELP bridge_pg_reconnects_total Total number of PostgreSQL reconnections
                \\# TYPE bridge_pg_reconnects_total counter
                \\bridge_pg_reconnects_total {d}
                \\
                \\# HELP bridge_nats_reconnects_total Total number of NATS reconnections
                \\# TYPE bridge_nats_reconnects_total counter
                \\bridge_nats_reconnects_total {d}
                \\
                \\# HELP bridge_slot_active Replication slot active status (1=active, 0=inactive)
                \\# TYPE bridge_slot_active gauge
                \\bridge_slot_active {d}
                \\
                \\# HELP bridge_wal_lag_bytes Bytes of WAL retained for replication slot
                \\# TYPE bridge_wal_lag_bytes gauge
                \\bridge_wal_lag_bytes {d}
                \\
            , .{
                snap.uptime_seconds,
                snap.wal_messages_received,
                snap.cdc_events_published,
                snap.last_ack_lsn,
                if (snap.is_connected) @as(u8, 1) else @as(u8, 0),
                snap.reconnect_count,
                snap.nats_reconnect_count,
                if (snap.slot_active) @as(u8, 1) else @as(u8, 0),
                snap.wal_lag_bytes,
            });

            try sendResponse(stream, "200 OK", "text/plain; version=0.0.4", prom_metrics);
        } else {
            // Empty metrics if not available
            try sendResponse(stream, "200 OK", "text/plain; version=0.0.4", "# No metrics available\n");
        }
    }

    fn handleShutdown(self: *Server, stream: std.net.Stream) !void {
        log.info("👋 Shutdown requested via HTTP", .{});

        // Set shutdown flag
        self.should_stop.store(true, .seq_cst);

        try sendResponse(stream, "200 OK", "text/plain", "Shutdown initiated\n");
    }

    fn handleNotFound(self: *Server, stream: std.net.Stream) !void {
        _ = self;
        try sendResponse(stream, "404 Not Found", "text/plain", "Not Found\n");
    }

    // NATS Stream Management Endpoints

    fn parseQueryParam(path: []const u8, param_name: []const u8) ?[]const u8 {
        const query_start = std.mem.indexOf(u8, path, "?") orelse return null;
        const query = path[query_start + 1 ..];

        var params = std.mem.splitScalar(u8, query, '&');
        while (params.next()) |param| {
            if (std.mem.indexOf(u8, param, "=")) |eq_pos| {
                const key = param[0..eq_pos];
                const value = param[eq_pos + 1 ..];
                if (std.mem.eql(u8, key, param_name)) {
                    return value;
                }
            }
        }
        return null;
    }

    fn handleStreamInfo(self: *Server, stream: std.net.Stream, path: []const u8) !void {
        const stream_name = parseQueryParam(path, "stream") orelse {
            try sendResponse(stream, "400 Bad Request", "text/plain", "⚠️ Missing 'stream' parameter\n");
            return;
        };

        if (self.nats_publisher) |publisher| {
            // Get stream info from NATS
            const info = publisher.getStreamInfo(stream_name) catch |err| {
                var err_buf: [256]u8 = undefined;
                const err_msg = try std.fmt.bufPrint(&err_buf, "Failed to get stream info: {}\n", .{err});
                try sendResponse(stream, "500 Internal Server Error", "text/plain", err_msg);
                return;
            };
            defer self.allocator.free(info);

            try sendResponse(stream, "200 OK", "application/json", info);
        } else {
            try sendResponse(stream, "503 Service Unavailable", "text/plain", "⚠️ NATS publisher not available\n");
        }
    }

    // fn handleStreamCreate(self: *Server, stream: std.net.Stream, path: []const u8) !void {
    //     const stream_name = parseQueryParam(path, "stream") orelse {
    //         try sendResponse(stream, "400 Bad Request", "text/plain", "⚠️ Missing 'stream' parameter\n");
    //         return;
    //     };

    //     const subjects = parseQueryParam(path, "subjects") orelse "cdc.>";

    //     if (self.nats_publisher) |publisher| {
    //         // Convert query param strings to null-terminated for C API
    //         const stream_name_z = try self.allocator.dupeZ(u8, stream_name);
    //         defer self.allocator.free(stream_name_z);

    //         const subjects_z = try self.allocator.dupeZ(u8, subjects);
    //         defer self.allocator.free(subjects_z);

    //         // Create stream config
    //         const config = nats_publisher.StreamConfig{
    //             .name = stream_name_z,
    //             .subjects = &.{subjects_z},
    //         };

    //         // Call standalone createStream function
    //         nats_publisher.createStream(publisher.js.?, self.allocator, config) catch |err| {
    //             var err_buf: [256]u8 = undefined;
    //             const err_msg = try std.fmt.bufPrint(&err_buf, "Failed to create stream: {}\n", .{err});
    //             try sendResponse(stream, "500 Internal Server Error", "text/plain", err_msg);
    //             return;
    //         };

    //         var response_buf: [256]u8 = undefined;
    //         const response = try std.fmt.bufPrint(&response_buf, "Stream '{s}' created with subjects: {s}\n", .{ stream_name, subjects });
    //         try sendResponse(stream, "200 OK", "text/plain", response);
    //     } else {
    //         try sendResponse(stream, "503 Service Unavailable", "text/plain", "⚠️ NATS publisher not available\n");
    //     }
    // }

    // fn handleStreamDelete(self: *Server, stream: std.net.Stream, path: []const u8) !void {
    //     const stream_name = parseQueryParam(path, "stream") orelse {
    //         try sendResponse(stream, "400 Bad Request", "text/plain", "Missing 'stream' parameter\n");
    //         return;
    //     };

    //     if (self.nats_publisher) |publisher| {
    //         publisher.deleteStream(stream_name) catch |err| {
    //             var err_buf: [256]u8 = undefined;
    //             const err_msg = try std.fmt.bufPrint(&err_buf, "⚠️ Failed to delete stream: {}\n", .{err});
    //             try sendResponse(stream, "500 Internal Server Error", "text/plain", err_msg);
    //             return;
    //         };

    //         var response_buf: [256]u8 = undefined;
    //         const response = try std.fmt.bufPrint(&response_buf, "Stream '{s}' deleted\n", .{stream_name});
    //         try sendResponse(stream, "200 OK", "text/plain", response);
    //     } else {
    //         try sendResponse(stream, "503 Service Unavailable", "text/plain", "⚠️ NATS publisher not available\n");
    //     }
    // }

    fn handleStreamPurge(self: *Server, stream: std.net.Stream, path: []const u8) !void {
        const stream_name = parseQueryParam(path, "stream") orelse {
            try sendResponse(stream, "400 Bad Request", "text/plain", "⚠️ Missing 'stream' parameter\n");
            return;
        };

        if (self.nats_publisher) |publisher| {
            publisher.purgeStream(stream_name) catch |err| {
                var err_buf: [256]u8 = undefined;
                const err_msg = try std.fmt.bufPrint(&err_buf, "⚠️ Failed to purge stream: {}\n", .{err});
                try sendResponse(stream, "500 Internal Server Error", "text/plain", err_msg);
                return;
            };

            var response_buf: [256]u8 = undefined;
            const response = try std.fmt.bufPrint(&response_buf, "Stream '{s}' purged\n", .{stream_name});
            try sendResponse(stream, "200 OK", "text/plain", response);
        } else {
            try sendResponse(stream, "503 Service Unavailable", "text/plain", "⚠️ NATS publisher not available\n");
        }
    }
};
