//! Centralized configuration for CDC Bridge
//!
//! This module consolidates all configuration constants used throughout the application.
//! Instead of hardcoded values scattered across files, all tunables are defined here.

const std = @import("std");

/// PostgreSQL connection and replication configuration
pub const Postgres = struct {
    /// Default replication slot name
    pub const default_slot_name = "cdc_slot";

    /// Default publication name
    pub const default_publication_name = "cdc_pub";

    /// Connection timeout in milliseconds
    pub const connection_timeout_ms = 5000;

    /// Replication connection receive timeout (milliseconds)
    /// Set to 0 for blocking mode
    pub const replication_receive_timeout_ms = 0;

    /// WAL sender timeout (PostgreSQL server-side, configured in docker-compose.yml)
    /// This is just for documentation - actual value set in PostgreSQL config
    pub const wal_sender_timeout_seconds = 300; // 5 minutes

    /// Maximum WAL retention size (10GB)
    pub const max_wal_retention_gb = 10;
};

/// NATS JetStream configuration
pub const Nats = struct {
    /// Default NATS server URL
    pub const default_port = 4222;
    pub const default_url = "nats://127.0.0.1:4222";

    /// Maximum reconnection attempts (-1 = infinite)
    pub const max_reconnect_attempts = -1;

    /// Wait time between reconnection attempts (milliseconds)
    /// 1s is aggressive but appropriate for CDC (minimize replication lag)
    pub const reconnect_wait_ms = 1000;

    /// Async publish flush timeout (milliseconds)
    /// Must be >= reconnect_wait_ms * max attempts
    pub const flush_timeout_ms = 10_000; // 10 seconds
    pub const nats_flush_interval_seconds = 5; // 5 seconds
    pub const status_update_interval_seconds = 1; // 1 second
    pub const status_update_byte_threshold: u64 = 1024 * 1024; // 1MB

    /// JetStream stream names
    pub const stream_cdc = "CDC";
    pub const stream_schema = "SCHEMA";
    pub const stream_init = "INIT";

    /// Default streams to verify on startup
    pub const default_streams = &[_][]const u8{stream_cdc};

    /// Subject prefixes
    pub const subject_cdc_prefix = "cdc";
    pub const subject_schema_prefix = "schema";
    pub const subject_init_prefix = "init";

    /// CDC subject patterns: "cdc.<table>.<operation>"
    pub const cdc_subject_pattern = "cdc.{s}.{s}";

    /// CDC wildcard subject for subscribing to all CDC events
    pub const cdc_subject_wildcard = "cdc.>";

    /// Schema KV bucket name
    pub const schema_kv_bucket = "schemas";

    pub const publisher_max_wait = 10_000; // 10 seconds

    /// JetStream stream default configuration
    pub const stream_max_msgs = 1_000_000; // Maximum messages per stream
    pub const stream_max_bytes = 1024 * 1024 * 1024; // 1GB maximum stream size
    pub const stream_max_age_ns = 60 * 1_000_000_000; // 1 minute retention in nanoseconds
};

/// HTTP metrics server configuration
pub const Http = struct {
    /// Default HTTP port for metrics endpoint
    pub const default_port = 9090;

    /// HTTP server bind address
    pub const bind_address = "0.0.0.0";

    /// Metrics endpoint path
    pub const metrics_path = "/metrics";

    /// Health check endpoint path
    pub const health_path = "/health";
};

/// Batch publishing configuration
pub const Batch = struct {
    /// Maximum events per batch
    pub const max_events = 500;

    /// Maximum batch age before force flush (milliseconds)
    pub const max_age_ms = 100;

    /// Size of the ring buffer (must be power of 2)
    /// Sized for NATS/PostgreSQL reconnection resilience:
    /// - 32768 slots = ~546ms buffer at 60K events/s
    /// - Absorbs jitter and provides meaningful buffer during reconnection
    /// - NATS reconnect_wait_ms = 1000ms → queue covers 54% of retry interval
    /// - PostgreSQL reconnect_delay = 5000ms
    /// - Memory cost: ~2MB (negligible for production resilience)
    pub const ring_buffer_size = 32768;
    pub const max_payload_bytes = 256 * 1024; // 256KB
};

/// WAL monitoring configuration
pub const WalMonitor = struct {
    /// Default check interval (seconds)
    pub const default_check_interval_seconds = 30;

    /// Warning threshold for WAL lag (bytes)
    pub const warning_threshold_bytes = 512 * 1024 * 1024; // 512MB

    /// Critical threshold for WAL lag (bytes)
    pub const critical_threshold_bytes = 1024 * 1024 * 1024; // 1GB
};

pub const Bridge = struct {
    /// Maximum size of a single CDC message (bytes)
    pub const max_cdc_message_size_bytes = 900 * 1024; // 900KB
    pub const keepalive_interval_seconds = 30;
};

/// Snapshot generation configuration
pub const Snapshot = struct {
    /// PostgreSQL NOTIFY channel for snapshot requests (deprecated - using NATS now)
    pub const notify_channel = "snapshot_request";

    /// Rows per snapshot chunk
    pub const chunk_size = 10_000;

    /// Snapshot ID prefix
    pub const id_prefix = "snap";

    /// Maximum concurrent snapshot requests
    pub const max_concurrent_snapshots = 3;

    /// Snapshot polling interval (milliseconds)
    /// How often to check for new snapshot requests via LISTEN/NOTIFY
    pub const poll_interval_ms = 100;

    /// NATS subject prefix for snapshot requests: "snapshot.request.<table>"
    pub const request_subject_prefix = "snapshot.request.";

    /// NATS subject wildcard for subscribing to all snapshot requests
    pub const request_subject_wildcard = "snapshot.request.>";

    /// NATS subject pattern for data chunks: "init.snap.<table>.<snapshot_id>.<chunk>"
    pub const data_subject_pattern = "init.snap.{s}.{s}.{d}";

    /// NATS subject pattern for metadata: "init.meta.<table>"
    pub const meta_subject_pattern = "init.meta.{s}";

    /// Message ID pattern for data chunks: "init-<table>-<snapshot_id>-<chunk>"
    pub const data_msg_id_pattern = "init-{s}-{s}-{d}";
};

/// Logging and metrics configuration
pub const Metrics = struct {
    /// Metric log interval (seconds)
    /// How often to emit structured metric logs for observability
    pub const log_interval_seconds = 60;

    /// Enable debug logging
    pub const debug_enabled = false;
    pub const metric_log_interval_seconds = 15;
};

/// Reconnection and retry configuration
pub const Retry = struct {
    /// PostgreSQL reconnection delay (seconds)
    pub const pg_reconnect_delay_seconds = 5;

    /// NATS reconnection is handled by NATS client library
    /// See Nats.reconnect_wait_ms and Nats.max_reconnect_attempts
    /// Exponential backoff base (for future use)
    pub const backoff_base_ms = 1000;

    /// Maximum backoff time (for future use)
    pub const max_backoff_ms = 60_000; // 1 minute
};

/// Threading configuration
pub const Threading = struct {
    /// Number of WAL monitor threads
    pub const wal_monitor_threads = 1;

    /// Number of snapshot generator threads
    pub const snapshot_generator_threads = 1;

    /// Number of HTTP server threads
    pub const http_server_threads = 1;

    /// Main loop sleep interval when idle (milliseconds)
    pub const main_loop_sleep_ms = 1;
};

/// Buffer sizes
pub const Buffers = struct {
    /// Subject buffer size (for formatting NATS subjects)
    pub const subject_buffer_size = 128;

    /// Message ID buffer size
    pub const msg_id_buffer_size = 128;

    /// Connection string buffer size
    pub const conninfo_buffer_size = 512;

    /// URL buffer size
    pub const url_buffer_size = 256;
};

/// Get default log level based on environment
pub fn getDefaultLogLevel() std.log.Level {
    const env_log = std.process.getEnvVarOwned(
        std.heap.page_allocator,
        "LOG_LEVEL",
    ) catch null;
    defer if (env_log) |e| std.heap.page_allocator.free(e);

    if (env_log) |level_str| {
        if (std.mem.eql(u8, level_str, "debug")) return .debug;
        if (std.mem.eql(u8, level_str, "info")) return .info;
        if (std.mem.eql(u8, level_str, "warn")) return .warn;
        if (std.mem.eql(u8, level_str, "err")) return .err;
    }

    return .info;
}
