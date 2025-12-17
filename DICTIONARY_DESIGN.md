# Zstd Dictionary Management Design

## Overview

Implement complete dictionary management for zstd compression in snapshots:
- Store dictionaries in NATS KV bucket
- Include dictionary_id in snapshot metadata
- Enable consumers to fetch and use dictionaries for decompression

## Architecture

### NATS KV Buckets

1. **`schemas`** (existing) - Table schemas
2. **`dictionaries`** (new) - Zstd compression dictionaries per table

### Dictionary Lifecycle

**Bridge (Producer) Side:**
1. On startup (if `--zstd` enabled):
   - Create `dictionaries` KV bucket if needed
   - Upload dictionaries from `dictionaries/` directory
   - Format: `dictionaries/{table}.zstd.dict` → KV key: `{table}_dict`

2. On snapshot generation:
   - Look up dictionary ID for table
   - Include `dictionary_id` and `compression_enabled` in:
     - `init.start.{table}` (snapshot start notification)
     - `init.meta.{table}` (snapshot completion metadata)

**Consumer (Elixir) Side:**
1. On initialization:
   - Fetch schemas from `schemas` KV bucket
   - Check if compression enabled (from env or config)
   - If enabled, fetch dictionaries from `dictionaries` KV bucket
   - Cache dictionaries in memory

2. On snapshot start (`init.start.{table}`):
   - Parse `compression_enabled` and `dictionary_id`
   - Prepare zstd decompressor with dictionary

3. On chunk received:
   - Decompress using dictionary if `compression_enabled`
   - Decode MessagePack
   - Insert into database

## Implementation

### Zig Bridge Changes

#### src/config.zig
```zig
pub const Snapshot = struct {
    // ... existing fields ...

    /// NATS KV bucket name for schemas
    pub const kv_bucket_schemas = "schemas";

    /// NATS KV bucket name for zstd dictionaries
    pub const kv_bucket_dictionaries = "dictionaries";
};
```

#### src/snapshot_listener.zig

**New Functions:**
```zig
/// Generate dictionary ID from table name
fn generateDictionaryId(allocator: std.mem.Allocator, table_name: []const u8) ![]const u8 {
    return try std.fmt.allocPrint(allocator, "{s}_dict", .{table_name});
}

/// Upload zstd dictionary to NATS KV store
fn uploadDictionary(
    allocator: std.mem.Allocator,
    js_ctx: *anyopaque,
    table_name: []const u8,
    dictionary_path: []const u8,
) ![]const u8 {
    // Read from dictionaries/{table}.zstd.dict
    // Upload to KV bucket "dictionaries" with key "{table}_dict"
    // Return dictionary_id
}

/// Initialize dictionaries KV bucket and upload all dictionaries
pub fn initializeDictionaries(
    allocator: std.mem.Allocator,
    js_ctx: *anyopaque,
    monitored_tables: []const []const u8,
) !void {
    // Create bucket if doesn't exist
    // For each table, upload dictionary if file exists
}
```

**Updated Functions:**
```zig
fn publishSnapshotStart(
    // ... existing params ...
    dictionary_id: ?[]const u8,
    compression_enabled: bool,
) !void {
    // Add fields to start notification:
    // - compression_enabled: bool
    // - dictionary_id: string (optional)
}

fn publishSnapshotMetadata(
    // ... existing params ...
    dictionary_id: ?[]const u8,
    compression_enabled: bool,
) !void {
    // Add fields to metadata:
    // - compression_enabled: bool
    // - dictionary_id: string (optional)
}

fn generateIncrementalSnapshot(
    // ... existing params ...
    js_ctx: *anyopaque, // NEW: for KV access
) !void {
    // Get dictionary_id for table
    const dict_id = if (enable_compression) blk: {
        break :blk try generateDictionaryId(allocator, table_name);
    } else null;
    defer if (dict_id) |id| allocator.free(id);

    // Pass dict_id to publishSnapshotStart and publishSnapshotMetadata
}
```

#### src/bridge.zig

**On startup (after NATS connection established):**
```zig
// Initialize dictionaries KV bucket if compression enabled
if (parsed_args.enable_compression) {
    snapshot_listener.initializeDictionaries(
        allocator,
        js_ctx,
        monitored_tables,
    ) catch |err| {
        log.warn("Failed to initialize dictionaries: {}", .{err});
        // Continue without dictionaries (fallback to no-dict compression)
    };
}
```

### Elixir Consumer Changes

#### consumer/lib/consumer/init_consumer.ex

**Initialization:**
```elixir
def init(%Gnat.Jetstream.API.Consumer{} = consumer_config) do
  with :ok <- ensure_jetstream_enabled(),
       :ok <- ensure_stream_exists(consumer_config),
       :ok <- create_consumer(consumer_config),
       {:ok, schemas} <- fetch_schemas_from_kv(),
       {:ok, dictionaries} <- maybe_fetch_dictionaries(),
       :ok <- maybe_run_migration(schemas),
       :ok <- check_and_request_snapshots_if_needed(stream_name) do
    # Store dictionaries in state or persistent_term
    :persistent_term.put(:dictionaries, dictionaries)

    {:ok, schemas,
     connection_name: :gnat, stream_name: stream_name, consumer_name: consumer_name}
  end
end

defp maybe_fetch_dictionaries do
  if System.get_env("COMPRESSION") == "true" do
    tables = System.get_env("TABLES") |> String.split(",") |> Enum.map(&String.trim/1)

    dictionaries = Enum.reduce(tables, %{}, fn table_name, acc ->
      dict_id = "#{table_name}_dict"

      case Gnat.Jetstream.API.KV.get_value(:gnat, "dictionaries", dict_id) do
        dict_data when is_binary(dict_data) ->
          Logger.info("[INIT Consumer] Fetched dictionary for '#{table_name}' (#{byte_size(dict_data)} bytes)")
          Map.put(acc, table_name, dict_data)

        _ ->
          Logger.warn("[INIT Consumer] No dictionary found for '#{table_name}'")
          acc
      end
    end)

    {:ok, dictionaries}
  else
    {:ok, %{}}
  end
end
```

**Handle snapshot start:**
```elixir
def handle_message(%{topic: "init.start." <> table} = message, state) do
  decoded = Msgpax.unpack!(message.body)

  snapshot_lsn = decoded["lsn"]
  compression_enabled = decoded["compression_enabled"]
  dictionary_id = decoded["dictionary_id"]

  # Store watermark and compression info
  :persistent_term.put({:snapshot_lsn, table}, snapshot_lsn)
  :persistent_term.put({:compression_enabled, table}, compression_enabled)
  :persistent_term.put({:dictionary_id, table}, dictionary_id)

  Logger.info("[INIT Consumer] Snapshot starting for #{table} (LSN watermark: #{snapshot_lsn}, compression: #{compression_enabled})")

  {:ack, state}
end
```

**Handle snapshot chunk:**
```elixir
def handle_message(message, state) do
  # Check if compression enabled
  compression_enabled = message.topic
    |> String.split(".")
    |> Enum.at(2)  # Extract table name
    |> then(&:persistent_term.get({:compression_enabled, &1}, false))

  # Decompress if needed
  body = if compression_enabled do
    table = String.split(message.topic, ".") |> Enum.at(2)
    dictionaries = :persistent_term.get(:dictionaries, %{})
    dictionary = Map.get(dictionaries, table)

    # Decompress using zstd with dictionary
    :zstd.decompress(message.body, dictionary)
  else
    message.body
  end

  # Decode MessagePack
  decoded = Msgpax.unpack!(body)

  # Process snapshot data...
end
```

## File Structure

```
bridge/
├── dictionaries/          # Local dictionary files
│   ├── users.zstd.dict
│   ├── orders.zstd.dict
│   └── products.zstd.dict
├── src/
│   ├── config.zig        # KV bucket names
│   ├── snapshot_listener.zig  # Dictionary upload logic
│   └── bridge.zig        # Initialize on startup
└── consumer/
    └── lib/consumer/
        └── init_consumer.ex  # Fetch & use dictionaries
```

## Consumer Flow

```
1. Consumer starts
   ├─> Fetch schemas from KV
   ├─> Fetch dictionaries from KV (if compression enabled)
   └─> Store in :persistent_term

2. Snapshot requested
   └─> pub("snapshot.request.users", "")

3. Bridge receives request
   ├─> BEGIN REPEATABLE READ
   ├─> Get LSN watermark
   ├─> Publish init.start.users {lsn, compression_enabled, dictionary_id}
   ├─> COPY chunks (compressed)
   └─> Publish init.meta.users {batch_count, compression_enabled, dictionary_id}

4. Consumer receives init.start.users
   ├─> Store LSN watermark
   ├─> Store compression_enabled flag
   └─> Prepare decompressor with dictionary

5. Consumer receives chunks
   ├─> Decompress using dictionary
   ├─> Decode MessagePack
   └─> Insert into DB

6. Consumer receives CDC events
   └─> Filter: ignore if event.lsn < snapshot_lsn
```

## Benefits

1. **Efficient Compression**: ~94% reduction with dictionaries vs ~70% without
2. **Automatic Distribution**: Dictionaries distributed via NATS KV, no manual sync
3. **Version Control**: KV revisions track dictionary updates
4. **Graceful Degradation**: Works without dictionaries (lower compression ratio)
5. **Per-Table Dictionaries**: Optimized compression for each table's schema

## Next Steps

1. ✅ Add KV bucket config constants
2. ✅ Update snapshot metadata structures
3. ✅ Add dictionary upload functions
4. ⏳ Wire up dictionary initialization on bridge startup
5. ⏳ Update snapshot generation to include dictionary_id
6. ⏳ Update all publishSnapshotStart/Meta call sites
7. ⏳ Implement Elixir consumer dictionary fetching
8. ⏳ Implement Elixir consumer zstd decompression
9. ⏳ Test end-to-end with compression
10. ⏳ Document dictionary training process

## Implementation Status

**Completed:**
- Config updates with KV bucket names
- Function signatures updated for dictionary_id and compression_enabled
- Dictionary upload helper functions
- Design documentation

**TODO:**
- Pass js_ctx to generateIncrementalSnapshot
- Call uploadDictionary or get cached dictionary_id
- Update all call sites with new parameters
- Consumer-side implementation
- Testing
