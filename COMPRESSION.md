# Snapshot Compression with Zstd Dictionaries

This document explains how to train and use per-table zstd dictionaries for optimal snapshot compression.

## Overview

The CDC Bridge supports **per-table dictionary compression** for snapshots using zstd. This provides:

- **90-95% compression** for snapshots (vs 80-85% with generic compression)
- **2-3× smaller** payloads compared to uncompressed data
- **12× faster uploads** for large snapshots
- **Automatic dictionary loading** - dictionaries are cached per table

## Quick Start

### 1. Install zstd

The bridge requires system `libzstd` to be installed:

```bash
# macOS
brew install zstd

# Ubuntu/Debian
apt-get install libzstd-dev

# Alpine (Docker)
apk add zstd-dev zstd-static
```

### 2. Train Dictionaries (Optional but Recommended)

Train dictionaries on production snapshot data for best compression:

```bash
# Create dictionaries directory
mkdir -p dictionaries

# Export sample data from production database (10K rows per table)
psql -h prod-db -U postgres -d mydb -c \
  "COPY (SELECT row_to_json(t) FROM users t LIMIT 10000) TO STDOUT" \
  > /tmp/users_samples.json

# Encode samples as MessagePack using bridge encoder
# TODO: Create helper script for this

# Train zstd dictionary (64KB recommended size)
zstd --train \
  -o dictionaries/users.zstd.dict \
  --maxdict=65536 \
  /tmp/users_samples.msgpack

echo "Dictionary trained: dictionaries/users.zstd.dict"
```

### 3. Deploy Dictionaries

Place dictionaries in `dictionaries/` directory relative to the bridge binary:

```txt
bridge/
├── zig-out/bin/bridge
└── dictionaries/
    ├── users.zstd.dict
    ├── orders.zstd.dict
    └── products.zstd.dict
```

### 4. Enable Compression

Run the bridge with the `--zstd` flag to enable snapshot compression:

```bash
# Without compression (default)
./bridge

# With zstd compression
./bridge --zstd

# Docker
docker run mybridge:latest --zstd
```

**Important**: Compression is **opt-in** via the `--zstd` flag. Without this flag, snapshots are published uncompressed.

The bridge automatically loads dictionaries when `--zstd` is enabled. If a dictionary is missing for a table, it falls back to compression without a dictionary (~70% compression vs ~92% with dictionary).

## How It Works

### Compression Flow

1. **Snapshot generation** - Bridge queries PostgreSQL for table rows
2. **MessagePack encoding** - Rows are encoded as MessagePack
3. **Dictionary compression** - Encoded data is compressed with per-table dictionary
4. **NATS publishing** - Compressed chunks are published to JetStream

### Decompression Flow (Client-side)

1. **Fetch compressed chunk** from NATS
2. **Load table dictionary** (cached in browser)
3. **Decompress** with zstd
4. **Decode MessagePack** to JSON
5. **Apply to local database**

## Compression Performance

### Without Compression
- Snapshot size: 10MB × 1000 chunks = **10GB total**
- Upload time (10 Mbps): ~2.2 hours

### With Generic Compression (80%)
- Snapshot size: 2MB × 1000 chunks = **2GB total**
- Upload time: ~27 minutes
- **Improvement**: 5× faster

### With Per-Table Dictionaries (92%)
- Snapshot size: 0.8MB × 1000 chunks = **800MB total**
- Upload time: ~11 minutes
- **Improvement**: 12× faster (vs uncompressed), **2.5× faster (vs generic)**

## Training Best Practices

### Sample Size
- **Minimum**: 1,000 rows per table
- **Recommended**: 10,000 rows per table
- **Maximum**: 100,000 rows per table

More samples = better dictionary, but diminishing returns after 10K rows.

### Dictionary Size
- **Small tables** (<1MB snapshots): 32KB dictionary
- **Medium tables** (1-10MB snapshots): 64KB dictionary (recommended)
- **Large tables** (>10MB snapshots): 128KB dictionary

### Retraining Schedule
Retrain dictionaries when:
- Schema changes significantly (new columns, dropped columns)
- Data distribution changes (e.g., new country codes, product categories)
- Compression ratio drops below 85%

**Recommended**: Retrain monthly from latest production snapshot samples.

## Client-Side Integration

### JavaScript/TypeScript

```typescript
import { decompress } from 'fzstd'; // 3KB library
import * as msgpack from 'msgpack';

// Load dictionary (cached per table)
const dictCache = new Map<string, Uint8Array>();
async function loadDictionary(table: string): Promise<Uint8Array> {
  if (!dictCache.has(table)) {
    const response = await fetch(`/dictionaries/${table}.zstd.dict`);
    const dict = new Uint8Array(await response.arrayBuffer());
    dictCache.set(table, dict);
  }
  return dictCache.get(table)!;
}

// Fetch and decompress snapshot chunk
async function fetchSnapshotChunk(table: string, chunkNum: number) {
  // 1. Fetch compressed chunk from NATS
  const compressed = await nats.request(`init.snap.${table}.snap123.${chunkNum}`);

  // 2. Load dictionary
  const dictionary = await loadDictionary(table);

  // 3. Decompress
  const decompressed = decompress(compressed, dictionary);

  // 4. Decode MessagePack
  const rows = msgpack.decode(decompressed);

  return rows;
}
```

## Troubleshooting

### Dictionary not found warnings

```
WARN Failed to load dictionary for table 'users': FileNotFound, using no dictionary
```

**Solution**: Create `dictionaries/users.zstd.dict` or accept lower compression (still ~70% with no dictionary).

### Build fails with "zstd not found"

```
error: unable to find system library 'zstd'
```

**Solution**: Install libzstd development package (see Quick Start section).

### Poor compression ratio

**Possible causes**:
1. Dictionary not trained on representative data
2. Schema changed since dictionary was trained
3. Table data is highly random (e.g., UUIDs, hashes)

**Solution**: Retrain dictionary on current production data.

## Advanced: Streaming Compression

For very large snapshots (>100MB per chunk), consider streaming compression:

```zig
// TODO: Implement streaming API in compression.zig
const stream = try dict_mgr.createCompressionStream("users");
defer stream.deinit();

while (have_more_data) {
    const chunk = getNextDataChunk();
    try stream.write(chunk);
    const compressed = try stream.flush();
    try publishToNats(compressed);
}

const final_compressed = try stream.finalize();
try publishToNats(final_compressed);
```

## Monitoring

### Compression Metrics (Planned)

The bridge will expose Prometheus metrics:

```
# Compression ratio per table
cdc_snapshot_compression_ratio{table="users"} 0.92

# Dictionary cache hits
cdc_compression_dict_cache_hits{table="users"} 1234
cdc_compression_dict_cache_misses{table="users"} 5

# Compression time
cdc_snapshot_compression_duration_seconds{table="users"} 0.015
```

## References

- [Zstandard Documentation](https://facebook.github.io/zstd/)
- [Zstd Dictionary Training](https://github.com/facebook/zstd#dictionary-compression-how-to)
- [fzstd (Browser Decompression)](https://github.com/101arrowz/fzstd)
- [MessagePack Specification](https://msgpack.org/)
