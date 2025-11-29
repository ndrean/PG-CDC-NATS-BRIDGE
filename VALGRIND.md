# Valgrind Memory Leak Testing

This document explains how to test the bridge for memory leaks using Valgrind.

## Prerequisites

- Docker and docker-compose
- The main bridge services running (PostgreSQL, NATS)

## Quick Start

### Option 1: Using the helper script (Recommended)

```bash
# Make sure your services are running
docker-compose up -d postgres nats

# Run the valgrind test (builds, runs for 30s, shows report)
./scripts/valgrind_test.sh
```

### Option 2: Manual build and run

```bash
# 1. Build the Valgrind Docker image (automatically builds bridge inside)
docker build -f Dockerfile.valgrind -t bridge-valgrind .

# 2. Run with Valgrind (make sure postgres and nats are running)
docker run --rm \
    --network cdc-bridge \
    --name bridge-valgrind-test \
    bridge-valgrind \
    valgrind \
        --leak-check=full \
        --show-leak-kinds=all \
        --track-origins=yes \
        --verbose \
        --log-fd=1 \
        bridge \
        --stream CDC_VALGRIND \
        --slot valgrind_slot \
        --publication valgrind_pub

# Let it run for a while, then Ctrl+C to stop and see the report
```

### Option 3: Using docker-compose

```bash
# Run with docker-compose (builds automatically)
docker-compose -f docker-compose.valgrind.yml up bridge-valgrind

# Let it run, then Ctrl+C to see the Valgrind report
```

## Understanding the Output

Valgrind will show a summary at the end:

### ✅ Good (No Leaks)
```
HEAP SUMMARY:
    in use at exit: 0 bytes in 0 blocks
  total heap usage: X allocs, X frees, Y bytes allocated

All heap blocks were freed -- no leaks are possible
```

### ⚠️ Possible Leaks
```
LEAK SUMMARY:
   definitely lost: X bytes in Y blocks
   indirectly lost: A bytes in B blocks
     possibly lost: C bytes in D blocks
   still reachable: E bytes in F blocks
```

**What to look for:**
- **definitely lost**: Real memory leaks - **MUST FIX**
- **indirectly lost**: Memory lost because parent was lost - **MUST FIX**
- **possibly lost**: Could be valid or leak - **INVESTIGATE**
- **still reachable**: Memory held at exit (often OK for C libraries)

## Common Issues

### 1. "Still reachable" from C libraries

Some C libraries (like libpq, nats.c) may keep global state that shows as "still reachable". This is often intentional and not a leak, as the OS will clean it up on process exit.

### 2. Valgrind suppressions

If you see false positives from system libraries, you can create a suppression file:

```bash
valgrind --gen-suppressions=all --log-file=suppressions.txt ./bridge
# Edit suppressions.txt to create valgrind.supp
valgrind --suppressions=valgrind.supp ./bridge
```

### 3. Performance

Valgrind makes the program run **10-100x slower**. This is normal.

## Interpreting Results for Our Code

Focus on leaks that come from:
- ✅ `replication_setup.zig` - Our PostgreSQL connection management
- ✅ `wal_stream.zig` - WAL streaming
- ✅ `nats_publisher.zig` - NATS connection management
- ✅ `bridge.zig` - Main program

Ignore leaks from:
- System libraries (libc, libpthread)
- PostgreSQL internal functions (unless we forgot PQfinish/PQclear)
- NATS internal functions (unless we forgot natsConnection_Destroy)

## Build Modes

- **ReleaseSafe**: Recommended for Valgrind testing
  - Keeps safety checks
  - Optimized but debuggable
  - `zig build -Doptimize=ReleaseSafe`

- **Debug**: Alternative (slower but easier to debug)
  - Full debug info
  - No optimizations
  - `zig build -Doptimize=Debug`

## Cleanup

After testing, clean up the test resources:

```bash
# Connect to PostgreSQL and run:
psql -U postgres -d postgres -c "DROP PUBLICATION IF EXISTS valgrind_pub;"
psql -U postgres -d postgres -c "SELECT pg_drop_replication_slot('valgrind_slot');"
```

## Further Reading

- [Valgrind User Manual](https://valgrind.org/docs/manual/manual.html)
- [Valgrind Quick Start](https://valgrind.org/docs/manual/quick-start.html)
- [Understanding Valgrind Memory Leak Reports](https://valgrind.org/docs/manual/mc-manual.html#mc-manual.leaks)
