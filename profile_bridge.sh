#!/bin/bash
# Profile the bridge to find CPU hotspots

echo "Building bridge with debug symbols..."
zig build -Doptimize=ReleaseFast

echo ""
echo "Run the bridge and profile with 'sample' (macOS):"
echo "  1. Start bridge: ./zig-out/bin/bridge ..."
echo "  2. In another terminal: sample bridge 10 -file bridge_profile.txt"
echo "  3. Analyze: open bridge_profile.txt"
echo ""
echo "Or use Instruments (macOS):"
echo "  instruments -t 'Time Profiler' -D profile.trace ./zig-out/bin/bridge ..."
