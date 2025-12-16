#!/bin/bash
# Measure snapshot size for a table to determine if compression is worthwhile
#
# Usage: ./scripts/measure_snapshot_size.sh <table_name> [num_rows]

set -e

TABLE_NAME="${1:-users}"
NUM_ROWS="${2:-10000}"

echo "📊 Measuring snapshot size for table: $TABLE_NAME"
echo "   Sample size: $NUM_ROWS rows"
echo ""

# Export sample data as JSON
echo "1️⃣ Exporting $NUM_ROWS rows from table '$TABLE_NAME'..."
psql -h "${PG_HOST:-localhost}" \
     -p "${PG_PORT:-5432}" \
     -U "${PG_USER:-postgres}" \
     -d "${PG_DB:-postgres}" \
     -c "COPY (SELECT row_to_json(t) FROM $TABLE_NAME t LIMIT $NUM_ROWS) TO STDOUT" \
     > /tmp/${TABLE_NAME}_sample.json

JSON_SIZE=$(wc -c < /tmp/${TABLE_NAME}_sample.json)
echo "   JSON size: $(numfmt --to=iec $JSON_SIZE) ($JSON_SIZE bytes)"
echo ""

# Calculate MessagePack size (approximate: ~70-80% of JSON)
MSGPACK_SIZE=$(echo "$JSON_SIZE * 0.75" | bc | cut -d. -f1)
echo "2️⃣ Estimated MessagePack size: $(numfmt --to=iec $MSGPACK_SIZE) (~75% of JSON)"
echo ""

# Calculate compression scenarios
echo "3️⃣ Compression scenarios:"
echo ""

# No compression
echo "   📦 No compression:"
echo "      Size: $(numfmt --to=iec $MSGPACK_SIZE)"
echo "      Network transfer (10 Mbps): $(echo "scale=1; $MSGPACK_SIZE * 8 / 10000000" | bc)s"
echo ""

# Generic compression (70%)
GENERIC_COMPRESSED=$(echo "$MSGPACK_SIZE * 0.3" | bc | cut -d. -f1)
echo "   🗜️  Generic zstd (no dictionary, ~70% compression):"
echo "      Size: $(numfmt --to=iec $GENERIC_COMPRESSED)"
echo "      Network transfer (10 Mbps): $(echo "scale=1; $GENERIC_COMPRESSED * 8 / 10000000" | bc)s"
echo "      Savings: $(echo "scale=1; ($MSGPACK_SIZE - $GENERIC_COMPRESSED) / $MSGPACK_SIZE * 100" | bc)%"
echo ""

# Dictionary compression (92%)
DICT_COMPRESSED=$(echo "$MSGPACK_SIZE * 0.08" | bc | cut -d. -f1)
echo "   📚 Dictionary zstd (~92% compression):"
echo "      Size: $(numfmt --to=iec $DICT_COMPRESSED)"
echo "      Network transfer (10 Mbps): $(echo "scale=1; $DICT_COMPRESSED * 8 / 10000000" | bc)s"
echo "      Savings: $(echo "scale=1; ($MSGPACK_SIZE - $DICT_COMPRESSED) / $MSGPACK_SIZE * 100" | bc)%"
echo ""

# Calculate for full 150K rows
TOTAL_ROWS="${3:-150000}"
TOTAL_MSGPACK=$(echo "$MSGPACK_SIZE * $TOTAL_ROWS / $NUM_ROWS" | bc)
TOTAL_GENERIC=$(echo "$GENERIC_COMPRESSED * $TOTAL_ROWS / $NUM_ROWS" | bc)
TOTAL_DICT=$(echo "$DICT_COMPRESSED * $TOTAL_ROWS / $NUM_ROWS" | bc)

echo "4️⃣ Extrapolation for $TOTAL_ROWS rows:"
echo ""
echo "   📦 No compression: $(numfmt --to=iec $TOTAL_MSGPACK)"
echo "      Transfer time (10 Mbps): $(echo "scale=1; $TOTAL_MSGPACK * 8 / 10000000" | bc)s"
echo ""
echo "   🗜️  Generic zstd: $(numfmt --to=iec $TOTAL_GENERIC)"
echo "      Transfer time (10 Mbps): $(echo "scale=1; $TOTAL_GENERIC * 8 / 10000000" | bc)s"
echo "      Time saved: $(echo "scale=1; ($TOTAL_MSGPACK - $TOTAL_GENERIC) * 8 / 10000000" | bc)s"
echo ""
echo "   📚 Dictionary zstd: $(numfmt --to=iec $TOTAL_DICT)"
echo "      Transfer time (10 Mbps): $(echo "scale=1; $TOTAL_DICT * 8 / 10000000" | bc)s"
echo "      Time saved: $(echo "scale=1; ($TOTAL_MSGPACK - $TOTAL_DICT) * 8 / 10000000" | bc)s"
echo ""

# Recommendation
if [ $TOTAL_MSGPACK -gt 10485760 ]; then  # > 10MB
    echo "💡 RECOMMENDATION: Enable compression with --zstd"
    echo "   Your snapshot is $(numfmt --to=iec $TOTAL_MSGPACK), compression will significantly reduce transfer time."
elif [ $TOTAL_MSGPACK -gt 1048576 ]; then  # > 1MB
    echo "💡 RECOMMENDATION: Consider compression if network bandwidth is limited"
    echo "   Your snapshot is $(numfmt --to=iec $TOTAL_MSGPACK), compression may help on slow connections."
else
    echo "💡 RECOMMENDATION: Compression overhead not worth it"
    echo "   Your snapshot is only $(numfmt --to=iec $TOTAL_MSGPACK), transfer is already fast."
fi

# Cleanup
rm -f /tmp/${TABLE_NAME}_sample.json
