#!/bin/bash

# Test script to verify column data is published to NATS

echo "Starting bridge..."
./zig-out/bin/bridge --stream CDC --table users 2>&1 | tee bridge.log &
BRIDGE_PID=$!

sleep 3

echo ""
echo "Starting Elixir consumer in background..."
cd consumer
NATS_STREAM_NAME=CDC TABLES=users,orders MIX_ENV=prod mix run --no-halt 2>&1 | tee consumer.log &
CONSUMER_PID=$!
cd ..

sleep 2

echo ""
echo "Inserting test data via psql..."
docker exec postgres psql -U postgres -d postgres -c "INSERT INTO users (name, email) VALUES ('Alice Test', 'alice@test.com'), ('Bob Test', 'bob@test.com');"

echo ""
echo "Waiting for events to propagate..."
sleep 3

echo ""
echo "=== Bridge Output (last 30 lines) ==="
tail -30 bridge.log

echo ""
echo "=== Consumer Output (last 20 lines) ==="
tail -20 consumer/consumer.log

echo ""
echo "Cleaning up..."
kill $BRIDGE_PID 2>/dev/null
kill $CONSUMER_PID 2>/dev/null
wait $BRIDGE_PID 2>/dev/null
wait $CONSUMER_PID 2>/dev/null

echo "Done!"
