#!/bin/bash

# Test to identify the performance bottleneck

echo "=== Testing batch flush performance ==="
echo ""

# Start bridge in background
./zig-out/bin/bridge --stream PERF_TEST --table users > /tmp/bridge_perf.log 2>&1 &
BRIDGE_PID=$!
sleep 5

# Test 1: Moderate load (100 inserts/ms = 10k/sec)
echo "Test 1: 100 inserts in 1ms (10,000/sec)"
docker exec postgres psql -U postgres -d postgres -c "
DO \$\$
BEGIN
    FOR i IN 1..100 LOOP
        INSERT INTO users (name, email) VALUES ('User' || i, 'user' || i || '@test.com');
    END LOOP;
END \$\$;
"
sleep 2
grep -E "(Published batch|Slow flush)" /tmp/bridge_perf.log | tail -5

sleep 2

# Test 2: Heavy load (200 inserts/ms = 20k/sec)
echo ""
echo "Test 2: 200 inserts in 1ms (20,000/sec)"
docker exec postgres psql -U postgres -d postgres -c "
DO \$\$
BEGIN
    FOR i IN 1..200 LOOP
        INSERT INTO users (name, email) VALUES ('Heavy' || i, 'heavy' || i || '@test.com');
    END LOOP;
END \$\$;
"
sleep 2
grep -E "(Published batch|Slow flush)" /tmp/bridge_perf.log | tail -5

sleep 2

# Test 3: Extreme load (500 inserts/ms)
echo ""
echo "Test 3: 500 inserts in 1ms (50,000/sec)"
docker exec postgres psql -U postgres -d postgres -c "
DO \$\$
BEGIN
    FOR i IN 1..500 LOOP
        INSERT INTO users (name, email) VALUES ('Extreme' || i, 'extreme' || i || '@test.com');
    END LOOP;
END \$\$;
"
sleep 3
grep -E "(Published batch|Slow flush)" /tmp/bridge_perf.log | tail -10

echo ""
echo "=== Full bridge log (last 30 lines) ==="
tail -30 /tmp/bridge_perf.log

kill $BRIDGE_PID 2>/dev/null
wait $BRIDGE_PID 2>/dev/null

echo ""
echo "Done!"
