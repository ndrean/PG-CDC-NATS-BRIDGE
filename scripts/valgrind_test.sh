#!/bin/bash
set -e

echo "=== Building Valgrind Docker image (includes building bridge) ==="
docker build -f Dockerfile.valgrind -t bridge-valgrind .

echo ""
echo "=== Running bridge with Valgrind (will run for 30 seconds) ==="
echo "Note: The bridge will try to connect to PostgreSQL and NATS"
echo "Make sure your docker-compose services are running first!"
echo ""

# Run the container, let it run for 30 seconds, then stop
CONTAINER_ID=$(docker run --rm -d \
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
        --publication valgrind_pub)

echo "Container started: $CONTAINER_ID"
echo "Letting it run for 30 seconds..."
sleep 30

echo ""
echo "=== Stopping container and retrieving Valgrind report ==="
docker stop $CONTAINER_ID 2>/dev/null || true

echo ""
echo "=== Cleanup ==="
# Clean up the test slot and publication
echo "You may want to manually clean up the test replication slot and publication:"
echo "  DROP PUBLICATION IF EXISTS valgrind_pub;"
echo "  SELECT pg_drop_replication_slot('valgrind_slot');"
