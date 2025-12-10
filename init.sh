#!/bin/bash
set -ex  # Added -x for debugging

# Read from environment (set by docker-compose)
BRIDGE_READER="${POSTGRES_BRIDGE_USER:-bridge_reader}"
BRIDGE_READER_PASSWORD="${POSTGRES_BRIDGE_PASSWORD:-bridge_password_changeme}"
TARGET_DB="${TARGET_DB:-postgres}"
PG_PUB="${BRIDGE_CDC_PUBLICATION}"

echo "=========================================="
echo "[Bridge Init] Script execution started!"
echo "=========================================="
echo "[Bridge Init] Starting Postgres CDC setup..."
echo "[Bridge Init] Creating user: ${BRIDGE_READER}"
echo "[Bridge Init] Target database: ${TARGET_DB}"


psql -v ON_ERROR_STOP=1 --dbname="$TARGET_DB" <<EOF

DO \$\$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${BRIDGE_READER}') THEN
        CREATE USER ${BRIDGE_READER} WITH PASSWORD '${BRIDGE_READER_PASSWORD}';
        ALTER USER ${BRIDGE_READER} WITH REPLICATION;
    END IF;
END;
\$\$;

GRANT CONNECT ON DATABASE ${TARGET_DB} TO ${BRIDGE_READER};
GRANT CREATE ON DATABASE ${TARGET_DB} TO ${BRIDGE_READER};
GRANT USAGE ON SCHEMA public TO ${BRIDGE_READER};
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ${BRIDGE_READER};

ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO ${BRIDGE_READER};

DO \$\$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = '${PG_PUB}') THEN
        CREATE PUBLICATION ${PG_PUB} FOR ALL TABLES;
    END IF;
END;
\$\$;

-- Create test tables with various data types for testing

CREATE TABLE IF NOT EXISTS test_types (
  id SERIAL PRIMARY KEY,
  uid UUID DEFAULT gen_random_uuid(),
  age INT,
  temperature FLOAT8,
  price NUMERIC(20,8),
  is_true BOOLEAN,
  some_text TEXT,
  tags TEXT[],
  matrix INT[][],
  metadata JSONB,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS users (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT,
  create_at TIMESTAMPTZ DEFAULT now()
);

EOF

echo "[Bridge Init] CDC configuration completed successfully."
