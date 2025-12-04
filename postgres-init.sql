-- Initialize PostgreSQL with settings for CDC
-- Create a publication for all tables (for logical replication)
-- This will be used later for CDC
-- Note: Publications are created per database, we'll create it after tables exist
-- Set up some initial configuration
ALTER SYSTEM
SET
  wal_level = 'logical';

ALTER SYSTEM
SET
  max_replication_slots = 10;

ALTER SYSTEM
SET
  max_wal_senders = 10;

-- Limit WAL retention to prevent disk fill
ALTER SYSTEM
SET
  max_slot_wal_keep_size = '10GB';

SELECT
  pg_reload_conf ();