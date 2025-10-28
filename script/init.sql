-- Ensure required extensions are installed in the target database
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Create crypto_scout schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS crypto_scout;

-- Set the search path to include all necessary schemas
SET search_path TO public, crypto_scout;

-- Persist search_path defaults for the database and application role
ALTER DATABASE crypto_scout SET search_path TO public, crypto_scout;
ALTER ROLE crypto_scout_db IN DATABASE crypto_scout SET search_path TO public, crypto_scout;

-- Track last processed offsets per stream (external offset tracking)
CREATE TABLE IF NOT EXISTS crypto_scout.stream_offsets (
    stream TEXT PRIMARY KEY,
    "offset" BIGINT NOT NULL CHECK ("offset" >= 0),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Set ownership to application role
ALTER TABLE crypto_scout.stream_offsets OWNER TO crypto_scout_db;

-- Grant privileges
GRANT ALL PRIVILEGES ON SCHEMA crypto_scout TO crypto_scout_db;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA crypto_scout TO crypto_scout_db;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA crypto_scout TO crypto_scout_db;

-- Ensure future objects created by the application role have appropriate privileges
ALTER DEFAULT PRIVILEGES FOR ROLE crypto_scout_db IN SCHEMA crypto_scout GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO crypto_scout_db;
ALTER DEFAULT PRIVILEGES FOR ROLE crypto_scout_db IN SCHEMA crypto_scout GRANT USAGE, SELECT ON SEQUENCES TO crypto_scout_db;