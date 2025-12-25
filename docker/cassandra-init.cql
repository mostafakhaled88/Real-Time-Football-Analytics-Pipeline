-- 1. Create Keyspace
-- 'SimpleStrategy' is fine for a single-node Docker setup.
CREATE KEYSPACE IF NOT EXISTS football_streams 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE football_streams;

-- 2. Table: Raw Match Events
-- Partition Key: match_id (all events for one match stay together)
-- Clustering Column: timestamp (allows us to sort events chronologically)
CREATE TABLE IF NOT EXISTS match_events (
    match_id text,
    event_id uuid,
    timestamp timestamp,
    team text,
    player text,
    event_type text,
    location_x double,
    location_y double,
    details text,
    PRIMARY KEY (match_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

-- 3. Table: Live Match Stats (Aggregated View)
-- This table will be updated by Spark to show real-time totals
CREATE TABLE IF NOT EXISTS match_stats (
    match_id text PRIMARY KEY,
    team_a_goals int,
    team_b_goals int,
    total_passes int,
    total_shots int,
    last_update timestamp
);

-- 4. Create an Index for filtering by event type (Optional but useful)
CREATE INDEX IF NOT EXISTS ON match_events (event_type);