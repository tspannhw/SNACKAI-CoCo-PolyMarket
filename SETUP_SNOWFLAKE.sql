-- ============================================================
-- Polymarket Prediction Markets - Snowflake Setup
-- Snowpipe Streaming High-Performance v2
-- ============================================================
--
-- This script creates the full Snowflake schema for the Polymarket
-- streaming pipeline. Run it once to set up all tables, views,
-- grants, and validation queries.
--
-- Prerequisites:
--   - Snowflake account with Snowpipe Streaming enabled
--   - Role with CREATE DATABASE privileges (ACCOUNTADMIN recommended)
--
-- Components created:
--   DATABASE: POLYMARKET
--   SCHEMA:   POLYMARKET.STREAMING
--
--   TABLES (3):
--     MARKETS            - Landing table for market data (SSv2 default pipe)
--     MARKET_EVENTS      - Event data extracted from markets (SSv2 default pipe)
--     INGESTION_METRICS  - Pipeline performance tracking (SSv2 default pipe)
--
--   VIEWS (6):
--     V_ACTIVE_MARKETS              - Active, non-closed markets
--     V_LATEST_MARKETS              - Deduplicated latest snapshot per market
--     V_MARKET_VOLUME_SUMMARY       - Volume aggregated by category
--     V_INGESTION_HEALTH            - Streaming metrics bucketed by minute
--     V_STREAMING_CHANNEL_HEALTH    - SSv2 channel errors/latency (ACCOUNT_USAGE)
--     V_STREAMING_OPERATIONAL_HEALTH- Real-time pipeline status per table
--
-- Data Flow:
--   Polymarket API -> Python Fetcher -> SSv2 REST API -> MARKETS table
--                                    -> SSv2 REST API -> MARKET_EVENTS table
--                                    -> SSv2 REST API -> INGESTION_METRICS table
--   Dashboard (Next.js) -> Snowflake SDK -> Views -> React UI
--
-- SSv2 Default Pipes (auto-created, no DDL needed):
--   MARKETS            -> MARKETS-STREAMING
--   MARKET_EVENTS      -> MARKET_EVENTS-STREAMING
--   INGESTION_METRICS  -> INGESTION_METRICS-STREAMING
--
-- Usage:
--   Run this entire script in Snowsight, SnowSQL, or via the Snowflake CLI.
--   It is idempotent (uses IF NOT EXISTS / OR REPLACE).
-- ============================================================


-- ============================================================
-- SECTION 1: DATABASE AND SCHEMA
-- ============================================================

CREATE DATABASE IF NOT EXISTS POLYMARKET
    COMMENT = 'Polymarket prediction market data ingested via Snowpipe Streaming v2';

CREATE SCHEMA IF NOT EXISTS POLYMARKET.STREAMING
    COMMENT = 'Schema for streaming ingestion of Polymarket market data';

USE DATABASE POLYMARKET;
USE SCHEMA STREAMING;

-- Enable ROW_TIMESTAMP_DEFAULT so all new tables automatically get
-- METADATA$ROW_LAST_COMMIT_TIME tracking (Snowflake Feb 2026 feature).
ALTER SCHEMA POLYMARKET.STREAMING SET ROW_TIMESTAMP_DEFAULT = TRUE;


-- ============================================================
-- SECTION 2: TABLES
-- ============================================================

-- ------------------------------------------------------------
-- 2a. MARKETS - Primary landing table for SSv2 streaming
-- ------------------------------------------------------------
-- This is the main table where market data arrives via the
-- MARKETS-STREAMING default pipe. Each row represents one
-- market snapshot at a point in time. The same market ID
-- appears multiple times across ingestion batches (append-only).
-- Use V_LATEST_MARKETS for deduplicated current state.
--
-- Column naming convention: snake_case matching the Python
-- transformer output (polymarket_fetcher.py transform_market).
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE POLYMARKET.STREAMING.MARKETS (
    -- Identity
    id                    VARCHAR(255)    COMMENT 'Polymarket market ID (unique per market, not per row)',
    question              VARCHAR(4000)   COMMENT 'Market question text (e.g. "Will Bitcoin reach $100K?")',
    condition_id          VARCHAR(255)    COMMENT 'Polymarket condition identifier',
    slug                  VARCHAR(1000)   COMMENT 'URL-safe market slug',

    -- Content
    description           VARCHAR(16000)  COMMENT 'Full market description text',
    category              VARCHAR(255)    COMMENT 'Market category (Politics, Crypto, Sports, etc.)',
    end_date              TIMESTAMP_NTZ   COMMENT 'Market resolution/end date',
    start_date            TIMESTAMP_NTZ   COMMENT 'Market start date',
    image                 VARCHAR(2000)   COMMENT 'Market image URL',
    icon                  VARCHAR(2000)   COMMENT 'Market icon URL',

    -- Status flags
    active                BOOLEAN         COMMENT 'Market is currently active',
    closed                BOOLEAN         COMMENT 'Market is closed/resolved',
    archived              BOOLEAN         COMMENT 'Market is archived',
    featured              BOOLEAN         COMMENT 'Market is featured on homepage',
    restricted            BOOLEAN         COMMENT 'Market has trading restrictions',
    new_market            BOOLEAN         COMMENT 'Market is newly created',

    -- Type
    market_type           VARCHAR(100)    COMMENT 'Market type (binary, categorical, etc.)',
    format_type           VARCHAR(100)    COMMENT 'Display format type',

    -- Outcomes and pricing
    outcomes              VARCHAR(4000)   COMMENT 'JSON array of outcome names (e.g. ["Yes","No"])',
    outcome_prices        VARCHAR(4000)   COMMENT 'JSON array of outcome prices 0-1 (e.g. ["0.72","0.28"])',

    -- Volume metrics (all in USD)
    volume                FLOAT           COMMENT 'Raw volume field from API',
    volume_num            FLOAT           COMMENT 'Total trading volume (USD) - primary volume metric',
    volume_24hr           FLOAT           COMMENT '24-hour trading volume (USD)',
    volume_1wk            FLOAT           COMMENT '1-week trading volume (USD)',
    volume_1mo            FLOAT           COMMENT '1-month trading volume (USD)',
    volume_1yr            FLOAT           COMMENT '1-year trading volume (USD)',

    -- Liquidity metrics
    liquidity             FLOAT           COMMENT 'Raw liquidity field from API',
    liquidity_num         FLOAT           COMMENT 'Available liquidity (USD) - primary liquidity metric',
    spread                FLOAT           COMMENT 'Bid-ask spread',

    -- Trading params
    lower_bound           VARCHAR(255)    COMMENT 'Lower bound for range markets',
    upper_bound           VARCHAR(255)    COMMENT 'Upper bound for range markets',
    clob_token_ids        VARCHAR(4000)   COMMENT 'CLOB token identifiers (JSON)',
    accepting_orders      BOOLEAN         COMMENT 'Market is accepting new orders',
    comments_enabled      BOOLEAN         COMMENT 'Comments are enabled',
    enable_order_book     BOOLEAN         COMMENT 'Order book is enabled',
    maker_base_fee        FLOAT           COMMENT 'Maker fee rate',
    taker_base_fee        FLOAT           COMMENT 'Taker fee rate',
    notifications_enabled BOOLEAN         COMMENT 'Notifications are enabled',
    score                 FLOAT           COMMENT 'Polymarket relevance/ranking score',

    -- Timestamps
    created_at            TIMESTAMP_NTZ   COMMENT 'When market was created on Polymarket',
    updated_at            TIMESTAMP_NTZ   COMMENT 'When market was last updated on Polymarket',
    ingested_at           TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
                                          COMMENT 'When this row was streamed to Snowflake',
    batch_id              VARCHAR(100)    COMMENT 'Ingestion batch identifier (batch_YYYYMMDD_HHMMSS_xxxxxxxx)'
)
COMMENT = 'Polymarket prediction market data. Append-only via SSv2. Use V_LATEST_MARKETS for current state.'
ROW_TIMESTAMP = TRUE;


-- ------------------------------------------------------------
-- 2b. MARKET_EVENTS - Events extracted from market responses
-- ------------------------------------------------------------
-- Events are parent objects that group related markets.
-- For example, "2024 US Presidential Election" is an event
-- containing multiple markets (winner, popular vote, etc.).
--
-- Populated by main.py via a dedicated SSv2 channel targeting
-- the MARKET_EVENTS-STREAMING default pipe. Events are extracted
-- from the embedded 'events' array in each market API response
-- and deduplicated by event_id before streaming.
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE POLYMARKET.STREAMING.MARKET_EVENTS (
    event_id              VARCHAR(255)    COMMENT 'Polymarket event ID',
    ticker                VARCHAR(255)    COMMENT 'Event ticker symbol',
    slug                  VARCHAR(1000)   COMMENT 'URL-safe event slug',
    title                 VARCHAR(4000)   COMMENT 'Event title',
    description           VARCHAR(16000)  COMMENT 'Event description',
    category              VARCHAR(255)    COMMENT 'Event category',
    start_date            TIMESTAMP_NTZ   COMMENT 'Event start date',
    end_date              TIMESTAMP_NTZ   COMMENT 'Event end date',
    image                 VARCHAR(2000)   COMMENT 'Event image URL',
    active                BOOLEAN         COMMENT 'Event is active',
    closed                BOOLEAN         COMMENT 'Event is closed',
    volume                FLOAT           COMMENT 'Total event volume across all markets (USD)',
    liquidity             FLOAT           COMMENT 'Total event liquidity (USD)',
    open_interest         FLOAT           COMMENT 'Open interest across event markets',
    neg_risk              BOOLEAN         COMMENT 'Negative risk flag',
    comment_count         INTEGER         COMMENT 'Number of comments on event',
    market_count          INTEGER         COMMENT 'Number of markets in this event',
    ingested_at           TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
                                          COMMENT 'When this row was ingested',
    batch_id              VARCHAR(100)    COMMENT 'Ingestion batch identifier'
)
COMMENT = 'Polymarket events (parent groupings of related markets). Streamed via SSv2 MARKET_EVENTS-STREAMING pipe.'
ROW_TIMESTAMP = TRUE;


-- ------------------------------------------------------------
-- 2c. INGESTION_METRICS - Pipeline performance tracking
-- ------------------------------------------------------------
-- Populated by main.py via a dedicated SSv2 channel targeting
-- the INGESTION_METRICS-STREAMING default pipe.
--
-- Each row represents one ingestion cycle (batch). The dashboard
-- queries V_INGESTION_HEALTH (which aggregates this table) to
-- display the Ingestion Health chart and pipeline status KPIs.
--
-- This table is EMPTY until the Python streamer has run at
-- least once. Run: ./manage.sh stream-once
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE POLYMARKET.STREAMING.INGESTION_METRICS (
    metric_id             VARCHAR(100)    COMMENT 'Unique metric row ID',
    batch_id              VARCHAR(100)    COMMENT 'Matches batch_id in MARKETS table',
    batch_timestamp       TIMESTAMP_TZ    COMMENT 'When this batch was processed',
    markets_fetched       INTEGER         COMMENT 'Markets returned from Polymarket API',
    markets_streamed      INTEGER         COMMENT 'Markets successfully streamed to Snowflake',
    events_streamed       INTEGER         COMMENT 'Events found in API response',
    fetch_duration_ms     FLOAT           COMMENT 'Time to fetch from Polymarket API (ms)',
    stream_duration_ms    FLOAT           COMMENT 'Time to stream to Snowflake (ms)',
    total_duration_ms     FLOAT           COMMENT 'Total cycle time (ms)',
    api_status_code       INTEGER         COMMENT 'HTTP status from Polymarket API (200=OK, 0=no data)',
    error_message         VARCHAR(4000)   COMMENT 'Error description (NULL = no errors)',
    offset_token          INTEGER         COMMENT 'SSv2 channel offset token after this batch',
    channel_name          VARCHAR(255)    COMMENT 'SSv2 channel name used for this batch',
    ingested_at           TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
                                          COMMENT 'When this metrics row was written'
)
COMMENT = 'Pipeline ingestion metrics. One row per batch. Feeds V_INGESTION_HEALTH dashboard view.'
ROW_TIMESTAMP = TRUE;


-- ============================================================
-- SECTION 3: GRANTS AND ACCESS
-- ============================================================
-- Grant access to ACCOUNTADMIN. Adjust roles as needed for your
-- environment. For production, create a dedicated role:
--   CREATE ROLE POLYMARKET_READER;
--   GRANT USAGE ON DATABASE POLYMARKET TO ROLE POLYMARKET_READER;
--   GRANT USAGE ON SCHEMA POLYMARKET.STREAMING TO ROLE POLYMARKET_READER;
--   GRANT SELECT ON ALL VIEWS IN SCHEMA POLYMARKET.STREAMING TO ROLE POLYMARKET_READER;
--
-- NOTE on SSv2 Default Pipes:
--   The default auto-created pipes (e.g. MARKETS-STREAMING) are
--   Snowflake-managed (is_snowflake_managed=true, owner=NULL).
--   GRANT MONITOR/OPERATE is NOT supported on managed pipes.
--   ACCOUNTADMIN can already see them via:
--     SHOW PIPES IN SCHEMA POLYMARKET.STREAMING;
--   And monitor channel activity via:
--     SNOWFLAKE.ACCOUNT_USAGE.SNOWPIPE_STREAMING_CHANNEL_HISTORY
-- ============================================================

GRANT USAGE ON DATABASE POLYMARKET TO ROLE ACCOUNTADMIN;
GRANT USAGE ON SCHEMA POLYMARKET.STREAMING TO ROLE ACCOUNTADMIN;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA POLYMARKET.STREAMING TO ROLE ACCOUNTADMIN;
-- Grant access to ACCOUNT_USAGE for channel monitoring
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE ACCOUNTADMIN;


-- ============================================================
-- SECTION 3b: ENABLE ROW TIMESTAMPS ON EXISTING TABLES
-- ============================================================
-- ROW_TIMESTAMP enables METADATA$ROW_LAST_COMMIT_TIME on each table,
-- which tracks when each row was last committed. Useful for:
--   - Measuring ingestion latency (SSv2 commit time vs client timestamp)
--   - Change tracking and incremental processing
--   - Time-travel queries based on row modification time
--
-- The schema-level ROW_TIMESTAMP_DEFAULT (set above) ensures new tables
-- get this automatically. These ALTER statements enable it on tables
-- that were created before the schema default was set.
-- ============================================================

ALTER TABLE POLYMARKET.STREAMING.MARKETS SET ROW_TIMESTAMP = TRUE;
ALTER TABLE POLYMARKET.STREAMING.MARKET_EVENTS SET ROW_TIMESTAMP = TRUE;
ALTER TABLE POLYMARKET.STREAMING.INGESTION_METRICS SET ROW_TIMESTAMP = TRUE;


-- ============================================================
-- SECTION 3c: ENABLE ERROR LOGGING FOR SSv2 STREAMING
-- ============================================================
-- ERROR_LOGGING captures failed rows from Snowpipe Streaming v2
-- into dedicated error tables. When a row fails server-side
-- validation (schema mismatch, type errors, constraint violations),
-- it is logged with full payload and error metadata instead of
-- being silently dropped.
--
-- GA as of April 8, 2026.
-- Reference: https://docs.snowflake.com/en/release-notes/2026/other/2026-04-08-snowpipe-streaming-error-tables
--
-- Error tables are auto-created by Snowflake when errors occur.
-- Query them via: SELECT * FROM TABLE(INFORMATION_SCHEMA.STREAMING_ERROR_LOG('MARKETS'));
-- Or check SNOWPIPE_STREAMING_CHANNEL_HISTORY for ROW_ERROR_COUNT.
-- ============================================================

ALTER TABLE POLYMARKET.STREAMING.MARKETS SET ERROR_LOGGING = TRUE;
ALTER TABLE POLYMARKET.STREAMING.MARKET_EVENTS SET ERROR_LOGGING = TRUE;
ALTER TABLE POLYMARKET.STREAMING.INGESTION_METRICS SET ERROR_LOGGING = TRUE;


-- ============================================================
-- SECTION 3d: STREAMING CHANNEL MONITORING VIEW
-- ============================================================
-- Provides a convenient view over SNOWPIPE_STREAMING_CHANNEL_HISTORY
-- for monitoring channel health, errors, and latency.
--
-- NOTE: ACCOUNT_USAGE views have up to 45-minute latency and may
-- take longer to populate for SSv2 High-Performance channels.
-- If this view returns 0 rows, use V_STREAMING_OPERATIONAL_HEALTH
-- (Section 3e) for real-time pipeline status based on table data.
-- ============================================================

CREATE OR REPLACE VIEW POLYMARKET.STREAMING.V_STREAMING_CHANNEL_HEALTH AS
SELECT
    CHANNEL_NAME,
    PIPE_NAME,
    TABLE_NAME,
    ROWS_INSERTED,
    ROWS_PARSED,
    ROW_ERROR_COUNT,
    LAST_ERROR_MESSAGE,
    LAST_ERROR_OFFSET_UPPER_BOUND,
    SNOWFLAKE_PROCESSING_LATENCY_MS,
    CREATED_ON
FROM SNOWFLAKE.ACCOUNT_USAGE.SNOWPIPE_STREAMING_CHANNEL_HISTORY
WHERE TABLE_DATABASE_NAME = 'POLYMARKET'
  AND TABLE_SCHEMA_NAME = 'STREAMING'
ORDER BY CREATED_ON DESC;


-- ============================================================
-- SECTION 3e: OPERATIONAL HEALTH VIEW (real-time)
-- ============================================================
-- Unlike V_STREAMING_CHANNEL_HEALTH (which depends on ACCOUNT_USAGE
-- latency), this view derives pipeline health directly from table
-- data. It shows row counts, last ingestion time, and a status
-- classification for each streaming table.
--
-- Status logic:
--   HEALTHY  = last ingestion < 3 minutes ago
--   STALE    = last ingestion 3-10 minutes ago
--   OFFLINE  = last ingestion > 10 minutes ago or no data
-- ============================================================

CREATE OR REPLACE VIEW POLYMARKET.STREAMING.V_STREAMING_OPERATIONAL_HEALTH AS
SELECT
    'MARKETS' AS table_name,
    COUNT(*) AS row_count,
    COUNT(DISTINCT batch_id) AS batch_count,
    MAX(ingested_at) AS last_ingested,
    DATEDIFF('second', MAX(ingested_at), CURRENT_TIMESTAMP()) AS seconds_since_last,
    MAX(batch_id) AS latest_batch_id,
    CASE
        WHEN MAX(ingested_at) IS NULL THEN 'OFFLINE'
        WHEN DATEDIFF('minute', MAX(ingested_at), CURRENT_TIMESTAMP()) < 3 THEN 'HEALTHY'
        WHEN DATEDIFF('minute', MAX(ingested_at), CURRENT_TIMESTAMP()) < 10 THEN 'STALE'
        ELSE 'OFFLINE'
    END AS pipeline_status
FROM POLYMARKET.STREAMING.MARKETS

UNION ALL

SELECT
    'MARKET_EVENTS' AS table_name,
    COUNT(*) AS row_count,
    COUNT(DISTINCT batch_id) AS batch_count,
    MAX(ingested_at) AS last_ingested,
    DATEDIFF('second', MAX(ingested_at), CURRENT_TIMESTAMP()) AS seconds_since_last,
    MAX(batch_id) AS latest_batch_id,
    CASE
        WHEN MAX(ingested_at) IS NULL THEN 'OFFLINE'
        WHEN DATEDIFF('minute', MAX(ingested_at), CURRENT_TIMESTAMP()) < 3 THEN 'HEALTHY'
        WHEN DATEDIFF('minute', MAX(ingested_at), CURRENT_TIMESTAMP()) < 10 THEN 'STALE'
        ELSE 'OFFLINE'
    END AS pipeline_status
FROM POLYMARKET.STREAMING.MARKET_EVENTS

UNION ALL

SELECT
    'INGESTION_METRICS' AS table_name,
    COUNT(*) AS row_count,
    COUNT(DISTINCT batch_id) AS batch_count,
    MAX(ingested_at) AS last_ingested,
    DATEDIFF('second', MAX(ingested_at), CURRENT_TIMESTAMP()) AS seconds_since_last,
    MAX(batch_id) AS latest_batch_id,
    CASE
        WHEN MAX(ingested_at) IS NULL THEN 'OFFLINE'
        WHEN DATEDIFF('minute', MAX(ingested_at), CURRENT_TIMESTAMP()) < 3 THEN 'HEALTHY'
        WHEN DATEDIFF('minute', MAX(ingested_at), CURRENT_TIMESTAMP()) < 10 THEN 'STALE'
        ELSE 'OFFLINE'
    END AS pipeline_status
FROM POLYMARKET.STREAMING.INGESTION_METRICS;


-- ============================================================
-- SECTION 4: VIEWS
-- ============================================================

-- ------------------------------------------------------------
-- 4a. V_LATEST_MARKETS - Deduplicated current state
-- ------------------------------------------------------------
-- Since MARKETS is append-only (each streaming batch adds new
-- rows for the same market IDs), this view returns only the
-- most recent row per market. This is the primary view used
-- by the dashboard and other downstream views.
--
-- Deduplication strategy: MAX(ingested_at) per market ID.
-- If two rows share the same ingested_at, both appear (rare).
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW POLYMARKET.STREAMING.V_LATEST_MARKETS AS
SELECT m.*
FROM POLYMARKET.STREAMING.MARKETS m
INNER JOIN (
    SELECT id, MAX(ingested_at) AS max_ingested
    FROM POLYMARKET.STREAMING.MARKETS
    GROUP BY id
) latest ON m.id = latest.id AND m.ingested_at = latest.max_ingested;


-- ------------------------------------------------------------
-- 4b. V_ACTIVE_MARKETS - Active, non-closed markets
-- ------------------------------------------------------------
-- Convenience view for quickly finding tradeable markets.
-- Ordered by volume descending for relevance.
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW POLYMARKET.STREAMING.V_ACTIVE_MARKETS AS
SELECT
    id,
    question,
    category,
    outcome_prices,
    outcomes,
    volume_num AS volume,
    volume_24hr,
    liquidity_num AS liquidity,
    spread,
    active,
    closed,
    end_date,
    ingested_at,
    batch_id
FROM POLYMARKET.STREAMING.MARKETS
WHERE active = TRUE AND closed = FALSE
ORDER BY volume_num DESC;


-- ------------------------------------------------------------
-- 4c. V_MARKET_VOLUME_SUMMARY - Volume aggregated by category
-- ------------------------------------------------------------
-- Used by the dashboard's "Volume by Category" bar chart and
-- "Markets by Category" pie chart.
--
-- IMPORTANT: Queries V_LATEST_MARKETS (deduplicated) to avoid
-- inflated totals from duplicate rows across ingestion batches.
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW POLYMARKET.STREAMING.V_MARKET_VOLUME_SUMMARY AS
SELECT
    category,
    COUNT(*) AS market_count,
    SUM(volume_num) AS total_volume,
    SUM(volume_24hr) AS total_24hr_volume,
    SUM(liquidity_num) AS total_liquidity,
    AVG(spread) AS avg_spread,
    MAX(ingested_at) AS last_updated
FROM POLYMARKET.STREAMING.V_LATEST_MARKETS
WHERE active = TRUE
GROUP BY category
ORDER BY total_volume DESC;


-- ------------------------------------------------------------
-- 4d. V_INGESTION_HEALTH - Streaming metrics by minute
-- ------------------------------------------------------------
-- Used by the dashboard's "Ingestion Health" line chart.
-- Shows markets ingested and errors over time, bucketed by minute.
--
-- NOTE: This view is EMPTY until the Python streamer (main.py)
-- has run at least once to populate INGESTION_METRICS.
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW POLYMARKET.STREAMING.V_INGESTION_HEALTH AS
SELECT
    DATE_TRUNC('minute', batch_timestamp) AS minute_bucket,
    COUNT(*) AS batches,
    SUM(markets_streamed) AS total_markets,
    AVG(total_duration_ms) AS avg_duration_ms,
    MAX(total_duration_ms) AS max_duration_ms,
    SUM(CASE WHEN error_message IS NOT NULL THEN 1 ELSE 0 END) AS error_count
FROM POLYMARKET.STREAMING.INGESTION_METRICS
GROUP BY minute_bucket
ORDER BY minute_bucket DESC;


-- ============================================================
-- SECTION 5: VALIDATION QUERIES
-- ============================================================
-- Run these after setup and after starting the streamer to
-- verify everything is working. Each query should return data
-- once the pipeline has run at least one cycle.
-- ============================================================

-- 5a. Check table row counts
SELECT 'MARKETS' AS table_name,          COUNT(*) AS row_count FROM POLYMARKET.STREAMING.MARKETS
UNION ALL
SELECT 'MARKET_EVENTS',                  COUNT(*) FROM POLYMARKET.STREAMING.MARKET_EVENTS
UNION ALL
SELECT 'INGESTION_METRICS',              COUNT(*) FROM POLYMARKET.STREAMING.INGESTION_METRICS;

-- 5b. Check latest ingestion time (should be recent if streamer is running)
SELECT
    MAX(ingested_at) AS last_ingestion,
    DATEDIFF('minute', MAX(ingested_at), CURRENT_TIMESTAMP()) AS minutes_since_last,
    COUNT(DISTINCT batch_id) AS total_batches,
    COUNT(DISTINCT id) AS unique_markets
FROM POLYMARKET.STREAMING.MARKETS;

-- 5c. Verify deduplication: V_LATEST_MARKETS should have fewer rows than MARKETS
SELECT
    (SELECT COUNT(*) FROM POLYMARKET.STREAMING.MARKETS) AS raw_rows,
    (SELECT COUNT(*) FROM POLYMARKET.STREAMING.V_LATEST_MARKETS) AS deduped_rows,
    (SELECT COUNT(*) FROM POLYMARKET.STREAMING.MARKETS) -
    (SELECT COUNT(*) FROM POLYMARKET.STREAMING.V_LATEST_MARKETS) AS duplicate_rows;

-- 5d. Check ingestion metrics (pipeline health)
SELECT
    COUNT(*) AS total_batches,
    SUM(markets_streamed) AS total_markets_streamed,
    AVG(total_duration_ms) AS avg_cycle_ms,
    MAX(batch_timestamp) AS last_batch,
    SUM(CASE WHEN error_message IS NOT NULL THEN 1 ELSE 0 END) AS error_batches
FROM POLYMARKET.STREAMING.INGESTION_METRICS;

-- 5e. Verify views return data
SELECT 'V_ACTIVE_MARKETS' AS view_name,        COUNT(*) AS row_count FROM POLYMARKET.STREAMING.V_ACTIVE_MARKETS
UNION ALL
SELECT 'V_LATEST_MARKETS',                     COUNT(*) FROM POLYMARKET.STREAMING.V_LATEST_MARKETS
UNION ALL
SELECT 'V_MARKET_VOLUME_SUMMARY',              COUNT(*) FROM POLYMARKET.STREAMING.V_MARKET_VOLUME_SUMMARY
UNION ALL
SELECT 'V_INGESTION_HEALTH',                   COUNT(*) FROM POLYMARKET.STREAMING.V_INGESTION_HEALTH
UNION ALL
SELECT 'V_STREAMING_CHANNEL_HEALTH',           COUNT(*) FROM POLYMARKET.STREAMING.V_STREAMING_CHANNEL_HEALTH
UNION ALL
SELECT 'V_STREAMING_OPERATIONAL_HEALTH',       COUNT(*) FROM POLYMARKET.STREAMING.V_STREAMING_OPERATIONAL_HEALTH;


-- 5f. Verify SSv2 default pipes are visible
SHOW PIPES IN SCHEMA POLYMARKET.STREAMING;

-- 5g. Check streaming channel history (errors, latency, rows)
SELECT
    CHANNEL_NAME,
    PIPE_NAME,
    TABLE_NAME,
    ROWS_INSERTED,
    ROWS_PARSED,
    ROW_ERROR_COUNT,
    LAST_ERROR_MESSAGE,
    SNOWFLAKE_PROCESSING_LATENCY_MS,
    CREATED_ON
FROM SNOWFLAKE.ACCOUNT_USAGE.SNOWPIPE_STREAMING_CHANNEL_HISTORY
WHERE TABLE_DATABASE_NAME = 'POLYMARKET'
ORDER BY CREATED_ON DESC
LIMIT 20;

-- 5h. Check channel health monitoring view
SELECT * FROM POLYMARKET.STREAMING.V_STREAMING_CHANNEL_HEALTH
LIMIT 10;

-- 5i. Verify ERROR_LOGGING is enabled (should return rows after errors occur)
-- Error tables are auto-created when streaming errors are captured.
-- If no errors have occurred yet, this returns 0 rows (expected).
SELECT 'MARKETS errors' AS check_name, COUNT(*) AS error_rows
FROM TABLE(POLYMARKET.INFORMATION_SCHEMA.STREAMING_ERROR_LOG('STREAMING.MARKETS'));

-- 5j. Check operational health (real-time pipeline status per table)
SELECT * FROM POLYMARKET.STREAMING.V_STREAMING_OPERATIONAL_HEALTH;


-- ============================================================
-- SECTION 6: DATA EXPLORATION QUERIES
-- ============================================================
-- Useful queries for exploring and analyzing the data once
-- the pipeline is running.
-- ============================================================

-- 6a. Top 20 markets by trading volume
SELECT
    id,
    question,
    category,
    volume_num,
    volume_24hr,
    liquidity_num,
    outcome_prices,
    outcomes
FROM POLYMARKET.STREAMING.V_LATEST_MARKETS
WHERE active = TRUE
ORDER BY volume_num DESC NULLS LAST
LIMIT 20;

-- 6b. Category breakdown: market count, volume, liquidity
SELECT
    COALESCE(category, 'Uncategorized') AS category,
    COUNT(*) AS market_count,
    ROUND(SUM(volume_num), 2) AS total_volume,
    ROUND(SUM(volume_24hr), 2) AS total_24hr_volume,
    ROUND(SUM(liquidity_num), 2) AS total_liquidity,
    ROUND(AVG(spread), 4) AS avg_spread
FROM POLYMARKET.STREAMING.V_LATEST_MARKETS
WHERE active = TRUE
GROUP BY category
ORDER BY total_volume DESC NULLS LAST;

-- 6c. Markets closing soon (next 7 days)
SELECT
    id,
    question,
    category,
    end_date,
    DATEDIFF('hour', CURRENT_TIMESTAMP(), end_date) AS hours_remaining,
    volume_num,
    outcome_prices
FROM POLYMARKET.STREAMING.V_LATEST_MARKETS
WHERE active = TRUE
  AND end_date IS NOT NULL
  AND end_date BETWEEN CURRENT_TIMESTAMP() AND DATEADD('day', 7, CURRENT_TIMESTAMP())
ORDER BY end_date ASC
LIMIT 20;

-- 6d. Highest-spread markets (potential trading opportunities)
SELECT
    id,
    question,
    category,
    spread,
    volume_num,
    liquidity_num,
    outcome_prices
FROM POLYMARKET.STREAMING.V_LATEST_MARKETS
WHERE active = TRUE AND spread IS NOT NULL AND spread > 0
ORDER BY spread DESC
LIMIT 20;

-- 6e. Ingestion throughput over time (last 24 hours)
SELECT
    DATE_TRUNC('hour', batch_timestamp) AS hour_bucket,
    COUNT(*) AS batches,
    SUM(markets_streamed) AS markets_ingested,
    ROUND(AVG(total_duration_ms), 0) AS avg_cycle_ms,
    ROUND(AVG(fetch_duration_ms), 0) AS avg_fetch_ms,
    ROUND(AVG(stream_duration_ms), 0) AS avg_stream_ms,
    SUM(CASE WHEN error_message IS NOT NULL THEN 1 ELSE 0 END) AS errors
FROM POLYMARKET.STREAMING.INGESTION_METRICS
WHERE batch_timestamp >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
GROUP BY hour_bucket
ORDER BY hour_bucket DESC;

-- 6f. Data freshness per category
SELECT
    COALESCE(category, 'Uncategorized') AS category,
    COUNT(*) AS market_count,
    MAX(ingested_at) AS last_updated,
    DATEDIFF('minute', MAX(ingested_at), CURRENT_TIMESTAMP()) AS minutes_stale
FROM POLYMARKET.STREAMING.V_LATEST_MARKETS
WHERE active = TRUE
GROUP BY category
ORDER BY minutes_stale DESC;

-- 6g. Batch history (last 10 batches)
SELECT
    batch_id,
    batch_timestamp,
    markets_fetched,
    markets_streamed,
    events_streamed,
    ROUND(fetch_duration_ms, 0) AS fetch_ms,
    ROUND(stream_duration_ms, 0) AS stream_ms,
    ROUND(total_duration_ms, 0) AS total_ms,
    error_message
FROM POLYMARKET.STREAMING.INGESTION_METRICS
ORDER BY batch_timestamp DESC
LIMIT 10;


-- ============================================================
-- SETUP COMPLETE
-- ============================================================
-- Next steps:
--   1. Configure snowflake_config.json (Python streamer)
--   2. Configure .env.local (React dashboard)
--   3. Start streaming:  ./manage.sh stream
--   4. Start dashboard:  ./manage.sh start
--   5. Run validation queries above to verify data flow
--
-- Components created:
--   TABLES (3): MARKETS, MARKET_EVENTS, INGESTION_METRICS
--   VIEWS  (6): V_LATEST_MARKETS, V_ACTIVE_MARKETS,
--               V_MARKET_VOLUME_SUMMARY, V_INGESTION_HEALTH,
--               V_STREAMING_CHANNEL_HEALTH, V_STREAMING_OPERATIONAL_HEALTH
--
-- Features enabled:
--   ROW_TIMESTAMP = TRUE        (all tables)
--   ROW_TIMESTAMP_DEFAULT = TRUE (schema-level for new tables)
--   ERROR_LOGGING = TRUE        (all tables, captures failed SSv2 rows)
--
-- SSv2 Default Pipes (Snowflake-managed, auto-created):
--   MARKETS-STREAMING
--   MARKET_EVENTS-STREAMING
--   INGESTION_METRICS-STREAMING
--
-- Monitoring:
--   V_STREAMING_CHANNEL_HEALTH     - Channel errors, latency (ACCOUNT_USAGE, may have lag)
--   V_STREAMING_OPERATIONAL_HEALTH - Real-time pipeline status per table
--   SNOWPIPE_STREAMING_CHANNEL_HISTORY - Full account-usage history
-- ============================================================
