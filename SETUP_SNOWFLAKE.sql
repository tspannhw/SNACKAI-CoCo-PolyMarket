-- ============================================================
-- Polymarket Prediction Markets - Snowflake Setup
-- Snowpipe Streaming High-Performance v2
-- ============================================================
-- Run this as ACCOUNTADMIN or a role with CREATE DATABASE privileges

-- Database and Schema
CREATE DATABASE IF NOT EXISTS POLYMARKET;
CREATE SCHEMA IF NOT EXISTS POLYMARKET.STREAMING;

USE DATABASE POLYMARKET;
USE SCHEMA STREAMING;

-- ============================================================
-- Primary Markets Table (landing table for SSv2 streaming)
-- ============================================================
CREATE OR REPLACE TABLE POLYMARKET.STREAMING.MARKETS (
    id                    VARCHAR(255),
    question              VARCHAR(4000),
    condition_id          VARCHAR(255),
    slug                  VARCHAR(1000),
    description           VARCHAR(16000),
    category              VARCHAR(255),
    end_date              TIMESTAMP_NTZ,
    start_date            TIMESTAMP_NTZ,
    image                 VARCHAR(2000),
    icon                  VARCHAR(2000),
    active                BOOLEAN,
    closed                BOOLEAN,
    archived              BOOLEAN,
    featured              BOOLEAN,
    restricted            BOOLEAN,
    new_market            BOOLEAN,
    market_type           VARCHAR(100),
    format_type           VARCHAR(100),
    outcomes              VARCHAR(4000),
    outcome_prices        VARCHAR(4000),
    volume                FLOAT,
    volume_num            FLOAT,
    volume_24hr           FLOAT,
    volume_1wk            FLOAT,
    volume_1mo            FLOAT,
    volume_1yr            FLOAT,
    liquidity             FLOAT,
    liquidity_num         FLOAT,
    spread                FLOAT,
    lower_bound           VARCHAR(255),
    upper_bound           VARCHAR(255),
    clob_token_ids        VARCHAR(4000),
    accepting_orders      BOOLEAN,
    comments_enabled      BOOLEAN,
    enable_order_book     BOOLEAN,
    maker_base_fee        FLOAT,
    taker_base_fee        FLOAT,
    notifications_enabled BOOLEAN,
    score                 FLOAT,
    created_at            TIMESTAMP_NTZ,
    updated_at            TIMESTAMP_NTZ,
    ingested_at           TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    batch_id              VARCHAR(100)
);

-- ============================================================
-- Market Events Table
-- ============================================================
CREATE OR REPLACE TABLE POLYMARKET.STREAMING.MARKET_EVENTS (
    event_id              VARCHAR(255),
    ticker                VARCHAR(255),
    slug                  VARCHAR(1000),
    title                 VARCHAR(4000),
    description           VARCHAR(16000),
    category              VARCHAR(255),
    start_date            TIMESTAMP_NTZ,
    end_date              TIMESTAMP_NTZ,
    image                 VARCHAR(2000),
    active                BOOLEAN,
    closed                BOOLEAN,
    volume                FLOAT,
    liquidity             FLOAT,
    open_interest         FLOAT,
    neg_risk              BOOLEAN,
    comment_count         INTEGER,
    market_count          INTEGER,
    ingested_at           TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    batch_id              VARCHAR(100)
);

-- ============================================================
-- Streaming Metrics Table (tracks ingestion performance)
-- Populated by main.py via a dedicated SSv2 channel targeting
-- the INGESTION_METRICS-STREAMING default pipe.
-- Feeds the V_INGESTION_HEALTH view used by the dashboard's
-- Ingestion Health chart and pipeline status KPIs.
-- ============================================================
CREATE OR REPLACE TABLE POLYMARKET.STREAMING.INGESTION_METRICS (
    metric_id             VARCHAR(100),
    batch_id              VARCHAR(100),
    batch_timestamp       TIMESTAMP_TZ,
    markets_fetched       INTEGER,
    markets_streamed      INTEGER,
    events_streamed       INTEGER,
    fetch_duration_ms     FLOAT,
    stream_duration_ms    FLOAT,
    total_duration_ms     FLOAT,
    api_status_code       INTEGER,
    error_message         VARCHAR(4000),
    offset_token          INTEGER,
    channel_name          VARCHAR(255),
    ingested_at           TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- Grants
-- ============================================================
GRANT USAGE ON DATABASE POLYMARKET TO ROLE ACCOUNTADMIN;
GRANT USAGE ON SCHEMA POLYMARKET.STREAMING TO ROLE ACCOUNTADMIN;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA POLYMARKET.STREAMING TO ROLE ACCOUNTADMIN;

-- ============================================================
-- Useful Views
-- ============================================================

-- Active markets with pricing
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

-- Market volume summary
-- NOTE: Queries V_LATEST_MARKETS (deduplicated) to avoid inflated totals
-- from duplicate rows across ingestion batches.
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

-- Ingestion health dashboard
-- NOTE: This view is empty until the Python streamer (main.py) has run
-- at least once, since it populates the INGESTION_METRICS table.
-- The dashboard queries this view for the Ingestion Health chart.
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

-- Latest snapshot per market
CREATE OR REPLACE VIEW POLYMARKET.STREAMING.V_LATEST_MARKETS AS
SELECT m.*
FROM POLYMARKET.STREAMING.MARKETS m
INNER JOIN (
    SELECT id, MAX(ingested_at) AS max_ingested
    FROM POLYMARKET.STREAMING.MARKETS
    GROUP BY id
) latest ON m.id = latest.id AND m.ingested_at = latest.max_ingested;
