# Polymarket Prediction Markets Streaming Dashboard

Stream Polymarket prediction market data to Snowflake via **Snowpipe Streaming v2 High-Performance REST API**, with a real-time React dashboard built on Next.js.

## Architecture

```
╔══════════════════════════════════════════════════════════════════════════════╗
║                 POLYMARKET STREAMING PIPELINE v1.0                           ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  ┌──────────────────┐    ┌───────────────────┐    ┌─────────────────────┐    ║
║  │ POLYMARKET API   │    │ PYTHON STREAMER   │    │ SNOWFLAKE           │    ║
║  │                  │    │                   │    │                     │    ║
║  │ gamma-api.       │───▶│ polymarket_       │───▶│ Snowpipe Streaming  │    ║
║  │ polymarket.com/  │    │ fetcher.py        │    │ v2 REST API         │    ║
║  │ markets          │    │                   │    │                     │    ║
║  │                  │    │ snowpipe_         │    │ ┌─────────────────┐ │    ║
║  │ - 100+ markets   │    │ streaming_        │    │ │ MARKETS table   │ │    ║
║  │ - Prices         │    │ client.py         │    │ │ (default pipe)  │ │    ║
║  │ - Volume         │    │                   │    │ │ MARKET_EVENTS   │ │    ║
║  │ - Categories     │    │ Auth: PAT / JWT   │    │ │ INGESTION_      │ │    ║
║  │                  │    │                   │    │ │  METRICS        │ │    ║
║  └──────────────────┘    └───────────────────┘    │ └─────────────────┘ │    ║
║                                                   │                     │    ║
║                                                   │ Views:              │    ║
║                                                   │ V_ACTIVE_MARKETS    │    ║
║  ┌─────────────────────────────────────────┐      │ V_MARKET_VOLUME_    │    ║
║  │ REACT DASHBOARD (Next.js)               │      │   SUMMARY           │    ║
║  │                                         │      │ V_INGESTION_HEALTH  │    ║
║  │ ┌─────────┐ ┌──────────┐ ┌──────────┐   │◀──---│ V_LATEST_MARKETS    │    ║
║  │ │ Market  │ │ Charts   │ │ Stats    │   │      │                     │    ║
║  │ │ Cards   │ │ Recharts │ │ KPIs     │   │      └─────────────────────┘    ║
║  │ │ Table   │ │          │ │          │   │                                 ║
║  │ └─────────┘ └──────────┘ └──────────┘   │                                 ║
║  │                                         │                                 ║
║  │ API Routes: /api/markets                │                                 ║
║  │             /api/streaming              │                                 ║
║  │             /api/export                 │                                 ║
║  └─────────────────────────────────────────┘                                 ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

## Features

| Feature                   | Description                                | Status |
|---------------------------|--------------------------------------------|--------|
| Snowpipe Streaming v2     | High-Performance REST API (10GB/s capable) | Active |
| Default Auto-Created Pipe | No `CREATE PIPE` DDL required | Active |
| PAT + JWT Auth | Programmatic Access Token or Key-Pair | Active |
| React Dashboard | Next.js 15 + Tailwind CSS 4 + Recharts | Active |
| Real-time Refresh | 15-second auto-refresh with toggle | Active |
| Market Cards + Table | Dual view with price bars | Active |
| Volume Charts | Bar, line, and pie charts by category | Active |
| Ingestion Health | Streaming metrics monitoring | Active |
| CSV/JSON Export | Download market data from dashboard | Active |
| Manage Script | start/stop/install/setup/test/validate | Active |
| Zod Validation | Type-safe API data validation | Active |
| Full Test Suite | Python pytest + Jest | Active |
| HTTP Retry + Backoff | Exponential backoff for 429/5XX errors | Active |
| Channel Auto-Reopen | Auto-reopen on 409 and fatal channel errors | Active |
| Token Refresh | Proactive token refresh before expiry | Active |
| Connection Pooling | TCP/TLS reuse via `requests.Session` | Active |
| Error Logging | SSv2 error tables for failed rows (GA Apr 2026) | Active |
| Channel Monitoring | `V_STREAMING_CHANNEL_HEALTH` view | Active |

## Data Sources

| Source | Endpoint | Refresh Rate |
|--------|----------|-------------|
| Markets | `GET /markets` | 60 seconds (configurable) |
| Events | Embedded in market response | Per fetch cycle |
| Metrics | Internal pipeline tracking | Per batch |

API Reference: https://docs.polymarket.com/api-reference/markets/list-markets

## Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| Python | 3.9+ | Streaming client |
| Node.js | 18+ | React dashboard |
| Snowflake Account | Any edition | Snowpipe Streaming enabled |
| npm | 9+ | Package management |

## Quick Start

### 1. Clone & Install

```bash
git clone <your-repo-url>
cd polymarket

# Full setup (installs Python + Node deps, validates API)
./manage.sh setup

# Or install only
./manage.sh install
```

### 2. Configure Snowflake

```bash
# Create Snowflake config for the Python streamer
cp snowflake_config.example.json snowflake_config.json
# Edit with your credentials (account, user, PAT or key path)
```

**snowflake_config.json fields:**

| Field | Description | Example |
|-------|-------------|---------|
| `account` | Snowflake account identifier | `myorg-myaccount` |
| `user` | Snowflake username | `KAFKAGUY` |
| `database` | Target database | `POLYMARKET` |
| `schema` | Target schema | `STREAMING` |
| `table` | Landing table | `MARKETS` |
| `pipe` | Pipe name (auto-created) | `MARKETS-STREAMING` |
| `warehouse` | Compute warehouse | `INGEST` |
| `role` | Snowflake role | `ACCOUNTADMIN` |
| `pat` | Programmatic Access Token | `ver:1:...` |
| `private_key_file` | RSA key path (alt to PAT) | `rsa_key.p8` |

### 3. Create Snowflake Tables

Run `SETUP_SNOWFLAKE.sql` in Snowsight or via SnowSQL:

```sql
-- This creates:
-- POLYMARKET.STREAMING.MARKETS (landing table)
-- POLYMARKET.STREAMING.MARKET_EVENTS
-- POLYMARKET.STREAMING.INGESTION_METRICS
-- Plus 4 views for the dashboard
```

### 4. Configure React Dashboard

```bash
cp .env.example .env.local
# Edit .env.local with Snowflake credentials for the Node.js app
```

The dashboard supports three auth methods (choose ONE in `.env.local`):

| Method | Env Variable | Notes |
|--------|-------------|-------|
| Key-Pair JWT | `SNOWFLAKE_PRIVATE_KEY_PATH` | Recommended for production |
| PAT | `SNOWFLAKE_PAT` | Quick start, generate in Snowsight |
| Password | `SNOWFLAKE_PASSWORD` | Basic auth |

### 5. Start Everything

```bash
# Start the React dashboard (port 4000)
./manage.sh start

# Start streaming Polymarket data to Snowflake
./manage.sh stream

# Check status of all services
./manage.sh status
```

Dashboard: http://localhost:4000

## Project Structure

```
polymarket/
├── manage.sh                     # Management script (start/stop/install/test)
├── main.py                       # Main streaming application
├── polymarket_fetcher.py         # Polymarket API client & data transformer
├── snowpipe_streaming_client.py  # Snowpipe Streaming v2 REST API client
├── snowflake_jwt_auth.py         # JWT/PAT authentication module
├── validation.py                 # Pipeline validation checks
├── test_polymarket.py            # Python test suite
├── SETUP_SNOWFLAKE.sql           # Snowflake DDL (tables, views, grants)
├── snowflake_config.example.json # Snowflake config template
├── .env.example                  # React app env template
├── .gitignore                    # Git ignore rules
├── requirements.txt              # Python dependencies
├── pyproject.toml                # Python project metadata
├── package.json                  # Node.js dependencies
├── tsconfig.json                 # TypeScript configuration
├── next.config.ts                # Next.js configuration
├── postcss.config.mjs            # PostCSS / Tailwind config
├── jest.config.js                # Jest test configuration
├── README.md                     # This file
│
├── app/                          # Next.js App Router
│   ├── layout.tsx                # Root layout
│   ├── page.tsx                  # Main dashboard page
│   ├── globals.css               # Global styles (Tailwind)
│   ├── api/
│   │   ├── markets/route.ts      # GET /api/markets
│   │   ├── streaming/route.ts    # GET /api/streaming (metrics)
│   │   └── export/route.ts       # POST /api/export (CSV/JSON)
│   └── components/
│       ├── charts.tsx            # StatCard, VolumeByCategoryChart, etc.
│       └── market-cards.tsx      # MarketCard, MarketTable
│
├── lib/                          # Shared libraries
│   ├── snowflake.ts              # Snowflake connection pool
│   ├── utils.ts                  # Utility functions (cn, formatNumber)
│   └── validations.ts            # Zod schemas, type definitions
│
├── __tests__/                    # Jest tests
│   └── utils.test.ts             # Utility function tests
│
└── scripts/                      # Helper scripts
```

## manage.sh Commands Reference

### Dashboard

| Command | Description |
|---------|-------------|
| `./manage.sh install` | Install Python + Node.js dependencies |
| `./manage.sh setup` | Full setup: install, validate API, check config |
| `./manage.sh start` | Start React dashboard in background (port 4000) |
| `./manage.sh stop` | Stop the React dashboard |
| `./manage.sh restart` | Restart the React dashboard |
| `./manage.sh status` | Check status of all services |
| `./manage.sh dev` | Start dashboard in foreground (interactive) |
| `./manage.sh build` | Build React app for production |
| `./manage.sh prod` | Start production server |

### Streaming

| Command | Description |
|---------|-------------|
| `./manage.sh stream` | Start continuous streaming (background, 60s interval) |
| `./manage.sh stream-once` | Single fetch-and-stream cycle |
| `./manage.sh stream-stop` | Stop background streamer |
| `./manage.sh stream-logs` | Show streamer log output |

### Testing & Validation

| Command | Description |
|---------|-------------|
| `./manage.sh test` | Run all tests (Python + Jest) |
| `./manage.sh validate` | Run full pipeline validation |
| `./manage.sh test-api` | Test Polymarket API connectivity |
| `./manage.sh test-auth` | Test Snowflake authentication |
| `./manage.sh check-data` | Check Snowflake streaming client config |

### Maintenance

| Command | Description |
|---------|-------------|
| `./manage.sh logs` | Show dashboard server logs |
| `./manage.sh clean` | Remove build artifacts, node_modules, venv |

## Authentication

The Python streaming client supports two methods:

### Option 1: Programmatic Access Token (PAT) - Recommended

Generate a PAT in Snowsight under **User Menu > Preferences > Authentication > Programmatic Access Tokens**.

```json
{
  "pat": "ver:1:your_token_here"
}
```

### Option 2: JWT Key-Pair Authentication

```bash
# Generate RSA key pair
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub

# Register public key with Snowflake user
ALTER USER your_user SET RSA_PUBLIC_KEY='<paste public key body>';
```

```json
{
  "private_key_file": "rsa_key.p8"
}
```

## Snowpipe Streaming v2 Concepts

### Default Pipe

The high-performance architecture auto-creates a **default pipe** for every table. No `CREATE PIPE` DDL is required.

- **Naming convention**: `TABLE_NAME-STREAMING` (hyphen, not underscore)
- **Example**: Table `MARKETS` → Pipe `MARKETS-STREAMING`
- The pipe is created on-demand at first successful channel open

### Data Flow

1. Python client calls `GET /v2/streaming/hostname` to discover the ingest endpoint
2. Opens a channel via `PUT /v2/streaming/.../pipes/MARKETS-STREAMING/channels/...`
3. Appends rows as NDJSON via `POST /v2/streaming/data/.../rows`
4. Data appears in the table within 5-10 seconds

### Billing

Throughput-based billing. See [Snowpipe Streaming costs](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-overview).

## Snowflake Schema

### MARKETS Table (Landing)

| Column | Type | Description |
|--------|------|-------------|
| id | VARCHAR(255) | Market ID from Polymarket |
| question | VARCHAR(4000) | Market question text |
| category | VARCHAR(255) | Category (Politics, Crypto, etc.) |
| outcomes | VARCHAR(4000) | JSON array of outcome names |
| outcome_prices | VARCHAR(4000) | JSON array of prices (0-1) |
| volume_num | FLOAT | Total trading volume |
| volume_24hr | FLOAT | 24-hour trading volume |
| liquidity_num | FLOAT | Available liquidity |
| spread | FLOAT | Bid-ask spread |
| active | BOOLEAN | Market is active |
| closed | BOOLEAN | Market is closed |
| end_date | TIMESTAMP_NTZ | Market end date |
| ingested_at | TIMESTAMP_TZ | When streamed to Snowflake |
| batch_id | VARCHAR(100) | Ingestion batch identifier |

### Views

| View | Description |
|------|-------------|
| `V_ACTIVE_MARKETS` | Active, non-closed markets ordered by volume |
| `V_LATEST_MARKETS` | Latest snapshot per market (deduplicated) |
| `V_MARKET_VOLUME_SUMMARY` | Volume aggregated by category |
| `V_INGESTION_HEALTH` | Streaming metrics by minute |
| `V_STREAMING_CHANNEL_HEALTH` | SSv2 channel errors, latency, row counts (ACCOUNT_USAGE, may have lag) |
| `V_STREAMING_OPERATIONAL_HEALTH` | Real-time pipeline status per table (HEALTHY/STALE/OFFLINE) |

### Row Timestamps (METADATA$ROW_LAST_COMMIT_TIME)

All three tables have `ROW_TIMESTAMP = TRUE` enabled, which exposes the `METADATA$ROW_LAST_COMMIT_TIME` virtual column. This tracks when each row was last committed to Snowflake, enabling:

- **Ingestion latency measurement**: Compare `METADATA$ROW_LAST_COMMIT_TIME` against client-side timestamps to measure SSv2 commit delay
- **Change tracking**: Identify the most recently modified rows
- **Incremental processing**: Use row timestamps for efficient downstream ETL

The schema also has `ROW_TIMESTAMP_DEFAULT = TRUE`, so any new tables created in `POLYMARKET.STREAMING` automatically get row timestamps.

```sql
-- Query row timestamps
SELECT METADATA$ROW_LAST_COMMIT_TIME AS committed_at, id, question
FROM POLYMARKET.STREAMING.MARKETS
ORDER BY committed_at DESC
LIMIT 10;
```

### Timezone Display

The dashboard displays all dates and times in **US Eastern Time (EST/EDT)**:

- Header shows the full current date: e.g., "Wednesday, April 9, 2026, 2:30 PM EDT"
- Ingestion Health chart X-axis labels use 12-hour EST format
- Market card end dates show date + time in EST
- The `formatDateEST()` and `formatTimeEST()` utilities in `lib/utils.ts` use `America/New_York` timezone via `Intl.DateTimeFormat`

### Error Logging (SSv2 Error Tables)

All three tables have `ERROR_LOGGING = TRUE` enabled (GA April 8, 2026). When a row fails server-side validation during Snowpipe Streaming (schema mismatch, type errors, constraint violations), it is captured in a dedicated error table instead of being silently dropped.

```sql
-- Check for streaming errors on the MARKETS table
SELECT * FROM TABLE(POLYMARKET.INFORMATION_SCHEMA.STREAMING_ERROR_LOG('STREAMING.MARKETS'));
```

Error tables are auto-created by Snowflake when errors occur. If no errors have happened yet, the query returns 0 rows (expected).

### Channel Monitoring

The `V_STREAMING_CHANNEL_HEALTH` view provides a dashboard over `SNOWFLAKE.ACCOUNT_USAGE.SNOWPIPE_STREAMING_CHANNEL_HISTORY`:

```sql
-- Check channel health, errors, and latency
SELECT * FROM POLYMARKET.STREAMING.V_STREAMING_CHANNEL_HEALTH LIMIT 10;
```

Key columns: `ROWS_INSERTED`, `ROWS_PARSED`, `ROW_ERROR_COUNT`, `LAST_ERROR_MESSAGE`, `SNOWFLAKE_PROCESSING_LATENCY_MS`.

**Note**: ACCOUNT_USAGE views have up to 45-minute latency and may take longer to populate for SSv2 High-Performance channels. If `V_STREAMING_CHANNEL_HEALTH` returns 0 rows, use the operational health view instead.

### Operational Health (Real-Time)

The `V_STREAMING_OPERATIONAL_HEALTH` view provides real-time pipeline status by querying table data directly (no ACCOUNT_USAGE dependency):

```sql
-- Real-time status of all three streaming tables
SELECT * FROM POLYMARKET.STREAMING.V_STREAMING_OPERATIONAL_HEALTH;
```

Returns one row per table (MARKETS, MARKET_EVENTS, INGESTION_METRICS) with: `row_count`, `batch_count`, `last_ingested`, `seconds_since_last`, `pipeline_status` (HEALTHY/STALE/OFFLINE).

**SSv2 Default Pipe Visibility**: The auto-created pipes (`MARKETS-STREAMING`, `MARKET_EVENTS-STREAMING`, `INGESTION_METRICS-STREAMING`) are Snowflake-managed (`is_snowflake_managed=true`, `owner=NULL`). ACCOUNTADMIN can see them via `SHOW PIPES IN SCHEMA POLYMARKET.STREAMING` but GRANT MONITOR is not supported on managed pipes.

### SSv2 Client Error Handling

The Python streaming client (`snowpipe_streaming_client.py`) implements the full SSv2 error handling spec:

| HTTP Code | Error | Client Action |
|-----------|-------|---------------|
| 429 | Throttling | Exponential backoff retry (respects `Retry-After` header) |
| 408 | Request timeout | Exponential backoff retry |
| 500/502/503/504 | Server error | Exponential backoff retry (up to 5 retries) |
| 409 | Channel invalidated | Auto-reopen channel, resume from last committed offset |
| 401 | Unauthorized | Refresh token, retry once |
| 403 | Forbidden | Refresh token, retry once; if persistent, fail with error |

**Fatal channel error codes** (auto-reopen via `check_channel_health()`):

- `ERR_PIPE_DOES_NOT_EXIST_OR_NOT_AUTHORIZED`
- `ERR_TABLE_DOES_NOT_EXIST_NOT_AUTHORIZED`
- `ERR_CHANNEL_HAS_INVALID_ROW_SEQUENCER`
- `ERR_CHANNEL_HAS_INVALID_CLIENT_SEQUENCER`
- `ERR_CHANNEL_MUST_BE_REOPENED`
- `ERR_CHANNEL_MUST_BE_REOPENED_DUE_TO_ROW_SEQ_GAP`

**Performance optimizations**:

- **Connection pooling**: Uses `requests.Session()` for TCP/TLS connection reuse across requests
- **Token refresh**: Proactively refreshes auth tokens 5 minutes before expiry (no mid-batch auth failures)
- **Channel health checks**: After each batch, polls channel status to detect errors early

### INGESTION_METRICS Table

| Column | Type | Description |
|--------|------|-------------|
| metric_id | VARCHAR(100) | Unique metric row ID (`m_YYYYMMDD_HHMMSS_hex`) |
| batch_id | VARCHAR(100) | Matches the batch in MARKETS table |
| batch_timestamp | TIMESTAMP_NTZ | When the batch was processed |
| markets_fetched | NUMBER | Markets returned from Polymarket API |
| markets_streamed | NUMBER | Markets successfully streamed to Snowflake |
| events_streamed | NUMBER | Events successfully streamed |
| fetch_duration_ms | NUMBER | API fetch time in milliseconds |
| stream_duration_ms | NUMBER | Snowflake streaming time in milliseconds |
| total_duration_ms | NUMBER | Total batch processing time |
| api_status_code | NUMBER | HTTP status code (200 = success) |
| error_message | VARCHAR(4000) | Error details (NULL if no errors) |
| offset_token | VARCHAR(255) | SSv2 offset token for exactly-once semantics |
| channel_name | VARCHAR(255) | SSv2 channel name |

### MARKET_EVENTS Table

Events are parent objects that group related markets. Populated via a dedicated SSv2 channel targeting the `MARKET_EVENTS-STREAMING` default pipe. Events are extracted from the embedded `events` array in each market API response and deduplicated by event_id.

| Column | Type | Description |
|--------|------|-------------|
| event_id | VARCHAR(255) | Event ID from Polymarket |
| ticker | VARCHAR(255) | Event ticker symbol |
| slug | VARCHAR(1000) | URL-safe event slug |
| title | VARCHAR(4000) | Event title text |
| description | VARCHAR(16000) | Event description |
| category | VARCHAR(255) | Event category |
| start_date | TIMESTAMP_NTZ | Event start date |
| end_date | TIMESTAMP_NTZ | Event end date |
| volume | FLOAT | Total event volume across all markets (USD) |
| liquidity | FLOAT | Total event liquidity (USD) |
| market_count | INTEGER | Number of markets in this event |
| ingested_at | TIMESTAMP_TZ | When streamed to Snowflake |
| batch_id | VARCHAR(100) | Ingestion batch identifier |

## Pipeline Status Monitoring

The dashboard displays real-time pipeline health based on the `LAST_INGESTED` timestamp from `V_LATEST_MARKETS`:

| Status | Indicator | Condition | Meaning |
|--------|-----------|-----------|---------|
| **Healthy** | Green pulsing dot | Last ingestion < 3 minutes ago | Streamer is running normally |
| **Stale** | Yellow dot + warning banner | Last ingestion 3-10 minutes ago | Streamer may have stopped or is lagging |
| **Offline** | Red dot + error banner | Last ingestion > 10 minutes ago or no data | Streamer is not running |

When the pipeline is stale or offline, the dashboard shows a banner with instructions to start the streamer via `./manage.sh stream`.

### Ingestion Metrics Flow

Each streaming cycle in `main.py` produces one row in `INGESTION_METRICS`:

```
Polymarket API → fetch markets → transform → stream to MARKETS table
                                           → stream to MARKET_EVENTS table
                                           → compute metrics
                                           → stream to INGESTION_METRICS table
```

The markets, events, and metrics are each streamed via **separate SSv2 clients** (one channel per table is required by SSv2). Events are extracted from the embedded `events` array in each market API response and deduplicated by event_id. The dashboard's `/api/streaming` route reads `V_INGESTION_HEALTH` to show batch history, throughput, and error rates.

### Connection Recovery

The dashboard's Snowflake connection (`lib/snowflake.ts`) includes automatic recovery:

1. If a query fails with a connection error (network, socket, timeout, ECONNRESET)
2. The cached connection is destroyed via `resetConnection()`
3. A new connection is established and the query is retried once
4. If the retry also fails, the error is returned to the caller

## API Endpoints (React Dashboard)

| Endpoint | Method | Cache | Description |
|----------|--------|-------|-------------|
| `/api/markets` | GET | 15s | Active markets with prices and volume |
| `/api/streaming` | GET | 30s | Volume summary, ingestion health, totals |
| `/api/export` | POST | — | Export data as CSV or JSON |

### `/api/markets` Query Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `limit` | 500 | Max markets to return (max 1000) |
| `category` | — | Filter by category |
| `active` | `true` | Filter active markets |

### `/api/export` Request Body

```json
{
  "data": [{"id": "1", "question": "..."}],
  "filename": "polymarket_export",
  "format": "csv"
}
```

## Python Streaming Client Usage

### Continuous mode (default)

```bash
python main.py --interval 60 --pages 10 --batch-size 50
```

### Single run

```bash
python main.py --once --pages 3
```

### CLI Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--config` | `snowflake_config.json` | Config file path |
| `--once` | false | Single cycle then exit |
| `--interval` | 60 | Seconds between cycles |
| `--pages` | 10 | Max API pages per cycle (100 markets/page) |
| `--batch-size` | 50 | Rows per streaming batch |

## Testing

```bash
# All tests (Python + Jest)
./manage.sh test

# Python tests only
python -m pytest test_polymarket.py -v

# Jest tests only
npm test

# Pipeline validation (7 checks)
./manage.sh validate

# Polymarket API check
./manage.sh test-api
```

### Test Coverage

**Python tests** (`test_polymarket.py` — 32 tests):

| Test Class | Tests | What It Covers |
|------------|-------|----------------|
| `TestPolymarketFetcher` | 4 | API fetch, pagination, error handling, data transform |
| `TestDataTransform` | 3 | Market/event row building, safe_float/safe_bool helpers |
| `TestDataIntegrity` | 6 | Required fields, edge cases, empty lists, deduplication |
| `TestMetricsRow` | 3 | Metrics row structure, error rows, failure isolation |
| `TestStreamingClient` | 3 | Client init, auth selection, row serialization |
| `TestStreamingClientRetry` | 10 | HTTP retry (429/5XX), channel reopen (409), token refresh, connection pooling, channel health |
| `TestEventsStreaming` | 3 | Events client creation, events streaming via SSv2, graceful fallback without events client |

**Jest tests** (`__tests__/utils.test.ts` — 28 tests):

| Suite | Tests | What It Covers |
|-------|-------|----------------|
| `parseOutcomes` | 5 | JSON parsing, malformed input, empty arrays |
| `parseOutcomePrices` | 5 | Price arrays, non-numeric values, edge cases |
| `formatVolume` | 5 | K/M/B suffixes, zero, negative, small values |
| `formatPrice` | 2 | Percentage formatting, null handling |
| `formatDateEST` | 3 | EST timezone output, known date formatting, null handling |
| `formatTimeEST` | 2 | EST time formatting, null/undefined handling |
| `ExportRequestSchema` | 5 | Zod validation, required fields, format enum |

### Pipeline Validation Checks

Run via `./manage.sh validate` or `python validation.py`:

| # | Check | What It Validates |
|---|-------|-------------------|
| 1 | `validate_polymarket_api` | Polymarket API is reachable and returns valid market data |
| 2 | `validate_data_transform` | Market and event row transformations produce correct schema |
| 3 | `validate_config_file` | `snowflake_config.json` exists with all required fields |
| 4 | `validate_streaming_client` | SSv2 client initializes with correct table/pipe/channel |
| 5 | `validate_metrics_client` | Metrics SSv2 client targets `INGESTION_METRICS` table |
| 6 | `validate_metrics_row_format` | Metrics rows contain all 13 required fields |
| 7 | `validate_fetch_and_transform` | End-to-end fetch → transform produces valid rows |

## Troubleshooting

| Issue | Cause | Fix |
|-------|-------|-----|
| `Connection refused` | Account URL incorrect | Verify `account` in config |
| `401 Unauthorized` on `/oauth/token` | JWT fingerprint mismatch | Ensure RSA public key is registered: `ALTER USER ... SET RSA_PUBLIC_KEY=...` |
| `400 Bad Request` on `/oauth/token` | Duplicate token exchange | Update to latest `snowpipe_streaming_client.py` (uses single auth flow) |
| `Authentication failed` | Invalid PAT or RSA key | Check `pat` value or re-register public key |
| `Table not found` | DDL not run | Execute `SETUP_SNOWFLAKE.sql` |
| `Rate limited (429)` | Too many API calls | Increase `--interval`, fetcher has retry logic |
| `Empty markets` | API pagination | Check Polymarket API status |
| Dashboard shows no data | Streamer not running | Run `./manage.sh stream` first |
| Ingestion Health chart empty | `INGESTION_METRICS` table has no rows | The Python streamer writes metrics each cycle. Run `./manage.sh stream` or `./manage.sh stream-once` |
| "Pipeline Offline" banner | No recent ingestion detected | Start the streamer: `./manage.sh stream`. The dashboard checks `LAST_INGESTED` from `V_LATEST_MARKETS` |
| "Pipeline Stale" warning | Streamer stopped or lagging | Restart with `./manage.sh stream-stop && ./manage.sh stream`. Check `./manage.sh stream-logs` for errors |
| Inflated volume numbers | `V_MARKET_VOLUME_SUMMARY` querying raw table | Re-run `SETUP_SNOWFLAKE.sql` to update the view to use `V_LATEST_MARKETS` |
| Dashboard connection drops | Stale Snowflake connection cached | The dashboard auto-reconnects on connection errors. Restart if persistent: `./manage.sh restart` |
| `Snowflake not configured` | Missing `.env.local` | Copy `.env.example` to `.env.local` and set credentials |
| `ModuleNotFoundError` | Venv not activated | Run `source venv/bin/activate` |
| `409 Channel invalidated` | Channel superseded or invalidated | Client auto-reopens channel. If persistent, check for concurrent clients |
| `429 Too Many Requests` | SSv2 throttling | Client auto-retries with exponential backoff. Reduce `--batch-size` if persistent |
| `ERR_CHANNEL_MUST_BE_REOPENED` | Fatal channel error | Client auto-detects via `check_channel_health()` and reopens. Check `V_STREAMING_CHANNEL_HEALTH` for details |
| Token expiry mid-batch | OAuth token expired | Client proactively refreshes tokens 5 min before expiry. If using PAT, ensure it hasn't been revoked |
| Streaming errors not captured | `ERROR_LOGGING` not enabled | Run `ALTER TABLE ... SET ERROR_LOGGING = TRUE` (see `SETUP_SNOWFLAKE.sql` Section 3c) |
| `MARKET_EVENTS` table empty | Events not streamed (pre-v1.1) | Update `main.py` to latest version with `create_events_client()`. Run `./manage.sh stream-once` to populate |
| `V_STREAMING_CHANNEL_HEALTH` empty | ACCOUNT_USAGE latency | ACCOUNT_USAGE views can take hours to populate for SSv2 HP channels. Use `V_STREAMING_OPERATIONAL_HEALTH` for real-time status |

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Data Source | Polymarket Gamma API |
| Streaming | Snowpipe Streaming v2 High-Performance REST API |
| Data Warehouse | Snowflake |
| Auth | PAT / JWT Key-Pair |
| Backend | Next.js 15 API Routes |
| Frontend | React 19, Tailwind CSS 4, Recharts |
| Validation | Zod (TypeScript), pytest (Python) |
| Testing | Jest + ts-jest, pytest |

## References

- [Snowpipe Streaming v2 Overview](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-overview)
- [SSv2 Error Handling](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-error-handling)
- [SSv2 Error Logging (GA Apr 2026)](https://docs.snowflake.com/en/release-notes/2026/other/2026-04-08-snowpipe-streaming-error-tables)
- [Polymarket API Docs](https://docs.polymarket.com/api-reference/markets/list-markets)
- [Snowflake PAT Auth](https://docs.snowflake.com/en/user-guide/authentication-programmatic-tokens)
- [Snowflake JWT Auth](https://docs.snowflake.com/en/developer-guide/sql-api/guide#using-key-pair-authentication)
- [SNACKAI-CoCo-NYC-OPSCenter](https://github.com/tspannhw/SNACKAI-CoCo-NYC-OPSCenter)
- [SNACKAI-CoCo-Meshtastic](https://github.com/tspannhw/SNACKAI-CoCo-Meshtastic)
- [NYC Camera Pipeline](https://github.com/tspannhw/nyccamera)

## License

Apache-2.0
