#!/usr/bin/env python3
"""
Polymarket Snowpipe Streaming v2 - Main Application

Continuously fetches Polymarket prediction market data and streams it
to Snowflake via Snowpipe Streaming v2 High-Performance REST API.

Usage:
    python main.py                    # Run continuous streaming (default 60s interval)
    python main.py --once             # Single fetch-and-stream cycle
    python main.py --interval 30      # Custom interval in seconds
    python main.py --pages 5          # Fetch 5 pages per cycle
    python main.py --config myconfig.json  # Custom config file

Architecture:
    Polymarket API -> Python Fetcher -> SSv2 REST API -> Snowflake Tables
"""

import argparse
import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from uuid import uuid4

from polymarket_fetcher import PolymarketFetcher
from snowpipe_streaming_client import SnowpipeStreamingClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('polymarket_streaming.log', mode='a')
    ]
)
logger = logging.getLogger(__name__)

# Graceful shutdown
RUNNING = True

def signal_handler(sig, frame):
    global RUNNING
    logger.info("Shutdown signal received. Finishing current batch...")
    RUNNING = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def stream_markets(
    client: SnowpipeStreamingClient,
    fetcher: PolymarketFetcher,
    max_pages: int = 5,
    batch_size: int = 50,
    events_client: SnowpipeStreamingClient = None
) -> dict:
    """
    Fetch markets from Polymarket and stream to Snowflake.

    Returns:
        Metrics dictionary for the ingestion batch
    """
    start_time = time.time()
    metrics = {
        'batch_id': '',
        'markets_fetched': 0,
        'markets_streamed': 0,
        'events_streamed': 0,
        'errors': 0,
        'fetch_duration_ms': 0,
        'stream_duration_ms': 0,
    }

    try:
        # Fetch and transform
        fetch_start = time.time()
        markets, events, batch_id = fetcher.fetch_and_transform(max_pages=max_pages)
        metrics['fetch_duration_ms'] = (time.time() - fetch_start) * 1000
        metrics['batch_id'] = batch_id
        metrics['markets_fetched'] = len(markets)

        if not markets:
            logger.warning("No markets fetched")
            return metrics

        # Stream markets in batches
        stream_start = time.time()
        for i in range(0, len(markets), batch_size):
            batch = markets[i:i + batch_size]
            try:
                client.append_rows(batch)
                metrics['markets_streamed'] += len(batch)
            except Exception as e:
                logger.error(f"Failed to stream market batch {i}: {e}")
                metrics['errors'] += 1
                client.stats['errors'] += 1

        # Stream events via dedicated events client (SSv2 binds one channel per table)
        if events_client and events:
            events_streamed = 0
            for i in range(0, len(events), batch_size):
                event_batch = events[i:i + batch_size]
                try:
                    events_client.append_rows(event_batch)
                    events_streamed += len(event_batch)
                except Exception as e:
                    logger.error(f"Failed to stream event batch {i}: {e}")
                    metrics['errors'] += 1
            metrics['events_streamed'] = events_streamed
        else:
            metrics['events_streamed'] = len(events)

        metrics['stream_duration_ms'] = (time.time() - stream_start) * 1000
        metrics['total_duration_ms'] = (time.time() - start_time) * 1000

        # Check channel health after batch (detects fatal errors, logs metrics)
        try:
            channel_healthy = client.check_channel_health()
            if not channel_healthy:
                logger.warning("Channel was unhealthy and has been reopened")
        except Exception as e:
            logger.warning(f"Channel health check failed: {e}")

        logger.info(
            f"Batch {batch_id}: "
            f"{metrics['markets_streamed']}/{metrics['markets_fetched']} markets streamed, "
            f"{metrics['events_streamed']} events, "
            f"{metrics['total_duration_ms']:.0f}ms total"
        )

    except Exception as e:
        logger.error(f"Batch failed: {e}", exc_info=True)
        metrics['errors'] += 1

    return metrics


def create_events_client(config_path: str) -> SnowpipeStreamingClient:
    """
    Create a separate streaming client for the MARKET_EVENTS table.

    SSv2 binds one channel to one table, so we need a dedicated client
    instance for events that targets MARKET_EVENTS instead of MARKETS.
    """
    with open(config_path, 'r') as f:
        events_config = json.load(f)

    events_config['table'] = 'MARKET_EVENTS'
    events_config['pipe'] = 'MARKET_EVENTS-STREAMING'
    events_config['channel_name'] = 'EVENTS'

    events_config_path = config_path.replace('.json', '_events.json')
    with open(events_config_path, 'w') as f:
        json.dump(events_config, f, indent=2)

    return SnowpipeStreamingClient(events_config_path)


def create_metrics_client(config_path: str) -> SnowpipeStreamingClient:
    """
    Create a separate streaming client for the INGESTION_METRICS table.

    The SSv2 REST API binds one channel to one table, so we need a dedicated
    client instance for metrics that targets INGESTION_METRICS instead of MARKETS.
    """
    with open(config_path, 'r') as f:
        metrics_config = json.load(f)

    metrics_config['table'] = 'INGESTION_METRICS'
    # Override pipe to match the metrics table default pipe convention
    metrics_config['pipe'] = 'INGESTION_METRICS-STREAMING'
    metrics_config['channel_name'] = 'METRICS'

    # Write temp config for the metrics client
    metrics_config_path = config_path.replace('.json', '_metrics.json')
    with open(metrics_config_path, 'w') as f:
        json.dump(metrics_config, f, indent=2)

    return SnowpipeStreamingClient(metrics_config_path)


def stream_ingestion_metrics(metrics_client: SnowpipeStreamingClient, metrics: dict):
    """
    Stream a batch's ingestion metrics to the INGESTION_METRICS table.

    This populates the V_INGESTION_HEALTH view that the dashboard queries
    for the Ingestion Health chart and pipeline status KPIs.
    """
    try:
        row = {
            'metric_id': f"m_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}",
            'batch_id': metrics.get('batch_id', ''),
            'batch_timestamp': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f'),
            'markets_fetched': metrics.get('markets_fetched', 0),
            'markets_streamed': metrics.get('markets_streamed', 0),
            'events_streamed': metrics.get('events_streamed', 0),
            'fetch_duration_ms': metrics.get('fetch_duration_ms', 0),
            'stream_duration_ms': metrics.get('stream_duration_ms', 0),
            'total_duration_ms': metrics.get('total_duration_ms', 0),
            'api_status_code': 200 if metrics.get('markets_fetched', 0) > 0 else 0,
            'error_message': None if metrics.get('errors', 0) == 0 else f"{metrics['errors']} error(s) in batch",
            'offset_token': metrics_client.offset_token,
            'channel_name': metrics_client.channel_name,
        }

        # Log channel stats for observability
        client_stats = metrics.get('client_stats')
        if client_stats:
            logger.info(
                f"Channel stats: retries={client_stats.get('retries', 0)}, "
                f"reopens={client_stats.get('channel_reopens', 0)}, "
                f"token_refreshes={client_stats.get('token_refreshes', 0)}"
            )

        metrics_client.append_rows([row])
        logger.info(f"Ingestion metrics streamed for batch {metrics.get('batch_id', 'unknown')}")
    except Exception as e:
        logger.error(f"Failed to stream ingestion metrics: {e}")


def run_continuous(
    config_path: str = "snowflake_config.json",
    interval: int = 60,
    max_pages: int = 5,
    batch_size: int = 50
):
    """Run continuous streaming loop."""
    logger.info("=" * 70)
    logger.info("POLYMARKET STREAMING - CONTINUOUS MODE")
    logger.info(f"Interval: {interval}s | Pages: {max_pages} | Batch size: {batch_size}")
    logger.info("=" * 70)

    fetcher = PolymarketFetcher()
    client = SnowpipeStreamingClient(config_path)

    try:
        client.discover_ingest_host()
        client.open_channel()
    except Exception as e:
        logger.error(f"Failed to initialize streaming client: {e}")
        logger.error("Check your snowflake_config.json and authentication settings")
        sys.exit(1)

    # Initialize a separate client for streaming events to MARKET_EVENTS table
    events_client = None
    try:
        events_client = create_events_client(config_path)
        events_client.discover_ingest_host()
        events_client.open_channel()
        logger.info("Events streaming client initialized")
    except Exception as e:
        logger.warning(f"Could not initialize events client (events will not be streamed): {e}")

    # Initialize a separate client for streaming ingestion metrics
    metrics_client = None
    try:
        metrics_client = create_metrics_client(config_path)
        metrics_client.discover_ingest_host()
        metrics_client.open_channel()
        logger.info("Metrics streaming client initialized")
    except Exception as e:
        logger.warning(f"Could not initialize metrics client (ingestion health will not be tracked): {e}")

    cycle = 0
    while RUNNING:
        cycle += 1
        logger.info(f"\n--- Cycle {cycle} ---")

        metrics = stream_markets(client, fetcher, max_pages=max_pages, batch_size=batch_size, events_client=events_client)

        # Attach client stats for observability logging in metrics streaming
        metrics['client_stats'] = client.stats.copy()

        # Stream ingestion metrics so the dashboard can display pipeline health
        if metrics_client:
            stream_ingestion_metrics(metrics_client, metrics)

        if RUNNING and interval > 0:
            logger.info(f"Sleeping {interval}s until next fetch...")
            for _ in range(interval):
                if not RUNNING:
                    break
                time.sleep(1)

    # Shutdown
    logger.info("Shutting down...")
    client.close_channel()
    if events_client:
        events_client.close_channel()
    if metrics_client:
        metrics_client.close_channel()
    logger.info("Streaming stopped.")


def run_once(
    config_path: str = "snowflake_config.json",
    max_pages: int = 5,
    batch_size: int = 50
):
    """Run a single fetch-and-stream cycle."""
    logger.info("=" * 70)
    logger.info("POLYMARKET STREAMING - SINGLE RUN MODE")
    logger.info("=" * 70)

    fetcher = PolymarketFetcher()
    client = SnowpipeStreamingClient(config_path)

    try:
        client.discover_ingest_host()
        client.open_channel()
    except Exception as e:
        logger.error(f"Failed to initialize streaming client: {e}")
        sys.exit(1)

    # Initialize events client for streaming to MARKET_EVENTS table
    events_client = None
    try:
        events_client = create_events_client(config_path)
        events_client.discover_ingest_host()
        events_client.open_channel()
    except Exception as e:
        logger.warning(f"Could not initialize events client: {e}")

    # Initialize metrics client for tracking ingestion health
    metrics_client = None
    try:
        metrics_client = create_metrics_client(config_path)
        metrics_client.discover_ingest_host()
        metrics_client.open_channel()
    except Exception as e:
        logger.warning(f"Could not initialize metrics client: {e}")

    metrics = stream_markets(client, fetcher, max_pages=max_pages, batch_size=batch_size, events_client=events_client)

    # Stream ingestion metrics so dashboard can display pipeline health
    if metrics_client:
        stream_ingestion_metrics(metrics_client, metrics)
        metrics_client.close_channel()

    client.close_channel()
    if events_client:
        events_client.close_channel()

    print(f"\nResults:")
    print(f"  Batch ID:         {metrics['batch_id']}")
    print(f"  Markets fetched:  {metrics['markets_fetched']}")
    print(f"  Markets streamed: {metrics['markets_streamed']}")
    print(f"  Events found:     {metrics['events_streamed']}")
    print(f"  Errors:           {metrics['errors']}")
    print(f"  Fetch time:       {metrics['fetch_duration_ms']:.0f}ms")
    print(f"  Stream time:      {metrics.get('stream_duration_ms', 0):.0f}ms")
    print(f"  Total time:       {metrics.get('total_duration_ms', 0):.0f}ms")


def main():
    parser = argparse.ArgumentParser(
        description='Polymarket Snowpipe Streaming v2 High-Performance Ingestion'
    )
    parser.add_argument('--config', default='snowflake_config.json',
                        help='Path to snowflake_config.json (default: snowflake_config.json)')
    parser.add_argument('--once', action='store_true',
                        help='Run a single fetch-and-stream cycle then exit')
    parser.add_argument('--interval', type=int, default=60,
                        help='Seconds between fetch cycles (default: 60)')
    parser.add_argument('--pages', type=int, default=10,
                        help='Max API pages to fetch per cycle (default: 10, 100 markets/page)')
    parser.add_argument('--batch-size', type=int, default=50,
                        help='Rows per streaming batch (default: 50)')

    args = parser.parse_args()

    if args.once:
        run_once(
            config_path=args.config,
            max_pages=args.pages,
            batch_size=args.batch_size
        )
    else:
        run_continuous(
            config_path=args.config,
            interval=args.interval,
            max_pages=args.pages,
            batch_size=args.batch_size
        )


if __name__ == '__main__':
    main()
