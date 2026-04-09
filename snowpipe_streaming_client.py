#!/usr/bin/env python3
"""
Snowpipe Streaming v2 High-Performance REST API Client

Implements the Snowpipe Streaming v2 REST API for high-throughput
real-time data ingestion into Snowflake.

Architecture:
  Local Python Client -> Snowpipe Streaming v2 REST API -> Snowflake Table

Reference:
  https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-overview
"""

import json
import logging
import time
import requests
from datetime import datetime
from typing import List, Dict, Optional

from snowflake_jwt_auth import SnowflakeJWTAuth

logger = logging.getLogger(__name__)


class SnowpipeStreamingClient:
    """
    Snowpipe Streaming v2 High-Performance REST API Client.

    Uses the default auto-created pipe (TABLE_NAME-STREAMING) for simplified
    setup. No CREATE PIPE DDL required.

    Supports PAT and JWT key-pair authentication.
    """

    def __init__(self, config_path: str = "snowflake_config.json"):
        """
        Initialize the streaming client from a JSON config file.

        Args:
            config_path: Path to snowflake_config.json
        """
        with open(config_path, 'r') as f:
            self.config = json.load(f)

        self.account = self.config['account']
        self.user = self.config['user']
        self.database = self.config['database']
        self.schema = self.config['schema']
        self.table = self.config['table']
        # Default pipe: TABLE_NAME-STREAMING (hyphen, not underscore)
        self.pipe = self.config.get('pipe', f"{self.table}-STREAMING")

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.channel_name = f"{self.config.get('channel_name', 'POLYMARKET')}_{timestamp}"

        # Initialize auth (supports PAT and JWT)
        self.auth = SnowflakeJWTAuth(self.config)

        self.ingest_host = None
        self.scoped_token = None
        self.continuation_token = None
        self.offset_token = 0

        self.stats = {
            'rows_sent': 0,
            'batches': 0,
            'bytes_sent': 0,
            'errors': 0,
            'start_time': time.time()
        }

        logger.info("=" * 70)
        logger.info("SNOWPIPE STREAMING v2 CLIENT - HIGH PERFORMANCE MODE")
        logger.info("Using ONLY Snowpipe Streaming v2 REST API")
        logger.info("=" * 70)
        logger.info(f"Database: {self.database}")
        logger.info(f"Schema:   {self.schema}")
        logger.info(f"Table:    {self.table}")
        logger.info(f"Pipe:     {self.pipe}")
        logger.info(f"Channel:  {self.channel_name}")

    def _get_account_url(self) -> str:
        """Construct the Snowflake account URL."""
        account_parts = self.account.lower().replace('_', '-').split('.')
        if len(account_parts) >= 2:
            return f"https://{account_parts[0]}.{account_parts[1]}.snowflakecomputing.com"
        return f"https://{account_parts[0]}.snowflakecomputing.com"

    def _get_scoped_token(self) -> str:
        """Get authentication token for API calls."""
        if self.scoped_token:
            return self.scoped_token

        # Use the auth module to get the token (PAT or JWT-exchanged OAuth)
        self.scoped_token = self.auth.get_scoped_token()
        logger.info("Scoped token obtained")
        return self.scoped_token

    def discover_ingest_host(self) -> str:
        """Discover the streaming ingest host endpoint."""
        logger.info("Discovering ingest host...")
        self._get_scoped_token()

        url = f"{self._get_account_url()}/v2/streaming/hostname"
        headers = {
            "Authorization": f"Bearer {self.scoped_token}",
            "Content-Type": "application/json"
        }

        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            logger.error(f"Failed to discover host: {response.status_code}")
            response.raise_for_status()

        response_text = response.text.strip()
        if not response_text:
            account = self.account.lower().replace('_', '-')
            self.ingest_host = f"{account}.snowflakecomputing.com"
        elif response_text.startswith('{'):
            result = response.json()
            self.ingest_host = result.get('hostname')
        else:
            self.ingest_host = response_text

        logger.info(f"Ingest host: {self.ingest_host}")
        return self.ingest_host

    def open_channel(self) -> dict:
        """Open a streaming channel against the default pipe."""
        if not self.ingest_host:
            self.discover_ingest_host()

        logger.info(f"Opening channel: {self.channel_name}")

        url = (f"https://{self.ingest_host}/v2/streaming/"
               f"databases/{self.database}/schemas/{self.schema}/"
               f"pipes/{self.pipe}/channels/{self.channel_name}")

        headers = {
            "Authorization": f"Bearer {self.scoped_token}",
            "Content-Type": "application/json"
        }

        response = requests.put(url, headers=headers, json={})
        response.raise_for_status()

        result = response.json()
        self.continuation_token = result.get('next_continuation_token')
        channel_status = result.get('channel_status', {})
        self.offset_token = int(
            channel_status.get('last_committed_offset_token', '0') or '0'
        )

        logger.info("Channel opened successfully")
        logger.info(f"Continuation token: {self.continuation_token}")
        logger.info(f"Initial offset: {self.offset_token}")
        return result

    def append_rows(self, rows: List[Dict]) -> dict:
        """
        Append rows to the streaming channel.

        Args:
            rows: List of dictionaries, each representing a row.
                  Keys must match table column names (case-insensitive).

        Returns:
            API response dictionary
        """
        if not self.continuation_token:
            raise ValueError("Channel not open. Call open_channel() first.")

        logger.info(f"Appending {len(rows)} rows...")

        # NDJSON format (newline-delimited JSON)
        ndjson_payload = '\n'.join(json.dumps(row) for row in rows)
        payload_bytes = ndjson_payload.encode('utf-8')

        self.offset_token += 1

        url = (f"https://{self.ingest_host}/v2/streaming/data/"
               f"databases/{self.database}/schemas/{self.schema}/"
               f"pipes/{self.pipe}/channels/{self.channel_name}/rows"
               f"?continuationToken={self.continuation_token}"
               f"&offsetToken={self.offset_token}")

        headers = {
            "Authorization": f"Bearer {self.scoped_token}",
            "Content-Type": "application/x-ndjson"
        }

        response = requests.post(url, headers=headers, data=payload_bytes)
        response.raise_for_status()

        result = response.json()
        self.continuation_token = result.get('next_continuation_token')

        self.stats['rows_sent'] += len(rows)
        self.stats['batches'] += 1
        self.stats['bytes_sent'] += len(payload_bytes)

        logger.info(f"Appended {len(rows)} rows (batch {self.stats['batches']})")
        return result

    def get_channel_status(self) -> Optional[dict]:
        """Get the current channel status."""
        if not self.ingest_host:
            return None

        url = (f"https://{self.ingest_host}/v2/streaming/"
               f"databases/{self.database}/schemas/{self.schema}/"
               f"pipes/{self.pipe}/channels/{self.channel_name}")

        headers = {
            "Authorization": f"Bearer {self.scoped_token}",
            "Content-Type": "application/json"
        }

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.warning(f"Failed to get channel status: {e}")
            return None

    def close_channel(self):
        """Close the streaming channel."""
        logger.info(f"Closing channel: {self.channel_name}")
        # Channels auto-close after inactivity
        self.print_stats()

    def print_stats(self):
        """Print ingestion statistics."""
        elapsed = time.time() - self.stats['start_time']
        logger.info("=" * 60)
        logger.info("INGESTION STATISTICS")
        logger.info("=" * 60)
        logger.info(f"Total rows sent:   {self.stats['rows_sent']}")
        logger.info(f"Total batches:     {self.stats['batches']}")
        logger.info(f"Total bytes sent:  {self.stats['bytes_sent']:,}")
        logger.info(f"Errors:            {self.stats['errors']}")
        logger.info(f"Elapsed time:      {elapsed:.2f}s")
        if elapsed > 0:
            logger.info(f"Throughput:        {self.stats['rows_sent']/elapsed:.2f} rows/sec")
        logger.info(f"Current offset:    {self.offset_token}")
        logger.info("=" * 60)

    def get_stats(self) -> dict:
        """Return stats as a dictionary."""
        elapsed = time.time() - self.stats['start_time']
        return {
            **self.stats,
            'elapsed_seconds': elapsed,
            'throughput_rps': self.stats['rows_sent'] / elapsed if elapsed > 0 else 0,
            'offset_token': self.offset_token,
            'channel_name': self.channel_name
        }
