#!/usr/bin/env python3
"""
Snowpipe Streaming v2 High-Performance REST API Client

Implements the Snowpipe Streaming v2 REST API for high-throughput
real-time data ingestion into Snowflake.

Features:
  - HTTP retry with exponential backoff (429, 408, 5XX)
  - Channel reopen on 409 (channel invalidated)
  - Channel status monitoring with fatal error detection
  - Token refresh before expiry
  - Connection pooling via requests.Session
  - Comprehensive error handling per SSv2 docs

Architecture:
  Local Python Client -> Snowpipe Streaming v2 REST API -> Snowflake Table

Reference:
  https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-overview
  https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-error-handling
"""

import json
import logging
import time
import requests
from datetime import datetime
from typing import List, Dict, Optional

from snowflake_jwt_auth import SnowflakeJWTAuth

logger = logging.getLogger(__name__)

# Fatal channel error codes that require channel reopen (per SSv2 docs)
FATAL_CHANNEL_ERRORS = {
    'ERR_PIPE_DOES_NOT_EXIST_OR_NOT_AUTHORIZED',
    'ERR_TABLE_DOES_NOT_EXIST_NOT_AUTHORIZED',
    'ERR_CHANNEL_HAS_INVALID_ROW_SEQUENCER',
    'ERR_CHANNEL_HAS_INVALID_CLIENT_SEQUENCER',
    'ERR_CHANNEL_MUST_BE_REOPENED',
    'ERR_CHANNEL_MUST_BE_REOPENED_DUE_TO_ROW_SEQ_GAP',
}

# HTTP status codes that are retryable (per SSv2 docs)
RETRYABLE_STATUS_CODES = {429, 408, 500, 502, 503, 504}

# Retry configuration
MAX_RETRIES = 5
INITIAL_BACKOFF_SEC = 1.0
MAX_BACKOFF_SEC = 60.0
BACKOFF_MULTIPLIER = 2.0

# Token refresh: refresh 5 minutes before expiry
TOKEN_REFRESH_MARGIN_SEC = 300
# Default token lifetime (55 minutes for JWT OAuth, PAT doesn't expire via API)
DEFAULT_TOKEN_LIFETIME_SEC = 3300


class SnowpipeStreamingClient:
    """
    Snowpipe Streaming v2 High-Performance REST API Client.

    Uses the default auto-created pipe (TABLE_NAME-STREAMING) for simplified
    setup. No CREATE PIPE DDL required.

    Supports PAT and JWT key-pair authentication.

    Error handling follows SSv2 docs:
    - 429/408/5XX: Exponential backoff retry
    - 409: Channel invalidated, auto-reopen from last committed offset
    - 401/403: Auth error, refresh token and retry once
    - Fatal channel errors: Auto-reopen channel
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
        self._token_obtained_at = 0.0
        self.continuation_token = None
        self.offset_token = 0
        self._channel_open = False

        # Connection pooling via requests.Session for TCP/TLS reuse
        self._session = requests.Session()

        self.stats = {
            'rows_sent': 0,
            'batches': 0,
            'bytes_sent': 0,
            'errors': 0,
            'retries': 0,
            'channel_reopens': 0,
            'token_refreshes': 0,
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
        """Get authentication token, refreshing if near expiry."""
        now = time.time()
        token_age = now - self._token_obtained_at

        if self.scoped_token and token_age < (DEFAULT_TOKEN_LIFETIME_SEC - TOKEN_REFRESH_MARGIN_SEC):
            return self.scoped_token

        if self.scoped_token:
            logger.info(f"Token age {token_age:.0f}s, refreshing before expiry...")
            self.stats['token_refreshes'] += 1

        self.scoped_token = self.auth.get_scoped_token()
        self._token_obtained_at = time.time()
        logger.info("Scoped token obtained")
        return self.scoped_token

    def _refresh_token(self):
        """Force token refresh (called after 401/403 errors)."""
        logger.info("Forcing token refresh due to auth error...")
        self.scoped_token = None
        self._token_obtained_at = 0.0
        self.stats['token_refreshes'] += 1
        self._get_scoped_token()

    def _make_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        Make an HTTP request with retry logic per SSv2 docs.

        Handles:
        - 429/408/5XX: Exponential backoff retry
        - 401/403: Token refresh + single retry
        - 409: Raise immediately (caller handles channel reopen)
        """
        headers = kwargs.pop('headers', {})
        headers['Authorization'] = f"Bearer {self._get_scoped_token()}"
        if 'Content-Type' not in headers:
            headers['Content-Type'] = 'application/json'

        last_exception = None
        backoff = INITIAL_BACKOFF_SEC

        for attempt in range(MAX_RETRIES + 1):
            try:
                response = self._session.request(method, url, headers=headers, **kwargs)

                # Success
                if response.status_code < 400:
                    return response

                # Channel invalidated — raise immediately for caller to handle
                if response.status_code == 409:
                    logger.warning(f"409 Channel invalidated: {url}")
                    response.raise_for_status()

                # Auth errors — refresh token and retry once
                if response.status_code in (401, 403):
                    if attempt == 0:
                        logger.warning(f"{response.status_code} Auth error, refreshing token...")
                        self._refresh_token()
                        headers['Authorization'] = f"Bearer {self.scoped_token}"
                        continue
                    logger.error(f"{response.status_code} Auth error persists after token refresh")
                    response.raise_for_status()

                # Retryable errors (429, 408, 5XX)
                if response.status_code in RETRYABLE_STATUS_CODES:
                    self.stats['retries'] += 1
                    retry_after = response.headers.get('Retry-After')
                    wait = float(retry_after) if retry_after else backoff

                    logger.warning(
                        f"{response.status_code} on attempt {attempt + 1}/{MAX_RETRIES + 1}, "
                        f"retrying in {wait:.1f}s..."
                    )
                    time.sleep(wait)
                    backoff = min(backoff * BACKOFF_MULTIPLIER, MAX_BACKOFF_SEC)
                    continue

                # Non-retryable error
                response.raise_for_status()

            except requests.exceptions.ConnectionError as e:
                self.stats['retries'] += 1
                last_exception = e
                if attempt < MAX_RETRIES:
                    logger.warning(f"Connection error on attempt {attempt + 1}, retrying in {backoff:.1f}s: {e}")
                    time.sleep(backoff)
                    backoff = min(backoff * BACKOFF_MULTIPLIER, MAX_BACKOFF_SEC)
                    continue
                raise

        # Should not reach here, but just in case
        if last_exception:
            raise last_exception
        raise RuntimeError(f"Request failed after {MAX_RETRIES + 1} attempts")

    def discover_ingest_host(self) -> str:
        """Discover the streaming ingest host endpoint."""
        logger.info("Discovering ingest host...")

        url = f"{self._get_account_url()}/v2/streaming/hostname"
        response = self._make_request('GET', url)

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

        response = self._make_request('PUT', url, json={})
        response.raise_for_status()

        result = response.json()
        self.continuation_token = result.get('next_continuation_token')
        channel_status = result.get('channel_status', {})
        self.offset_token = int(
            channel_status.get('last_committed_offset_token', '0') or '0'
        )
        self._channel_open = True

        logger.info("Channel opened successfully")
        logger.info(f"Continuation token: {self.continuation_token}")
        logger.info(f"Initial offset: {self.offset_token}")
        return result

    def reopen_channel(self) -> dict:
        """
        Close and reopen the channel with a new name.

        Called when channel enters an invalid state (409, fatal error codes).
        Resumes from the last committed offset per SSv2 docs.
        """
        logger.warning(f"Reopening channel (was: {self.channel_name})...")
        self.stats['channel_reopens'] += 1
        self._channel_open = False

        # Generate a new channel name with fresh timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        base_name = self.config.get('channel_name', 'POLYMARKET')
        self.channel_name = f"{base_name}_{timestamp}_r{self.stats['channel_reopens']}"
        self.continuation_token = None

        logger.info(f"New channel name: {self.channel_name}")
        return self.open_channel()

    def append_rows(self, rows: List[Dict]) -> dict:
        """
        Append rows to the streaming channel.

        Handles:
        - 409: Auto-reopen channel and retry
        - Retryable errors: Exponential backoff via _make_request

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

        try:
            response = self._make_request(
                'POST', url,
                headers={'Content-Type': 'application/x-ndjson'},
                data=payload_bytes
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 409:
                # Channel invalidated — reopen and retry once
                logger.warning("Channel invalidated (409), reopening and retrying...")
                self.reopen_channel()
                return self.append_rows(rows)
            self.stats['errors'] += 1
            raise

        result = response.json()
        self.continuation_token = result.get('next_continuation_token')

        self.stats['rows_sent'] += len(rows)
        self.stats['batches'] += 1
        self.stats['bytes_sent'] += len(payload_bytes)

        logger.info(f"Appended {len(rows)} rows (batch {self.stats['batches']})")
        return result

    def get_channel_status(self) -> Optional[dict]:
        """
        Get the current channel status.

        Returns channel health info including:
        - channel_status_code: SUCCESS or fatal error code
        - last_committed_offset_token: progress tracking
        - rows_inserted, rows_parsed, rows_error_count
        - last_error_message, last_error_timestamp
        - snowflake_avg_processing_latency_ms
        """
        if not self.ingest_host:
            return None

        url = (f"https://{self.ingest_host}/v2/streaming/"
               f"databases/{self.database}/schemas/{self.schema}/"
               f"pipes/{self.pipe}/channels/{self.channel_name}")

        try:
            response = self._make_request('GET', url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.warning(f"Failed to get channel status: {e}")
            return None

    def check_channel_health(self) -> bool:
        """
        Check channel health and auto-reopen if in a fatal error state.

        Returns:
            True if channel is healthy (SUCCESS), False if reopen was needed.
        """
        status = self.get_channel_status()
        if status is None:
            logger.warning("Could not retrieve channel status")
            return True  # Assume OK if we can't check

        channel_info = status.get('channel_status', status)
        status_code = channel_info.get('channel_status_code', 'SUCCESS')

        if status_code == 'SUCCESS':
            # Log metrics
            rows_inserted = channel_info.get('rows_inserted', 'N/A')
            rows_parsed = channel_info.get('rows_parsed', 'N/A')
            error_count = channel_info.get('rows_error_count', 0)
            latency = channel_info.get('snowflake_avg_processing_latency_ms', 'N/A')

            logger.info(
                f"Channel healthy: inserted={rows_inserted}, parsed={rows_parsed}, "
                f"errors={error_count}, latency={latency}ms"
            )
            return True

        # Fatal error — reopen required
        if status_code in FATAL_CHANNEL_ERRORS:
            error_msg = channel_info.get('last_error_message', 'unknown')
            logger.error(f"Fatal channel error: {status_code} - {error_msg}")
            self.reopen_channel()
            return False

        # Unknown status code
        logger.warning(f"Unexpected channel status: {status_code}")
        return True

    def close_channel(self):
        """Close the streaming channel."""
        logger.info(f"Closing channel: {self.channel_name}")
        self._channel_open = False
        # Channels auto-close after inactivity
        self._session.close()
        self.print_stats()

    def print_stats(self):
        """Print ingestion statistics."""
        elapsed = time.time() - self.stats['start_time']
        logger.info("=" * 60)
        logger.info("INGESTION STATISTICS")
        logger.info("=" * 60)
        logger.info(f"Total rows sent:     {self.stats['rows_sent']}")
        logger.info(f"Total batches:       {self.stats['batches']}")
        logger.info(f"Total bytes sent:    {self.stats['bytes_sent']:,}")
        logger.info(f"Errors:              {self.stats['errors']}")
        logger.info(f"Retries:             {self.stats['retries']}")
        logger.info(f"Channel reopens:     {self.stats['channel_reopens']}")
        logger.info(f"Token refreshes:     {self.stats['token_refreshes']}")
        logger.info(f"Elapsed time:        {elapsed:.2f}s")
        if elapsed > 0:
            logger.info(f"Throughput:          {self.stats['rows_sent']/elapsed:.2f} rows/sec")
        logger.info(f"Current offset:      {self.offset_token}")
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
