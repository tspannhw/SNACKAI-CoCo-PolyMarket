#!/usr/bin/env python3
"""
Tests for Polymarket Streaming Pipeline

Run: python -m pytest test_polymarket.py -v
"""

import json
import time
import pytest
import requests
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

from polymarket_fetcher import PolymarketFetcher


# ============================================================
# PolymarketFetcher Tests
# ============================================================

class TestPolymarketFetcher:
    """Tests for the Polymarket data fetcher."""

    def test_transform_market_basic(self):
        """Test basic market transformation."""
        raw = {
            "id": "market-001",
            "question": "Will Bitcoin reach $100K?",
            "conditionId": "cond-001",
            "slug": "bitcoin-100k",
            "category": "Crypto",
            "active": True,
            "closed": False,
            "volumeNum": 500000,
            "volume24hr": 12000,
            "liquidityNum": 250000,
            "outcomes": '["Yes","No"]',
            "outcomePrices": '["0.72","0.28"]',
        }

        result = PolymarketFetcher.transform_market(raw, "batch-test")

        assert result['id'] == "market-001"
        assert result['question'] == "Will Bitcoin reach $100K?"
        assert result['category'] == "Crypto"
        assert result['active'] is True
        assert result['closed'] is False
        assert result['volume_num'] == 500000
        assert result['volume_24hr'] == 12000
        assert result['liquidity'] is None  # raw has liquidityNum not liquidity
        assert result['batch_id'] == "batch-test"
        assert result['ingested_at'] is not None

    def test_transform_market_null_values(self):
        """Test transformation with null/missing values."""
        raw = {
            "id": "market-002",
            "question": None,
            "volumeNum": None,
            "active": None,
        }

        result = PolymarketFetcher.transform_market(raw, "batch-null")

        assert result['id'] == "market-002"
        assert result['question'] == ""
        assert result['volume_num'] is None
        assert result['active'] is None

    def test_transform_market_string_numbers(self):
        """Test that string numbers are converted correctly."""
        raw = {
            "id": "market-003",
            "question": "Test",
            "volume": "123456.78",
            "liquidity": "50000",
            "score": "42.5",
        }

        result = PolymarketFetcher.transform_market(raw, "batch-str")

        assert result['volume'] == 123456.78
        assert result['liquidity'] == 50000.0
        assert result['score'] == 42.5

    def test_transform_market_date_parsing(self):
        """Test date parsing from ISO format."""
        raw = {
            "id": "market-004",
            "question": "Test",
            "endDate": "2025-12-31T23:59:59Z",
            "startDate": "2025-01-01T00:00:00Z",
            "createdAt": "2025-06-15T10:30:00.123Z",
        }

        result = PolymarketFetcher.transform_market(raw, "batch-date")

        assert result['end_date'] == "2025-12-31 23:59:59"
        assert result['start_date'] == "2025-01-01 00:00:00"
        assert result['created_at'] == "2025-06-15 10:30:00"

    def test_transform_market_long_strings_truncated(self):
        """Test that long strings are truncated to column limits."""
        raw = {
            "id": "market-005",
            "question": "Q" * 5000,
            "description": "D" * 20000,
            "slug": "S" * 2000,
        }

        result = PolymarketFetcher.transform_market(raw, "batch-trunc")

        assert len(result['question']) <= 4000
        assert len(result['description']) <= 16000
        assert len(result['slug']) <= 1000

    def test_transform_event(self):
        """Test event transformation."""
        raw_event = {
            "id": "event-001",
            "ticker": "BTC",
            "slug": "bitcoin-event",
            "title": "Bitcoin Milestone",
            "category": "Crypto",
            "volume": 1000000,
            "liquidity": 500000,
            "openInterest": 250000,
            "negRisk": False,
            "commentCount": 42,
            "markets": [1, 2, 3],
        }

        result = PolymarketFetcher.transform_event(raw_event, "batch-event")

        assert result['event_id'] == "event-001"
        assert result['ticker'] == "BTC"
        assert result['title'] == "Bitcoin Milestone"
        assert result['volume'] == 1000000
        assert result['market_count'] == 3
        assert result['batch_id'] == "batch-event"

    @patch('polymarket_fetcher.requests.Session')
    def test_fetch_markets_success(self, mock_session_class):
        """Test successful market fetch."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"id": "m1", "question": "Test 1"},
            {"id": "m2", "question": "Test 2"},
        ]
        mock_session.get.return_value = mock_response

        fetcher = PolymarketFetcher()
        fetcher.session = mock_session

        markets, status = fetcher.fetch_markets(limit=10)

        assert status == 200
        assert len(markets) == 2
        assert markets[0]['id'] == 'm1'

    @patch('polymarket_fetcher.requests.Session')
    def test_fetch_markets_rate_limit(self, mock_session_class):
        """Test rate limit handling."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        mock_429 = MagicMock()
        mock_429.status_code = 429

        mock_200 = MagicMock()
        mock_200.status_code = 200
        mock_200.json.return_value = [{"id": "m1"}]

        mock_session.get.side_effect = [mock_429, mock_200]

        fetcher = PolymarketFetcher()
        fetcher.session = mock_session

        markets, status = fetcher.fetch_markets(limit=10)

        assert status == 200
        assert len(markets) == 1


class TestDataIntegrity:
    """Tests for data integrity checks."""

    def test_batch_id_format(self):
        """Test batch ID format."""
        raw = {"id": "test", "question": "Test"}
        result = PolymarketFetcher.transform_market(raw, "batch_20250101_120000_abc12345")
        assert result['batch_id'].startswith("batch_")

    def test_ingested_at_is_utc(self):
        """Test that ingested_at is in UTC."""
        raw = {"id": "test", "question": "Test"}
        result = PolymarketFetcher.transform_market(raw, "test")

        ingested = datetime.strptime(result['ingested_at'], '%Y-%m-%d %H:%M:%S.%f')
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        diff = abs((now - ingested).total_seconds())
        assert diff < 5  # within 5 seconds

    def test_all_required_market_fields_present(self):
        """Test that transform_market always produces all required columns."""
        raw = {"id": "field-check", "question": "Test"}
        result = PolymarketFetcher.transform_market(raw, "test-batch")

        required_keys = [
            'id', 'question', 'condition_id', 'slug', 'description',
            'category', 'end_date', 'start_date', 'active', 'closed',
            'volume', 'volume_num', 'volume_24hr', 'liquidity', 'liquidity_num',
            'spread', 'outcomes', 'outcome_prices', 'ingested_at', 'batch_id',
        ]
        for key in required_keys:
            assert key in result, f"Missing required key: {key}"

    def test_all_required_event_fields_present(self):
        """Test that transform_event always produces all required columns."""
        raw = {"id": "evt-check", "title": "Test Event"}
        result = PolymarketFetcher.transform_event(raw, "test-batch")

        required_keys = [
            'event_id', 'ticker', 'slug', 'title', 'description',
            'category', 'volume', 'liquidity', 'market_count',
            'ingested_at', 'batch_id',
        ]
        for key in required_keys:
            assert key in result, f"Missing required key: {key}"

    def test_safe_float_edge_cases(self):
        """Test that string 'null', empty, and invalid values are handled."""
        raw = {
            "id": "edge-cases",
            "question": "Test",
            "volume": "null",
            "volumeNum": "",
            "liquidity": "not-a-number",
            "score": "0",
        }
        result = PolymarketFetcher.transform_market(raw, "test")

        assert result['volume'] is None       # "null" string -> None
        assert result['volume_num'] is None   # empty string -> None
        assert result['liquidity'] is None    # invalid string -> None
        assert result['score'] == 0.0         # "0" -> 0.0

    def test_safe_bool_edge_cases(self):
        """Test boolean handling for various input types."""
        # True values
        raw_true = {"id": "bool-t", "question": "T", "active": "true"}
        assert PolymarketFetcher.transform_market(raw_true, "t")['active'] is True

        # False values
        raw_false = {"id": "bool-f", "question": "F", "active": "false"}
        assert PolymarketFetcher.transform_market(raw_false, "t")['active'] is False

        # Native bool
        raw_native = {"id": "bool-n", "question": "N", "active": True}
        assert PolymarketFetcher.transform_market(raw_native, "t")['active'] is True

    def test_empty_market_list_transform(self):
        """Test fetch_and_transform handles empty API response gracefully."""
        fetcher = PolymarketFetcher()
        # Patch fetch_all_markets to return empty
        with patch.object(fetcher, 'fetch_all_markets', return_value=[]):
            markets, events, batch_id = fetcher.fetch_and_transform(max_pages=1)
            assert markets == []
            assert events == []
            assert batch_id.startswith("batch_")

    def test_duplicate_events_deduplicated(self):
        """Test that duplicate events from different markets are deduplicated."""
        fetcher = PolymarketFetcher()
        shared_event = {"id": "evt-shared", "title": "Shared Event", "markets": [1, 2]}
        raw_markets = [
            {"id": "m1", "question": "Q1", "events": [shared_event]},
            {"id": "m2", "question": "Q2", "events": [shared_event]},
        ]
        with patch.object(fetcher, 'fetch_all_markets', return_value=raw_markets):
            markets, events, batch_id = fetcher.fetch_and_transform(max_pages=1)
            assert len(markets) == 2
            assert len(events) == 1  # deduped by event ID


class TestMetricsRow:
    """Tests for ingestion metrics row generation."""

    def test_metrics_row_structure(self):
        """Test that stream_ingestion_metrics builds a valid row."""
        from main import stream_ingestion_metrics
        from unittest.mock import MagicMock

        mock_client = MagicMock()
        mock_client.offset_token = 42
        mock_client.channel_name = "TEST_CHANNEL"

        metrics = {
            'batch_id': 'batch_test_123',
            'markets_fetched': 100,
            'markets_streamed': 98,
            'events_streamed': 5,
            'errors': 0,
            'fetch_duration_ms': 1500.0,
            'stream_duration_ms': 300.0,
            'total_duration_ms': 1800.0,
        }

        stream_ingestion_metrics(mock_client, metrics)

        # Verify append_rows was called with one row
        mock_client.append_rows.assert_called_once()
        rows = mock_client.append_rows.call_args[0][0]
        assert len(rows) == 1

        row = rows[0]
        assert row['batch_id'] == 'batch_test_123'
        assert row['markets_fetched'] == 100
        assert row['markets_streamed'] == 98
        assert row['events_streamed'] == 5
        assert row['api_status_code'] == 200
        assert row['error_message'] is None
        assert row['offset_token'] == 42
        assert row['channel_name'] == "TEST_CHANNEL"
        assert row['metric_id'].startswith("m_")
        assert row['batch_timestamp'] is not None

    def test_metrics_row_with_errors(self):
        """Test metrics row when batch has errors."""
        from main import stream_ingestion_metrics
        from unittest.mock import MagicMock

        mock_client = MagicMock()
        mock_client.offset_token = 10
        mock_client.channel_name = "ERR_CHANNEL"

        metrics = {
            'batch_id': 'batch_err',
            'markets_fetched': 0,
            'markets_streamed': 0,
            'events_streamed': 0,
            'errors': 3,
            'fetch_duration_ms': 0,
            'stream_duration_ms': 0,
            'total_duration_ms': 100.0,
        }

        stream_ingestion_metrics(mock_client, metrics)

        row = mock_client.append_rows.call_args[0][0][0]
        assert row['api_status_code'] == 0
        assert row['error_message'] == "3 error(s) in batch"

    def test_metrics_stream_failure_does_not_raise(self):
        """Test that metrics streaming failures are caught, not propagated."""
        from main import stream_ingestion_metrics
        from unittest.mock import MagicMock

        mock_client = MagicMock()
        mock_client.append_rows.side_effect = Exception("Network error")
        mock_client.offset_token = 0
        mock_client.channel_name = "FAIL_CHANNEL"

        metrics = {'batch_id': 'test', 'markets_fetched': 10, 'errors': 0}

        # Should not raise
        stream_ingestion_metrics(mock_client, metrics)


class TestStreamingClientRetry:
    """Tests for SSv2 client retry, reopen, and token refresh logic."""

    def _make_client(self):
        """Create a SnowpipeStreamingClient with mocked config and auth."""
        with patch('snowpipe_streaming_client.SnowflakeJWTAuth'):
            with patch('builtins.open', MagicMock(
                return_value=MagicMock(
                    __enter__=MagicMock(return_value=MagicMock(
                        read=MagicMock(return_value=json.dumps({
                            'account': 'testorg-testacct',
                            'user': 'TESTUSER',
                            'database': 'TESTDB',
                            'schema': 'TESTSCHEMA',
                            'table': 'TESTTABLE',
                            'pat': 'ver:1:test',
                        }))
                    )),
                    __exit__=MagicMock(return_value=False),
                )
            )):
                from snowpipe_streaming_client import SnowpipeStreamingClient
                client = SnowpipeStreamingClient.__new__(SnowpipeStreamingClient)
                client.config = {
                    'account': 'testorg-testacct',
                    'user': 'TESTUSER',
                    'database': 'TESTDB',
                    'schema': 'TESTSCHEMA',
                    'table': 'TESTTABLE',
                    'channel_name': 'TEST',
                }
                client.account = 'testorg-testacct'
                client.user = 'TESTUSER'
                client.database = 'TESTDB'
                client.schema = 'TESTSCHEMA'
                client.table = 'TESTTABLE'
                client.pipe = 'TESTTABLE-STREAMING'
                client.channel_name = 'TEST_20260409'
                client.auth = MagicMock()
                client.auth.get_scoped_token.return_value = 'mock-token'
                client.ingest_host = 'test.snowflakecomputing.com'
                client.scoped_token = 'mock-token'
                client._token_obtained_at = time.time()
                client.continuation_token = 'ct-123'
                client.offset_token = 0
                client._channel_open = True
                client._session = MagicMock()
                client.stats = {
                    'rows_sent': 0, 'batches': 0, 'bytes_sent': 0,
                    'errors': 0, 'retries': 0, 'channel_reopens': 0,
                    'token_refreshes': 0, 'start_time': time.time(),
                }
                return client

    def test_retry_on_429_throttling(self):
        """Test exponential backoff retry on 429 Too Many Requests."""
        from snowpipe_streaming_client import SnowpipeStreamingClient
        client = self._make_client()

        mock_429 = MagicMock()
        mock_429.status_code = 429
        mock_429.headers = {'Retry-After': '0.01'}

        mock_200 = MagicMock()
        mock_200.status_code = 200
        mock_200.json.return_value = {'next_continuation_token': 'ct-new'}

        client._session.request.side_effect = [mock_429, mock_200]

        result = client._make_request('POST', 'https://test.example.com/rows')
        assert result.status_code == 200
        assert client.stats['retries'] == 1

    def test_retry_on_500_server_error(self):
        """Test retry on 500 server error."""
        client = self._make_client()

        mock_500 = MagicMock()
        mock_500.status_code = 500
        mock_500.headers = {}

        mock_200 = MagicMock()
        mock_200.status_code = 200

        client._session.request.side_effect = [mock_500, mock_200]

        with patch('snowpipe_streaming_client.INITIAL_BACKOFF_SEC', 0.01):
            result = client._make_request('GET', 'https://test.example.com')
        assert result.status_code == 200
        assert client.stats['retries'] == 1

    def test_409_raises_for_channel_reopen(self):
        """Test that 409 raises HTTPError for caller to handle channel reopen."""
        client = self._make_client()

        mock_409 = MagicMock()
        mock_409.status_code = 409
        mock_409.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=mock_409
        )

        client._session.request.return_value = mock_409

        with pytest.raises(requests.exceptions.HTTPError):
            client._make_request('POST', 'https://test.example.com/rows')

    def test_token_refresh_on_401(self):
        """Test token refresh when 401 Unauthorized is received."""
        client = self._make_client()

        mock_401 = MagicMock()
        mock_401.status_code = 401

        mock_200 = MagicMock()
        mock_200.status_code = 200

        client._session.request.side_effect = [mock_401, mock_200]

        result = client._make_request('GET', 'https://test.example.com')
        assert result.status_code == 200
        assert client.stats['token_refreshes'] == 1

    def test_token_refresh_before_expiry(self):
        """Test that token is proactively refreshed before expiry."""
        client = self._make_client()
        # Set token obtained long ago (expired)
        client._token_obtained_at = time.time() - 4000
        client.scoped_token = 'old-token'

        new_token = client._get_scoped_token()
        assert client.auth.get_scoped_token.called
        assert client.stats['token_refreshes'] == 1

    def test_reopen_channel_generates_new_name(self):
        """Test that reopen_channel creates a new channel name."""
        client = self._make_client()
        old_name = client.channel_name

        # Mock open_channel to succeed
        with patch.object(client, 'open_channel', return_value={'channel_status': {}}):
            client.reopen_channel()

        assert client.channel_name != old_name
        assert '_r1' in client.channel_name
        assert client.stats['channel_reopens'] == 1

    def test_check_channel_health_success(self):
        """Test check_channel_health returns True for healthy channel."""
        client = self._make_client()

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'channel_status': {
                'channel_status_code': 'SUCCESS',
                'rows_inserted': 100,
                'rows_parsed': 100,
                'rows_error_count': 0,
                'snowflake_avg_processing_latency_ms': 50,
            }
        }
        client._session.request.return_value = mock_response

        assert client.check_channel_health() is True

    def test_check_channel_health_fatal_error_triggers_reopen(self):
        """Test that fatal channel errors trigger auto-reopen."""
        client = self._make_client()

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'channel_status': {
                'channel_status_code': 'ERR_CHANNEL_MUST_BE_REOPENED',
                'last_error_message': 'Channel must be reopened',
            }
        }
        client._session.request.return_value = mock_response

        with patch.object(client, 'reopen_channel', return_value={}):
            result = client.check_channel_health()
            assert result is False
            client.reopen_channel.assert_called_once()

    def test_connection_pooling_uses_session(self):
        """Test that the client uses requests.Session for connection pooling."""
        client = self._make_client()
        assert client._session is not None

        mock_response = MagicMock()
        mock_response.status_code = 200
        client._session.request.return_value = mock_response

        client._make_request('GET', 'https://test.example.com')
        client._session.request.assert_called_once()

    def test_append_rows_retries_on_409(self):
        """Test that append_rows auto-reopens channel on 409 and retries."""
        client = self._make_client()

        mock_409 = MagicMock()
        mock_409.status_code = 409
        mock_409.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=mock_409
        )

        mock_200 = MagicMock()
        mock_200.status_code = 200
        mock_200.json.return_value = {'next_continuation_token': 'ct-new'}

        # First call to _make_request raises 409, after reopen succeeds
        call_count = [0]
        original_make_request = client._make_request

        def mock_make_request(method, url, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise requests.exceptions.HTTPError(response=mock_409)
            return mock_200

        with patch.object(client, '_make_request', side_effect=mock_make_request):
            with patch.object(client, 'reopen_channel', return_value={'channel_status': {}}):
                result = client.append_rows([{'id': 'test', 'question': 'Test?'}])
                assert result == {'next_continuation_token': 'ct-new'}


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
