#!/usr/bin/env python3
"""
Tests for Polymarket Streaming Pipeline

Run: python -m pytest test_polymarket.py -v
"""

import json
import pytest
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


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
