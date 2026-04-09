#!/usr/bin/env python3
"""
Validation Module for Polymarket Streaming Pipeline

Validates:
1. Polymarket API connectivity and data format
2. Snowflake configuration and authentication
3. Streaming client initialization
4. Data transformation correctness
"""

import json
import logging
import sys
import time
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)


class ValidationResult:
    def __init__(self, name: str):
        self.name = name
        self.passed = False
        self.skipped = False
        self.message = ""
        self.duration_ms = 0

    def __str__(self):
        status = "PASS" if self.passed else "SKIP" if self.skipped else "FAIL"
        return f"  [{status}] {self.name}: {self.message} ({self.duration_ms:.0f}ms)"


def validate_polymarket_api() -> ValidationResult:
    """Validate Polymarket API connectivity."""
    result = ValidationResult("Polymarket API")
    start = time.time()

    try:
        import requests
        response = requests.get(
            "https://gamma-api.polymarket.com/markets",
            params={"limit": 5},
            timeout=15
        )
        result.duration_ms = (time.time() - start) * 1000

        if response.status_code == 200:
            markets = response.json()
            if isinstance(markets, list) and len(markets) > 0:
                m = markets[0]
                has_id = 'id' in m
                has_question = 'question' in m
                has_volume = 'volumeNum' in m or 'volume' in m

                if has_id and has_question:
                    result.passed = True
                    result.message = f"OK - {len(markets)} markets, fields valid (id={has_id}, question={has_question}, volume={has_volume})"
                else:
                    result.message = f"Unexpected schema: missing 'id' or 'question'"
            else:
                result.message = f"Empty or invalid response"
        else:
            result.message = f"HTTP {response.status_code}"
    except Exception as e:
        result.duration_ms = (time.time() - start) * 1000
        result.message = f"Error: {e}"

    return result


def validate_data_transform() -> ValidationResult:
    """Validate data transformation logic."""
    result = ValidationResult("Data Transform")
    start = time.time()

    try:
        from polymarket_fetcher import PolymarketFetcher

        sample_market = {
            "id": "test-123",
            "question": "Will it rain tomorrow?",
            "conditionId": "cond-456",
            "slug": "will-it-rain",
            "description": "Test market",
            "category": "Weather",
            "endDate": "2025-12-31T23:59:59Z",
            "startDate": "2025-01-01T00:00:00Z",
            "active": True,
            "closed": False,
            "volumeNum": 50000.5,
            "volume24hr": 1200.3,
            "liquidityNum": 25000.0,
            "outcomes": '["Yes","No"]',
            "outcomePrices": '["0.65","0.35"]',
            "acceptingOrders": True,
            "score": 42.5,
        }

        transformed = PolymarketFetcher.transform_market(sample_market, "test-batch")

        checks = [
            transformed['id'] == 'test-123',
            transformed['question'] == 'Will it rain tomorrow?',
            transformed['active'] is True,
            transformed['closed'] is False,
            transformed['volume_num'] == 50000.5,
            transformed['volume_24hr'] == 1200.3,
            transformed['batch_id'] == 'test-batch',
            transformed['ingested_at'] is not None,
        ]

        result.duration_ms = (time.time() - start) * 1000

        if all(checks):
            result.passed = True
            result.message = f"OK - All {len(checks)} field checks passed"
        else:
            failed = [i for i, c in enumerate(checks) if not c]
            result.message = f"Failed checks at indices: {failed}"

    except Exception as e:
        result.duration_ms = (time.time() - start) * 1000
        result.message = f"Error: {e}"

    return result


def validate_config_file() -> ValidationResult:
    """Validate snowflake_config.json exists and has required fields."""
    result = ValidationResult("Config File")
    start = time.time()

    try:
        with open('snowflake_config.json', 'r') as f:
            config = json.load(f)

        required = ['account', 'user', 'database', 'schema', 'table']
        missing = [k for k in required if k not in config or not config[k]]

        result.duration_ms = (time.time() - start) * 1000

        if missing:
            result.message = f"Missing required fields: {missing}"
        elif config.get('account', '').startswith('your_'):
            result.message = "Config has placeholder values - update with real credentials"
        else:
            result.passed = True
            auth = 'PAT' if config.get('pat') else 'JWT' if config.get('private_key_file') else 'None'
            result.message = f"OK - account={config['account']}, auth={auth}"

    except FileNotFoundError:
        result.duration_ms = (time.time() - start) * 1000
        result.skipped = True
        result.message = "snowflake_config.json not found (cp snowflake_config.example.json snowflake_config.json)"
    except json.JSONDecodeError as e:
        result.duration_ms = (time.time() - start) * 1000
        result.message = f"Invalid JSON: {e}"

    return result


def validate_streaming_client() -> ValidationResult:
    """Validate streaming client initialization."""
    result = ValidationResult("Streaming Client")
    start = time.time()

    try:
        from snowpipe_streaming_client import SnowpipeStreamingClient

        client = SnowpipeStreamingClient('snowflake_config.json')

        checks = [
            client.database is not None,
            client.schema is not None,
            client.table is not None,
            client.pipe is not None,
            client.channel_name is not None,
        ]

        result.duration_ms = (time.time() - start) * 1000

        if all(checks):
            result.passed = True
            result.message = f"OK - pipe={client.pipe}, channel={client.channel_name}"
        else:
            result.message = "Client missing required attributes"

    except FileNotFoundError:
        result.duration_ms = (time.time() - start) * 1000
        result.skipped = True
        result.message = "snowflake_config.json not found"
    except Exception as e:
        result.duration_ms = (time.time() - start) * 1000
        result.message = f"Error: {e}"

    return result


def validate_fetch_and_transform() -> ValidationResult:
    """Validate end-to-end fetch and transform."""
    result = ValidationResult("Fetch & Transform")
    start = time.time()

    try:
        from polymarket_fetcher import PolymarketFetcher

        fetcher = PolymarketFetcher()
        markets, events, batch_id = fetcher.fetch_and_transform(max_pages=1)

        result.duration_ms = (time.time() - start) * 1000

        if len(markets) > 0:
            m = markets[0]
            has_required = all(k in m for k in ['id', 'question', 'batch_id', 'ingested_at'])
            if has_required:
                result.passed = True
                result.message = f"OK - {len(markets)} markets, {len(events)} events, batch={batch_id}"
            else:
                result.message = f"Transformed data missing required fields"
        else:
            result.message = "No markets returned"

    except Exception as e:
        result.duration_ms = (time.time() - start) * 1000
        result.message = f"Error: {e}"

    return result


def run_all_validations():
    """Run all validation checks."""
    print("=" * 60)
    print("POLYMARKET STREAMING PIPELINE VALIDATION")
    print(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)
    print()

    validations = [
        validate_polymarket_api,
        validate_data_transform,
        validate_config_file,
        validate_streaming_client,
        validate_fetch_and_transform,
    ]

    results = []
    for v in validations:
        r = v()
        results.append(r)
        print(r)

    print()
    passed = sum(1 for r in results if r.passed)
    skipped = sum(1 for r in results if r.skipped)
    failed = sum(1 for r in results if not r.passed and not r.skipped)
    total = len(results)
    print(f"Results: {passed}/{total} passed, {skipped} skipped, {failed} failed")

    if failed > 0:
        print("Status: SOME VALIDATIONS FAILED")
        return 1
    elif skipped > 0:
        print("Status: PASSED (some checks skipped - configure snowflake_config.json to run all)")
        return 0
    else:
        print("Status: ALL VALIDATIONS PASSED")
        return 0


if __name__ == '__main__':
    sys.exit(run_all_validations())
