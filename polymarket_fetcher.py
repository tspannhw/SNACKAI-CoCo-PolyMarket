#!/usr/bin/env python3
"""
Polymarket Data Fetcher

Fetches prediction market data from Polymarket's Gamma API
and transforms it for Snowflake ingestion via Snowpipe Streaming v2.

API Reference: https://docs.polymarket.com/api-reference/markets/list-markets
Endpoint: GET https://gamma-api.polymarket.com/markets
"""

import json
import logging
import time
import requests
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple
from uuid import uuid4

logger = logging.getLogger(__name__)

POLYMARKET_API_BASE = "https://gamma-api.polymarket.com"
DEFAULT_LIMIT = 100
MAX_RETRIES = 3
RETRY_DELAY = 2


class PolymarketFetcher:
    """Fetches and transforms Polymarket prediction market data."""

    def __init__(self, api_base: str = POLYMARKET_API_BASE):
        self.api_base = api_base
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'PolymarketSnowflakeStreamer/1.0'
        })

    def fetch_markets(
        self,
        limit: int = DEFAULT_LIMIT,
        offset: int = 0,
        active: Optional[bool] = None,
        closed: Optional[bool] = None,
    ) -> Tuple[List[Dict], int]:
        """
        Fetch markets from Polymarket API.

        Args:
            limit: Number of markets to fetch (max 100)
            offset: Pagination offset
            active: Filter by active status
            closed: Filter by closed status

        Returns:
            Tuple of (list of market dicts, HTTP status code)
        """
        params = {
            'limit': min(limit, 100),
            'offset': offset,
        }
        if active is not None:
            params['active'] = str(active).lower()
        if closed is not None:
            params['closed'] = str(closed).lower()

        url = f"{self.api_base}/markets"

        for attempt in range(MAX_RETRIES):
            try:
                logger.info(f"Fetching markets: offset={offset}, limit={limit} (attempt {attempt+1})")
                response = self.session.get(url, params=params, timeout=30)

                if response.status_code == 429:
                    wait = RETRY_DELAY * (attempt + 1)
                    logger.warning(f"Rate limited. Waiting {wait}s...")
                    time.sleep(wait)
                    continue

                response.raise_for_status()
                markets = response.json()

                if not isinstance(markets, list):
                    logger.warning(f"Unexpected response type: {type(markets)}")
                    return [], response.status_code

                logger.info(f"Fetched {len(markets)} markets")
                return markets, response.status_code

            except requests.exceptions.Timeout:
                logger.warning(f"Request timeout (attempt {attempt+1})")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)

        return [], 0

    def fetch_all_markets(self, max_pages: int = 10) -> List[Dict]:
        """
        Fetch multiple pages of markets.

        Args:
            max_pages: Maximum number of API pages to fetch

        Returns:
            Combined list of all market dictionaries
        """
        all_markets = []
        offset = 0

        for page in range(max_pages):
            markets, status = self.fetch_markets(limit=100, offset=offset)
            if not markets:
                break
            all_markets.extend(markets)
            offset += len(markets)
            if len(markets) < 100:
                break
            time.sleep(0.5)  # Rate limiting courtesy

        logger.info(f"Fetched {len(all_markets)} total markets across {page+1} pages")
        return all_markets

    @staticmethod
    def transform_market(market: Dict, batch_id: str) -> Dict:
        """
        Transform a raw Polymarket market into Snowflake row format.

        Maps API field names to our table column names.
        """
        def safe_float(val, default=None):
            if val is None or val == '' or val == 'null':
                return default
            try:
                return float(val)
            except (ValueError, TypeError):
                return default

        def safe_ts(val):
            if val is None or val == '' or val == 'null':
                return None
            try:
                if isinstance(val, str):
                    return val.replace('T', ' ').replace('Z', '')[:19]
                return val
            except Exception:
                return None

        def safe_bool(val):
            if val is None:
                return None
            if isinstance(val, bool):
                return val
            return str(val).lower() == 'true'

        return {
            'id': market.get('id'),
            'question': (market.get('question') or '')[:4000],
            'condition_id': market.get('conditionId'),
            'slug': (market.get('slug') or '')[:1000],
            'description': (market.get('description') or '')[:16000],
            'category': market.get('category'),
            'end_date': safe_ts(market.get('endDate')),
            'start_date': safe_ts(market.get('startDate')),
            'image': market.get('image'),
            'icon': market.get('icon'),
            'active': safe_bool(market.get('active')),
            'closed': safe_bool(market.get('closed')),
            'archived': safe_bool(market.get('archived')),
            'featured': safe_bool(market.get('featured')),
            'restricted': safe_bool(market.get('restricted')),
            'new_market': safe_bool(market.get('new')),
            'market_type': market.get('marketType'),
            'format_type': market.get('formatType'),
            'outcomes': (market.get('outcomes') or '')[:4000],
            'outcome_prices': (market.get('outcomePrices') or '')[:4000],
            'volume': safe_float(market.get('volume')),
            'volume_num': safe_float(market.get('volumeNum')),
            'volume_24hr': safe_float(market.get('volume24hr')),
            'volume_1wk': safe_float(market.get('volume1wk')),
            'volume_1mo': safe_float(market.get('volume1mo')),
            'volume_1yr': safe_float(market.get('volume1yr')),
            'liquidity': safe_float(market.get('liquidity')),
            'liquidity_num': safe_float(market.get('liquidityNum')),
            'spread': safe_float(market.get('spread')),
            'lower_bound': market.get('lowerBound'),
            'upper_bound': market.get('upperBound'),
            'clob_token_ids': (market.get('clobTokenIds') or '')[:4000],
            'accepting_orders': safe_bool(market.get('acceptingOrders')),
            'comments_enabled': safe_bool(market.get('commentsEnabled')),
            'enable_order_book': safe_bool(market.get('enableOrderBook')),
            'maker_base_fee': safe_float(market.get('makerBaseFee')),
            'taker_base_fee': safe_float(market.get('takerBaseFee')),
            'notifications_enabled': safe_bool(market.get('notificationsEnabled')),
            'score': safe_float(market.get('score')),
            'created_at': safe_ts(market.get('createdAt')),
            'updated_at': safe_ts(market.get('updatedAt')),
            'ingested_at': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f'),
            'batch_id': batch_id,
        }

    @staticmethod
    def transform_event(event: Dict, batch_id: str) -> Dict:
        """Transform a raw event into Snowflake row format."""
        def safe_float(val, default=None):
            if val is None or val == '':
                return default
            try:
                return float(val)
            except (ValueError, TypeError):
                return default

        def safe_ts(val):
            if val is None or val == '':
                return None
            try:
                return str(val).replace('T', ' ').replace('Z', '')[:19]
            except Exception:
                return None

        return {
            'event_id': event.get('id'),
            'ticker': event.get('ticker'),
            'slug': (event.get('slug') or '')[:1000],
            'title': (event.get('title') or '')[:4000],
            'description': (event.get('description') or '')[:16000],
            'category': event.get('category'),
            'start_date': safe_ts(event.get('startDate')),
            'end_date': safe_ts(event.get('endDate')),
            'image': event.get('image'),
            'active': event.get('active'),
            'closed': event.get('closed'),
            'volume': safe_float(event.get('volume')),
            'liquidity': safe_float(event.get('liquidity')),
            'open_interest': safe_float(event.get('openInterest')),
            'neg_risk': event.get('negRisk'),
            'comment_count': event.get('commentCount'),
            'market_count': len(event.get('markets', [])) if isinstance(event.get('markets'), list) else 0,
            'ingested_at': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f'),
            'batch_id': batch_id,
        }

    def fetch_and_transform(self, max_pages: int = 5) -> Tuple[List[Dict], List[Dict], str]:
        """
        Fetch markets and transform for Snowflake streaming.

        Returns:
            Tuple of (transformed_markets, transformed_events, batch_id)
        """
        batch_id = f"batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}"
        logger.info(f"Starting fetch batch: {batch_id}")

        raw_markets = self.fetch_all_markets(max_pages=max_pages)

        transformed_markets = []
        transformed_events = []
        seen_events = set()

        for market in raw_markets:
            transformed_markets.append(self.transform_market(market, batch_id))

            # Extract embedded events
            events = market.get('events', [])
            if isinstance(events, list):
                for event in events:
                    if isinstance(event, dict):
                        eid = event.get('id')
                        if eid and eid not in seen_events:
                            seen_events.add(eid)
                            transformed_events.append(self.transform_event(event, batch_id))

        logger.info(f"Transformed {len(transformed_markets)} markets, {len(transformed_events)} events")
        return transformed_markets, transformed_events, batch_id


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    fetcher = PolymarketFetcher()
    markets, events, bid = fetcher.fetch_and_transform(max_pages=2)

    print(f"\nBatch ID: {bid}")
    print(f"Markets:  {len(markets)}")
    print(f"Events:   {len(events)}")

    if markets:
        m = markets[0]
        print(f"\nSample market:")
        print(f"  Question: {m.get('question', 'N/A')[:80]}")
        print(f"  Volume:   {m.get('volume_num', 'N/A')}")
        print(f"  Active:   {m.get('active', 'N/A')}")
        print(f"  Category: {m.get('category', 'N/A')}")
