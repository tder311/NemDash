"""Tests for raw-data retention.

Raw 5-minute dispatch rows and bid tables are trimmed to a retention
window (default 30 days); the hourly rollup preserves longer history for
the >=60min display bands, so retention must refuse to delete dispatch
rows the rollup does not cover.
"""
import pandas as pd
import pytest
from datetime import datetime, timedelta

from app.data_ingester import resolve_backfill_start
from tests.unit.test_hourly_rollup import make_dispatch_df


async def count(db, table):
    async with db._pool.acquire() as conn:
        return await conn.fetchval(f"SELECT COUNT(*) FROM {table}")


def bid_per_offer_df(settlementdates):
    return pd.DataFrame([
        {
            'settlementdate': sd,
            'duid': 'BAYSW1',
            'offerdate': sd,
            **{f'bandavail{i}': 10.0 for i in range(1, 11)},
            'maxavail': 100.0,
            'fixedload': 0.0,
            'rocup': 5.0,
            'rocdown': 5.0,
            'pasaavailability': 100.0,
        }
        for sd in settlementdates
    ])


def bid_day_offer_df(settlementdates):
    return pd.DataFrame([
        {
            'settlementdate': sd,
            'duid': 'BAYSW1',
            'offerdate': sd,
            **{f'priceband{i}': float(i) for i in range(1, 11)},
        }
        for sd in settlementdates
    ])


class TestRawRetention:
    pytestmark = pytest.mark.asyncio

    async def test_deletes_old_raw_keeps_recent_and_rollup(self, test_db):
        old = datetime.now() - timedelta(days=40)
        recent = datetime.now() - timedelta(days=1)
        await test_db.insert_dispatch_data(make_dispatch_df([
            (old, 'BAYSW1', 100.0),
            (recent, 'BAYSW1', 200.0),
        ]))

        deleted = await test_db.apply_raw_retention(days=30)

        assert deleted['dispatch_data'] == 1
        assert await count(test_db, 'dispatch_data') == 1
        # Rollup still holds both hours
        assert await count(test_db, 'dispatch_data_hourly') == 2

    async def test_refuses_dispatch_delete_when_rollup_missing_coverage(self, test_db):
        old = datetime.now() - timedelta(days=40)
        await test_db.insert_dispatch_data(make_dispatch_df([(old, 'BAYSW1', 100.0)]))
        async with test_db._pool.acquire() as conn:
            await conn.execute("TRUNCATE dispatch_data_hourly")

        deleted = await test_db.apply_raw_retention(days=30)

        assert deleted['dispatch_data'] == 0
        assert await count(test_db, 'dispatch_data') == 1

    async def test_trims_bid_tables(self, test_db):
        old = datetime.now() - timedelta(days=40)
        recent = datetime.now() - timedelta(days=1)
        await test_db.insert_bid_per_offer(bid_per_offer_df([old, recent]))
        await test_db.insert_bid_day_offer(bid_day_offer_df([old, recent]))

        deleted = await test_db.apply_raw_retention(days=30)

        assert deleted['bid_per_offer'] == 1
        assert deleted['bid_day_offer'] == 1
        assert await count(test_db, 'bid_per_offer') == 1
        assert await count(test_db, 'bid_day_offer') == 1

    async def test_multi_day_spans_delete_in_batches(self, test_db):
        rows = [(datetime.now() - timedelta(days=d, hours=1), 'BAYSW1', 100.0)
                for d in range(31, 36)]
        await test_db.insert_dispatch_data(make_dispatch_df(rows))

        deleted = await test_db.apply_raw_retention(days=30)

        assert deleted['dispatch_data'] == 5
        assert await count(test_db, 'dispatch_data') == 0


class TestResolveBackfillStart:
    """Backfill must never reach past the retention window, or every restart
    re-downloads months of NEMWEB data that retention immediately deletes."""

    NOW = datetime(2026, 7, 24, 12, 0)

    def test_default_is_capped_at_retention_window(self, monkeypatch):
        monkeypatch.delenv('BACKFILL_START_DATE', raising=False)
        monkeypatch.delenv('RAW_RETENTION_DAYS', raising=False)
        assert resolve_backfill_start(self.NOW) == self.NOW - timedelta(days=30)

    def test_env_date_within_window_is_used(self, monkeypatch):
        monkeypatch.setenv('BACKFILL_START_DATE', '2026-07-20')
        monkeypatch.delenv('RAW_RETENTION_DAYS', raising=False)
        assert resolve_backfill_start(self.NOW) == datetime(2026, 7, 20)

    def test_env_date_beyond_window_is_capped(self, monkeypatch):
        monkeypatch.setenv('BACKFILL_START_DATE', '2025-01-01')
        monkeypatch.setenv('RAW_RETENTION_DAYS', '30')
        assert resolve_backfill_start(self.NOW) == self.NOW - timedelta(days=30)

    def test_retention_disabled_allows_full_year(self, monkeypatch):
        monkeypatch.delenv('BACKFILL_START_DATE', raising=False)
        monkeypatch.setenv('RAW_RETENTION_DAYS', '0')
        assert resolve_backfill_start(self.NOW) == self.NOW - timedelta(days=365)

    def test_invalid_env_date_falls_back_and_caps(self, monkeypatch):
        monkeypatch.setenv('BACKFILL_START_DATE', 'not-a-date')
        monkeypatch.setenv('RAW_RETENTION_DAYS', '30')
        assert resolve_backfill_start(self.NOW) == self.NOW - timedelta(days=30)
