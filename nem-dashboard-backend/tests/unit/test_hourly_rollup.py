"""Tests for the dispatch_data_hourly rollup table.

The rollup stores per-(hour, duid) sum/count of scadavalue so that
long-horizon generation queries never scan raw 5-minute rows, and raw
rows older than the retention window can be deleted without losing
hourly/daily/weekly display bands.
"""
import pandas as pd
import pytest
from datetime import datetime

pytestmark = pytest.mark.asyncio


def make_dispatch_df(rows):
    """rows: list of (settlementdate, duid, scadavalue)."""
    return pd.DataFrame([
        {
            'settlementdate': sd,
            'duid': duid,
            'scadavalue': scada,
            'uigf': 0.0,
            'totalcleared': scada,
            'ramprate': 0.0,
            'availability': scada,
            'raise1sec': 0.0,
            'lower1sec': 0.0,
        }
        for sd, duid, scada in rows
    ])


async def fetch_rollup(db, duid=None):
    async with db._pool.acquire() as conn:
        if duid:
            rows = await conn.fetch(
                "SELECT * FROM dispatch_data_hourly WHERE duid = $1 ORDER BY hour", duid)
        else:
            rows = await conn.fetch(
                "SELECT * FROM dispatch_data_hourly ORDER BY hour, duid")
    return [dict(r) for r in rows]


class TestRollupMaintenance:
    async def test_insert_populates_hourly_rollup(self, test_db):
        df = make_dispatch_df([
            (datetime(2025, 1, 15, 10, 5), 'BAYSW1', 100.0),
            (datetime(2025, 1, 15, 10, 10), 'BAYSW1', 200.0),
            (datetime(2025, 1, 15, 11, 5), 'BAYSW1', 300.0),
        ])
        await test_db.insert_dispatch_data(df)

        rollup = await fetch_rollup(test_db, 'BAYSW1')
        assert len(rollup) == 2
        hour10, hour11 = rollup
        assert hour10['hour'] == datetime(2025, 1, 15, 10, 0)
        assert hour10['sum_scada'] == pytest.approx(300.0)
        assert hour10['sample_count'] == 2
        assert hour11['hour'] == datetime(2025, 1, 15, 11, 0)
        assert hour11['sum_scada'] == pytest.approx(300.0)
        assert hour11['sample_count'] == 1

    async def test_reinsert_is_idempotent(self, test_db):
        df = make_dispatch_df([
            (datetime(2025, 1, 15, 10, 5), 'BAYSW1', 100.0),
            (datetime(2025, 1, 15, 10, 10), 'BAYSW1', 200.0),
        ])
        await test_db.insert_dispatch_data(df)
        await test_db.insert_dispatch_data(df)

        rollup = await fetch_rollup(test_db, 'BAYSW1')
        assert len(rollup) == 1
        assert rollup[0]['sum_scada'] == pytest.approx(300.0)
        assert rollup[0]['sample_count'] == 2

    async def test_upsert_with_revised_values_recomputes(self, test_db):
        await test_db.insert_dispatch_data(make_dispatch_df([
            (datetime(2025, 1, 15, 10, 5), 'BAYSW1', 100.0),
        ]))
        # Same interval re-ingested with a corrected value
        await test_db.insert_dispatch_data(make_dispatch_df([
            (datetime(2025, 1, 15, 10, 5), 'BAYSW1', 150.0),
        ]))

        rollup = await fetch_rollup(test_db, 'BAYSW1')
        assert len(rollup) == 1
        assert rollup[0]['sum_scada'] == pytest.approx(150.0)
        assert rollup[0]['sample_count'] == 1

    async def test_null_scadavalue_excluded_from_count(self, test_db):
        df = make_dispatch_df([
            (datetime(2025, 1, 15, 10, 5), 'BAYSW1', 100.0),
            (datetime(2025, 1, 15, 10, 10), 'BAYSW1', None),
        ])
        await test_db.insert_dispatch_data(df)

        rollup = await fetch_rollup(test_db, 'BAYSW1')
        assert len(rollup) == 1
        assert rollup[0]['sum_scada'] == pytest.approx(100.0)
        assert rollup[0]['sample_count'] == 1


class TestRollupBackfill:
    async def test_backfill_rebuilds_from_raw(self, test_db):
        df = make_dispatch_df([
            (datetime(2025, 1, 15, 10, 5), 'BAYSW1', 100.0),
            (datetime(2025, 1, 15, 10, 10), 'AGLHAL', 50.0),
        ])
        await test_db.insert_dispatch_data(df)
        async with test_db._pool.acquire() as conn:
            await conn.execute("TRUNCATE dispatch_data_hourly")

        inserted = await test_db.backfill_dispatch_hourly()

        assert inserted == 2
        rollup = await fetch_rollup(test_db)
        assert {r['duid'] for r in rollup} == {'BAYSW1', 'AGLHAL'}

    async def test_backfill_noop_when_already_populated(self, test_db):
        await test_db.insert_dispatch_data(make_dispatch_df([
            (datetime(2025, 1, 15, 10, 5), 'BAYSW1', 100.0),
        ]))
        # Rollup already maintained by insert; backfill must not double anything
        await test_db.backfill_dispatch_hourly()

        rollup = await fetch_rollup(test_db, 'BAYSW1')
        assert len(rollup) == 1
        assert rollup[0]['sample_count'] == 1


class TestHourlySourcedQueries:
    async def _seed(self, db):
        await db.update_generator_info([
            {'duid': 'BAYSW1', 'station_name': 'Bayswater', 'region': 'NSW',
             'fuel_source': 'Coal', 'technology_type': 'Steam', 'capacity_mw': 660},
            {'duid': 'ARWF1', 'station_name': 'Ararat', 'region': 'VIC',
             'fuel_source': 'Wind', 'technology_type': 'Wind', 'capacity_mw': 240},
        ])
        rows = []
        for hour in (10, 11):
            for minute in (5, 10, 15):
                rows.append((datetime(2025, 1, 15, hour, minute), 'BAYSW1', 600.0 + hour))
                rows.append((datetime(2025, 1, 15, hour, minute), 'ARWF1', 100.0))
        await db.insert_dispatch_data(make_dispatch_df(rows))

    async def test_hourly_aggregation_reads_rollup(self, test_db):
        await self._seed(test_db)
        # Delete raw rows: only the rollup can answer now
        async with test_db._pool.acquire() as conn:
            await conn.execute("DELETE FROM dispatch_data")

        df = await test_db.get_region_generation_history_by_dates(
            'NSW', datetime(2025, 1, 15), datetime(2025, 1, 16), aggregation_minutes=60)

        assert not df.empty
        assert set(df['fuel_source']) == {'Coal'}
        by_period = df.set_index('period')['generation_mw']
        assert by_period[pd.Timestamp(2025, 1, 15, 10)] == pytest.approx(610.0)
        assert by_period[pd.Timestamp(2025, 1, 15, 11)] == pytest.approx(611.0)

    async def test_daily_aggregation_from_rollup_matches_raw_average(self, test_db):
        await self._seed(test_db)

        df = await test_db.get_region_generation_history_by_dates(
            'NSW', datetime(2025, 1, 15), datetime(2025, 1, 16), aggregation_minutes=1440)

        assert len(df) == 1
        # Average of the two hourly totals (610 and 611)
        assert df.iloc[0]['generation_mw'] == pytest.approx(610.5)

    async def test_sub_hourly_aggregation_still_uses_raw(self, test_db):
        await self._seed(test_db)
        # Remove the rollup: raw path must still answer 5/30-min bands
        async with test_db._pool.acquire() as conn:
            await conn.execute("TRUNCATE dispatch_data_hourly")

        df = await test_db.get_region_generation_history_by_dates(
            'NSW', datetime(2025, 1, 15), datetime(2025, 1, 16), aggregation_minutes=5)

        assert len(df) == 6
