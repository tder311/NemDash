"""
Unit tests for NEMDatabase
"""
import pytest
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import aiosqlite

from app.database import NEMDatabase


class TestNEMDatabaseInit:
    """Tests for NEMDatabase initialization"""

    @pytest.mark.asyncio
    async def test_initialize_creates_tables(self, test_db):
        """Test that initialize creates all required tables"""
        async with aiosqlite.connect(test_db.db_path) as conn:
            cursor = await conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
            tables = [row[0] for row in await cursor.fetchall()]

        assert 'dispatch_data' in tables
        assert 'price_data' in tables
        assert 'interconnector_data' in tables
        assert 'generator_info' in tables

    @pytest.mark.asyncio
    async def test_initialize_creates_indexes(self, test_db):
        """Test that initialize creates performance indexes"""
        async with aiosqlite.connect(test_db.db_path) as conn:
            cursor = await conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            )
            indexes = [row[0] for row in await cursor.fetchall()]

        # Check for expected indexes
        assert any('dispatch_settlement' in idx for idx in indexes)
        assert any('price_region' in idx for idx in indexes)
        assert any('interconnector_settlement' in idx for idx in indexes)

    @pytest.mark.asyncio
    async def test_initialize_idempotent(self, test_db):
        """Test that initialize can be called multiple times safely"""
        # Call initialize again (should not raise)
        await test_db.initialize()

        async with aiosqlite.connect(test_db.db_path) as conn:
            cursor = await conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
            count = (await cursor.fetchone())[0]

        # Should still have same number of application tables (excludes sqlite internal tables)
        assert count == 4


class TestDispatchDataInsert:
    """Tests for insert_dispatch_data method"""

    @pytest.mark.asyncio
    async def test_insert_dispatch_data(self, test_db, sample_dispatch_df):
        """Test inserting dispatch data DataFrame"""
        count = await test_db.insert_dispatch_data(sample_dispatch_df)
        assert count == 1

    @pytest.mark.asyncio
    async def test_insert_dispatch_data_empty_df(self, test_db):
        """Test that empty DataFrame returns 0"""
        df = pd.DataFrame()
        count = await test_db.insert_dispatch_data(df)
        assert count == 0

    @pytest.mark.asyncio
    async def test_insert_dispatch_data_upsert(self, test_db):
        """Test that duplicate records are replaced (upsert)"""
        df1 = pd.DataFrame([{
            'settlementdate': datetime(2025, 1, 15, 10, 30),
            'duid': 'TEST1',
            'scadavalue': 100.0,
            'uigf': 0.0,
            'totalcleared': 0.0,
            'ramprate': 0.0,
            'availability': 0.0,
            'raise1sec': 0.0,
            'lower1sec': 0.0
        }])

        await test_db.insert_dispatch_data(df1)

        # Insert same record with updated value
        df2 = pd.DataFrame([{
            'settlementdate': datetime(2025, 1, 15, 10, 30),
            'duid': 'TEST1',
            'scadavalue': 200.0,
            'uigf': 0.0,
            'totalcleared': 0.0,
            'ramprate': 0.0,
            'availability': 0.0,
            'raise1sec': 0.0,
            'lower1sec': 0.0
        }])

        await test_db.insert_dispatch_data(df2)

        # Should have only 1 record with updated value
        result = await test_db.get_latest_dispatch_data()
        assert len(result) == 1
        assert result.loc[0, 'scadavalue'] == 200.0


class TestPriceDataInsert:
    """Tests for insert_price_data method"""

    @pytest.mark.asyncio
    async def test_insert_price_data(self, test_db, sample_price_df):
        """Test inserting price data"""
        count = await test_db.insert_price_data(sample_price_df)
        assert count == 1

    @pytest.mark.asyncio
    async def test_insert_price_data_empty(self, test_db):
        """Test empty DataFrame returns 0"""
        df = pd.DataFrame()
        count = await test_db.insert_price_data(df)
        assert count == 0

    @pytest.mark.asyncio
    async def test_insert_price_data_upsert(self, test_db):
        """Test price data upsert behavior"""
        df1 = pd.DataFrame([{
            'settlementdate': datetime(2025, 1, 15, 10, 30),
            'region': 'NSW',
            'price': 85.50,
            'totaldemand': 7500.0,
            'price_type': 'DISPATCH'
        }])

        await test_db.insert_price_data(df1)

        # Update price
        df2 = pd.DataFrame([{
            'settlementdate': datetime(2025, 1, 15, 10, 30),
            'region': 'NSW',
            'price': 90.00,
            'totaldemand': 7500.0,
            'price_type': 'DISPATCH'
        }])

        await test_db.insert_price_data(df2)

        # Should have updated price
        result = await test_db.get_latest_prices('DISPATCH')
        assert result.loc[0, 'price'] == 90.00


class TestInterconnectorDataInsert:
    """Tests for insert_interconnector_data method"""

    @pytest.mark.asyncio
    async def test_insert_interconnector_data(self, test_db, sample_interconnector_df):
        """Test inserting interconnector data"""
        count = await test_db.insert_interconnector_data(sample_interconnector_df)
        assert count == 1

    @pytest.mark.asyncio
    async def test_insert_interconnector_data_empty(self, test_db):
        """Test empty DataFrame returns 0"""
        df = pd.DataFrame()
        count = await test_db.insert_interconnector_data(df)
        assert count == 0


class TestDispatchQueries:
    """Tests for dispatch data query methods"""

    @pytest.mark.asyncio
    async def test_get_latest_dispatch_data(self, populated_db):
        """Test retrieving latest dispatch data"""
        result = await populated_db.get_latest_dispatch_data()

        # Should only return records from the most recent timestamp
        assert len(result) > 0
        # All records should have the same (max) timestamp
        assert result['settlementdate'].nunique() == 1

    @pytest.mark.asyncio
    async def test_get_latest_dispatch_data_with_limit(self, populated_db):
        """Test limit parameter"""
        result = await populated_db.get_latest_dispatch_data(limit=1)
        assert len(result) <= 1

    @pytest.mark.asyncio
    async def test_get_latest_dispatch_data_empty(self, test_db):
        """Test empty database returns empty DataFrame"""
        result = await test_db.get_latest_dispatch_data()
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_get_dispatch_data_by_date_range(self, populated_db):
        """Test date range query"""
        start = datetime(2025, 1, 15, 10, 0)
        end = datetime(2025, 1, 15, 11, 0)

        result = await populated_db.get_dispatch_data_by_date_range(start, end)
        assert len(result) > 0

    @pytest.mark.asyncio
    async def test_get_dispatch_data_by_date_range_with_duid(self, populated_db):
        """Test date range query with DUID filter"""
        start = datetime(2025, 1, 15, 10, 0)
        end = datetime(2025, 1, 15, 11, 0)

        result = await populated_db.get_dispatch_data_by_date_range(start, end, duid='BAYSW1')
        if len(result) > 0:
            assert all(result['duid'] == 'BAYSW1')

    @pytest.mark.asyncio
    async def test_get_unique_duids(self, populated_db):
        """Test getting unique DUIDs"""
        duids = await populated_db.get_unique_duids()
        assert isinstance(duids, list)
        assert 'BAYSW1' in duids


class TestPriceQueries:
    """Tests for price data query methods"""

    @pytest.mark.asyncio
    async def test_get_latest_prices(self, populated_db):
        """Test retrieving latest prices by type"""
        dispatch = await populated_db.get_latest_prices('DISPATCH')
        trading = await populated_db.get_latest_prices('TRADING')

        assert len(dispatch) > 0
        assert len(trading) > 0

    @pytest.mark.asyncio
    async def test_get_latest_prices_filters_by_type(self, populated_db):
        """Test that price type filter works"""
        dispatch = await populated_db.get_latest_prices('DISPATCH')
        if len(dispatch) > 0:
            assert all(dispatch['price_type'] == 'DISPATCH')

    @pytest.mark.asyncio
    async def test_get_price_history(self, populated_db):
        """Test price history query"""
        start = datetime(2025, 1, 1)
        end = datetime(2025, 1, 31)

        result = await populated_db.get_price_history(start, end, price_type='DISPATCH')
        assert len(result) > 0

    @pytest.mark.asyncio
    async def test_get_price_history_with_region(self, populated_db):
        """Test price history with region filter"""
        start = datetime(2025, 1, 1)
        end = datetime(2025, 1, 31)

        result = await populated_db.get_price_history(start, end, region='NSW', price_type='DISPATCH')
        if len(result) > 0:
            assert all(result['region'] == 'NSW')

    @pytest.mark.asyncio
    async def test_get_latest_price_timestamp(self, populated_db):
        """Test getting latest price timestamp for a price type"""
        # Get latest DISPATCH timestamp
        dispatch_ts = await populated_db.get_latest_price_timestamp('DISPATCH')
        assert dispatch_ts is not None
        assert isinstance(dispatch_ts, datetime)

        # Get latest PUBLIC timestamp
        public_ts = await populated_db.get_latest_price_timestamp('PUBLIC')
        assert public_ts is not None
        assert isinstance(public_ts, datetime)

    @pytest.mark.asyncio
    async def test_get_latest_price_timestamp_nonexistent_type(self, test_db):
        """Test getting latest timestamp for non-existent price type returns None"""
        result = await test_db.get_latest_price_timestamp('NONEXISTENT')
        assert result is None


class TestInterconnectorQueries:
    """Tests for interconnector data query methods"""

    @pytest.mark.asyncio
    async def test_get_latest_interconnector_flows(self, populated_db):
        """Test retrieving latest interconnector flows"""
        result = await populated_db.get_latest_interconnector_flows()
        assert len(result) > 0

    @pytest.mark.asyncio
    async def test_get_interconnector_history(self, populated_db):
        """Test interconnector history query"""
        start = datetime(2025, 1, 1)
        end = datetime(2025, 1, 31)

        result = await populated_db.get_interconnector_history(start, end)
        assert len(result) > 0


class TestGeneratorInfo:
    """Tests for generator info methods"""

    @pytest.mark.asyncio
    async def test_update_generator_info(self, test_db):
        """Test updating generator info"""
        generators = [
            {
                'duid': 'TEST1',
                'station_name': 'Test Station',
                'region': 'NSW',
                'fuel_source': 'Coal',
                'technology_type': 'Steam',
                'capacity_mw': 500
            }
        ]

        await test_db.update_generator_info(generators)

        # Verify insertion
        async with aiosqlite.connect(test_db.db_path) as conn:
            cursor = await conn.execute(
                "SELECT * FROM generator_info WHERE duid = 'TEST1'"
            )
            row = await cursor.fetchone()

        assert row is not None

    @pytest.mark.asyncio
    async def test_get_generators_by_region_fuel(self, populated_db):
        """Test filtered generator query"""
        result = await populated_db.get_generators_by_region_fuel(region='NSW')

        if len(result) > 0:
            # All should be from NSW
            assert all(result['region'] == 'NSW')


class TestRegionQueries:
    """Tests for region-specific queries"""

    @pytest.mark.asyncio
    async def test_get_region_fuel_mix(self, populated_db):
        """Test region fuel mix query"""
        result = await populated_db.get_region_fuel_mix('NSW')

        if len(result) > 0:
            # Should have percentage column
            assert 'percentage' in result.columns
            assert 'fuel_source' in result.columns

    @pytest.mark.asyncio
    async def test_get_region_price_history(self, populated_db):
        """Test region price history query"""
        # This uses relative time, so may not return data depending on test data
        result = await populated_db.get_region_price_history('NSW', hours=24)

        # Should be a DataFrame (possibly empty)
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_get_region_summary(self, populated_db):
        """Test region summary query"""
        result = await populated_db.get_region_summary('NSW')

        assert isinstance(result, dict)
        assert 'region' in result
        assert result['region'] == 'NSW'
        assert 'generator_count' in result


class TestDataSummary:
    """Tests for data summary methods"""

    @pytest.mark.asyncio
    async def test_get_data_summary(self, populated_db):
        """Test data summary query"""
        result = await populated_db.get_data_summary()

        assert isinstance(result, dict)
        assert 'total_records' in result
        assert 'unique_duids' in result
        assert 'fuel_breakdown' in result

    @pytest.mark.asyncio
    async def test_get_data_coverage(self, populated_db):
        """Test data coverage query"""
        result = await populated_db.get_data_coverage('price_data')

        assert isinstance(result, dict)
        assert 'total_records' in result
        assert 'days_with_data' in result


class TestMissingDates:
    """Tests for missing dates detection"""

    @pytest.mark.asyncio
    async def test_get_missing_dates(self, populated_db):
        """Test finding missing dates"""
        start = datetime(2025, 1, 10)
        end = datetime(2025, 1, 15)

        missing = await populated_db.get_missing_dates(start, end, 'PUBLIC')

        # Should be a list of datetime objects
        assert isinstance(missing, list)

    @pytest.mark.asyncio
    async def test_get_missing_dates_identifies_gaps(self, populated_db):
        """Test that gaps in data are identified"""
        # We have data for Jan 12 and Jan 15, so Jan 11, 13, 14 should be missing
        start = datetime(2025, 1, 10)
        end = datetime(2025, 1, 15)

        missing = await populated_db.get_missing_dates(start, end, 'PUBLIC')

        missing_dates = [d.date() for d in missing]
        # Jan 11 should be missing (no data for that day)
        assert datetime(2025, 1, 11).date() in missing_dates


class TestGenerationByFuelType:
    """Tests for generation aggregation queries"""

    @pytest.mark.asyncio
    async def test_get_generation_by_fuel_type(self, populated_db):
        """Test generation by fuel type aggregation"""
        start = datetime(2025, 1, 15, 10, 0)
        end = datetime(2025, 1, 15, 11, 0)

        result = await populated_db.get_generation_by_fuel_type(start, end)

        if len(result) > 0:
            assert 'fuel_source' in result.columns
            assert 'total_generation' in result.columns
            assert 'unit_count' in result.columns


class TestGetMergedPriceHistory:
    """Tests for get_merged_price_history method (MERGED price type)"""

    @pytest.mark.asyncio
    async def test_returns_public_when_no_dispatch(self, test_db):
        """When only PUBLIC data exists, return PUBLIC with source_type"""
        # Insert only PUBLIC data
        public_df = pd.DataFrame([
            {
                'settlementdate': datetime.now() - timedelta(hours=1),
                'region': 'NSW',
                'price': 85.50,
                'totaldemand': 7500.0,
                'price_type': 'PUBLIC'
            }
        ])
        await test_db.insert_price_data(public_df)

        result = await test_db.get_merged_price_history('NSW', hours=24)

        assert len(result) == 1
        assert 'source_type' in result.columns
        assert result.iloc[0]['source_type'] == 'PUBLIC'

    @pytest.mark.asyncio
    async def test_returns_dispatch_when_no_public(self, test_db):
        """When only DISPATCH data exists, return DISPATCH with source_type"""
        # Insert only DISPATCH data
        dispatch_df = pd.DataFrame([
            {
                'settlementdate': datetime.now() - timedelta(hours=1),
                'region': 'NSW',
                'price': 90.00,
                'totaldemand': 7600.0,
                'price_type': 'DISPATCH'
            }
        ])
        await test_db.insert_price_data(dispatch_df)

        result = await test_db.get_merged_price_history('NSW', hours=24)

        assert len(result) == 1
        assert 'source_type' in result.columns
        assert result.iloc[0]['source_type'] == 'DISPATCH'

    @pytest.mark.asyncio
    async def test_fills_gap_with_dispatch_after_public(self, test_db):
        """DISPATCH data fills gap after latest PUBLIC timestamp"""
        now = datetime.now()

        # Insert PUBLIC data for earlier time (simulating 4am cutoff)
        public_df = pd.DataFrame([
            {
                'settlementdate': now - timedelta(hours=6),
                'region': 'NSW',
                'price': 80.00,
                'totaldemand': 7000.0,
                'price_type': 'PUBLIC'
            },
            {
                'settlementdate': now - timedelta(hours=5),
                'region': 'NSW',
                'price': 82.00,
                'totaldemand': 7100.0,
                'price_type': 'PUBLIC'
            }
        ])
        await test_db.insert_price_data(public_df)

        # Insert DISPATCH data for more recent times (after PUBLIC cutoff)
        dispatch_df = pd.DataFrame([
            {
                'settlementdate': now - timedelta(hours=2),
                'region': 'NSW',
                'price': 95.00,
                'totaldemand': 7800.0,
                'price_type': 'DISPATCH'
            },
            {
                'settlementdate': now - timedelta(hours=1),
                'region': 'NSW',
                'price': 98.00,
                'totaldemand': 7900.0,
                'price_type': 'DISPATCH'
            }
        ])
        await test_db.insert_price_data(dispatch_df)

        result = await test_db.get_merged_price_history('NSW', hours=24)

        # Should have 4 records total: 2 PUBLIC + 2 DISPATCH
        assert len(result) == 4

        # Check source types
        public_count = (result['source_type'] == 'PUBLIC').sum()
        dispatch_count = (result['source_type'] == 'DISPATCH').sum()
        assert public_count == 2
        assert dispatch_count == 2

    @pytest.mark.asyncio
    async def test_no_duplicate_timestamps(self, test_db):
        """Should not have overlapping timestamps from both sources"""
        now = datetime.now()
        same_time = now - timedelta(hours=3)

        # Insert both PUBLIC and DISPATCH for same timestamp
        # PUBLIC should take precedence
        df = pd.DataFrame([
            {
                'settlementdate': same_time,
                'region': 'NSW',
                'price': 80.00,
                'totaldemand': 7000.0,
                'price_type': 'PUBLIC'
            },
            {
                'settlementdate': same_time,
                'region': 'NSW',
                'price': 85.00,
                'totaldemand': 7200.0,
                'price_type': 'DISPATCH'
            },
            # Add a later DISPATCH record to fill gap
            {
                'settlementdate': now - timedelta(hours=1),
                'region': 'NSW',
                'price': 90.00,
                'totaldemand': 7500.0,
                'price_type': 'DISPATCH'
            }
        ])
        await test_db.insert_price_data(df)

        result = await test_db.get_merged_price_history('NSW', hours=24)

        # Check no duplicate timestamps
        duplicates = result.duplicated(subset=['settlementdate', 'region'])
        assert duplicates.sum() == 0

    @pytest.mark.asyncio
    async def test_includes_source_type_column(self, test_db):
        """Response should include source_type indicating PUBLIC or DISPATCH"""
        now = datetime.now()

        # Insert mixed data
        df = pd.DataFrame([
            {
                'settlementdate': now - timedelta(hours=5),
                'region': 'NSW',
                'price': 80.00,
                'totaldemand': 7000.0,
                'price_type': 'PUBLIC'
            },
            {
                'settlementdate': now - timedelta(hours=1),
                'region': 'NSW',
                'price': 90.00,
                'totaldemand': 7500.0,
                'price_type': 'DISPATCH'
            }
        ])
        await test_db.insert_price_data(df)

        result = await test_db.get_merged_price_history('NSW', hours=24)

        assert 'source_type' in result.columns
        assert set(result['source_type'].unique()) == {'PUBLIC', 'DISPATCH'}

    @pytest.mark.asyncio
    async def test_returns_empty_dataframe_when_no_data(self, test_db):
        """Should return empty DataFrame when no data exists"""
        result = await test_db.get_merged_price_history('NSW', hours=24)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_sorted_by_settlementdate(self, test_db):
        """Results should be sorted by settlementdate ascending"""
        now = datetime.now()

        # Insert data in random order
        df = pd.DataFrame([
            {
                'settlementdate': now - timedelta(hours=1),
                'region': 'NSW',
                'price': 90.00,
                'totaldemand': 7500.0,
                'price_type': 'DISPATCH'
            },
            {
                'settlementdate': now - timedelta(hours=5),
                'region': 'NSW',
                'price': 80.00,
                'totaldemand': 7000.0,
                'price_type': 'PUBLIC'
            },
            {
                'settlementdate': now - timedelta(hours=3),
                'region': 'NSW',
                'price': 85.00,
                'totaldemand': 7200.0,
                'price_type': 'PUBLIC'
            }
        ])
        await test_db.insert_price_data(df)

        result = await test_db.get_merged_price_history('NSW', hours=24)

        # Check sorting
        timestamps = result['settlementdate'].tolist()
        assert timestamps == sorted(timestamps)

    @pytest.mark.asyncio
    async def test_fills_gaps_within_public_range(self, test_db):
        """DISPATCH data fills gaps WITHIN the PUBLIC data range, not just after.

        This tests the scenario where PUBLIC data has gaps (e.g., sparse days)
        and DISPATCH data is available to fill those gaps.
        """
        now = datetime.now()

        # Insert PUBLIC data with a GAP in the middle
        # PUBLIC at hours 6, 4 (gap at hour 5), 2 (gap at hours 3, 1)
        public_df = pd.DataFrame([
            {
                'settlementdate': now - timedelta(hours=6),
                'region': 'NSW',
                'price': 70.00,
                'totaldemand': 6500.0,
                'price_type': 'PUBLIC'
            },
            {
                'settlementdate': now - timedelta(hours=4),
                'region': 'NSW',
                'price': 75.00,
                'totaldemand': 6800.0,
                'price_type': 'PUBLIC'
            },
            {
                'settlementdate': now - timedelta(hours=2),
                'region': 'NSW',
                'price': 80.00,
                'totaldemand': 7000.0,
                'price_type': 'PUBLIC'
            }
        ])
        await test_db.insert_price_data(public_df)

        # Insert DISPATCH data for the gaps WITHIN the PUBLIC range
        dispatch_df = pd.DataFrame([
            {
                'settlementdate': now - timedelta(hours=5),  # Gap between 6 and 4
                'region': 'NSW',
                'price': 72.00,
                'totaldemand': 6700.0,
                'price_type': 'DISPATCH'
            },
            {
                'settlementdate': now - timedelta(hours=3),  # Gap between 4 and 2
                'region': 'NSW',
                'price': 77.00,
                'totaldemand': 6900.0,
                'price_type': 'DISPATCH'
            }
        ])
        await test_db.insert_price_data(dispatch_df)

        result = await test_db.get_merged_price_history('NSW', hours=24)

        # Should have 5 records: 3 PUBLIC + 2 DISPATCH filling the gaps
        assert len(result) == 5

        # Check that gaps are filled with DISPATCH
        dispatch_rows = result[result['source_type'] == 'DISPATCH']
        assert len(dispatch_rows) == 2

        # Verify the DISPATCH records are at hours 5 and 3 (the gaps)
        dispatch_times = dispatch_rows['settlementdate'].tolist()
        expected_gap_times = [now - timedelta(hours=5), now - timedelta(hours=3)]
        for expected_time in expected_gap_times:
            # Allow small time difference for test timing
            found = any(abs((dt - expected_time).total_seconds()) < 2 for dt in dispatch_times)
            assert found, f"Expected DISPATCH at {expected_time} but not found"
