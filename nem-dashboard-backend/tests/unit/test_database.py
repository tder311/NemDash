"""
Unit tests for NEMDatabase (PostgreSQL)

Requires DATABASE_URL environment variable set to a PostgreSQL database.
"""
import pytest
import pandas as pd
from datetime import datetime, date, timedelta
from unittest.mock import AsyncMock, MagicMock

from app.database import NEMDatabase, filter_binding_constraints


class TestFilterBindingConstraints:
    """Tests for the pure filter_binding_constraints helper (no DB required)."""

    def test_keeps_nonzero_marginalvalue(self):
        df = pd.DataFrame([
            {'constraintid': 'A', 'marginalvalue': 15.0, 'violationdegree': 0.0},
            {'constraintid': 'B', 'marginalvalue': 0.0, 'violationdegree': 0.0},
        ])
        out = filter_binding_constraints(df)
        assert list(out['constraintid']) == ['A']

    def test_keeps_nonzero_violationdegree(self):
        df = pd.DataFrame([
            {'constraintid': 'A', 'marginalvalue': 0.0, 'violationdegree': 5.0},
            {'constraintid': 'B', 'marginalvalue': 0.0, 'violationdegree': 0.0},
        ])
        out = filter_binding_constraints(df)
        assert list(out['constraintid']) == ['A']

    def test_drops_all_zero_rows(self):
        df = pd.DataFrame([
            {'constraintid': 'A', 'marginalvalue': 0.0, 'violationdegree': 0.0},
        ])
        out = filter_binding_constraints(df)
        assert out.empty

    def test_empty_dataframe_returns_empty(self):
        df = pd.DataFrame(columns=['constraintid', 'marginalvalue', 'violationdegree'])
        out = filter_binding_constraints(df)
        assert out.empty

    def test_nan_in_both_columns_is_dropped(self):
        df = pd.DataFrame([
            {'constraintid': 'A', 'marginalvalue': float('nan'), 'violationdegree': float('nan')},
        ])
        out = filter_binding_constraints(df)
        assert out.empty

    def test_nan_marginalvalue_with_nonzero_violationdegree_is_kept(self):
        df = pd.DataFrame([
            {'constraintid': 'A', 'marginalvalue': float('nan'), 'violationdegree': 5.0},
            {'constraintid': 'B', 'marginalvalue': float('nan'), 'violationdegree': 0.0},
        ])
        out = filter_binding_constraints(df)
        assert list(out['constraintid']) == ['A']


class TestNEMDatabaseInit:
    """Tests for NEMDatabase initialization"""

    @pytest.mark.asyncio
    async def test_initialize_creates_tables(self, test_db):
        """Test that initialize creates all required tables"""
        async with test_db._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT tablename FROM pg_tables
                WHERE schemaname = 'public'
            """)
            tables = [row['tablename'] for row in rows]

        assert 'dispatch_data' in tables
        assert 'price_data' in tables
        assert 'generator_info' in tables

    @pytest.mark.asyncio
    async def test_initialize_creates_indexes(self, test_db):
        """Test that initialize creates performance indexes"""
        async with test_db._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT indexname FROM pg_indexes
                WHERE schemaname = 'public'
            """)
            indexes = [row['indexname'] for row in rows]

        # Check for expected indexes
        assert any('dispatch_settlement' in idx for idx in indexes)
        assert any('price_region' in idx for idx in indexes)

    @pytest.mark.asyncio
    async def test_initialize_idempotent(self, test_db):
        """Test that initialize can be called multiple times safely"""
        # Call initialize again (should not raise)
        await test_db.initialize()

        async with test_db._pool.acquire() as conn:
            count = await conn.fetchval("""
                SELECT COUNT(*) FROM pg_tables
                WHERE schemaname = 'public'
            """)

        # Should still have same number of application tables (14: dispatch_data, price_data, generator_info,
        # pdpasa_data, stpasa_data, predispatch_price, predispatch_interconnector, predispatch_constraint,
        # constraint_equation_terms, forecast_history, daily_metrics, price_setter_data, bid_day_offer, bid_per_offer)
        assert count == 14


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
        async with test_db._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM generator_info WHERE duid = 'TEST1'"
            )

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
    async def test_returns_empty_dataframe_when_no_data(self, test_db):
        """Should return empty DataFrame when no data exists"""
        result = await test_db.get_merged_price_history('NSW', hours=24)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_merges_public_and_dispatch_data(self, test_db):
        """When both PUBLIC and DISPATCH data exist, PUBLIC takes priority"""
        base_time = datetime.now()

        # Insert PUBLIC data at time T
        public_df = pd.DataFrame([
            {
                'settlementdate': base_time - timedelta(hours=1),
                'region': 'NSW',
                'price': 85.50,
                'totaldemand': 7500.0,
                'price_type': 'PUBLIC'
            }
        ])
        await test_db.insert_price_data(public_df)

        # Insert DISPATCH data at time T (same timestamp) and T+5min (different timestamp)
        dispatch_df = pd.DataFrame([
            {
                'settlementdate': base_time - timedelta(hours=1),  # Same as PUBLIC
                'region': 'NSW',
                'price': 90.00,
                'totaldemand': 7600.0,
                'price_type': 'DISPATCH'
            },
            {
                'settlementdate': base_time - timedelta(minutes=55),  # Different timestamp
                'region': 'NSW',
                'price': 92.00,
                'totaldemand': 7700.0,
                'price_type': 'DISPATCH'
            }
        ])
        await test_db.insert_price_data(dispatch_df)

        result = await test_db.get_merged_price_history('NSW', hours=24)

        # Should have 2 records: PUBLIC at T and DISPATCH at T+5min
        assert len(result) == 2
        # PUBLIC should be used for the overlapping timestamp
        public_row = result[result['source_type'] == 'PUBLIC']
        assert len(public_row) == 1
        assert public_row.iloc[0]['price'] == 85.50
        # DISPATCH fills in the gap
        dispatch_row = result[result['source_type'] == 'DISPATCH']
        assert len(dispatch_row) == 1
        assert dispatch_row.iloc[0]['price'] == 92.00


class TestAggregatedPriceHistory:
    """Tests for get_aggregated_price_history method"""

    @pytest.mark.asyncio
    async def test_returns_non_aggregated_for_short_hours(self, test_db):
        """When hours is small, should return non-aggregated data (via get_merged_price_history)"""
        # Insert some price data
        price_df = pd.DataFrame([
            {
                'settlementdate': datetime.now() - timedelta(hours=1),
                'region': 'NSW',
                'price': 85.50,
                'totaldemand': 7500.0,
                'price_type': 'PUBLIC'
            }
        ])
        await test_db.insert_price_data(price_df)

        # With small hours, should use merged (non-aggregated) path
        result = await test_db.get_aggregated_price_history('NSW', hours=6, aggregation_minutes=30)

        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_returns_aggregated_data_for_long_hours(self, test_db):
        """When hours is large, should return aggregated data"""
        base_time = datetime.now()

        # Insert multiple price points
        price_data = []
        for i in range(10):
            price_data.append({
                'settlementdate': base_time - timedelta(hours=i),
                'region': 'NSW',
                'price': 80.0 + i,
                'totaldemand': 7000.0 + i * 100,
                'price_type': 'PUBLIC'
            })

        price_df = pd.DataFrame(price_data)
        await test_db.insert_price_data(price_df)

        # Request aggregated data with 60-minute buckets
        result = await test_db.get_aggregated_price_history('NSW', hours=12, aggregation_minutes=60)

        assert isinstance(result, pd.DataFrame)
        if len(result) > 0:
            assert 'source_type' in result.columns

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_data(self, test_db):
        """Should return empty DataFrame when no data exists"""
        result = await test_db.get_aggregated_price_history('NSW', hours=24, aggregation_minutes=60)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0


class TestDispatchTimestampQueries:
    """Tests for dispatch timestamp query methods"""

    @pytest.mark.asyncio
    async def test_get_latest_dispatch_timestamp(self, populated_db):
        """Test getting latest dispatch timestamp"""
        result = await populated_db.get_latest_dispatch_timestamp()

        assert result is not None
        assert isinstance(result, datetime)

    @pytest.mark.asyncio
    async def test_get_latest_dispatch_timestamp_empty_db(self, test_db):
        """Test getting latest timestamp from empty database"""
        result = await test_db.get_latest_dispatch_timestamp()

        assert result is None

    @pytest.mark.asyncio
    async def test_get_earliest_dispatch_timestamp(self, populated_db):
        """Test getting earliest dispatch timestamp"""
        result = await populated_db.get_earliest_dispatch_timestamp()

        assert result is not None
        assert isinstance(result, datetime)

    @pytest.mark.asyncio
    async def test_get_earliest_dispatch_timestamp_empty_db(self, test_db):
        """Test getting earliest timestamp from empty database"""
        result = await test_db.get_earliest_dispatch_timestamp()

        assert result is None

    @pytest.mark.asyncio
    async def test_get_dispatch_dates_with_data(self, test_db):
        """Test getting dates with sufficient dispatch data (interval coverage)"""
        # Insert dispatch data for multiple dates
        records = []
        base_date = datetime(2025, 1, 15, 10, 0)

        # Insert 150 records with distinct 5-min intervals (should meet threshold of 100)
        for i in range(150):
            records.append({
                'settlementdate': base_date + timedelta(minutes=i * 5),
                'duid': f'TEST{i % 10}',
                'scadavalue': 100.0,
                'uigf': 0.0,
                'totalcleared': 100.0,
                'ramprate': 0.0,
                'availability': 110.0,
                'raise1sec': 0.0,
                'lower1sec': 0.0
            })

        df = pd.DataFrame(records)
        await test_db.insert_dispatch_data(df)

        start = datetime(2025, 1, 14)
        end = datetime(2025, 1, 16)

        result = await test_db.get_dispatch_dates_with_data(start, end, min_intervals=100)

        assert isinstance(result, set)
        assert '2025-01-15' in result


class TestPDPASADataInsert:
    """Tests for insert_pdpasa_data method"""

    @pytest.mark.asyncio
    async def test_insert_pdpasa_data(self, test_db):
        """Test inserting PDPASA data DataFrame"""
        df = pd.DataFrame([{
            'run_datetime': datetime(2025, 1, 15, 10, 0),
            'interval_datetime': datetime(2025, 1, 15, 10, 30),
            'regionid': 'NSW1',
            'demand10': 7200.0,
            'demand50': 7500.0,
            'demand90': 7800.0,
            'reservereq': 1500.0,
            'capacityreq': 9000.0,
            'aggregatecapacityavailable': 10500.0,
            'aggregatepasaavailability': 10000.0,
            'surplusreserve': 1500.0,
            'lorcondition': 0,
            'calculatedlor1level': 2000.0,
            'calculatedlor2level': 1500.0
        }])

        count = await test_db.insert_pdpasa_data(df)
        assert count == 1

    @pytest.mark.asyncio
    async def test_insert_pdpasa_data_empty_df(self, test_db):
        """Test that empty DataFrame returns 0"""
        df = pd.DataFrame()
        count = await test_db.insert_pdpasa_data(df)
        assert count == 0

    @pytest.mark.asyncio
    async def test_insert_pdpasa_data_upsert(self, test_db):
        """Test that duplicate records are replaced (upsert)"""
        df1 = pd.DataFrame([{
            'run_datetime': datetime(2025, 1, 15, 10, 0),
            'interval_datetime': datetime(2025, 1, 15, 10, 30),
            'regionid': 'NSW1',
            'demand50': 7500.0,
            'lorcondition': 0
        }])
        await test_db.insert_pdpasa_data(df1)

        # Insert same record with updated values
        df2 = pd.DataFrame([{
            'run_datetime': datetime(2025, 1, 15, 10, 0),
            'interval_datetime': datetime(2025, 1, 15, 10, 30),
            'regionid': 'NSW1',
            'demand50': 8000.0,
            'lorcondition': 1
        }])
        await test_db.insert_pdpasa_data(df2)

        # Should have only 1 record with updated values
        result = await test_db.get_latest_pdpasa('NSW1')
        assert len(result) == 1
        assert result[0]['demand50'] == 8000.0
        assert result[0]['lorcondition'] == 1


class TestPredispatchInterconnectorInsert:
    """Tests for insert_predispatch_interconnector method"""

    @pytest.mark.asyncio
    async def test_insert_predispatch_interconnector(self, test_db):
        df = pd.DataFrame([{
            'run_datetime': datetime(2025, 6, 22, 0, 30),
            'interval_datetime': datetime(2025, 6, 22, 1, 0),
            'interconnectorid': 'NSW1-QLD1',
            'mwflow': -100.0,
            'exportlimit': 300.0,
            'importlimit': -500.0,
            'marginalvalue': 12.5,
        }])
        count = await test_db.insert_predispatch_interconnector(df)
        assert count == 1

    @pytest.mark.asyncio
    async def test_insert_predispatch_interconnector_empty_df(self, test_db):
        count = await test_db.insert_predispatch_interconnector(pd.DataFrame())
        assert count == 0

    @pytest.mark.asyncio
    async def test_insert_predispatch_interconnector_upsert(self, test_db):
        df1 = pd.DataFrame([{
            'run_datetime': datetime(2025, 6, 22, 0, 30),
            'interval_datetime': datetime(2025, 6, 22, 1, 0),
            'interconnectorid': 'NSW1-QLD1',
            'mwflow': -100.0,
            'exportlimit': 300.0,
            'importlimit': -500.0,
            'marginalvalue': 12.5,
        }])
        await test_db.insert_predispatch_interconnector(df1)

        df2 = df1.copy()
        df2['mwflow'] = -150.0
        await test_db.insert_predispatch_interconnector(df2)

        async with test_db._pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM predispatch_interconnector")
        assert len(rows) == 1
        assert rows[0]['mwflow'] == pytest.approx(-150.0)


class TestPredispatchConstraintInsert:
    """Tests for insert_predispatch_constraint method"""

    @pytest.mark.asyncio
    async def test_insert_predispatch_constraint_keeps_binding_only(self, test_db):
        df = pd.DataFrame([
            {
                'run_datetime': datetime(2025, 6, 22, 0, 30),
                'interval_datetime': datetime(2025, 6, 22, 1, 0),
                'constraintid': 'BINDING_CON',
                'rhs': 100.0,
                'marginalvalue': 25.0,
                'violationdegree': 0.0,
                'lhs': 55.5,
            },
            {
                'run_datetime': datetime(2025, 6, 22, 0, 30),
                'interval_datetime': datetime(2025, 6, 22, 1, 0),
                'constraintid': 'SLACK_CON',
                'rhs': 100.0,
                'marginalvalue': 0.0,
                'violationdegree': 0.0,
                'lhs': 10.0,
            },
        ])
        count = await test_db.insert_predispatch_constraint(df)
        assert count == 1

        async with test_db._pool.acquire() as conn:
            rows = await conn.fetch("SELECT constraintid, lhs FROM predispatch_constraint")
        assert [r['constraintid'] for r in rows] == ['BINDING_CON']
        assert rows[0]['lhs'] == pytest.approx(55.5)

    @pytest.mark.asyncio
    async def test_insert_predispatch_constraint_empty_df(self, test_db):
        count = await test_db.insert_predispatch_constraint(pd.DataFrame())
        assert count == 0

    @pytest.mark.asyncio
    async def test_insert_predispatch_constraint_all_slack_returns_zero(self, test_db):
        df = pd.DataFrame([{
            'run_datetime': datetime(2025, 6, 22, 0, 30),
            'interval_datetime': datetime(2025, 6, 22, 1, 0),
            'constraintid': 'SLACK_CON',
            'rhs': 100.0,
            'marginalvalue': 0.0,
            'violationdegree': 0.0,
        }])
        count = await test_db.insert_predispatch_constraint(df)
        assert count == 0

    @pytest.mark.asyncio
    async def test_insert_predispatch_constraint_upsert(self, test_db):
        df1 = pd.DataFrame([{
            'run_datetime': datetime(2025, 6, 22, 0, 30),
            'interval_datetime': datetime(2025, 6, 22, 1, 0),
            'constraintid': 'BINDING_CON',
            'rhs': 100.0,
            'marginalvalue': 25.0,
            'violationdegree': 0.0,
            'lhs': 50.0,
        }])
        await test_db.insert_predispatch_constraint(df1)

        df2 = df1.copy()
        df2['marginalvalue'] = 40.0
        df2['lhs'] = 65.0
        await test_db.insert_predispatch_constraint(df2)

        async with test_db._pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM predispatch_constraint")
        assert len(rows) == 1
        assert rows[0]['marginalvalue'] == pytest.approx(40.0)
        assert rows[0]['lhs'] == pytest.approx(65.0)


class TestConstraintEquationTermsInsert:
    """Tests for insert_constraint_equation_terms method (versioned upsert semantics)."""

    @pytest.mark.asyncio
    async def test_insert_constraint_equation_terms(self, test_db):
        df = pd.DataFrame([
            {'constraintid': 'C_BINDING', 'version': 1, 'effective_date': date(2024, 1, 1),
             'term_type': 'duid', 'term_id': 'BAYSW1', 'factor': 1.0},
            {'constraintid': 'C_BINDING', 'version': 1, 'effective_date': date(2024, 1, 1),
             'term_type': 'interconnector', 'term_id': 'NSW1-QLD1', 'factor': -1.0},
        ])
        count = await test_db.insert_constraint_equation_terms(df)
        assert count == 2

        async with test_db._pool.acquire() as conn:
            rows = await conn.fetch("SELECT term_type, term_id, factor, effective_date, first_seen, last_seen "
                                     "FROM constraint_equation_terms ORDER BY term_type")
        assert [r['term_type'] for r in rows] == ['duid', 'interconnector']
        assert rows[0]['effective_date'] == date(2024, 1, 1)
        assert rows[0]['first_seen'] == date.today()
        assert rows[0]['last_seen'] == date.today()

    @pytest.mark.asyncio
    async def test_insert_constraint_equation_terms_empty_df(self, test_db):
        count = await test_db.insert_constraint_equation_terms(pd.DataFrame())
        assert count == 0

    @pytest.mark.asyncio
    async def test_insert_constraint_equation_terms_without_effective_date_stores_null(self, test_db):
        """The legacy MMSDM ingest supplies no effective_date -- it must stay NULL, not error."""
        df = pd.DataFrame([
            {'constraintid': 'C_LEGACY', 'version': -1, 'term_type': 'duid', 'term_id': 'BAYSW1', 'factor': 1.0},
        ])
        count = await test_db.insert_constraint_equation_terms(df)
        assert count == 1

        async with test_db._pool.acquire() as conn:
            rows = await conn.fetch("SELECT effective_date FROM constraint_equation_terms")
        assert rows[0]['effective_date'] is None

    @pytest.mark.asyncio
    async def test_reingest_same_version_updates_factor_and_last_seen_only(self, test_db):
        """A version is immutable once seen -- re-seeing it just refreshes factor/last_seen."""
        df1 = pd.DataFrame([
            {'constraintid': 'C_BINDING', 'version': 1, 'effective_date': date(2024, 1, 1),
             'term_type': 'duid', 'term_id': 'BAYSW1', 'factor': 1.0},
        ])
        await test_db.insert_constraint_equation_terms(df1)

        df2 = pd.DataFrame([
            {'constraintid': 'C_BINDING', 'version': 1, 'effective_date': date(2024, 1, 1),
             'term_type': 'duid', 'term_id': 'BAYSW1', 'factor': 0.5},
        ])
        await test_db.insert_constraint_equation_terms(df2)

        async with test_db._pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM constraint_equation_terms")
        assert len(rows) == 1
        assert rows[0]['version'] == 1
        assert rows[0]['factor'] == pytest.approx(0.5)

    @pytest.mark.asyncio
    async def test_new_version_of_same_constraint_is_kept_alongside_the_old_one(self, test_db):
        """Versions are never purged -- a later version of a constraint adds a new row."""
        df1 = pd.DataFrame([
            {'constraintid': 'C_BINDING', 'version': 1, 'effective_date': date(2024, 1, 1),
             'term_type': 'duid', 'term_id': 'BAYSW1', 'factor': 1.0},
        ])
        await test_db.insert_constraint_equation_terms(df1)

        df2 = pd.DataFrame([
            {'constraintid': 'C_BINDING', 'version': 2, 'effective_date': date(2026, 1, 1),
             'term_type': 'duid', 'term_id': 'BAYSW1', 'factor': 0.5},
        ])
        await test_db.insert_constraint_equation_terms(df2)

        async with test_db._pool.acquire() as conn:
            rows = await conn.fetch("SELECT version, factor FROM constraint_equation_terms ORDER BY version")
        assert [r['version'] for r in rows] == [1, 2]
        assert rows[0]['factor'] == pytest.approx(1.0)
        assert rows[1]['factor'] == pytest.approx(0.5)


class TestInferredUnitGenerationMocked:
    """Tests for insert/get_inferred_unit_generation and get_dispatch_data_for_duids.

    Mocked at the pool level (like TestForecastHistoryInsert) so these run without
    a real database and assert on the query/records contract.
    """

    def _mocked_db(self, fetch_return=None):
        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = fetch_return if fetch_return is not None else []
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_pool.acquire.return_value.__aexit__.return_value = None
        db = NEMDatabase("postgresql://unused")
        db._pool = mock_pool
        return db, mock_conn

    @pytest.mark.asyncio
    async def test_insert_empty_df_returns_zero_without_acquiring(self):
        db, _ = self._mocked_db()
        count = await db.insert_inferred_unit_generation(pd.DataFrame())
        assert count == 0
        db._pool.acquire.assert_not_called()

    @pytest.mark.asyncio
    async def test_insert_drops_unidentifiable_rows(self):
        db, mock_conn = self._mocked_db()
        run = datetime(2026, 7, 9, 10, 0)
        interval = datetime(2026, 7, 9, 10, 30)
        df = pd.DataFrame([
            {"run_datetime": run, "interval_datetime": interval, "duid": "A", "mw_inferred": 30.0,
             "quality": "good", "n_equations": 2, "system_residual": 0.01},
            {"run_datetime": run, "interval_datetime": interval, "duid": "B", "mw_inferred": 10.0,
             "quality": "weak", "n_equations": 1, "system_residual": 0.5},
            {"run_datetime": run, "interval_datetime": interval, "duid": "C", "mw_inferred": 5.0,
             "quality": "unidentifiable", "n_equations": 1, "system_residual": 0.9},
        ])

        count = await db.insert_inferred_unit_generation(df)

        assert count == 2
        _, records = mock_conn.executemany.call_args.args
        duids = [r[2] for r in records]
        assert duids == ["A", "B"]
        assert all(len(r) == 7 for r in records)

    @pytest.mark.asyncio
    async def test_insert_all_unidentifiable_returns_zero(self):
        db, mock_conn = self._mocked_db()
        run = datetime(2026, 7, 9, 10, 0)
        interval = datetime(2026, 7, 9, 10, 30)
        df = pd.DataFrame([{
            "run_datetime": run, "interval_datetime": interval, "duid": "C", "mw_inferred": 5.0,
            "quality": "unidentifiable", "n_equations": 1, "system_residual": 0.9,
        }])

        count = await db.insert_inferred_unit_generation(df)

        assert count == 0
        mock_conn.executemany.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_inferred_unit_generation_empty_returns_columns(self):
        db, _ = self._mocked_db(fetch_return=[])
        out = await db.get_inferred_unit_generation(datetime(2026, 7, 1))
        assert out.empty
        assert list(out.columns) == [
            "run_datetime", "interval_datetime", "duid", "mw_inferred", "quality", "n_equations", "residual",
        ]

    @pytest.mark.asyncio
    async def test_get_inferred_unit_generation_filters_by_duid_when_given(self):
        db, mock_conn = self._mocked_db(fetch_return=[])
        await db.get_inferred_unit_generation(datetime(2026, 7, 1), duid="A")
        sql, start_arg, duid_arg = mock_conn.fetch.call_args.args
        assert "duid = $2" in sql
        assert duid_arg == "A"

    @pytest.mark.asyncio
    async def test_get_dispatch_data_for_duids_empty_list_returns_columns_without_acquiring(self):
        db, _ = self._mocked_db()
        out = await db.get_dispatch_data_for_duids([], datetime(2026, 7, 1), datetime(2026, 7, 2))
        assert out.empty
        assert list(out.columns) == ["settlementdate", "duid", "scadavalue"]
        db._pool.acquire.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_dispatch_data_for_duids_passes_duid_list(self):
        db, mock_conn = self._mocked_db(fetch_return=[])
        await db.get_dispatch_data_for_duids(["A", "B"], datetime(2026, 7, 1), datetime(2026, 7, 2))
        (sql, duids_arg, start_arg, end_arg) = mock_conn.fetch.call_args.args
        assert duids_arg == ["A", "B"]
        assert "duid = ANY($1::text[])" in sql


class TestSTPASADataInsert:
    """Tests for insert_stpasa_data method"""

    @pytest.mark.asyncio
    async def test_insert_stpasa_data(self, test_db):
        """Test inserting STPASA data DataFrame"""
        df = pd.DataFrame([{
            'run_datetime': datetime(2025, 1, 15, 6, 0),
            'interval_datetime': datetime(2025, 1, 16, 0, 0),
            'regionid': 'VIC1',
            'demand10': 4500.0,
            'demand50': 4900.0,
            'demand90': 5300.0,
            'reservereq': 1200.0,
            'capacityreq': 6100.0,
            'aggregatecapacityavailable': 7200.0,
            'aggregatepasaavailability': 6900.0,
            'surplusreserve': 1100.0,
            'lorcondition': 0,
            'calculatedlor1level': 1800.0,
            'calculatedlor2level': 1400.0
        }])

        count = await test_db.insert_stpasa_data(df)
        assert count == 1

    @pytest.mark.asyncio
    async def test_insert_stpasa_data_empty_df(self, test_db):
        """Test that empty DataFrame returns 0"""
        df = pd.DataFrame()
        count = await test_db.insert_stpasa_data(df)
        assert count == 0


class TestPASAQueries:
    """Tests for PASA data query methods"""

    @pytest.mark.asyncio
    async def test_get_latest_pdpasa(self, test_db):
        """Test retrieving latest PDPASA data for a region"""
        # Insert PDPASA data for multiple runs
        df1 = pd.DataFrame([{
            'run_datetime': datetime(2025, 1, 15, 10, 0),
            'interval_datetime': datetime(2025, 1, 15, 10, 30),
            'regionid': 'NSW1',
            'demand50': 7500.0,
            'lorcondition': 0
        }])
        await test_db.insert_pdpasa_data(df1)

        df2 = pd.DataFrame([{
            'run_datetime': datetime(2025, 1, 15, 10, 30),  # Later run
            'interval_datetime': datetime(2025, 1, 15, 11, 0),
            'regionid': 'NSW1',
            'demand50': 7600.0,
            'lorcondition': 1
        }])
        await test_db.insert_pdpasa_data(df2)

        result = await test_db.get_latest_pdpasa('NSW1')

        # Should return data from the latest run only
        assert len(result) == 1
        assert result[0]['demand50'] == 7600.0
        assert result[0]['lorcondition'] == 1

    @pytest.mark.asyncio
    async def test_get_latest_pdpasa_empty(self, test_db):
        """Test retrieving PDPASA for region with no data"""
        result = await test_db.get_latest_pdpasa('NOREGION')
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_get_latest_stpasa(self, test_db):
        """Test retrieving latest STPASA data for a region"""
        df = pd.DataFrame([{
            'run_datetime': datetime(2025, 1, 15, 6, 0),
            'interval_datetime': datetime(2025, 1, 16, 0, 0),
            'regionid': 'QLD1',
            'demand50': 6500.0,
            'lorcondition': 0
        }])
        await test_db.insert_stpasa_data(df)

        result = await test_db.get_latest_stpasa('QLD1')

        assert len(result) == 1
        assert result[0]['demand50'] == 6500.0

    @pytest.mark.asyncio
    async def test_get_latest_stpasa_empty(self, test_db):
        """Test retrieving STPASA for region with no data"""
        result = await test_db.get_latest_stpasa('NOREGION')
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_get_latest_pdpasa_run_datetime(self, test_db):
        """Test getting latest PDPASA run datetime"""
        df = pd.DataFrame([{
            'run_datetime': datetime(2025, 1, 15, 10, 30),
            'interval_datetime': datetime(2025, 1, 15, 11, 0),
            'regionid': 'SA1',
            'demand50': 2100.0
        }])
        await test_db.insert_pdpasa_data(df)

        result = await test_db.get_latest_pdpasa_run_datetime()

        assert result == datetime(2025, 1, 15, 10, 30)

    @pytest.mark.asyncio
    async def test_get_latest_pdpasa_run_datetime_empty(self, test_db):
        """Test getting latest PDPASA run datetime when no data"""
        result = await test_db.get_latest_pdpasa_run_datetime()
        assert result is None

    @pytest.mark.asyncio
    async def test_get_latest_stpasa_run_datetime(self, test_db):
        """Test getting latest STPASA run datetime"""
        df = pd.DataFrame([{
            'run_datetime': datetime(2025, 1, 15, 12, 0),
            'interval_datetime': datetime(2025, 1, 16, 6, 0),
            'regionid': 'TAS1',
            'demand50': 1100.0
        }])
        await test_db.insert_stpasa_data(df)

        result = await test_db.get_latest_stpasa_run_datetime()

        assert result == datetime(2025, 1, 15, 12, 0)

    @pytest.mark.asyncio
    async def test_get_latest_stpasa_run_datetime_empty(self, test_db):
        """Test getting latest STPASA run datetime when no data"""
        result = await test_db.get_latest_stpasa_run_datetime()
        assert result is None


class TestExportMethods:
    """Tests for export methods used in CSV downloads"""

    @pytest.mark.asyncio
    async def test_get_unique_fuel_sources(self, test_db):
        """Test getting unique fuel sources from generator_info"""
        # Insert some generator info with different fuel sources
        generators = [
            {'duid': 'GEN1', 'station_name': 'Coal Station', 'region': 'NSW',
             'fuel_source': 'Coal', 'technology_type': 'Steam', 'capacity_mw': 500},
            {'duid': 'GEN2', 'station_name': 'Wind Farm', 'region': 'VIC',
             'fuel_source': 'Wind', 'technology_type': 'Wind Turbine', 'capacity_mw': 200},
            {'duid': 'GEN3', 'station_name': 'Solar Farm', 'region': 'QLD',
             'fuel_source': 'Solar', 'technology_type': 'Photovoltaic', 'capacity_mw': 150},
        ]
        await test_db.update_generator_info(generators)

        result = await test_db.get_unique_fuel_sources()

        assert isinstance(result, list)
        assert 'Coal' in result
        assert 'Wind' in result
        assert 'Solar' in result

    @pytest.mark.asyncio
    async def test_get_unique_fuel_sources_empty(self, test_db):
        """Test getting unique fuel sources when no generators exist"""
        result = await test_db.get_unique_fuel_sources()
        assert isinstance(result, list)
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_export_price_data(self, test_db):
        """Test exporting price data as DataFrame"""
        # Insert price data
        price_df = pd.DataFrame([
            {
                'settlementdate': datetime(2025, 1, 15, 10, 0),
                'region': 'NSW',
                'price': 85.50,
                'totaldemand': 7500.0,
                'price_type': 'DISPATCH'
            },
            {
                'settlementdate': datetime(2025, 1, 15, 10, 30),
                'region': 'VIC',
                'price': 90.00,
                'totaldemand': 5000.0,
                'price_type': 'DISPATCH'
            }
        ])
        await test_db.insert_price_data(price_df)

        start = datetime(2025, 1, 15, 0, 0)
        end = datetime(2025, 1, 15, 23, 59)

        result = await test_db.export_price_data(start, end)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert 'settlementdate' in result.columns
        assert 'region' in result.columns
        assert 'price' in result.columns

    @pytest.mark.asyncio
    async def test_export_price_data_with_region_filter(self, test_db):
        """Test exporting price data filtered by region"""
        # Insert price data for multiple regions
        price_df = pd.DataFrame([
            {
                'settlementdate': datetime(2025, 1, 15, 10, 0),
                'region': 'NSW',
                'price': 85.50,
                'totaldemand': 7500.0,
                'price_type': 'DISPATCH'
            },
            {
                'settlementdate': datetime(2025, 1, 15, 10, 0),
                'region': 'VIC',
                'price': 90.00,
                'totaldemand': 5000.0,
                'price_type': 'DISPATCH'
            }
        ])
        await test_db.insert_price_data(price_df)

        start = datetime(2025, 1, 15, 0, 0)
        end = datetime(2025, 1, 15, 23, 59)

        result = await test_db.export_price_data(start, end, regions=['NSW'])

        assert len(result) == 1
        assert result.iloc[0]['region'] == 'NSW'

    @pytest.mark.asyncio
    async def test_export_price_data_empty(self, test_db):
        """Test exporting price data when no data exists"""
        start = datetime(2025, 1, 15, 0, 0)
        end = datetime(2025, 1, 15, 23, 59)

        result = await test_db.export_price_data(start, end)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert 'settlementdate' in result.columns

    @pytest.mark.asyncio
    async def test_export_generation_data(self, test_db):
        """Test exporting generation data as DataFrame"""
        # Insert generator info
        generators = [
            {'duid': 'GEN1', 'station_name': 'Test Station', 'region': 'NSW',
             'fuel_source': 'Coal', 'technology_type': 'Steam', 'capacity_mw': 500}
        ]
        await test_db.update_generator_info(generators)

        # Insert dispatch data
        dispatch_df = pd.DataFrame([{
            'settlementdate': datetime(2025, 1, 15, 10, 0),
            'duid': 'GEN1',
            'scadavalue': 400.0,
            'uigf': 0.0,
            'totalcleared': 400.0,
            'ramprate': 0.0,
            'availability': 500.0,
            'raise1sec': 0.0,
            'lower1sec': 0.0
        }])
        await test_db.insert_dispatch_data(dispatch_df)

        start = datetime(2025, 1, 15, 0, 0)
        end = datetime(2025, 1, 15, 23, 59)

        result = await test_db.export_generation_data(start, end)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert 'generation_mw' in result.columns
        assert 'station_name' in result.columns

    @pytest.mark.asyncio
    async def test_export_generation_data_with_filters(self, test_db):
        """Test exporting generation data with region and fuel source filters"""
        # Insert generator info
        generators = [
            {'duid': 'GEN1', 'station_name': 'Coal Station', 'region': 'NSW',
             'fuel_source': 'Coal', 'technology_type': 'Steam', 'capacity_mw': 500},
            {'duid': 'GEN2', 'station_name': 'Wind Farm', 'region': 'VIC',
             'fuel_source': 'Wind', 'technology_type': 'Wind Turbine', 'capacity_mw': 200}
        ]
        await test_db.update_generator_info(generators)

        # Insert dispatch data for both generators
        dispatch_df = pd.DataFrame([
            {
                'settlementdate': datetime(2025, 1, 15, 10, 0),
                'duid': 'GEN1',
                'scadavalue': 400.0,
                'uigf': 0.0,
                'totalcleared': 400.0,
                'ramprate': 0.0,
                'availability': 500.0,
                'raise1sec': 0.0,
                'lower1sec': 0.0
            },
            {
                'settlementdate': datetime(2025, 1, 15, 10, 0),
                'duid': 'GEN2',
                'scadavalue': 150.0,
                'uigf': 0.0,
                'totalcleared': 150.0,
                'ramprate': 0.0,
                'availability': 200.0,
                'raise1sec': 0.0,
                'lower1sec': 0.0
            }
        ])
        await test_db.insert_dispatch_data(dispatch_df)

        start = datetime(2025, 1, 15, 0, 0)
        end = datetime(2025, 1, 15, 23, 59)

        # Filter by region
        result = await test_db.export_generation_data(start, end, regions=['NSW'])
        assert len(result) == 1
        assert result.iloc[0]['region'] == 'NSW'

        # Filter by fuel source
        result = await test_db.export_generation_data(start, end, fuel_sources=['Wind'])
        assert len(result) == 1
        assert result.iloc[0]['fuel_source'] == 'Wind'

    @pytest.mark.asyncio
    async def test_export_generation_data_empty(self, test_db):
        """Test exporting generation data when no data exists"""
        start = datetime(2025, 1, 15, 0, 0)
        end = datetime(2025, 1, 15, 23, 59)

        result = await test_db.export_generation_data(start, end)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_export_latest_pasa_data_pdpasa(self, test_db):
        """Test exporting latest PDPASA data as DataFrame"""
        # Insert PDPASA data
        df = pd.DataFrame([{
            'run_datetime': datetime(2025, 1, 15, 10, 0),
            'interval_datetime': datetime(2025, 1, 15, 10, 30),
            'regionid': 'NSW1',
            'demand10': 7200.0,
            'demand50': 7500.0,
            'demand90': 7800.0,
            'reservereq': 1500.0,
            'capacityreq': 9000.0,
            'aggregatecapacityavailable': 10500.0,
            'aggregatepasaavailability': 10000.0,
            'surplusreserve': 1500.0,
            'lorcondition': 0,
            'calculatedlor1level': 2000.0,
            'calculatedlor2level': 1500.0
        }])
        await test_db.insert_pdpasa_data(df)

        result = await test_db.export_latest_pasa_data('pdpasa')

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert 'run_datetime' in result.columns
        assert 'interval_datetime' in result.columns
        assert 'regionid' in result.columns

    @pytest.mark.asyncio
    async def test_export_latest_pasa_data_stpasa(self, test_db):
        """Test exporting latest STPASA data as DataFrame"""
        # Insert STPASA data
        df = pd.DataFrame([{
            'run_datetime': datetime(2025, 1, 15, 6, 0),
            'interval_datetime': datetime(2025, 1, 16, 0, 0),
            'regionid': 'VIC1',
            'demand10': 4500.0,
            'demand50': 4900.0,
            'demand90': 5300.0,
            'reservereq': 1200.0,
            'capacityreq': 6100.0,
            'aggregatecapacityavailable': 7200.0,
            'aggregatepasaavailability': 6900.0,
            'surplusreserve': 1100.0,
            'lorcondition': 0,
            'calculatedlor1level': 1800.0,
            'calculatedlor2level': 1400.0
        }])
        await test_db.insert_stpasa_data(df)

        result = await test_db.export_latest_pasa_data('stpasa')

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_export_latest_pasa_data_with_region_filter(self, test_db):
        """Test exporting PASA data with region filter"""
        # Insert PDPASA data for multiple regions
        df = pd.DataFrame([
            {
                'run_datetime': datetime(2025, 1, 15, 10, 0),
                'interval_datetime': datetime(2025, 1, 15, 10, 30),
                'regionid': 'NSW1',
                'demand50': 7500.0
            },
            {
                'run_datetime': datetime(2025, 1, 15, 10, 0),
                'interval_datetime': datetime(2025, 1, 15, 10, 30),
                'regionid': 'VIC1',
                'demand50': 5000.0
            }
        ])
        await test_db.insert_pdpasa_data(df)

        # Filter by region (should auto-convert NSW to NSW1)
        result = await test_db.export_latest_pasa_data('pdpasa', regions=['NSW'])

        assert len(result) == 1
        assert result.iloc[0]['regionid'] == 'NSW1'

    @pytest.mark.asyncio
    async def test_export_latest_pasa_data_empty(self, test_db):
        """Test exporting PASA data when no data exists"""
        result = await test_db.export_latest_pasa_data('pdpasa')

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert 'run_datetime' in result.columns

    @pytest.mark.asyncio
    async def test_get_export_data_ranges(self, test_db):
        """Test getting export data ranges"""
        # Insert some data
        price_df = pd.DataFrame([{
            'settlementdate': datetime(2025, 1, 15, 10, 0),
            'region': 'NSW',
            'price': 85.50,
            'totaldemand': 7500.0,
            'price_type': 'DISPATCH'
        }])
        await test_db.insert_price_data(price_df)

        result = await test_db.get_export_data_ranges()

        assert isinstance(result, dict)
        assert 'prices' in result
        assert 'generation' in result
        assert 'pasa' in result
        assert 'pdpasa' in result['pasa']
        assert 'stpasa' in result['pasa']
        assert 'earliest_date' in result['prices']
        assert 'latest_date' in result['prices']

    @pytest.mark.asyncio
    async def test_get_export_data_ranges_empty(self, test_db):
        """Test getting export data ranges when no data exists"""
        result = await test_db.get_export_data_ranges()

        assert isinstance(result, dict)
        assert result['prices']['earliest_date'] is None
        assert result['prices']['latest_date'] is None
        assert result['pasa']['pdpasa']['earliest_date'] is None
        assert result['pasa']['stpasa']['earliest_date'] is None


class TestBidDayOfferInsert:
    """Tests for insert_bid_day_offer method"""

    @pytest.mark.asyncio
    async def test_insert_bid_day_offer(self, test_db):
        """Test inserting bid day offer data."""
        df = pd.DataFrame([{
            'settlementdate': pd.Timestamp('2026-02-21'),
            'duid': 'BAYSW1',
            'offerdate': pd.Timestamp('2026-02-20 12:00:00'),
            'priceband1': -987.0, 'priceband2': 0.0, 'priceband3': 30.0,
            'priceband4': 50.0, 'priceband5': 100.0, 'priceband6': 300.0,
            'priceband7': 1000.0, 'priceband8': 5000.0, 'priceband9': 10000.0,
            'priceband10': 15000.0,
            'minimumload': 200.0,
            't1': 3.0, 't2': 3.0, 't3': 3.0, 't4': 3.0,
        }])
        count = await test_db.insert_bid_day_offer(df)
        assert count == 1

    @pytest.mark.asyncio
    async def test_insert_bid_day_offer_empty(self, test_db):
        """Test empty DataFrame returns 0."""
        count = await test_db.insert_bid_day_offer(pd.DataFrame())
        assert count == 0

    @pytest.mark.asyncio
    async def test_insert_bid_day_offer_upsert(self, test_db):
        """Test upsert replaces existing data."""
        df1 = pd.DataFrame([{
            'settlementdate': pd.Timestamp('2026-02-21'),
            'duid': 'BAYSW1',
            'offerdate': pd.Timestamp('2026-02-20 12:00:00'),
            'priceband1': -987.0, 'priceband2': 0.0, 'priceband3': 30.0,
            'priceband4': 50.0, 'priceband5': 100.0, 'priceband6': 300.0,
            'priceband7': 1000.0, 'priceband8': 5000.0, 'priceband9': 10000.0,
            'priceband10': 15000.0,
            'minimumload': 200.0,
            't1': 3.0, 't2': 3.0, 't3': 3.0, 't4': 3.0,
        }])
        await test_db.insert_bid_day_offer(df1)

        # Insert with updated price
        df2 = pd.DataFrame([{
            'settlementdate': pd.Timestamp('2026-02-21'),
            'duid': 'BAYSW1',
            'offerdate': pd.Timestamp('2026-02-20 12:00:00'),
            'priceband1': -500.0, 'priceband2': 0.0, 'priceband3': 25.0,
            'priceband4': 45.0, 'priceband5': 90.0, 'priceband6': 250.0,
            'priceband7': 900.0, 'priceband8': 4500.0, 'priceband9': 9000.0,
            'priceband10': 14000.0,
            'minimumload': 180.0,
            't1': 3.0, 't2': 3.0, 't3': 3.0, 't4': 3.0,
        }])
        count = await test_db.insert_bid_day_offer(df2)
        assert count == 1


class TestBidPerOfferInsert:
    """Tests for insert_bid_per_offer method"""

    @pytest.mark.asyncio
    async def test_insert_bid_per_offer(self, test_db):
        """Test inserting bid per offer data."""
        df = pd.DataFrame([{
            'settlementdate': pd.Timestamp('2026-02-21 00:05:00'),
            'duid': 'BAYSW1',
            'offerdate': pd.Timestamp('2026-02-20 12:00:00'),
            'bandavail1': 100.0, 'bandavail2': 50.0, 'bandavail3': 200.0,
            'bandavail4': 0.0, 'bandavail5': 0.0, 'bandavail6': 0.0,
            'bandavail7': 0.0, 'bandavail8': 0.0, 'bandavail9': 0.0,
            'bandavail10': 0.0,
            'maxavail': 660.0, 'fixedload': 0.0,
            'rocup': 5.0, 'rocdown': 5.0, 'pasaavailability': 660.0,
        }])
        count = await test_db.insert_bid_per_offer(df)
        assert count == 1

    @pytest.mark.asyncio
    async def test_insert_bid_per_offer_empty(self, test_db):
        """Test empty DataFrame returns 0."""
        count = await test_db.insert_bid_per_offer(pd.DataFrame())
        assert count == 0

    @pytest.mark.asyncio
    async def test_insert_bid_per_offer_upsert(self, test_db):
        """Test upsert replaces existing data."""
        df1 = pd.DataFrame([{
            'settlementdate': pd.Timestamp('2026-02-21 00:05:00'),
            'duid': 'BAYSW1',
            'offerdate': pd.Timestamp('2026-02-20 12:00:00'),
            'bandavail1': 100.0, 'bandavail2': 50.0, 'bandavail3': 200.0,
            'bandavail4': 0.0, 'bandavail5': 0.0, 'bandavail6': 0.0,
            'bandavail7': 0.0, 'bandavail8': 0.0, 'bandavail9': 0.0,
            'bandavail10': 0.0,
            'maxavail': 660.0, 'fixedload': 0.0,
            'rocup': 5.0, 'rocdown': 5.0, 'pasaavailability': 660.0,
        }])
        await test_db.insert_bid_per_offer(df1)

        # Update band availability
        df2 = pd.DataFrame([{
            'settlementdate': pd.Timestamp('2026-02-21 00:05:00'),
            'duid': 'BAYSW1',
            'offerdate': pd.Timestamp('2026-02-20 12:00:00'),
            'bandavail1': 150.0, 'bandavail2': 60.0, 'bandavail3': 250.0,
            'bandavail4': 0.0, 'bandavail5': 0.0, 'bandavail6': 0.0,
            'bandavail7': 0.0, 'bandavail8': 0.0, 'bandavail9': 0.0,
            'bandavail10': 0.0,
            'maxavail': 660.0, 'fixedload': 0.0,
            'rocup': 5.0, 'rocdown': 5.0, 'pasaavailability': 660.0,
        }])
        count = await test_db.insert_bid_per_offer(df2)
        assert count == 1


class TestGetBidBandsForDuid:
    """Tests for get_bid_bands_for_duid method"""

    @pytest.mark.asyncio
    async def test_get_bid_bands_success(self, test_db):
        """Test fetching combined bid band data."""
        # Insert day offer
        day_df = pd.DataFrame([{
            'settlementdate': pd.Timestamp('2026-02-21'),
            'duid': 'BAYSW1',
            'offerdate': pd.Timestamp('2026-02-20 12:00:00'),
            'priceband1': -987.0, 'priceband2': 0.0, 'priceband3': 30.0,
            'priceband4': 50.0, 'priceband5': 100.0, 'priceband6': 300.0,
            'priceband7': 1000.0, 'priceband8': 5000.0, 'priceband9': 10000.0,
            'priceband10': 15000.0,
            'minimumload': 200.0,
            't1': 3.0, 't2': 3.0, 't3': 3.0, 't4': 3.0,
        }])
        await test_db.insert_bid_day_offer(day_df)

        # Insert per-offer intervals
        per_df = pd.DataFrame([
            {
                'settlementdate': pd.Timestamp('2026-02-21 00:05:00'),
                'duid': 'BAYSW1',
                'offerdate': pd.Timestamp('2026-02-20 12:00:00'),
                'bandavail1': 100.0, 'bandavail2': 50.0, 'bandavail3': 200.0,
                'bandavail4': 0.0, 'bandavail5': 0.0, 'bandavail6': 0.0,
                'bandavail7': 0.0, 'bandavail8': 0.0, 'bandavail9': 0.0,
                'bandavail10': 0.0,
                'maxavail': 660.0, 'fixedload': 0.0,
                'rocup': 5.0, 'rocdown': 5.0, 'pasaavailability': 660.0,
            },
            {
                'settlementdate': pd.Timestamp('2026-02-21 00:10:00'),
                'duid': 'BAYSW1',
                'offerdate': pd.Timestamp('2026-02-20 12:00:00'),
                'bandavail1': 120.0, 'bandavail2': 40.0, 'bandavail3': 180.0,
                'bandavail4': 10.0, 'bandavail5': 0.0, 'bandavail6': 0.0,
                'bandavail7': 0.0, 'bandavail8': 0.0, 'bandavail9': 0.0,
                'bandavail10': 0.0,
                'maxavail': 660.0, 'fixedload': 0.0,
                'rocup': 5.0, 'rocdown': 5.0, 'pasaavailability': 660.0,
            },
        ])
        await test_db.insert_bid_per_offer(per_df)

        result = await test_db.get_bid_bands_for_duid('BAYSW1', date(2026, 2, 21))

        assert len(result) == 2
        assert result[0]['priceband1'] == -987.0
        assert result[0]['bandavail1'] == 100.0
        assert result[1]['bandavail1'] == 120.0
        assert result[0]['minimumload'] == 200.0

    @pytest.mark.asyncio
    async def test_get_bid_bands_empty(self, test_db):
        """Test fetching bids for nonexistent DUID."""
        result = await test_db.get_bid_bands_for_duid('NONEXIST', date(2026, 2, 21))
        assert len(result) == 0


class TestHasBidDataForDate:
    """Tests for has_bid_data_for_date method"""

    @pytest.mark.asyncio
    async def test_has_bid_data_true(self, test_db):
        """Test returns True when bid data exists."""
        df = pd.DataFrame([{
            'settlementdate': pd.Timestamp('2026-02-21 00:05:00'),
            'duid': 'BAYSW1',
            'offerdate': pd.Timestamp('2026-02-20 12:00:00'),
            'bandavail1': 100.0, 'bandavail2': 0.0, 'bandavail3': 0.0,
            'bandavail4': 0.0, 'bandavail5': 0.0, 'bandavail6': 0.0,
            'bandavail7': 0.0, 'bandavail8': 0.0, 'bandavail9': 0.0,
            'bandavail10': 0.0,
            'maxavail': 100.0, 'fixedload': 0.0,
            'rocup': 5.0, 'rocdown': 5.0, 'pasaavailability': 100.0,
        }])
        await test_db.insert_bid_per_offer(df)

        result = await test_db.has_bid_data_for_date(datetime(2026, 2, 21))
        assert result is True

    @pytest.mark.asyncio
    async def test_has_bid_data_false(self, test_db):
        """Test returns False when no bid data exists."""
        result = await test_db.has_bid_data_for_date(datetime(2026, 2, 21))
        assert result is False


class TestSearchDuids:
    """Tests for search_duids method"""

    @pytest.mark.asyncio
    async def test_search_duids_by_duid(self, test_db):
        """Test searching by DUID prefix."""
        await test_db.update_generator_info([
            {'duid': 'BAYSW1', 'station_name': 'Bayswater', 'region': 'NSW1',
             'fuel_source': 'Black Coal', 'technology_type': 'Steam', 'capacity_mw': 660.0},
            {'duid': 'BAYSW2', 'station_name': 'Bayswater', 'region': 'NSW1',
             'fuel_source': 'Black Coal', 'technology_type': 'Steam', 'capacity_mw': 660.0},
            {'duid': 'LOYS1', 'station_name': 'Loy Yang A', 'region': 'VIC1',
             'fuel_source': 'Brown Coal', 'technology_type': 'Steam', 'capacity_mw': 560.0},
        ])

        results = await test_db.search_duids('BAYSW')
        assert len(results) == 2
        assert results[0]['duid'] == 'BAYSW1'
        assert results[1]['duid'] == 'BAYSW2'

    @pytest.mark.asyncio
    async def test_search_duids_by_station_name(self, test_db):
        """Test searching by station name."""
        await test_db.update_generator_info([
            {'duid': 'BAYSW1', 'station_name': 'Bayswater', 'region': 'NSW1',
             'fuel_source': 'Black Coal', 'technology_type': 'Steam', 'capacity_mw': 660.0},
        ])

        results = await test_db.search_duids('Bayswater')
        assert len(results) == 1
        assert results[0]['duid'] == 'BAYSW1'

    @pytest.mark.asyncio
    async def test_search_duids_no_results(self, test_db):
        """Test search with no matches."""
        results = await test_db.search_duids('ZZZZZ')
        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_search_duids_case_insensitive(self, test_db):
        """Test that search is case-insensitive."""
        await test_db.update_generator_info([
            {'duid': 'BAYSW1', 'station_name': 'Bayswater', 'region': 'NSW1',
             'fuel_source': 'Black Coal', 'technology_type': 'Steam', 'capacity_mw': 660.0},
        ])

        results = await test_db.search_duids('baysw')
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_search_duids_limit(self, test_db):
        """Test search respects limit."""
        await test_db.update_generator_info([
            {'duid': f'TEST{i}', 'station_name': 'TestStation', 'region': 'NSW1',
             'fuel_source': 'Gas', 'technology_type': 'OCGT', 'capacity_mw': 100.0}
            for i in range(5)
        ])

        results = await test_db.search_duids('TEST', limit=2)
        assert len(results) == 2


class TestForecastHistoryInsert:
    """Tests for insert_forecast_history method.

    Mocked at the pool level (rather than the live ``test_db`` fixture used
    elsewhere in this file) since the assertions here are about *how* the
    pool is called, not what ends up in a real database.
    """

    @pytest.mark.asyncio
    async def test_insert_forecast_history_empty_rows(self):
        """Empty input returns 0 without acquiring a connection."""
        db = NEMDatabase("postgresql://unused")
        db._pool = MagicMock()

        count = await db.insert_forecast_history([])

        assert count == 0
        db._pool.acquire.assert_not_called()

    @pytest.mark.asyncio
    async def test_insert_forecast_history(self):
        """Non-empty input calls executemany once with one 7-tuple per row."""
        mock_conn = AsyncMock()
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_pool.acquire.return_value.__aexit__.return_value = None

        db = NEMDatabase("postgresql://unused")
        db._pool = mock_pool

        run_at = datetime(2026, 7, 9, 10, 0)
        interval_dt = datetime(2026, 7, 9, 10, 30)
        rows = [{
            "run_at": run_at, "interval_datetime": interval_dt, "region": "NSW1",
            "p50": 85.5, "p10": 70.1, "p90": 110.9, "model_trained_at": "2026-07-01T00:00:00",
        }]

        count = await db.insert_forecast_history(rows)

        assert count == 1
        mock_conn.executemany.assert_called_once()
        _, records = mock_conn.executemany.call_args.args
        assert records == [(run_at, interval_dt, "NSW1", 85.5, 70.1, 110.9, "2026-07-01T00:00:00")]
        assert all(len(r) == 7 for r in records)

class TestLatestPredispatchAccessorsMocked:
    """Tests for the latest-predispatch network accessors.

    Mocked at the pool level (like TestInsertForecastHistoryMocked) since the
    assertions are about the query contract, not what a real database returns.
    """

    def _mocked_db(self):
        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = []
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_pool.acquire.return_value.__aexit__.return_value = None
        db = NEMDatabase("postgresql://unused")
        db._pool = mock_pool
        return db, mock_conn

    @pytest.mark.asyncio
    async def test_constraints_query_is_forward_bounded_to_latest_run(self):
        db, mock_conn = self._mocked_db()

        rows = await db.get_latest_predispatch_constraints()

        assert rows == []
        (sql,) = mock_conn.fetch.call_args.args
        assert "interval_datetime >= NOW()" in sql
        assert "SELECT MAX(run_datetime) FROM predispatch_constraint" in sql

    @pytest.mark.asyncio
    async def test_interconnectors_query_is_forward_bounded_to_latest_run(self):
        db, mock_conn = self._mocked_db()

        rows = await db.get_latest_predispatch_interconnectors()

        assert rows == []
        (sql,) = mock_conn.fetch.call_args.args
        assert "interval_datetime >= NOW()" in sql
        assert "SELECT MAX(run_datetime) FROM predispatch_interconnector" in sql
