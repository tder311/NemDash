"""
Unit tests for NEMDatabase (PostgreSQL)

Requires DATABASE_URL environment variable set to a PostgreSQL database.
"""
import pytest
import pandas as pd
from datetime import datetime, timedelta

from app.database import NEMDatabase


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

        # Should still have same number of application tables (5: dispatch_data, price_data, generator_info, pdpasa_data, stpasa_data)
        assert count == 5


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
        """Test getting dates with sufficient dispatch data"""
        # Insert dispatch data for multiple dates
        records = []
        base_date = datetime(2025, 1, 15, 10, 0)

        # Insert 150 records for Jan 15 (should meet threshold of 100)
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

        result = await test_db.get_dispatch_dates_with_data(start, end, min_records=100)

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
