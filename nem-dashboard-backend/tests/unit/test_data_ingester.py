"""
Unit tests for DataIngester

Requires DATABASE_URL environment variable for tests that need a real database.
"""
import pytest
import os
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch, MagicMock
import pandas as pd
import tempfile
from pathlib import Path

from app.data_ingester import DataIngester, update_sample_generator_info, import_generator_info_from_csv, SAMPLE_GENERATOR_INFO


def get_test_db_url():
    """Get test database URL from environment or skip."""
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        pytest.skip("DATABASE_URL environment variable not set")
    return db_url


class TestDataIngesterInit:
    """Tests for DataIngester initialization"""

    @pytest.mark.asyncio
    async def test_init_creates_clients(self):
        """Test that init creates database and client instances"""
        db_url = get_test_db_url()
        ingester = DataIngester(db_url)

        assert ingester.db is not None
        assert ingester.nem_client is not None
        assert ingester.price_client is not None
        assert ingester.is_running is False

    @pytest.mark.asyncio
    async def test_initialize(self):
        """Test that initialize creates database connection"""
        db_url = get_test_db_url()
        ingester = DataIngester(db_url)
        await ingester.initialize()

        assert ingester.db._pool is not None
        await ingester.db.close()


class TestStopContinuousIngestion:
    """Tests for stop_continuous_ingestion method"""

    @pytest.mark.asyncio
    async def test_stop_sets_flag(self):
        """Test that stop sets is_running to False"""
        db_url = get_test_db_url()
        ingester = DataIngester(db_url)
        ingester.is_running = True

        ingester.stop_continuous_ingestion()

        assert ingester.is_running is False


class TestIngestCurrentData:
    """Tests for ingest_current_data method"""

    @pytest.mark.asyncio
    async def test_ingest_current_data_success(self):
        """Test successful current data ingestion"""
        db_url = get_test_db_url()
        ingester = DataIngester(db_url)
        await ingester.initialize()

        # Mock all client methods to return sample data
        sample_df = pd.DataFrame([{
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

        price_df = pd.DataFrame([{
            'settlementdate': datetime(2025, 1, 15, 10, 30),
            'region': 'NSW',
            'price': 85.50,
            'totaldemand': 7500.0,
            'price_type': 'DISPATCH'
        }])

        interconnector_df = pd.DataFrame([{
            'settlementdate': datetime(2025, 1, 15, 10, 30),
            'interconnector': 'NSW1-QLD1',
            'meteredmwflow': 350.5,
            'mwflow': 355.0,
            'mwloss': 4.5,
            'marginalvalue': 12.30
        }])

        ingester.nem_client.get_current_dispatch_data = AsyncMock(return_value=sample_df)
        ingester.price_client.get_current_dispatch_prices = AsyncMock(return_value=price_df)
        ingester.price_client.get_trading_prices = AsyncMock(return_value=price_df)
        ingester.price_client.get_daily_prices = AsyncMock(return_value=price_df)
        ingester.price_client.get_interconnector_flows = AsyncMock(return_value=interconnector_df)

        success = await ingester.ingest_current_data()

        # Should succeed even if some sources fail
        assert isinstance(success, bool)
        await ingester.db.close()

    @pytest.mark.asyncio
    async def test_ingest_current_data_partial_failure(self):
        """Test that partial failures don't stop ingestion"""
        db_url = get_test_db_url()
        ingester = DataIngester(db_url)
        await ingester.initialize()

        # Some return None (failure), some return data
        ingester.nem_client.get_current_dispatch_data = AsyncMock(return_value=None)
        ingester.price_client.get_current_dispatch_prices = AsyncMock(return_value=None)
        ingester.price_client.get_trading_prices = AsyncMock(return_value=None)
        ingester.price_client.get_daily_prices = AsyncMock(return_value=None)
        ingester.price_client.get_interconnector_flows = AsyncMock(return_value=None)

        # Should not raise, just return success indicator
        success = await ingester.ingest_current_data()
        assert isinstance(success, bool)
        await ingester.db.close()


class TestIngestHistoricalData:
    """Tests for ingest_historical_data method"""

    @pytest.mark.asyncio
    async def test_ingest_historical_data(self):
        """Test historical data ingestion"""
        db_url = get_test_db_url()
        ingester = DataIngester(db_url)
        await ingester.initialize()

        sample_df = pd.DataFrame([{
            'settlementdate': datetime(2025, 1, 10, 10, 30),
            'duid': 'TEST1',
            'scadavalue': 100.0,
            'uigf': 0.0,
            'totalcleared': 0.0,
            'ramprate': 0.0,
            'availability': 0.0,
            'raise1sec': 0.0,
            'lower1sec': 0.0
        }])

        ingester.nem_client.get_historical_dispatch_data = AsyncMock(return_value=sample_df)

        start = datetime(2025, 1, 10)
        end = datetime(2025, 1, 11)

        total = await ingester.ingest_historical_data(start, end)
        assert isinstance(total, int)
        await ingester.db.close()


class TestIngestHistoricalPrices:
    """Tests for ingest_historical_prices method"""

    @pytest.mark.asyncio
    async def test_ingest_historical_prices(self):
        """Test historical price ingestion"""
        db_url = get_test_db_url()
        ingester = DataIngester(db_url)
        await ingester.initialize()

        price_df = pd.DataFrame([{
            'settlementdate': datetime(2025, 1, 10, 10, 30),
            'region': 'NSW',
            'price': 85.50,
            'totaldemand': 7500.0,
            'price_type': 'PUBLIC'
        }])

        ingester.price_client.get_daily_prices = AsyncMock(return_value=price_df)

        start = datetime(2025, 1, 10)
        end = datetime(2025, 1, 11)

        total = await ingester.ingest_historical_prices(start, end)
        assert isinstance(total, int)
        await ingester.db.close()


class TestBackfillMissingData:
    """Tests for backfill_missing_data method"""

    @pytest.mark.asyncio
    async def test_backfill_missing_data(self):
        """Test backfill missing data"""
        db_url = get_test_db_url()
        ingester = DataIngester(db_url)
        await ingester.initialize()

        price_df = pd.DataFrame([{
            'settlementdate': datetime(2025, 1, 10, 10, 30),
            'region': 'NSW',
            'price': 85.50,
            'totaldemand': 7500.0,
            'price_type': 'PUBLIC'
        }])

        ingester.price_client.get_daily_prices = AsyncMock(return_value=price_df)

        # Backfill with small days_back for testing
        total = await ingester.backfill_missing_data(days_back=2, max_gaps_per_run=2)
        assert isinstance(total, int)
        await ingester.db.close()


class TestGetDataSummary:
    """Tests for get_data_summary method"""

    @pytest.mark.asyncio
    async def test_get_data_summary(self):
        """Test data summary retrieval"""
        db_url = get_test_db_url()
        ingester = DataIngester(db_url)
        await ingester.initialize()

        summary = await ingester.get_data_summary()

        assert isinstance(summary, dict)
        assert 'total_records' in summary
        await ingester.db.close()


class TestSampleGeneratorInfo:
    """Tests for sample generator info"""

    def test_sample_generator_info_structure(self):
        """Test SAMPLE_GENERATOR_INFO has correct structure"""
        assert isinstance(SAMPLE_GENERATOR_INFO, list)
        assert len(SAMPLE_GENERATOR_INFO) > 0

        for gen in SAMPLE_GENERATOR_INFO:
            assert 'duid' in gen
            assert 'station_name' in gen
            assert 'region' in gen
            assert 'fuel_source' in gen

    @pytest.mark.asyncio
    async def test_update_sample_generator_info(self, test_db):
        """Test updating sample generator info"""
        await update_sample_generator_info(test_db)

        # Verify data was inserted using asyncpg
        async with test_db._pool.acquire() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM generator_info")

        assert count >= len(SAMPLE_GENERATOR_INFO)

    @pytest.mark.asyncio
    async def test_import_generator_info_from_csv_with_valid_csv(self, test_db, tmp_path):
        """Test importing generator info from a CSV file"""
        # Create a temporary CSV file
        csv_content = """DUID,Site Name,Region,Fuel Type,Technology Type,Asset Type,Nameplate Capacity (MW)
TEST1,Test Station 1,NSW1,Coal,Steam Turbine,Existing,500
TEST2,Test Station 2,VIC1,Wind,Wind Turbine,Existing,200
TEST3,Test Station 3,QLD1,Solar,Solar PV,Existing,150
"""
        csv_file = tmp_path / "GenInfo.csv"
        csv_file.write_text(csv_content)

        await import_generator_info_from_csv(test_db, str(csv_file))

        # Verify data was imported using asyncpg
        async with test_db._pool.acquire() as conn:
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM generator_info WHERE duid IN ('TEST1', 'TEST2', 'TEST3')"
            )

        assert count == 3

    @pytest.mark.asyncio
    async def test_import_generator_info_from_csv_fallback_to_sample(self, test_db):
        """Test that missing CSV falls back to sample generator info"""
        await import_generator_info_from_csv(test_db, "/nonexistent/path/GenInfo.csv")

        # Should have fallen back to sample data
        async with test_db._pool.acquire() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM generator_info")

        assert count >= len(SAMPLE_GENERATOR_INFO)


class TestRunContinuousIngestion:
    """Tests for run_continuous_ingestion method"""

    @pytest.mark.asyncio
    async def test_run_continuous_ingestion_can_be_stopped(self):
        """Test that continuous ingestion can be stopped"""
        import asyncio

        db_url = get_test_db_url()
        ingester = DataIngester(db_url)
        await ingester.initialize()

        # Mock methods to return quickly
        ingester.backfill_missing_data = AsyncMock(return_value=0)
        ingester.ingest_current_data = AsyncMock(return_value=True)

        # Start ingestion in background, then stop it
        async def stop_after_delay():
            await asyncio.sleep(0.1)
            ingester.stop_continuous_ingestion()

        # Run both concurrently
        await asyncio.gather(
            ingester.run_continuous_ingestion(interval_minutes=0.01),
            stop_after_delay()
        )

        assert ingester.is_running is False
        await ingester.db.close()

    @pytest.mark.asyncio
    async def test_run_continuous_ingestion_handles_exceptions(self):
        """Test that exceptions in the loop don't stop ingestion"""
        import asyncio

        db_url = get_test_db_url()
        ingester = DataIngester(db_url)
        await ingester.initialize()

        call_count = 0

        async def mock_ingest():
            nonlocal call_count
            call_count += 1
            # Raise exception on second call (inside the while loop)
            # First call is outside the try/except
            if call_count == 2:
                raise Exception("Test error")
            return True

        ingester.backfill_missing_data = AsyncMock(return_value=0)
        ingester.ingest_current_data = mock_ingest

        async def stop_after_calls():
            # Wait for at least 3 calls (1 initial + 2 in loop)
            while call_count < 3:
                await asyncio.sleep(0.05)
            ingester.stop_continuous_ingestion()

        await asyncio.gather(
            ingester.run_continuous_ingestion(interval_minutes=0.001),
            stop_after_calls()
        )

        # Should have continued after the exception in the loop
        assert call_count >= 3
        await ingester.db.close()
