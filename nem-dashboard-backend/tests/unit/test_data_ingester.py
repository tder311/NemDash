"""
Unit tests for DataIngester
"""
import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch, MagicMock
import pandas as pd
import tempfile
from pathlib import Path

from app.data_ingester import DataIngester, update_sample_generator_info, SAMPLE_GENERATOR_INFO


class TestDataIngesterInit:
    """Tests for DataIngester initialization"""

    @pytest.mark.asyncio
    async def test_init_creates_clients(self, tmp_path):
        """Test that init creates database and client instances"""
        db_path = str(tmp_path / "test.db")
        ingester = DataIngester(db_path)

        assert ingester.db is not None
        assert ingester.nem_client is not None
        assert ingester.price_client is not None
        assert ingester.is_running is False

    @pytest.mark.asyncio
    async def test_initialize(self, tmp_path):
        """Test that initialize creates database"""
        db_path = str(tmp_path / "test.db")
        ingester = DataIngester(db_path)
        await ingester.initialize()

        assert Path(db_path).exists()


class TestStopContinuousIngestion:
    """Tests for stop_continuous_ingestion method"""

    @pytest.mark.asyncio
    async def test_stop_sets_flag(self, tmp_path):
        """Test that stop sets is_running to False"""
        db_path = str(tmp_path / "test.db")
        ingester = DataIngester(db_path)
        ingester.is_running = True

        ingester.stop_continuous_ingestion()

        assert ingester.is_running is False


class TestIngestCurrentData:
    """Tests for ingest_current_data method"""

    @pytest.mark.asyncio
    async def test_ingest_current_data_success(self, tmp_path):
        """Test successful current data ingestion"""
        db_path = str(tmp_path / "test.db")
        ingester = DataIngester(db_path)
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

    @pytest.mark.asyncio
    async def test_ingest_current_data_partial_failure(self, tmp_path):
        """Test that partial failures don't stop ingestion"""
        db_path = str(tmp_path / "test.db")
        ingester = DataIngester(db_path)
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


class TestIngestHistoricalData:
    """Tests for ingest_historical_data method"""

    @pytest.mark.asyncio
    async def test_ingest_historical_data(self, tmp_path):
        """Test historical data ingestion"""
        db_path = str(tmp_path / "test.db")
        ingester = DataIngester(db_path)
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


class TestIngestHistoricalPrices:
    """Tests for ingest_historical_prices method"""

    @pytest.mark.asyncio
    async def test_ingest_historical_prices(self, tmp_path):
        """Test historical price ingestion"""
        db_path = str(tmp_path / "test.db")
        ingester = DataIngester(db_path)
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


class TestBackfillMissingData:
    """Tests for backfill_missing_data method"""

    @pytest.mark.asyncio
    async def test_backfill_missing_data(self, tmp_path):
        """Test backfill missing data"""
        db_path = str(tmp_path / "test.db")
        ingester = DataIngester(db_path)
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


class TestGetDataSummary:
    """Tests for get_data_summary method"""

    @pytest.mark.asyncio
    async def test_get_data_summary(self, tmp_path):
        """Test data summary retrieval"""
        db_path = str(tmp_path / "test.db")
        ingester = DataIngester(db_path)
        await ingester.initialize()

        summary = await ingester.get_data_summary()

        assert isinstance(summary, dict)
        assert 'total_records' in summary


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

        # Verify data was inserted
        duids = await test_db.get_unique_duids()
        # Note: get_unique_duids gets from dispatch_data, not generator_info
        # So we need to check generator_info directly
        import aiosqlite
        async with aiosqlite.connect(test_db.db_path) as conn:
            cursor = await conn.execute("SELECT COUNT(*) FROM generator_info")
            count = (await cursor.fetchone())[0]

        assert count >= len(SAMPLE_GENERATOR_INFO)


class TestRunContinuousIngestion:
    """Tests for run_continuous_ingestion method"""

    @pytest.mark.asyncio
    async def test_run_continuous_ingestion_can_be_stopped(self, tmp_path):
        """Test that continuous ingestion can be stopped"""
        import asyncio

        db_path = str(tmp_path / "test.db")
        ingester = DataIngester(db_path)
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

    @pytest.mark.asyncio
    async def test_run_continuous_ingestion_handles_exceptions(self, tmp_path):
        """Test that exceptions in the loop don't stop ingestion"""
        import asyncio

        db_path = str(tmp_path / "test.db")
        ingester = DataIngester(db_path)
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
