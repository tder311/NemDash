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

from app.data_ingester import (
    DataIngester,
    update_sample_generator_info,
    import_generator_info_from_csv,
    thin_pasa_for_multilead_backfill,
    SAMPLE_GENERATOR_INFO,
)
from app.forecaster import LEAD_BUCKETS


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


@pytest.mark.slow
class TestIngestCurrentData:
    """Tests for ingest_current_data method (SLOW - uses real database).

    Note: These tests are marked slow because they create real database connections.
    For fast CI runs, use TestIngestCurrentDataFast instead.
    """

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
        ingester.price_client.get_monthly_archive_prices = AsyncMock(return_value=price_df)

        # Backfill from a recent start date for testing
        start_date = datetime.now() - timedelta(days=2)
        total = await ingester.backfill_missing_data(start_date=start_date)
        assert isinstance(total, int)
        await ingester.db.close()


class TestThinPasaForMultileadBackfill:
    """Tests for thin_pasa_for_multilead_backfill (pure PASA backfill thinning)."""

    def _runs_frame(self):
        """One interval, one region, an hourly run every hour from 1h to 175h lead."""
        interval = pd.Timestamp("2026-07-08 19:00:00")
        leads = range(1, 176)
        return pd.DataFrame({
            "run_datetime": [interval - timedelta(hours=h) for h in leads],
            "interval_datetime": interval,
            "regionid": "NSW1",
            "demand50": [float(h) for h in leads],
        })

    def test_keeps_one_run_per_lead_bucket(self):
        """Many runs at many leads should thin to ~one row per LEAD_BUCKETS target, dropping the rest."""
        runs = self._runs_frame()
        out = thin_pasa_for_multilead_backfill(runs)

        assert len(out) == len(LEAD_BUCKETS)
        assert len(out) < len(runs)

    def test_drops_selection_metadata_columns(self):
        """lead_hours/lead_bucket are selection metadata, not table columns."""
        out = thin_pasa_for_multilead_backfill(self._runs_frame())

        assert "lead_hours" not in out.columns
        assert "lead_bucket" not in out.columns
        assert "demand50" in out.columns

    def test_single_run_keeps_only_one_row(self):
        """A single run can only serve one bucket after dedup."""
        interval = pd.Timestamp("2026-07-08 19:00:00")
        one_run = pd.DataFrame({
            "run_datetime": [interval - timedelta(hours=20)],
            "interval_datetime": interval,
            "regionid": "NSW1",
            "demand50": [1.0],
        })

        out = thin_pasa_for_multilead_backfill(one_run)

        assert len(out) == 1


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


@pytest.mark.slow
class TestRunContinuousIngestion:
    """Tests for run_continuous_ingestion method (SLOW - uses real database).

    Note: These tests are marked slow because they create real database connections.
    For fast CI runs, use TestRunContinuousIngestionFast instead.
    """

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


# ============================================================================
# Fast Test Classes (Using Mocked Dependencies)
# ============================================================================


class TestIngestCurrentDataFast:
    """Fast tests for ingest_current_data method using mocked dependencies.

    These tests run in milliseconds instead of minutes because they don't
    create real database connections.
    """

    @pytest.mark.asyncio
    async def test_ingest_current_data_success(self, mock_ingester, mock_db, mock_nem_client, mock_price_client):
        """Test successful current data ingestion"""
        # Setup mock return data
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

        mock_nem_client.get_all_current_dispatch_data.return_value = sample_df
        mock_price_client.get_all_current_dispatch_prices.return_value = price_df
        mock_price_client.get_all_current_trading_prices.return_value = price_df

        success = await mock_ingester.ingest_current_data()

        assert success is True
        mock_db.insert_dispatch_data.assert_called_once()
        mock_db.insert_price_data.assert_called()

    @pytest.mark.asyncio
    async def test_ingest_current_data_partial_failure(self, mock_ingester, mock_db, mock_nem_client, mock_price_client):
        """Test that partial failures don't stop ingestion"""
        # All return None (simulating API failures)
        mock_nem_client.get_all_current_dispatch_data.return_value = None
        mock_price_client.get_all_current_dispatch_prices.return_value = None
        mock_price_client.get_all_current_trading_prices.return_value = None

        success = await mock_ingester.ingest_current_data()

        assert isinstance(success, bool)
        # Should not have inserted anything
        mock_db.insert_dispatch_data.assert_not_called()

    @pytest.mark.asyncio
    async def test_ingest_current_data_exception_handling(self, mock_ingester, mock_nem_client):
        """Test that exceptions are caught and return False"""
        mock_nem_client.get_all_current_dispatch_data.side_effect = Exception("Network error")

        success = await mock_ingester.ingest_current_data()

        assert success is False


class TestRunContinuousIngestionFast:
    """Fast tests for run_continuous_ingestion method using mocked dependencies.

    These tests run in milliseconds instead of minutes because they don't
    create real database connections.
    """

    @pytest.mark.asyncio
    async def test_run_continuous_ingestion_can_be_stopped(self, mock_ingester, mock_db, mock_nem_client, mock_price_client):
        """Test that continuous ingestion can be stopped"""
        import asyncio

        # Mock all the data fetching to return empty/None quickly
        mock_nem_client.get_all_current_dispatch_data.return_value = None
        mock_price_client.get_all_current_dispatch_prices.return_value = None
        mock_price_client.get_all_current_trading_prices.return_value = None
        mock_price_client.get_daily_prices.return_value = None

        async def stop_after_delay():
            await asyncio.sleep(0.1)
            mock_ingester.stop_continuous_ingestion()

        # Run both concurrently - should complete very quickly
        await asyncio.gather(
            mock_ingester.run_continuous_ingestion(interval_minutes=0.01),
            stop_after_delay()
        )

        assert mock_ingester.is_running is False

    @pytest.mark.asyncio
    async def test_run_continuous_ingestion_handles_exceptions(self, mock_ingester, mock_db, mock_nem_client, mock_price_client):
        """Test that exceptions in the loop don't stop ingestion"""
        import asyncio

        call_count = 0

        async def mock_ingest():
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise Exception("Test error")
            return True

        # Setup mocks
        mock_nem_client.get_all_current_dispatch_data.return_value = None
        mock_price_client.get_all_current_dispatch_prices.return_value = None
        mock_price_client.get_all_current_trading_prices.return_value = None
        mock_price_client.get_daily_prices.return_value = None

        # Replace ingest_current_data with our mock
        mock_ingester.ingest_current_data = mock_ingest

        async def stop_after_calls():
            while call_count < 3:
                await asyncio.sleep(0.05)
            mock_ingester.stop_continuous_ingestion()

        await asyncio.gather(
            mock_ingester.run_continuous_ingestion(interval_minutes=0.001),
            stop_after_calls()
        )

        # Should have continued after the exception in the loop
        assert call_count >= 3

    @pytest.mark.asyncio
    async def test_run_continuous_ingestion_initializes_timestamps(self, mock_ingester, mock_db):
        """Test that continuous ingestion initializes timestamps from database"""
        import asyncio

        # Setup mock timestamps
        mock_db.get_latest_dispatch_timestamp.return_value = datetime(2025, 1, 15, 10, 0)
        mock_db.get_latest_price_timestamp.side_effect = [
            datetime(2025, 1, 15, 10, 0),  # DISPATCH
            datetime(2025, 1, 15, 10, 0),  # TRADING
        ]

        async def stop_quickly():
            await asyncio.sleep(0.05)
            mock_ingester.stop_continuous_ingestion()

        await asyncio.gather(
            mock_ingester.run_continuous_ingestion(interval_minutes=0.01),
            stop_quickly()
        )

        # Verify timestamps were fetched
        mock_db.get_latest_dispatch_timestamp.assert_called_once()
        assert mock_db.get_latest_price_timestamp.call_count == 2


class TestInferUnitGeneration:
    """Tests for DataIngester._infer_unit_generation (joint unit-inference live hook)."""

    def _ingester(self):
        return DataIngester("postgresql://mock:mock@localhost/test")

    def _con_df(self):
        run = pd.Timestamp("2026-07-09 10:00:00")
        ivl = pd.Timestamp("2026-07-09 10:30:00")
        return pd.DataFrame([
            {"run_datetime": run, "interval_datetime": ivl, "constraintid": "C1", "lhs": 30.0},
        ])

    def _terms(self):
        return pd.DataFrame([
            {"constraintid": "C1", "term_type": "duid", "term_id": "A", "factor": 1.0},
        ])

    @pytest.mark.asyncio
    async def test_solves_and_persists_good_weak_rows(self):
        ingester = self._ingester()
        ingester.db = MagicMock()
        ingester.db.insert_inferred_unit_generation = AsyncMock(return_value=1)
        mock_fetch_terms = AsyncMock(return_value=self._terms())

        with patch("app.data_ingester.fetch_terms", mock_fetch_terms), \
             patch("app.data_ingester.fetch_bounds", AsyncMock(return_value=pd.DataFrame(columns=["duid", "maxavail"]))):
            await ingester._infer_unit_generation(self._con_df(), None)

        ingester.db.insert_inferred_unit_generation.assert_called_once()
        solved = ingester.db.insert_inferred_unit_generation.call_args[0][0]
        assert solved.iloc[0]["duid"] == "A"
        assert solved.iloc[0]["mw_inferred"] == pytest.approx(30.0)

        # fetch_terms must be run-date aware: called with this run's run_datetime.
        mock_fetch_terms.assert_called_once()
        assert mock_fetch_terms.call_args[0][1] == pd.Timestamp("2026-07-09 10:00:00")

    @pytest.mark.asyncio
    async def test_empty_or_missing_constraint_frame_is_a_noop(self):
        ingester = self._ingester()
        ingester.db = MagicMock()
        ingester.db.insert_inferred_unit_generation = AsyncMock(return_value=0)

        await ingester._infer_unit_generation(pd.DataFrame(), None)
        await ingester._infer_unit_generation(None, None)

        ingester.db.insert_inferred_unit_generation.assert_not_called()

    @pytest.mark.asyncio
    async def test_solve_failure_is_caught_and_does_not_raise(self):
        ingester = self._ingester()
        ingester.db = MagicMock()
        ingester.db.insert_inferred_unit_generation = AsyncMock(return_value=0)

        with patch("app.data_ingester.fetch_terms", AsyncMock(side_effect=Exception("db down"))):
            await ingester._infer_unit_generation(self._con_df(), None)  # must not raise

        ingester.db.insert_inferred_unit_generation.assert_not_called()


class TestIngestPredispatchDataTriggersInference:
    """Tests that the PD7Day ingest cycle hooks into joint unit inference after the existing insert."""

    @pytest.mark.asyncio
    async def test_new_run_triggers_unit_inference_with_unfiltered_frames(self):
        ingester = DataIngester("postgresql://mock:mock@localhost/test")
        ingester.db = MagicMock()
        ingester.db.insert_predispatch_price = AsyncMock(return_value=5)
        ingester.db.insert_predispatch_interconnector = AsyncMock(return_value=2)
        ingester.db.insert_predispatch_constraint = AsyncMock(return_value=1)
        ingester.last_predispatch_run = None

        run = pd.Timestamp("2026-07-09 10:00:00")
        prices = pd.DataFrame([{"run_datetime": run, "interval_datetime": run, "regionid": "NSW1", "rrp": 80.0}])
        con_df = pd.DataFrame([{"run_datetime": run, "interval_datetime": run, "constraintid": "C1", "lhs": 1.0}])
        ic_df = pd.DataFrame([{"run_datetime": run, "interval_datetime": run, "interconnectorid": "IC1", "mwflow": 1.0}])
        ingester.predispatch_client = MagicMock()
        ingester.predispatch_client.get_latest_predispatch_all = AsyncMock(return_value={
            "prices": prices, "interconnector": ic_df, "constraint": con_df,
        })
        ingester._infer_unit_generation = AsyncMock()

        result = await ingester.ingest_predispatch_data()

        assert result is True
        ingester._infer_unit_generation.assert_called_once()
        called_con_df, called_ic_df = ingester._infer_unit_generation.call_args[0]
        assert called_con_df is con_df
        assert called_ic_df is ic_df

    @pytest.mark.asyncio
    async def test_already_ingested_run_skips_inference(self):
        ingester = DataIngester("postgresql://mock:mock@localhost/test")
        ingester.db = MagicMock()
        run = pd.Timestamp("2026-07-09 10:00:00")
        ingester.last_predispatch_run = run

        prices = pd.DataFrame([{"run_datetime": run, "interval_datetime": run, "regionid": "NSW1", "rrp": 80.0}])
        ingester.predispatch_client = MagicMock()
        ingester.predispatch_client.get_latest_predispatch_all = AsyncMock(return_value={
            "prices": prices, "interconnector": pd.DataFrame(), "constraint": pd.DataFrame(),
        })
        ingester._infer_unit_generation = AsyncMock()

        result = await ingester.ingest_predispatch_data()

        assert result is True
        ingester._infer_unit_generation.assert_not_called()
