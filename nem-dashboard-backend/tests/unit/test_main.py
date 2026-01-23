"""
Unit tests for main.py FastAPI application.

Tests cover lifespan context manager and endpoint error handling paths.
"""
import pytest
import os
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
import httpx

from app.main import app


class TestLifespanContextManager:
    """Tests for the lifespan context manager."""

    @pytest.mark.asyncio
    async def test_lifespan_skips_init_when_db_already_set(self):
        """Test that lifespan skips initialization when db is already set (test mode)."""
        from app.main import lifespan
        import app.main as main_module

        # Pre-set db to simulate test mode
        mock_db = MagicMock()
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with lifespan(app):
                # Should have yielded without reinitializing
                # db should still be our mock
                assert main_module.db is mock_db
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_lifespan_raises_without_database_url(self):
        """Test that lifespan raises RuntimeError without DATABASE_URL."""
        from app.main import lifespan
        import app.main as main_module

        # Ensure db is None and DATABASE_URL is not set
        original_db = main_module.db
        main_module.db = None
        original_env = os.environ.pop('DATABASE_URL', None)

        try:
            with pytest.raises(RuntimeError) as exc_info:
                async with lifespan(app):
                    pass
            assert "DATABASE_URL" in str(exc_info.value)
        finally:
            main_module.db = original_db
            if original_env:
                os.environ['DATABASE_URL'] = original_env

    @pytest.mark.asyncio
    async def test_lifespan_full_startup_shutdown(self):
        """Test full lifespan startup and shutdown sequence."""
        from app.main import lifespan
        import app.main as main_module

        # Ensure db is None to trigger full initialization
        original_db = main_module.db
        original_ingester = main_module.data_ingester
        original_task = main_module.background_task
        main_module.db = None
        main_module.data_ingester = None
        main_module.background_task = None

        with patch.dict(os.environ, {'DATABASE_URL': 'postgresql://test:test@localhost/test'}):
            with patch('app.main.DataIngester') as MockIngester:
                mock_ingester = MagicMock()
                mock_ingester.initialize = AsyncMock()
                mock_ingester.db = MagicMock()
                mock_ingester.run_continuous_ingestion = AsyncMock()
                mock_ingester.stop_continuous_ingestion = MagicMock()
                mock_ingester.cleanup = AsyncMock()
                mock_ingester.db.close = AsyncMock()
                MockIngester.return_value = mock_ingester

                with patch('app.main.import_generator_info_from_csv', new_callable=AsyncMock) as mock_import:
                    try:
                        async with lifespan(app):
                            # During lifespan, db should be set
                            assert main_module.db is not None
                            mock_ingester.initialize.assert_called_once()
                            mock_import.assert_called_once()

                        # After exiting lifespan, cleanup should have been called
                        mock_ingester.stop_continuous_ingestion.assert_called_once()
                        mock_ingester.cleanup.assert_called_once()
                    finally:
                        main_module.db = original_db
                        main_module.data_ingester = original_ingester
                        main_module.background_task = original_task


class TestEndpointErrorHandling:
    """Tests for endpoint error handling branches."""

    @pytest.mark.asyncio
    async def test_get_latest_dispatch_data_exception(self, async_client):
        """Test error handling in get_latest_dispatch_data."""
        import app.main as main_module

        original_db = main_module.db
        mock_db = MagicMock()
        mock_db.get_latest_dispatch_data = AsyncMock(side_effect=Exception("DB error"))
        main_module.db = mock_db

        try:
            response = await async_client.get("/api/dispatch/latest")
            assert response.status_code == 500
            assert "DB error" in response.json()["detail"]
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_get_dispatch_range_exception(self, async_client):
        """Test error handling in get_dispatch_data_by_range."""
        import app.main as main_module

        original_db = main_module.db
        mock_db = MagicMock()
        mock_db.get_dispatch_data_by_date_range = AsyncMock(side_effect=Exception("Range query error"))
        main_module.db = mock_db

        try:
            response = await async_client.get(
                "/api/dispatch/range?start_date=2025-01-15T00:00:00&end_date=2025-01-16T00:00:00"
            )
            assert response.status_code == 500
            assert "Range query error" in response.json()["detail"]
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_get_generation_by_fuel_exception(self, async_client):
        """Test error handling in get_generation_by_fuel_type."""
        import app.main as main_module

        original_db = main_module.db
        mock_db = MagicMock()
        mock_db.get_generation_by_fuel_type = AsyncMock(side_effect=Exception("Fuel query error"))
        main_module.db = mock_db

        try:
            response = await async_client.get(
                "/api/generation/by-fuel?start_date=2025-01-15T00:00:00&end_date=2025-01-16T00:00:00"
            )
            assert response.status_code == 500
            assert "Fuel query error" in response.json()["detail"]
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_get_unique_duids_exception(self, async_client):
        """Test error handling in get_unique_duids."""
        import app.main as main_module

        original_db = main_module.db
        mock_db = MagicMock()
        mock_db.get_unique_duids = AsyncMock(side_effect=Exception("DUIDs query error"))
        main_module.db = mock_db

        try:
            response = await async_client.get("/api/duids")
            assert response.status_code == 500
            assert "DUIDs query error" in response.json()["detail"]
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_get_data_summary_exception(self, async_client):
        """Test error handling in get_data_summary."""
        import app.main as main_module

        original_db = main_module.db
        mock_db = MagicMock()
        mock_db.get_data_summary = AsyncMock(side_effect=Exception("Summary query error"))
        main_module.db = mock_db

        try:
            response = await async_client.get("/api/summary")
            assert response.status_code == 500
            assert "Summary query error" in response.json()["detail"]
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_get_latest_prices_exception(self, async_client):
        """Test error handling in get_latest_prices."""
        import app.main as main_module

        original_db = main_module.db
        mock_db = MagicMock()
        mock_db.get_latest_prices = AsyncMock(side_effect=Exception("Price query error"))
        main_module.db = mock_db

        try:
            response = await async_client.get("/api/prices/latest")
            assert response.status_code == 500
            assert "Price query error" in response.json()["detail"]
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_get_price_history_exception(self, async_client):
        """Test error handling in get_price_history."""
        import app.main as main_module

        original_db = main_module.db
        mock_db = MagicMock()
        mock_db.get_price_history = AsyncMock(side_effect=Exception("Price history error"))
        main_module.db = mock_db

        try:
            response = await async_client.get(
                "/api/prices/history?start_date=2025-01-15T00:00:00&end_date=2025-01-16T00:00:00"
            )
            assert response.status_code == 500
            assert "Price history error" in response.json()["detail"]
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_get_generators_filter_exception(self, async_client):
        """Test error handling in get_generators_by_region_fuel."""
        import app.main as main_module

        original_db = main_module.db
        mock_db = MagicMock()
        mock_db.get_generators_by_region_fuel = AsyncMock(side_effect=Exception("Generators query error"))
        main_module.db = mock_db

        try:
            response = await async_client.get("/api/generators/filter?region=NSW")
            assert response.status_code == 500
            assert "Generators query error" in response.json()["detail"]
        finally:
            main_module.db = original_db


class TestTriggerIngestionEndpoints:
    """Tests for manual ingestion trigger endpoints."""

    @pytest.mark.asyncio
    async def test_trigger_current_ingestion_failure(self):
        """Test when ingest_current_data returns False."""
        import app.main as main_module

        mock_ingester = MagicMock()
        mock_ingester.ingest_current_data = AsyncMock(return_value=False)
        original_ingester = main_module.data_ingester
        main_module.data_ingester = mock_ingester

        # Also need to set db to avoid NoneType errors
        mock_db = MagicMock()
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.post("/api/ingest/current")
                assert response.status_code == 500
                assert "Failed to ingest" in response.json()["detail"]
        finally:
            main_module.data_ingester = original_ingester
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_trigger_current_ingestion_exception(self):
        """Test when ingest_current_data raises an exception."""
        import app.main as main_module

        mock_ingester = MagicMock()
        mock_ingester.ingest_current_data = AsyncMock(side_effect=Exception("Ingestion error"))
        original_ingester = main_module.data_ingester
        main_module.data_ingester = mock_ingester

        mock_db = MagicMock()
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.post("/api/ingest/current")
                assert response.status_code == 500
                assert "Ingestion error" in response.json()["detail"]
        finally:
            main_module.data_ingester = original_ingester
            main_module.db = original_db


class TestHistoricalIngestionEndpoints:
    """Tests for historical ingestion trigger endpoints."""

    @pytest.mark.asyncio
    async def test_trigger_historical_ingestion_success(self):
        """Test successful historical ingestion trigger."""
        import app.main as main_module

        mock_ingester = MagicMock()
        mock_ingester.ingest_historical_data = AsyncMock()
        original_ingester = main_module.data_ingester
        main_module.data_ingester = mock_ingester

        mock_db = MagicMock()
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.post(
                    "/api/ingest/historical?start_date=2025-01-01T00:00:00"
                )
                assert response.status_code == 200
                assert "Historical data ingestion started" in response.json()["message"]
        finally:
            main_module.data_ingester = original_ingester
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_trigger_historical_ingestion_exception(self):
        """Test historical ingestion trigger when exception occurs."""
        import app.main as main_module

        # Create a mock BackgroundTasks that raises an exception
        original_ingester = main_module.data_ingester
        main_module.data_ingester = None  # This will cause AttributeError

        mock_db = MagicMock()
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.post(
                    "/api/ingest/historical?start_date=2025-01-01T00:00:00"
                )
                assert response.status_code == 500
        finally:
            main_module.data_ingester = original_ingester
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_trigger_historical_price_ingestion_success(self):
        """Test successful historical price ingestion trigger."""
        import app.main as main_module

        mock_ingester = MagicMock()
        mock_ingester.ingest_historical_prices = AsyncMock()
        original_ingester = main_module.data_ingester
        main_module.data_ingester = mock_ingester

        mock_db = MagicMock()
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.post(
                    "/api/ingest/historical-prices?start_date=2025-01-01T00:00:00&end_date=2025-01-02T00:00:00"
                )
                assert response.status_code == 200
                assert "Historical price data ingestion started" in response.json()["message"]
        finally:
            main_module.data_ingester = original_ingester
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_trigger_historical_price_ingestion_exception(self):
        """Test historical price ingestion trigger when exception occurs."""
        import app.main as main_module

        original_ingester = main_module.data_ingester
        main_module.data_ingester = None  # This will cause AttributeError

        mock_db = MagicMock()
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.post(
                    "/api/ingest/historical-prices?start_date=2025-01-01T00:00:00"
                )
                assert response.status_code == 500
        finally:
            main_module.data_ingester = original_ingester
            main_module.db = original_db


class TestDataCoverageEndpoint:
    """Tests for data coverage endpoint."""

    @pytest.mark.asyncio
    async def test_get_data_coverage_invalid_table(self):
        """Test get_data_coverage with invalid table name."""
        import app.main as main_module

        mock_db = MagicMock()
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/data/coverage?table=invalid_table")
                assert response.status_code == 400
                assert "Invalid table" in response.json()["detail"]
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_get_data_coverage_exception(self):
        """Test get_data_coverage exception handling."""
        import app.main as main_module

        original_db = main_module.db
        mock_db = MagicMock()
        mock_db.get_data_coverage = AsyncMock(side_effect=Exception("Coverage query error"))
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/data/coverage?table=price_data")
                assert response.status_code == 500
                assert "Coverage query error" in response.json()["detail"]
        finally:
            main_module.db = original_db


class TestDatabaseHealthEndpoint:
    """Tests for database health endpoint."""

    @pytest.mark.asyncio
    async def test_get_database_health_exception(self):
        """Test get_database_health exception handling."""
        import app.main as main_module

        original_db = main_module.db
        mock_db = MagicMock()
        mock_db.get_database_health = AsyncMock(side_effect=Exception("Health check error"))
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/database/health")
                assert response.status_code == 500
                assert "Health check error" in response.json()["detail"]
        finally:
            main_module.db = original_db


class TestRegionEndpointExceptions:
    """Tests for region-specific endpoint error handling."""

    @pytest.mark.asyncio
    async def test_get_region_current_generation_exception(self, async_client):
        """Test error handling in get_region_current_generation."""
        import app.main as main_module

        original_db = main_module.db
        mock_db = MagicMock()
        mock_db.get_region_fuel_mix = AsyncMock(side_effect=Exception("Fuel mix error"))
        main_module.db = mock_db

        try:
            response = await async_client.get("/api/region/NSW/generation/current")
            assert response.status_code == 500
            assert "Fuel mix error" in response.json()["detail"]
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_get_region_generation_history_exception(self, async_client):
        """Test error handling in get_region_generation_history."""
        import app.main as main_module

        original_db = main_module.db
        mock_db = MagicMock()
        mock_db.get_region_generation_history = AsyncMock(side_effect=Exception("Gen history error"))
        main_module.db = mock_db

        try:
            response = await async_client.get("/api/region/NSW/generation/history")
            assert response.status_code == 500
            assert "Gen history error" in response.json()["detail"]
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_get_region_price_history_exception(self, async_client):
        """Test error handling in get_region_price_history."""
        import app.main as main_module

        original_db = main_module.db
        mock_db = MagicMock()
        mock_db.get_region_price_history = AsyncMock(side_effect=Exception("Price history error"))
        main_module.db = mock_db

        try:
            response = await async_client.get("/api/region/NSW/prices/history")
            assert response.status_code == 500
            assert "Price history error" in response.json()["detail"]
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_get_region_summary_exception(self, async_client):
        """Test error handling in get_region_summary."""
        import app.main as main_module

        original_db = main_module.db
        mock_db = MagicMock()
        mock_db.get_region_summary = AsyncMock(side_effect=Exception("Summary error"))
        main_module.db = mock_db

        try:
            response = await async_client.get("/api/region/NSW/summary")
            assert response.status_code == 500
            assert "Summary error" in response.json()["detail"]
        finally:
            main_module.db = original_db
