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


class TestRootEndpoint:
    """Tests for root and health endpoints."""

    @pytest.mark.asyncio
    async def test_root_endpoint(self):
        """Test the root endpoint returns API info."""
        import app.main as main_module

        mock_db = MagicMock()
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/")
                assert response.status_code == 200
                assert "NEM Dispatch Data API" in response.json()["message"]
        finally:
            main_module.db = original_db


class TestRegionValidation:
    """Tests for region validation in endpoints."""

    @pytest.mark.asyncio
    async def test_get_region_fuel_mix_invalid_region(self):
        """Test invalid region returns 400 error."""
        import app.main as main_module

        mock_db = MagicMock()
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/region/INVALID/generation/current")
                assert response.status_code == 400
                assert "Invalid region" in response.json()["detail"]
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_get_region_price_history_invalid_region(self):
        """Test invalid region in price history returns 400 error."""
        import app.main as main_module

        mock_db = MagicMock()
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/region/INVALID/prices/history")
                assert response.status_code == 400
                assert "Invalid region" in response.json()["detail"]
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_get_region_price_history_invalid_price_type(self):
        """Test invalid price_type returns 400 error."""
        import app.main as main_module

        mock_db = MagicMock()
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/region/NSW/prices/history?price_type=INVALID")
                assert response.status_code == 400
                assert "Invalid price_type" in response.json()["detail"]
        finally:
            main_module.db = original_db


class TestEmptyDataResponses:
    """Tests for endpoints that return empty data."""

    @pytest.mark.asyncio
    async def test_get_latest_dispatch_empty(self):
        """Test empty dispatch data response."""
        import app.main as main_module
        import pandas as pd

        mock_db = MagicMock()
        mock_db.get_latest_dispatch_data = AsyncMock(return_value=pd.DataFrame())
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/dispatch/latest")
                assert response.status_code == 200
                assert response.json()["count"] == 0
                assert "No data available" in response.json()["message"]
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_get_generation_by_fuel_empty(self):
        """Test empty generation by fuel response."""
        import app.main as main_module
        import pandas as pd

        mock_db = MagicMock()
        mock_db.get_generation_by_fuel_type = AsyncMock(return_value=pd.DataFrame())
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get(
                    "/api/generation/by-fuel?start_date=2025-01-15T00:00:00&end_date=2025-01-16T00:00:00"
                )
                assert response.status_code == 200
                assert response.json()["count"] == 0
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_get_price_history_empty(self):
        """Test empty price history response."""
        import app.main as main_module
        import pandas as pd

        mock_db = MagicMock()
        mock_db.get_price_history = AsyncMock(return_value=pd.DataFrame())
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get(
                    "/api/prices/history?start_date=2025-01-15T00:00:00&end_date=2025-01-16T00:00:00"
                )
                assert response.status_code == 200
                assert response.json()["count"] == 0
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_get_generators_filter_empty(self):
        """Test empty generators filter response."""
        import app.main as main_module
        import pandas as pd

        mock_db = MagicMock()
        mock_db.get_generators_by_region_fuel = AsyncMock(return_value=pd.DataFrame())
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/generators/filter?region=NSW")
                assert response.status_code == 200
                assert response.json()["count"] == 0
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_get_region_fuel_mix_empty(self):
        """Test empty region fuel mix response."""
        import app.main as main_module
        import pandas as pd

        mock_db = MagicMock()
        mock_db.get_region_fuel_mix = AsyncMock(return_value=pd.DataFrame())
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/region/NSW/generation/current")
                assert response.status_code == 200
                assert response.json()["total_generation"] == 0
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_get_region_generation_history_empty(self):
        """Test empty region generation history response."""
        import app.main as main_module
        import pandas as pd

        mock_db = MagicMock()
        mock_db.get_region_generation_history = AsyncMock(return_value=pd.DataFrame())
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/region/NSW/generation/history")
                assert response.status_code == 200
                assert response.json()["count"] == 0
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_get_region_price_history_empty(self):
        """Test empty region price history response."""
        import app.main as main_module
        import pandas as pd

        mock_db = MagicMock()
        # Mock both methods since either could be called depending on parameters
        mock_db.get_region_price_history = AsyncMock(return_value=pd.DataFrame())
        mock_db.get_aggregated_price_history = AsyncMock(return_value=pd.DataFrame())
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/region/NSW/prices/history")
                assert response.status_code == 200
                assert response.json()["count"] == 0
        finally:
            main_module.db = original_db


class TestSuccessfulIngestion:
    """Tests for successful ingestion paths."""

    @pytest.mark.asyncio
    async def test_trigger_current_ingestion_success(self):
        """Test successful current data ingestion."""
        import app.main as main_module

        mock_ingester = MagicMock()
        mock_ingester.ingest_current_data = AsyncMock(return_value=True)
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
                assert response.status_code == 200
                assert "successfully" in response.json()["message"]
        finally:
            main_module.data_ingester = original_ingester
            main_module.db = original_db


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


class TestPASAEndpoints:
    """Tests for PASA (Projected Assessment of System Adequacy) endpoints."""

    @pytest.mark.asyncio
    async def test_get_pdpasa_valid_region(self):
        """Test PDPASA endpoint with valid region."""
        import app.main as main_module
        from datetime import datetime

        mock_db = MagicMock()
        mock_db.get_latest_pdpasa = AsyncMock(return_value=[{
            'run_datetime': datetime(2025, 1, 15, 10, 0),
            'interval_datetime': datetime(2025, 1, 15, 10, 30),
            'regionid': 'NSW1',
            'demand50': 7500.0,
            'lorcondition': 0,
            'surplusreserve': 1500.0
        }])
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/pasa/pdpasa/NSW1")
                assert response.status_code == 200
                data = response.json()
                assert data["count"] == 1
                assert data["region"] == "NSW1"
                assert len(data["data"]) == 1
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_get_pdpasa_region_normalization(self):
        """Test PDPASA endpoint normalizes region (NSW -> NSW1)."""
        import app.main as main_module

        mock_db = MagicMock()
        mock_db.get_latest_pdpasa = AsyncMock(return_value=[])
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/pasa/pdpasa/NSW")
                assert response.status_code == 200
                # Check that the region was normalized to NSW1
                assert response.json()["region"] == "NSW1"
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_get_pdpasa_invalid_region(self):
        """Test PDPASA endpoint with invalid region."""
        import app.main as main_module

        mock_db = MagicMock()
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/pasa/pdpasa/INVALID")
                assert response.status_code == 400
                assert "Invalid region" in response.json()["detail"]
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_get_pdpasa_empty_data(self):
        """Test PDPASA endpoint with no data available."""
        import app.main as main_module

        mock_db = MagicMock()
        mock_db.get_latest_pdpasa = AsyncMock(return_value=[])
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/pasa/pdpasa/VIC1")
                assert response.status_code == 200
                data = response.json()
                assert data["count"] == 0
                assert data["data"] == []
                assert "No PDPASA data" in data["message"]
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_get_pdpasa_exception(self):
        """Test PDPASA endpoint error handling."""
        import app.main as main_module

        mock_db = MagicMock()
        mock_db.get_latest_pdpasa = AsyncMock(side_effect=Exception("PDPASA error"))
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/pasa/pdpasa/NSW1")
                assert response.status_code == 500
                assert "PDPASA error" in response.json()["detail"]
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_get_stpasa_valid_region(self):
        """Test STPASA endpoint with valid region."""
        import app.main as main_module
        from datetime import datetime

        mock_db = MagicMock()
        mock_db.get_latest_stpasa = AsyncMock(return_value=[{
            'run_datetime': datetime(2025, 1, 15, 6, 0),
            'interval_datetime': datetime(2025, 1, 16, 0, 0),
            'regionid': 'QLD1',
            'demand50': 6500.0,
            'lorcondition': 0,
            'surplusreserve': 900.0
        }])
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/pasa/stpasa/QLD1")
                assert response.status_code == 200
                data = response.json()
                assert data["count"] == 1
                assert data["region"] == "QLD1"
                assert len(data["data"]) == 1
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_get_stpasa_region_normalization(self):
        """Test STPASA endpoint normalizes region (VIC -> VIC1)."""
        import app.main as main_module

        mock_db = MagicMock()
        mock_db.get_latest_stpasa = AsyncMock(return_value=[])
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/pasa/stpasa/VIC")
                assert response.status_code == 200
                # Check that the region was normalized to VIC1
                assert response.json()["region"] == "VIC1"
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_get_stpasa_invalid_region(self):
        """Test STPASA endpoint with invalid region."""
        import app.main as main_module

        mock_db = MagicMock()
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/pasa/stpasa/INVALID")
                assert response.status_code == 400
                assert "Invalid region" in response.json()["detail"]
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_get_stpasa_empty_data(self):
        """Test STPASA endpoint with no data available."""
        import app.main as main_module

        mock_db = MagicMock()
        mock_db.get_latest_stpasa = AsyncMock(return_value=[])
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/pasa/stpasa/SA1")
                assert response.status_code == 200
                data = response.json()
                assert data["count"] == 0
                assert data["data"] == []
                assert "No STPASA data" in data["message"]
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_get_stpasa_exception(self):
        """Test STPASA endpoint error handling."""
        import app.main as main_module

        mock_db = MagicMock()
        mock_db.get_latest_stpasa = AsyncMock(side_effect=Exception("STPASA error"))
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/pasa/stpasa/TAS1")
                assert response.status_code == 500
                assert "STPASA error" in response.json()["detail"]
        finally:
            main_module.db = original_db


class TestExportEndpoints:
    """Tests for CSV export endpoints."""

    @pytest.mark.asyncio
    async def test_get_export_options_success(self):
        """Test export options endpoint returns fuel sources and data ranges."""
        import app.main as main_module

        mock_db = MagicMock()
        mock_db.get_unique_fuel_sources = AsyncMock(return_value=['Coal', 'Gas', 'Wind', 'Solar'])
        mock_db.get_export_data_ranges = AsyncMock(return_value={
            'prices': {'earliest_date': '2025-01-01', 'latest_date': '2025-01-15'},
            'generation': {'earliest_date': '2025-01-01', 'latest_date': '2025-01-15'},
            'pdpasa': {'earliest_date': None, 'latest_date': None},
            'stpasa': {'earliest_date': None, 'latest_date': None}
        })
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/export/available-options")
                assert response.status_code == 200
                data = response.json()
                assert 'regions' in data
                assert 'fuel_sources' in data
                assert 'pasa_types' in data
                assert 'data_ranges' in data
                assert data['fuel_sources'] == ['Coal', 'Gas', 'Wind', 'Solar']
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_get_export_options_exception(self):
        """Test export options endpoint error handling."""
        import app.main as main_module

        mock_db = MagicMock()
        mock_db.get_unique_fuel_sources = AsyncMock(side_effect=Exception("Export options error"))
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/export/available-options")
                assert response.status_code == 500
                assert "Export options error" in response.json()["detail"]
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_export_prices_csv_success(self):
        """Test price export returns CSV file."""
        import app.main as main_module
        import pandas as pd

        mock_db = MagicMock()
        mock_db.export_price_data = AsyncMock(return_value=pd.DataFrame([
            {'settlementdate': '2025-01-15 10:00:00', 'region': 'NSW', 'price': 85.50,
             'totaldemand': 7500.0, 'price_type': 'DISPATCH'}
        ]))
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get(
                    "/api/export/prices?start_date=2025-01-15T00:00:00&end_date=2025-01-15T23:59:59"
                )
                assert response.status_code == 200
                assert response.headers['content-type'] == 'text/csv; charset=utf-8'
                assert 'content-disposition' in response.headers
                assert 'attachment' in response.headers['content-disposition']
                # Verify CSV content
                content = response.text
                assert 'settlementdate' in content
                assert 'region' in content
                assert 'NSW' in content
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_export_prices_csv_with_regions(self):
        """Test price export with region filter."""
        import app.main as main_module
        import pandas as pd

        mock_db = MagicMock()
        mock_db.export_price_data = AsyncMock(return_value=pd.DataFrame([
            {'settlementdate': '2025-01-15 10:00:00', 'region': 'NSW', 'price': 85.50,
             'totaldemand': 7500.0, 'price_type': 'DISPATCH'}
        ]))
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get(
                    "/api/export/prices?start_date=2025-01-15T00:00:00&end_date=2025-01-15T23:59:59&regions=NSW,VIC"
                )
                assert response.status_code == 200
                # Verify region filter was passed
                mock_db.export_price_data.assert_called_once()
                call_args = mock_db.export_price_data.call_args
                assert call_args[0][2] == ['NSW', 'VIC']
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_export_prices_csv_exception(self):
        """Test price export error handling."""
        import app.main as main_module

        mock_db = MagicMock()
        mock_db.export_price_data = AsyncMock(side_effect=Exception("Price export error"))
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get(
                    "/api/export/prices?start_date=2025-01-15T00:00:00&end_date=2025-01-15T23:59:59"
                )
                assert response.status_code == 500
                assert "Price export error" in response.json()["detail"]
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_export_generation_csv_success(self):
        """Test generation export returns CSV file."""
        import app.main as main_module
        import pandas as pd

        mock_db = MagicMock()
        mock_db.export_generation_data = AsyncMock(return_value=pd.DataFrame([
            {'settlementdate': '2025-01-15 10:00:00', 'duid': 'GEN1', 'station_name': 'Test Station',
             'region': 'NSW', 'fuel_source': 'Coal', 'technology_type': 'Steam',
             'generation_mw': 400.0, 'totalcleared': 400.0, 'availability': 500.0}
        ]))
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get(
                    "/api/export/generation?start_date=2025-01-15T00:00:00&end_date=2025-01-15T23:59:59"
                )
                assert response.status_code == 200
                assert response.headers['content-type'] == 'text/csv; charset=utf-8'
                content = response.text
                assert 'generation_mw' in content
                assert 'fuel_source' in content
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_export_generation_csv_with_filters(self):
        """Test generation export with region and fuel source filters."""
        import app.main as main_module
        import pandas as pd

        mock_db = MagicMock()
        mock_db.export_generation_data = AsyncMock(return_value=pd.DataFrame())
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get(
                    "/api/export/generation?start_date=2025-01-15T00:00:00&end_date=2025-01-15T23:59:59&regions=NSW&fuel_sources=Coal,Gas"
                )
                assert response.status_code == 200
                # Verify filters were passed
                mock_db.export_generation_data.assert_called_once()
                call_args = mock_db.export_generation_data.call_args
                assert call_args[0][2] == ['NSW']
                assert call_args[0][3] == ['Coal', 'Gas']
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_export_generation_csv_exception(self):
        """Test generation export error handling."""
        import app.main as main_module

        mock_db = MagicMock()
        mock_db.export_generation_data = AsyncMock(side_effect=Exception("Generation export error"))
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get(
                    "/api/export/generation?start_date=2025-01-15T00:00:00&end_date=2025-01-15T23:59:59"
                )
                assert response.status_code == 500
                assert "Generation export error" in response.json()["detail"]
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_export_pasa_csv_pdpasa_success(self):
        """Test PDPASA export returns CSV file."""
        import app.main as main_module
        import pandas as pd

        mock_db = MagicMock()
        mock_db.export_latest_pasa_data = AsyncMock(return_value=pd.DataFrame([
            {'run_datetime': '2025-01-15 10:00:00', 'interval_datetime': '2025-01-15 10:30:00',
             'regionid': 'NSW1', 'demand50': 7500.0, 'lorcondition': 0}
        ]))
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/export/pasa?pasa_type=pdpasa")
                assert response.status_code == 200
                assert response.headers['content-type'] == 'text/csv; charset=utf-8'
                content = response.text
                assert 'run_datetime' in content
                assert 'interval_datetime' in content
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_export_pasa_csv_stpasa_success(self):
        """Test STPASA export returns CSV file."""
        import app.main as main_module
        import pandas as pd

        mock_db = MagicMock()
        mock_db.export_latest_pasa_data = AsyncMock(return_value=pd.DataFrame([
            {'run_datetime': '2025-01-15 06:00:00', 'interval_datetime': '2025-01-16 00:00:00',
             'regionid': 'VIC1', 'demand50': 5000.0, 'lorcondition': 0}
        ]))
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/export/pasa?pasa_type=stpasa")
                assert response.status_code == 200
                # Verify stpasa was requested
                mock_db.export_latest_pasa_data.assert_called_once()
                call_args = mock_db.export_latest_pasa_data.call_args
                assert call_args[0][0] == 'stpasa'
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_export_pasa_csv_with_regions(self):
        """Test PASA export with region filter."""
        import app.main as main_module
        import pandas as pd

        mock_db = MagicMock()
        mock_db.export_latest_pasa_data = AsyncMock(return_value=pd.DataFrame())
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/export/pasa?pasa_type=pdpasa&regions=NSW,VIC")
                assert response.status_code == 200
                # Verify regions filter was passed
                mock_db.export_latest_pasa_data.assert_called_once()
                call_args = mock_db.export_latest_pasa_data.call_args
                assert call_args[0][1] == ['NSW', 'VIC']
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_export_pasa_csv_invalid_type(self):
        """Test PASA export with invalid pasa_type returns 400."""
        import app.main as main_module

        mock_db = MagicMock()
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/export/pasa?pasa_type=invalid")
                assert response.status_code == 400
                assert "pasa_type must be" in response.json()["detail"]
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_export_pasa_csv_exception(self):
        """Test PASA export error handling."""
        import app.main as main_module

        mock_db = MagicMock()
        mock_db.export_latest_pasa_data = AsyncMock(side_effect=Exception("PASA export error"))
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/export/pasa?pasa_type=pdpasa")
                assert response.status_code == 500
                assert "PASA export error" in response.json()["detail"]
        finally:
            main_module.db = original_db


class TestBidBandEndpoints:
    """Tests for bid band related endpoints."""

    @pytest.mark.asyncio
    async def test_get_bid_bands_success(self):
        """Test successful bid bands retrieval."""
        import app.main as main_module

        mock_db = MagicMock()
        mock_db.get_bid_bands_for_duid = AsyncMock(return_value=[
            {
                'settlementdate': datetime(2026, 2, 21, 0, 5),
                'bandavail1': 100.0, 'bandavail2': 50.0, 'bandavail3': 200.0,
                'bandavail4': 0.0, 'bandavail5': 0.0, 'bandavail6': 0.0,
                'bandavail7': 0.0, 'bandavail8': 0.0, 'bandavail9': 0.0,
                'bandavail10': 0.0,
                'priceband1': -987.0, 'priceband2': 0.0, 'priceband3': 30.0,
                'priceband4': 50.0, 'priceband5': 100.0, 'priceband6': 300.0,
                'priceband7': 1000.0, 'priceband8': 5000.0, 'priceband9': 10000.0,
                'priceband10': 15000.0,
                'maxavail': 660.0, 'minimumload': 200.0,
            }
        ])
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/bids/BAYSW1?date=2026-02-21")
                assert response.status_code == 200
                data = response.json()
                assert data["duid"] == "BAYSW1"
                assert data["count"] == 1
                assert len(data["price_bands"]) == 10
                assert data["price_bands"][0] == -987.0
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_get_bid_bands_empty(self):
        """Test bid bands with no data."""
        import app.main as main_module

        mock_db = MagicMock()
        mock_db.get_bid_bands_for_duid = AsyncMock(return_value=[])
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/bids/NODATA1?date=2026-02-21")
                assert response.status_code == 200
                data = response.json()
                assert data["count"] == 0
                assert data["data"] == []
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_get_bid_bands_invalid_date(self):
        """Test bid bands with invalid date format."""
        import app.main as main_module

        mock_db = MagicMock()
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/bids/BAYSW1?date=not-a-date")
                assert response.status_code == 400
                assert "Invalid date format" in response.json()["detail"]
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_get_bid_bands_exception(self):
        """Test bid bands error handling."""
        import app.main as main_module

        mock_db = MagicMock()
        mock_db.get_bid_bands_for_duid = AsyncMock(side_effect=Exception("DB error"))
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/bids/BAYSW1?date=2026-02-21")
                assert response.status_code == 500
                assert "DB error" in response.json()["detail"]
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_search_duids_success(self):
        """Test successful DUID search."""
        import app.main as main_module

        mock_db = MagicMock()
        mock_db.search_duids = AsyncMock(return_value=[
            {'duid': 'BAYSW1', 'station_name': 'Bayswater', 'region': 'NSW1',
             'fuel_source': 'Black Coal', 'capacity_mw': 660.0},
            {'duid': 'BAYSW2', 'station_name': 'Bayswater', 'region': 'NSW1',
             'fuel_source': 'Black Coal', 'capacity_mw': 660.0},
        ])
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/duids/search?q=BAYSW")
                assert response.status_code == 200
                data = response.json()
                assert data["count"] == 2
                assert data["results"][0]["duid"] == "BAYSW1"
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_search_duids_exception(self):
        """Test DUID search error handling."""
        import app.main as main_module

        mock_db = MagicMock()
        mock_db.search_duids = AsyncMock(side_effect=Exception("Search error"))
        original_db = main_module.db
        main_module.db = mock_db

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.get("/api/duids/search?q=BAYSW")
                assert response.status_code == 500
                assert "Search error" in response.json()["detail"]
        finally:
            main_module.db = original_db

    @pytest.mark.asyncio
    async def test_trigger_bid_backfill(self):
        """Test backfill endpoint triggers background task."""
        import app.main as main_module

        mock_db = MagicMock()
        original_db = main_module.db
        original_ingester = main_module.data_ingester
        mock_ingester = MagicMock()
        main_module.db = mock_db
        main_module.data_ingester = mock_ingester

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.post(
                    "/api/ingest/backfill-bids?start_date=2026-02-20T00:00:00"
                )
                assert response.status_code == 200
                assert "backfill started" in response.json()["message"]
        finally:
            main_module.db = original_db
            main_module.data_ingester = original_ingester
