"""
Integration tests for extended time range API endpoints.

Tests verify that endpoints accept extended hour values (up to 8760 for 365 days)
and return aggregation_minutes in responses.

Requires DATABASE_URL environment variable for PostgreSQL connection.
"""
import pytest
from fastapi.testclient import TestClient
from datetime import datetime, timedelta
import os
import asyncio

# Check for DATABASE_URL before importing app
_db_url = os.environ.get('DATABASE_URL')
if not _db_url:
    pytest.skip("DATABASE_URL environment variable not set", allow_module_level=True)

from app.main import app
from app.database import NEMDatabase, calculate_aggregation_minutes
import app.main as main_module
from tests.fixtures.extended_data import (
    generate_dispatch_data,
    generate_price_data,
    generate_generator_info,
)


def _setup_database_sync():
    """Setup database with extended test data"""
    import pandas as pd

    async def _setup():
        db = NEMDatabase(_db_url)
        await db.initialize()

        # Generate 7 days of test data (recent, relative to now)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)

        # Insert dispatch data
        dispatch_df = generate_dispatch_data(start_date, days=7, region='NSW')
        await db.insert_dispatch_data(dispatch_df)

        # Insert price data (PUBLIC and DISPATCH types)
        public_price_df = generate_price_data(
            start_date, days=7, region='NSW', price_type='PUBLIC'
        )
        await db.insert_price_data(public_price_df)

        dispatch_price_df = generate_price_data(
            start_date, days=7, region='NSW', price_type='DISPATCH'
        )
        await db.insert_price_data(dispatch_price_df)

        # Add generator info
        await db.update_generator_info(generate_generator_info('NSW'))

        return db

    return asyncio.get_event_loop().run_until_complete(_setup())


# Module-level setup
_db = None
_client = None


def setup_module(module):
    """Set up database once for all tests"""
    global _db, _client
    _db = _setup_database_sync()
    main_module.db = _db
    _client = TestClient(app, raise_server_exceptions=False)


def teardown_module(module):
    """Clean up after tests"""
    global _client, _db
    if _client:
        _client.close()
        _client = None
    if _db:
        asyncio.get_event_loop().run_until_complete(_db.close())
        _db = None


@pytest.fixture
def client():
    """Return the module-level test client"""
    return _client


class TestExtendedHoursValidation:
    """Tests for extended hours parameter validation"""

    def test_generation_history_accepts_720_hours(self, client):
        """30 days (720h) should be accepted"""
        response = client.get("/api/region/NSW/generation/history?hours=720")
        assert response.status_code == 200
        data = response.json()
        assert data["hours"] == 720

    def test_generation_history_accepts_2160_hours(self, client):
        """90 days (2160h) should be accepted"""
        response = client.get("/api/region/NSW/generation/history?hours=2160")
        assert response.status_code == 200
        data = response.json()
        assert data["hours"] == 2160

    def test_generation_history_accepts_8760_hours(self, client):
        """365 days (8760h) should be accepted"""
        response = client.get("/api/region/NSW/generation/history?hours=8760")
        assert response.status_code == 200
        data = response.json()
        assert data["hours"] == 8760

    def test_generation_history_rejects_over_8760(self, client):
        """Over 365 days should be rejected"""
        response = client.get("/api/region/NSW/generation/history?hours=9000")
        assert response.status_code == 422

    def test_price_history_accepts_720_hours(self, client):
        """30 days (720h) for prices should be accepted"""
        response = client.get("/api/region/NSW/prices/history?hours=720")
        assert response.status_code == 200
        data = response.json()
        assert data["hours"] == 720

    def test_price_history_accepts_8760_hours(self, client):
        """365 days (8760h) for prices should be accepted"""
        response = client.get("/api/region/NSW/prices/history?hours=8760")
        assert response.status_code == 200
        data = response.json()
        assert data["hours"] == 8760


class TestAggregationMinutesInResponse:
    """Tests for aggregation_minutes field in responses"""

    def test_generation_history_includes_aggregation_minutes(self, client):
        """Response should include aggregation_minutes field"""
        response = client.get("/api/region/NSW/generation/history?hours=720")
        assert response.status_code == 200
        data = response.json()
        assert "aggregation_minutes" in data

    def test_generation_history_auto_aggregation_30d(self, client):
        """30-day query should auto-aggregate to hourly (60 min)"""
        response = client.get("/api/region/NSW/generation/history?hours=720")
        data = response.json()
        assert data["aggregation_minutes"] == 60

    def test_generation_history_auto_aggregation_90d(self, client):
        """90-day query should auto-aggregate to daily (1440 min)"""
        response = client.get("/api/region/NSW/generation/history?hours=2160")
        data = response.json()
        assert data["aggregation_minutes"] == 1440

    def test_generation_history_auto_aggregation_365d(self, client):
        """365-day query should auto-aggregate to weekly (10080 min)"""
        response = client.get("/api/region/NSW/generation/history?hours=8760")
        data = response.json()
        assert data["aggregation_minutes"] == 10080

    def test_price_history_includes_aggregation_minutes(self, client):
        """Extended price response should include aggregation_minutes"""
        response = client.get("/api/region/NSW/prices/history?hours=720")
        assert response.status_code == 200
        data = response.json()
        assert "aggregation_minutes" in data

    def test_price_history_auto_aggregation_30d(self, client):
        """30-day price query should auto-aggregate to hourly"""
        response = client.get("/api/region/NSW/prices/history?hours=720")
        data = response.json()
        assert data["aggregation_minutes"] == 60


class TestExplicitAggregationParameter:
    """Tests for explicit aggregation parameter override"""

    def test_generation_history_explicit_aggregation(self, client):
        """Explicit aggregation should override auto-calculation"""
        response = client.get("/api/region/NSW/generation/history?hours=720&aggregation=30")
        assert response.status_code == 200
        data = response.json()
        assert data["aggregation_minutes"] == 30

    def test_aggregation_validation_min(self, client):
        """Aggregation below 5 minutes should be rejected"""
        response = client.get("/api/region/NSW/generation/history?hours=24&aggregation=3")
        assert response.status_code == 422

    def test_aggregation_validation_max(self, client):
        """Aggregation above 10080 minutes should be rejected"""
        response = client.get("/api/region/NSW/generation/history?hours=24&aggregation=20000")
        assert response.status_code == 422


class TestDataPointReduction:
    """Tests to verify aggregation reduces data point counts"""

    def test_30d_query_reasonable_data_points(self, client):
        """30-day query should return reasonable number of data points"""
        response = client.get("/api/region/NSW/generation/history?hours=720")
        data = response.json()

        if data["count"] > 0:
            # With hourly aggregation for 7 days of test data
            # Should have <= 168 hours * num_fuel_types
            # Our test data has 4 fuel types, so max ~672 records
            assert data["count"] <= 1000

    def test_90d_query_fewer_data_points(self, client):
        """90-day query should have fewer points than 30-day (higher aggregation)"""
        response_30d = client.get("/api/region/NSW/generation/history?hours=720")
        response_90d = client.get("/api/region/NSW/generation/history?hours=2160")

        data_30d = response_30d.json()
        data_90d = response_90d.json()

        if data_30d["count"] > 0 and data_90d["count"] > 0:
            # 90-day should have same or fewer points (daily vs hourly)
            # Both are looking at same 7-day test data
            assert data_90d["count"] <= data_30d["count"]


class TestExtendedRangeResponseFormat:
    """Tests for response format with extended ranges"""

    def test_generation_history_response_structure(self, client):
        """Extended generation response should have correct structure"""
        response = client.get("/api/region/NSW/generation/history?hours=720")
        assert response.status_code == 200
        data = response.json()

        assert "region" in data
        assert "data" in data
        assert "count" in data
        assert "hours" in data
        assert "aggregation_minutes" in data
        assert "message" in data

    def test_price_history_response_structure(self, client):
        """Extended price response should have correct structure"""
        response = client.get("/api/region/NSW/prices/history?hours=720")
        assert response.status_code == 200
        data = response.json()

        assert "region" in data
        assert "data" in data
        assert "count" in data
        assert "hours" in data
        assert "price_type" in data
        assert "aggregation_minutes" in data
        assert "message" in data

    def test_generation_data_records_have_expected_fields(self, client):
        """Generation history records should have expected fields"""
        response = client.get("/api/region/NSW/generation/history?hours=168")
        data = response.json()

        if data["count"] > 0:
            record = data["data"][0]
            assert "period" in record
            assert "fuel_source" in record
            assert "generation_mw" in record
            assert "sample_count" in record

    def test_price_data_records_have_expected_fields(self, client):
        """Price history records should have expected fields"""
        response = client.get("/api/region/NSW/prices/history?hours=720")
        data = response.json()

        if data["count"] > 0:
            record = data["data"][0]
            assert "settlementdate" in record
            assert "price" in record
            assert "totaldemand" in record


class TestAllRegionsExtendedSupport:
    """Tests that all regions support extended time ranges"""

    @pytest.mark.parametrize("region", ["NSW", "VIC", "QLD", "SA", "TAS"])
    def test_all_regions_accept_720_hours(self, client, region):
        """All regions should accept 30-day queries"""
        response = client.get(f"/api/region/{region}/generation/history?hours=720")
        assert response.status_code == 200
        data = response.json()
        assert data["region"] == region

    @pytest.mark.parametrize("region", ["NSW", "VIC", "QLD", "SA", "TAS"])
    def test_all_regions_price_history_720(self, client, region):
        """All regions should accept 30-day price queries"""
        response = client.get(f"/api/region/{region}/prices/history?hours=720")
        assert response.status_code == 200


class TestBackwardsCompatibility:
    """Tests to ensure existing short-range queries still work"""

    def test_existing_7d_query_still_works(self, client):
        """Existing 7-day queries should continue to work"""
        response = client.get("/api/region/NSW/generation/history?hours=168")
        assert response.status_code == 200
        data = response.json()
        assert data["hours"] == 168
        # Should still use 30-min aggregation for 7d
        assert data["aggregation_minutes"] == 30

    def test_existing_24h_query_still_works(self, client):
        """Existing 24-hour queries should work with raw data"""
        response = client.get("/api/region/NSW/generation/history?hours=24")
        assert response.status_code == 200
        data = response.json()
        assert data["hours"] == 24
        # Should use 5-min (raw) aggregation for 24h
        assert data["aggregation_minutes"] == 5

    def test_existing_price_history_works(self, client):
        """Existing price history endpoints should work unchanged"""
        response = client.get("/api/region/NSW/prices/history?hours=24")
        assert response.status_code == 200
        data = response.json()
        assert data["hours"] == 24
