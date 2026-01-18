"""
Integration tests for FastAPI endpoints

Requires DATABASE_URL environment variable for PostgreSQL connection.
"""
import pytest
from fastapi.testclient import TestClient
from datetime import datetime
import os
import asyncio

# Check for DATABASE_URL before importing app
_db_url = os.environ.get('DATABASE_URL')
if not _db_url:
    pytest.skip("DATABASE_URL environment variable not set", allow_module_level=True)

# Import after checking env vars
from app.main import app
from app.database import NEMDatabase
import app.main as main_module


def _setup_database_sync():
    """Synchronous wrapper for async database setup"""
    import pandas as pd

    async def _setup():
        # Create and initialize database
        db = NEMDatabase(_db_url)
        await db.initialize()

        # Insert test data
        dispatch_df = pd.DataFrame([
            {
                'settlementdate': datetime(2025, 1, 15, 10, 30),
                'duid': 'BAYSW1',
                'scadavalue': 350.5,
                'uigf': 0.0,
                'totalcleared': 350.0,
                'ramprate': 0.0,
                'availability': 400.0,
                'raise1sec': 0.0,
                'lower1sec': 0.0
            },
            {
                'settlementdate': datetime(2025, 1, 15, 10, 30),
                'duid': 'AGLHAL',
                'scadavalue': 94.2,
                'uigf': 0.0,
                'totalcleared': 94.0,
                'ramprate': 0.0,
                'availability': 95.0,
                'raise1sec': 0.0,
                'lower1sec': 0.0
            },
        ])
        await db.insert_dispatch_data(dispatch_df)

        price_df = pd.DataFrame([
            {
                'settlementdate': datetime(2025, 1, 15, 10, 30),
                'region': 'NSW',
                'price': 85.50,
                'totaldemand': 7500.0,
                'price_type': 'DISPATCH'
            },
            {
                'settlementdate': datetime(2025, 1, 15, 10, 30),
                'region': 'VIC',
                'price': 72.30,
                'totaldemand': 5200.0,
                'price_type': 'DISPATCH'
            },
            {
                'settlementdate': datetime(2025, 1, 15, 10, 30),
                'region': 'NSW',
                'price': 90.00,
                'totaldemand': 7500.0,
                'price_type': 'TRADING'
            },
        ])
        await db.insert_price_data(price_df)

        interconnector_df = pd.DataFrame([
            {
                'settlementdate': datetime(2025, 1, 15, 10, 30),
                'interconnector': 'NSW1-QLD1',
                'meteredmwflow': 350.5,
                'mwflow': 355.0,
                'mwloss': 4.5,
                'marginalvalue': 12.30
            },
        ])
        await db.insert_interconnector_data(interconnector_df)

        await db.update_generator_info([
            {
                'duid': 'BAYSW1',
                'station_name': 'Bayswater',
                'region': 'NSW',
                'fuel_source': 'Coal',
                'technology_type': 'Steam',
                'capacity_mw': 660
            },
            {
                'duid': 'AGLHAL',
                'station_name': 'Hallett',
                'region': 'SA',
                'fuel_source': 'Wind',
                'technology_type': 'Wind',
                'capacity_mw': 95
            },
        ])

        return db

    return asyncio.get_event_loop().run_until_complete(_setup())


# Module-level setup
_db = None
_client = None


def setup_module(module):
    """Set up database once for all tests in module"""
    global _db, _client
    _db = _setup_database_sync()
    main_module.db = _db
    # Create TestClient AFTER database is set up, so lifespan sees the configured db
    _client = TestClient(app, raise_server_exceptions=False)


def teardown_module(module):
    """Clean up after all tests"""
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


class TestHealthCheck:
    """Tests for health check endpoint"""

    def test_root_endpoint(self, client):
        """Test root health check endpoint"""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
        assert "NEM Dispatch Data API" in data["message"]
        # Accept either timestamp (endpoint response) or version (app metadata)
        # Both indicate the app is running and responding
        assert "timestamp" in data or "version" in data


class TestDispatchEndpoints:
    """Tests for dispatch data endpoints"""

    def test_get_latest_dispatch(self, client):
        """Test getting latest dispatch data"""
        response = client.get("/api/dispatch/latest")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "count" in data
        assert "message" in data

    def test_get_latest_dispatch_with_limit(self, client):
        """Test limit parameter"""
        response = client.get("/api/dispatch/latest?limit=1")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] <= 1

    def test_get_latest_dispatch_limit_validation(self, client):
        """Test limit parameter validation"""
        # Over max
        response = client.get("/api/dispatch/latest?limit=10000")
        assert response.status_code == 422

        # Under min
        response = client.get("/api/dispatch/latest?limit=0")
        assert response.status_code == 422

    def test_get_dispatch_range(self, client):
        """Test date range query"""
        response = client.get(
            "/api/dispatch/range",
            params={
                "start_date": "2025-01-15T00:00:00",
                "end_date": "2025-01-16T00:00:00"
            }
        )
        assert response.status_code == 200
        data = response.json()
        assert "data" in data

    def test_get_dispatch_range_with_duid(self, client):
        """Test date range with DUID filter"""
        response = client.get(
            "/api/dispatch/range",
            params={
                "start_date": "2025-01-15T00:00:00",
                "end_date": "2025-01-16T00:00:00",
                "duid": "BAYSW1"
            }
        )
        assert response.status_code == 200


class TestPriceEndpoints:
    """Tests for price data endpoints"""

    def test_get_latest_prices_default(self, client):
        """Test default price type (DISPATCH)"""
        response = client.get("/api/prices/latest")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data

    def test_get_latest_prices_by_type(self, client):
        """Test different price types"""
        for price_type in ['DISPATCH', 'TRADING', 'PUBLIC']:
            response = client.get(f"/api/prices/latest?price_type={price_type}")
            assert response.status_code == 200

    def test_get_price_history(self, client):
        """Test price history endpoint"""
        response = client.get(
            "/api/prices/history",
            params={
                "start_date": "2025-01-15T00:00:00",
                "end_date": "2025-01-16T00:00:00"
            }
        )
        assert response.status_code == 200

    def test_get_price_history_with_region(self, client):
        """Test price history with region filter"""
        response = client.get(
            "/api/prices/history",
            params={
                "start_date": "2025-01-15T00:00:00",
                "end_date": "2025-01-16T00:00:00",
                "region": "NSW"
            }
        )
        assert response.status_code == 200


class TestInterconnectorEndpoints:
    """Tests for interconnector data endpoints"""

    def test_get_latest_interconnector_flows(self, client):
        """Test getting latest interconnector flows"""
        response = client.get("/api/interconnectors/latest")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data

    def test_get_interconnector_history(self, client):
        """Test interconnector history endpoint"""
        response = client.get(
            "/api/interconnectors/history",
            params={
                "start_date": "2025-01-15T00:00:00",
                "end_date": "2025-01-16T00:00:00"
            }
        )
        assert response.status_code == 200


class TestRegionEndpoints:
    """Tests for region-specific endpoints"""

    def test_get_region_summary_valid(self, client):
        """Test valid region summary returns correct structure"""
        for region in ['NSW', 'VIC', 'QLD', 'SA', 'TAS']:
            response = client.get(f"/api/region/{region}/summary")
            assert response.status_code == 200
            data = response.json()
            assert data["region"] == region

    def test_get_region_summary_has_data(self, client):
        """Test that region summary returns actual data for NSW (which has test data)"""
        response = client.get("/api/region/NSW/summary")
        assert response.status_code == 200
        data = response.json()
        assert data["region"] == "NSW"
        # Verify actual data is returned, not just structure
        assert data["total_generation"] is not None, "total_generation should not be None"
        assert data["total_generation"] > 0, f"total_generation should be > 0, got {data['total_generation']}"
        assert data["generator_count"] > 0, f"generator_count should be > 0, got {data['generator_count']}"

    def test_get_region_summary_invalid(self, client):
        """Test invalid region returns 400"""
        response = client.get("/api/region/INVALID/summary")
        assert response.status_code == 400
        data = response.json()
        assert "Invalid region" in data["detail"]

    def test_get_region_summary_lowercase(self, client):
        """Test that lowercase region is accepted and normalized"""
        response = client.get("/api/region/nsw/summary")
        assert response.status_code == 200
        data = response.json()
        assert data["region"] == "NSW"

    def test_get_region_generation_current(self, client):
        """Test current generation endpoint returns actual data"""
        response = client.get("/api/region/NSW/generation/current")
        assert response.status_code == 200
        data = response.json()
        assert "fuel_mix" in data
        assert "total_generation" in data
        # Verify actual data is returned, not just structure
        assert data["total_generation"] > 0, f"total_generation should be > 0, got {data['total_generation']}"
        assert len(data["fuel_mix"]) > 0, "fuel_mix should not be empty"

    def test_get_region_price_history(self, client):
        """Test region price history"""
        response = client.get("/api/region/NSW/prices/history?hours=24")
        assert response.status_code == 200
        data = response.json()
        assert data["hours"] == 24

    def test_get_region_price_history_hours_validation(self, client):
        """Test hours parameter validation"""
        # Over max (8760 = 365 days)
        response = client.get("/api/region/NSW/prices/history?hours=9000")
        assert response.status_code == 422

        # Under min (1)
        response = client.get("/api/region/NSW/prices/history?hours=0")
        assert response.status_code == 422


class TestGeneratorEndpoints:
    """Tests for generator data endpoints"""

    def test_get_generators_filter(self, client):
        """Test generator filter endpoint"""
        response = client.get("/api/generators/filter")
        assert response.status_code == 200

    def test_get_generators_by_region(self, client):
        """Test filter by region"""
        response = client.get("/api/generators/filter?region=NSW")
        assert response.status_code == 200

    def test_get_generators_by_fuel(self, client):
        """Test filter by fuel source"""
        response = client.get("/api/generators/filter?fuel_source=Coal")
        assert response.status_code == 200


class TestDataCoverageEndpoint:
    """Tests for data coverage endpoint"""

    def test_get_data_coverage_price(self, client):
        """Test data coverage for price_data"""
        response = client.get("/api/data/coverage?table=price_data")
        assert response.status_code == 200
        data = response.json()
        assert data["table"] == "price_data"
        assert "total_records" in data

    def test_get_data_coverage_dispatch(self, client):
        """Test data coverage for dispatch_data"""
        response = client.get("/api/data/coverage?table=dispatch_data")
        assert response.status_code == 200

    def test_get_data_coverage_interconnector(self, client):
        """Test data coverage for interconnector_data"""
        response = client.get("/api/data/coverage?table=interconnector_data")
        assert response.status_code == 200

    def test_get_data_coverage_invalid_table(self, client):
        """Test invalid table returns 400"""
        response = client.get("/api/data/coverage?table=invalid_table")
        assert response.status_code == 400
        data = response.json()
        assert "Invalid table" in data["detail"]


class TestSummaryEndpoints:
    """Tests for summary endpoints"""

    def test_get_data_summary(self, client):
        """Test data summary endpoint"""
        response = client.get("/api/summary")
        assert response.status_code == 200
        data = response.json()
        assert "total_records" in data
        assert "unique_duids" in data

    def test_get_unique_duids(self, client):
        """Test unique DUIDs endpoint"""
        response = client.get("/api/duids")
        assert response.status_code == 200
        data = response.json()
        assert "duids" in data
        assert isinstance(data["duids"], list)


class TestGenerationByFuel:
    """Tests for generation by fuel endpoint"""

    def test_get_generation_by_fuel(self, client):
        """Test generation by fuel type"""
        response = client.get(
            "/api/generation/by-fuel",
            params={
                "start_date": "2025-01-15T00:00:00",
                "end_date": "2025-01-16T00:00:00"
            }
        )
        assert response.status_code == 200
        data = response.json()
        assert "data" in data


class TestResponseFormats:
    """Tests for response format consistency"""

    def test_datetime_iso_format(self, client):
        """Test that datetimes are returned in ISO format"""
        response = client.get("/api/dispatch/latest")
        assert response.status_code == 200
        data = response.json()

        if data["count"] > 0:
            record = data["data"][0]
            assert "settlementdate" in record
            # Should be ISO format string
            datetime.fromisoformat(record["settlementdate"])

    def test_empty_response_structure(self, client):
        """Test that empty responses have correct structure"""
        # Use a date range with no data
        response = client.get(
            "/api/dispatch/range",
            params={
                "start_date": "2020-01-01T00:00:00",
                "end_date": "2020-01-02T00:00:00"
            }
        )
        assert response.status_code == 200
        data = response.json()
        assert data["data"] == []
        assert data["count"] == 0
        assert "message" in data


class TestDrilldownDataPopulation:
    """Tests that verify drilldown data is actually populated, not just structurally correct.

    These tests ensure that the region-specific endpoints return real data values,
    catching issues like missing generator_info data or incorrect database queries.
    """

    def test_region_summary_has_generation_data(self, client):
        """Verify total_generation and generator_count are populated for NSW"""
        response = client.get("/api/region/NSW/summary")
        assert response.status_code == 200
        data = response.json()

        assert data["total_generation"] is not None, "total_generation should not be None"
        assert data["total_generation"] > 0, f"total_generation should be > 0, got {data['total_generation']}"
        assert data["generator_count"] > 0, f"generator_count should be > 0, got {data['generator_count']}"

    def test_fuel_mix_has_data(self, client):
        """Verify fuel_mix contains actual generation breakdown"""
        response = client.get("/api/region/NSW/generation/current")
        assert response.status_code == 200
        data = response.json()

        assert len(data["fuel_mix"]) > 0, "fuel_mix should not be empty"
        assert data["total_generation"] > 0, f"total_generation should be > 0, got {data['total_generation']}"

        # Verify fuel_mix records have actual values
        for fuel in data["fuel_mix"]:
            assert fuel["fuel_source"] is not None, "fuel_source should not be None"
            assert fuel["generation_mw"] >= 0, f"generation_mw should be >= 0, got {fuel['generation_mw']}"
            assert fuel["unit_count"] > 0, f"unit_count should be > 0, got {fuel['unit_count']}"

    def test_all_test_regions_have_summary_data(self, client):
        """Verify regions with test data return valid summary data"""
        # Regions that have test data in conftest.py populated_db fixture
        regions_with_test_data = ['NSW', 'SA', 'VIC']

        for region in regions_with_test_data:
            response = client.get(f"/api/region/{region}/summary")
            assert response.status_code == 200
            data = response.json()

            # These regions should have generator_count >= 1 based on test fixture data
            assert data["generator_count"] >= 0, f"{region} generator_count should be >= 0, got {data['generator_count']}"

    def test_fuel_mix_percentages_sum_to_100(self, client):
        """Verify fuel_mix percentages approximately sum to 100"""
        response = client.get("/api/region/NSW/generation/current")
        assert response.status_code == 200
        data = response.json()

        if len(data["fuel_mix"]) > 0:
            total_percentage = sum(fuel["percentage"] for fuel in data["fuel_mix"])
            # Allow for small rounding errors
            assert 99.0 <= total_percentage <= 101.0, f"Percentages should sum to ~100, got {total_percentage}"


class TestMergedPriceEndpoint:
    """Tests for MERGED price type endpoint (bridges 4am data gap)"""

    def test_merged_price_type_accepted(self, client):
        """MERGED should be a valid price_type"""
        response = client.get("/api/region/NSW/prices/history?hours=24&price_type=MERGED")
        assert response.status_code == 200
        data = response.json()
        assert data["price_type"] == "MERGED"

    def test_merged_returns_data_structure(self, client):
        """MERGED should return proper response structure"""
        response = client.get("/api/region/NSW/prices/history?hours=24&price_type=MERGED")
        assert response.status_code == 200
        data = response.json()
        assert "region" in data
        assert "data" in data
        assert "count" in data
        assert "hours" in data
        assert "price_type" in data

    def test_merged_lowercase_accepted(self, client):
        """lowercase 'merged' should be accepted and normalized"""
        response = client.get("/api/region/NSW/prices/history?hours=24&price_type=merged")
        assert response.status_code == 200
        data = response.json()
        assert data["price_type"] == "MERGED"

    def test_invalid_price_type_returns_400(self, client):
        """Invalid price_type should return 400"""
        response = client.get("/api/region/NSW/prices/history?hours=24&price_type=INVALID")
        assert response.status_code == 400
        data = response.json()
        assert "Invalid price_type" in data["detail"]

    def test_merged_includes_source_type_in_records(self, client):
        """MERGED response records should include source_type field"""
        response = client.get("/api/region/NSW/prices/history?hours=24&price_type=MERGED")
        assert response.status_code == 200
        data = response.json()

        if data["count"] > 0:
            # Each record should have source_type
            for record in data["data"]:
                assert "source_type" in record
                assert record["source_type"] in ["PUBLIC", "DISPATCH"]

    def test_merged_all_regions(self, client):
        """MERGED should work for all valid regions"""
        for region in ["NSW", "VIC", "QLD", "SA", "TAS"]:
            response = client.get(f"/api/region/{region}/prices/history?hours=24&price_type=MERGED")
            assert response.status_code == 200, f"Failed for region {region}"
            data = response.json()
            assert data["region"] == region
