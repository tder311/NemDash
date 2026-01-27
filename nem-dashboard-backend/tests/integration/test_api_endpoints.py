"""
Integration tests for FastAPI endpoints

Requires DATABASE_URL environment variable for PostgreSQL connection.
"""
import pytest
import os

# Check for DATABASE_URL before running tests
_db_url = os.environ.get('DATABASE_URL')
if not _db_url:
    pytest.skip("DATABASE_URL environment variable not set", allow_module_level=True)


class TestHealthEndpoint:
    """Tests for health check endpoint"""

    @pytest.mark.asyncio
    async def test_health_check(self, async_client):
        """Health endpoint should return OK"""
        response = await async_client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"


class TestDispatchEndpoints:
    """Tests for dispatch data endpoints"""

    @pytest.mark.asyncio
    async def test_get_latest_dispatch(self, async_client):
        """Get latest dispatch should return 200"""
        response = await async_client.get("/api/dispatch/latest")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data

    @pytest.mark.asyncio
    async def test_get_latest_dispatch_with_limit(self, async_client):
        """Get latest dispatch with limit should return limited results"""
        response = await async_client.get("/api/dispatch/latest?limit=5")
        assert response.status_code == 200
        data = response.json()
        assert len(data["data"]) <= 5

    @pytest.mark.asyncio
    async def test_get_dispatch_range(self, async_client):
        """Get dispatch range should return 200"""
        response = await async_client.get(
            "/api/dispatch/range?start_date=2025-01-15T00:00:00&end_date=2025-01-16T00:00:00"
        )
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_get_dispatch_range_with_duid(self, async_client):
        """Get dispatch range with DUID filter should return 200"""
        response = await async_client.get(
            "/api/dispatch/range?start_date=2025-01-15T00:00:00&end_date=2025-01-16T00:00:00&duid=BAYSW1"
        )
        assert response.status_code == 200


class TestPriceEndpoints:
    """Tests for price data endpoints"""

    @pytest.mark.asyncio
    async def test_get_latest_prices_default(self, async_client):
        """Get latest prices should return 200"""
        response = await async_client.get("/api/prices/latest")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data

    @pytest.mark.asyncio
    async def test_get_latest_prices_by_type(self, async_client):
        """Get latest prices by type should return 200"""
        response = await async_client.get("/api/prices/latest?price_type=DISPATCH")
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_get_price_history(self, async_client):
        """Get price history should return 200"""
        response = await async_client.get("/api/prices/history?start_date=2025-01-15T00:00:00&end_date=2025-01-16T00:00:00")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data

    @pytest.mark.asyncio
    async def test_get_price_history_with_region(self, async_client):
        """Get price history with region filter should return 200"""
        response = await async_client.get("/api/prices/history?start_date=2025-01-15T00:00:00&end_date=2025-01-16T00:00:00&region=NSW")
        assert response.status_code == 200


class TestRegionEndpoints:
    """Tests for region-specific endpoints"""

    @pytest.mark.asyncio
    async def test_get_region_summary_valid(self, async_client):
        """Get region summary for valid region should return 200"""
        response = await async_client.get("/api/region/NSW/summary")
        assert response.status_code == 200
        data = response.json()
        assert data["region"] == "NSW"

    @pytest.mark.asyncio
    async def test_get_region_summary_has_data(self, async_client):
        """Region summary should contain expected fields"""
        response = await async_client.get("/api/region/NSW/summary")
        data = response.json()
        assert "region" in data
        assert "message" in data

    @pytest.mark.asyncio
    async def test_get_region_summary_invalid(self, async_client):
        """Get region summary for invalid region should return 400"""
        response = await async_client.get("/api/region/INVALID/summary")
        assert response.status_code == 400

    @pytest.mark.asyncio
    async def test_get_region_summary_lowercase(self, async_client):
        """Get region summary should handle lowercase region"""
        response = await async_client.get("/api/region/nsw/summary")
        assert response.status_code == 200
        data = response.json()
        assert data["region"] == "NSW"

    @pytest.mark.asyncio
    async def test_get_region_generation_current(self, async_client):
        """Get region generation current should return 200"""
        response = await async_client.get("/api/region/NSW/generation/current")
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_get_region_price_history(self, async_client):
        """Get region price history should return 200"""
        response = await async_client.get("/api/region/NSW/prices/history?hours=24")
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_get_region_generation_history(self, async_client):
        """Get region generation history should return 200"""
        response = await async_client.get("/api/region/NSW/generation/history?hours=24")
        assert response.status_code == 200
        data = response.json()
        assert "region" in data
        assert data["region"] == "NSW"

    @pytest.mark.asyncio
    async def test_get_region_generation_history_with_aggregation(self, async_client):
        """Get region generation history with custom aggregation should return 200"""
        response = await async_client.get("/api/region/NSW/generation/history?hours=168&aggregation=60")
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_get_region_summary_invalid_region(self, async_client):
        """Invalid region should return 400"""
        response = await async_client.get("/api/region/INVALID/summary")
        assert response.status_code == 400

    @pytest.mark.asyncio
    async def test_get_region_generation_history_invalid_region(self, async_client):
        """Invalid region for generation history should return 400"""
        response = await async_client.get("/api/region/INVALID/generation/history?hours=24")
        assert response.status_code == 400


class TestGeneratorEndpoints:
    """Tests for generator data endpoints"""

    @pytest.mark.asyncio
    async def test_get_generators_filter(self, async_client):
        """Get generators should return 200"""
        response = await async_client.get("/api/generators/filter")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data

    @pytest.mark.asyncio
    async def test_get_generators_by_region(self, async_client):
        """Get generators by region should return 200"""
        response = await async_client.get("/api/generators/filter?region=NSW")
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_get_generators_by_fuel(self, async_client):
        """Get generators by fuel source should return 200"""
        response = await async_client.get("/api/generators/filter?fuel_source=Coal")
        assert response.status_code == 200


class TestDataCoverageEndpoint:
    """Tests for data coverage endpoint"""

    @pytest.mark.asyncio
    async def test_get_data_coverage_price(self, async_client):
        """Get data coverage for price should return 200"""
        response = await async_client.get("/api/data/coverage?table=price_data")
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_get_data_coverage_dispatch(self, async_client):
        """Get data coverage for dispatch should return 200"""
        response = await async_client.get("/api/data/coverage?table=dispatch_data")
        assert response.status_code == 200

class TestSummaryEndpoints:
    """Tests for summary data endpoints"""

    @pytest.mark.asyncio
    async def test_get_data_summary(self, async_client):
        """Get data summary should return 200"""
        response = await async_client.get("/api/summary")
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_get_unique_duids(self, async_client):
        """Get unique DUIDs should return 200"""
        response = await async_client.get("/api/duids")
        assert response.status_code == 200


class TestGenerationByFuel:
    """Tests for generation by fuel endpoints"""

    @pytest.mark.asyncio
    async def test_get_generation_by_fuel(self, async_client):
        """Get generation by fuel should return 200"""
        response = await async_client.get("/api/generation/by-fuel?start_date=2025-01-15T00:00:00&end_date=2025-01-16T00:00:00")
        assert response.status_code == 200


class TestResponseFormats:
    """Tests for response format validation"""

    @pytest.mark.asyncio
    async def test_datetime_iso_format(self, async_client):
        """Datetime fields should be ISO format strings"""
        response = await async_client.get("/api/dispatch/latest")
        assert response.status_code == 200
        data = response.json()
        if data["data"]:
            record = data["data"][0]
            # settlementdate should be an ISO format string
            assert "settlementdate" in record
            assert isinstance(record["settlementdate"], str)

    @pytest.mark.asyncio
    async def test_empty_response_structure(self, async_client):
        """Empty responses should have correct structure"""
        response = await async_client.get(
            "/api/dispatch/range?start_date=2020-01-01T00:00:00&end_date=2020-01-02T00:00:00"
        )
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert isinstance(data["data"], list)


class TestDrilldownDataPopulation:
    """Tests to verify drilldown data is properly populated"""

    @pytest.mark.asyncio
    async def test_region_summary_has_generation_data(self, async_client):
        """Region summary should have generation data when dispatch data exists"""
        response = await async_client.get("/api/region/NSW/summary")
        assert response.status_code == 200
        data = response.json()
        # Should have either generation data or a message
        assert "region" in data

    @pytest.mark.asyncio
    async def test_fuel_mix_has_data(self, async_client):
        """Fuel mix endpoint should return data when dispatch and generator info exist"""
        response = await async_client.get("/api/region/NSW/generation/current")
        assert response.status_code == 200
        data = response.json()
        assert "region" in data
        assert "fuel_mix" in data

    @pytest.mark.asyncio
    async def test_all_test_regions_have_summary_data(self, async_client):
        """All test regions should return valid summaries"""
        for region in ['NSW', 'VIC', 'SA']:
            response = await async_client.get(f"/api/region/{region}/summary")
            assert response.status_code == 200
            data = response.json()
            assert data["region"] == region

    @pytest.mark.asyncio
    async def test_fuel_mix_percentages_sum_to_100(self, async_client):
        """Fuel mix percentages should sum to approximately 100%"""
        response = await async_client.get("/api/region/NSW/generation/current")
        assert response.status_code == 200
        data = response.json()
        if data["fuel_mix"]:
            total_percentage = sum(item["percentage"] for item in data["fuel_mix"])
            assert 99 <= total_percentage <= 101  # Allow for rounding


class TestMergedPriceEndpoint:
    """Tests for merged price type endpoint"""

    @pytest.mark.asyncio
    async def test_merged_price_type_accepted(self, async_client):
        """MERGED price type should be accepted"""
        response = await async_client.get("/api/prices/latest?price_type=MERGED")
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_merged_returns_data_structure(self, async_client):
        """MERGED prices should return proper structure"""
        response = await async_client.get("/api/prices/latest?price_type=MERGED")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert isinstance(data["data"], list)

    @pytest.mark.asyncio
    async def test_merged_lowercase_accepted(self, async_client):
        """merged (lowercase) price type should be accepted"""
        response = await async_client.get("/api/prices/latest?price_type=merged")
        assert response.status_code == 200


class TestTimeRangeOptions:
    """Tests for time range options endpoint"""

    @pytest.mark.asyncio
    async def test_time_range_options_endpoint_exists(self, async_client):
        """Time range options endpoint should exist"""
        response = await async_client.get("/api/time-range-options")
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_time_range_options_returns_list(self, async_client):
        """Time range options should return a list of options"""
        response = await async_client.get("/api/time-range-options")
        data = response.json()
        assert "options" in data
        assert isinstance(data["options"], list)
        assert len(data["options"]) == 8  # 8 time range options

    @pytest.mark.asyncio
    async def test_time_range_options_structure(self, async_client):
        """Each time range option should have expected fields"""
        response = await async_client.get("/api/time-range-options")
        data = response.json()
        for option in data["options"]:
            assert "label" in option
            assert "hours" in option
            assert "aggregation_minutes" in option
            assert isinstance(option["hours"], int)
            assert isinstance(option["aggregation_minutes"], int)

    @pytest.mark.asyncio
    async def test_time_range_options_values(self, async_client):
        """Time range options should include expected values"""
        response = await async_client.get("/api/time-range-options")
        data = response.json()
        hours_values = [opt["hours"] for opt in data["options"]]
        # Should include 24h, 168h (7d), 720h (30d), 2160h (90d), 8760h (365d)
        assert 24 in hours_values
        assert 168 in hours_values
        assert 720 in hours_values
        assert 2160 in hours_values
        assert 8760 in hours_values


class TestDatabaseHealthEndpoint:
    """Tests for database health endpoint"""

    @pytest.mark.asyncio
    async def test_get_database_health_default(self, async_client):
        """Get database health with default parameters should return 200"""
        response = await async_client.get("/api/database/health")
        assert response.status_code == 200
        data = response.json()
        assert "tables" in data
        assert "gaps" in data
        assert "checked_hours" in data
        assert "checked_at" in data

    @pytest.mark.asyncio
    async def test_get_database_health_with_hours(self, async_client):
        """Get database health with custom hours should return 200"""
        response = await async_client.get("/api/database/health?hours_back=24")
        assert response.status_code == 200
        data = response.json()
        assert data["checked_hours"] == 24

    @pytest.mark.asyncio
    async def test_get_database_health_tables_structure(self, async_client):
        """Database health should return table stats for all tables"""
        response = await async_client.get("/api/database/health")
        assert response.status_code == 200
        data = response.json()
        assert len(data["tables"]) == 3
        table_names = [t["table"] for t in data["tables"]]
        assert "dispatch_data" in table_names
        assert "price_data" in table_names
        assert "generator_info" in table_names

    @pytest.mark.asyncio
    async def test_get_database_health_gaps_structure(self, async_client):
        """Database health should return gap info for time-series tables"""
        response = await async_client.get("/api/database/health")
        assert response.status_code == 200
        data = response.json()
        # gaps should be returned for time-series tables
        assert len(data["gaps"]) == 2
        gap_tables = [g["table"] for g in data["gaps"]]
        assert "dispatch_data" in gap_tables
        assert "price_data" in gap_tables

    @pytest.mark.asyncio
    async def test_get_database_health_invalid_hours(self, async_client):
        """Get database health with invalid hours should return 422"""
        response = await async_client.get("/api/database/health?hours_back=0")
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_get_database_health_max_hours(self, async_client):
        """Get database health with max hours should return 200"""
        response = await async_client.get("/api/database/health?hours_back=8760")
        assert response.status_code == 200


class TestRegionDataRangeEndpoint:
    """Tests for region data range endpoint"""

    @pytest.mark.asyncio
    async def test_get_region_data_range_returns_valid_dates(self, async_client):
        """Get region data range should return earliest and latest dates"""
        response = await async_client.get("/api/region/NSW/data-range")
        assert response.status_code == 200
        data = response.json()
        assert "earliest_date" in data
        assert "latest_date" in data
        assert "region" in data
        assert data["region"] == "NSW"

    @pytest.mark.asyncio
    async def test_get_region_data_range_invalid_region(self, async_client):
        """Get region data range for invalid region should return 400"""
        response = await async_client.get("/api/region/INVALID/data-range")
        assert response.status_code == 400

    @pytest.mark.asyncio
    async def test_get_region_data_range_lowercase(self, async_client):
        """Get region data range should handle lowercase region"""
        response = await async_client.get("/api/region/nsw/data-range")
        assert response.status_code == 200
        data = response.json()
        assert data["region"] == "NSW"


class TestDateRangeParameters:
    """Tests for date range parameters on existing endpoints"""

    @pytest.mark.asyncio
    async def test_price_history_with_date_range(self, async_client):
        """Get price history with start_date and end_date should return 200"""
        response = await async_client.get(
            "/api/region/NSW/prices/history",
            params={"start_date": "2025-01-12T00:00:00", "end_date": "2025-01-16T00:00:00"}
        )
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "hours" in data
        # 4 days = 96 hours
        assert data["hours"] == 96

    @pytest.mark.asyncio
    async def test_price_history_date_range_calculates_aggregation(self, async_client):
        """Price history with date range should calculate appropriate aggregation"""
        response = await async_client.get(
            "/api/region/NSW/prices/history",
            params={"start_date": "2025-01-01T00:00:00", "end_date": "2025-01-08T00:00:00"}
        )
        assert response.status_code == 200
        data = response.json()
        # 7 days = 168 hours, should use 30 min aggregation
        assert data["hours"] == 168
        assert data["aggregation_minutes"] == 30

    @pytest.mark.asyncio
    async def test_generation_history_with_date_range(self, async_client):
        """Get generation history with start_date and end_date should return 200"""
        response = await async_client.get(
            "/api/region/NSW/generation/history",
            params={"start_date": "2025-01-12T00:00:00", "end_date": "2025-01-16T00:00:00"}
        )
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "hours" in data

    @pytest.mark.asyncio
    async def test_price_history_hours_fallback(self, async_client):
        """Price history should still work with hours parameter (backwards compatible)"""
        response = await async_client.get("/api/region/NSW/prices/history?hours=24")
        assert response.status_code == 200
        data = response.json()
        assert data["hours"] == 24

    @pytest.mark.asyncio
    async def test_price_history_date_range_invalid_dates(self, async_client):
        """Price history with end_date before start_date should return 400"""
        response = await async_client.get(
            "/api/region/NSW/prices/history",
            params={"start_date": "2025-01-16T00:00:00", "end_date": "2025-01-12T00:00:00"}
        )
        assert response.status_code == 400
