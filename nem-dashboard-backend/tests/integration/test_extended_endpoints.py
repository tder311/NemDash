"""
Integration tests for extended time range API endpoints.

Tests verify that endpoints accept extended hour values (up to 8760 for 365 days)
and return aggregation_minutes in responses.

Requires DATABASE_URL environment variable for PostgreSQL connection.
"""
import pytest
import os

# Check for DATABASE_URL before running tests
_db_url = os.environ.get('DATABASE_URL')
if not _db_url:
    pytest.skip("DATABASE_URL environment variable not set", allow_module_level=True)


class TestExtendedHoursValidation:
    """Tests for extended hours parameter validation"""

    @pytest.mark.asyncio
    async def test_generation_history_accepts_720_hours(self, async_client_extended):
        """30 days (720h) should be accepted"""
        response = await async_client_extended.get("/api/region/NSW/generation/history?hours=720")
        assert response.status_code == 200
        data = response.json()
        assert data["hours"] == 720

    @pytest.mark.asyncio
    async def test_generation_history_accepts_2160_hours(self, async_client_extended):
        """90 days (2160h) should be accepted"""
        response = await async_client_extended.get("/api/region/NSW/generation/history?hours=2160")
        assert response.status_code == 200
        data = response.json()
        assert data["hours"] == 2160

    @pytest.mark.asyncio
    async def test_generation_history_accepts_8760_hours(self, async_client_extended):
        """365 days (8760h) should be accepted"""
        response = await async_client_extended.get("/api/region/NSW/generation/history?hours=8760")
        assert response.status_code == 200
        data = response.json()
        assert data["hours"] == 8760

    @pytest.mark.asyncio
    async def test_generation_history_rejects_over_8760(self, async_client_extended):
        """Over 365 days should be rejected"""
        response = await async_client_extended.get("/api/region/NSW/generation/history?hours=9000")
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_price_history_accepts_720_hours(self, async_client_extended):
        """30 days (720h) for prices should be accepted"""
        response = await async_client_extended.get("/api/region/NSW/prices/history?hours=720")
        assert response.status_code == 200
        data = response.json()
        assert data["hours"] == 720

    @pytest.mark.asyncio
    async def test_price_history_accepts_8760_hours(self, async_client_extended):
        """365 days (8760h) for prices should be accepted"""
        response = await async_client_extended.get("/api/region/NSW/prices/history?hours=8760")
        assert response.status_code == 200
        data = response.json()
        assert data["hours"] == 8760


class TestAggregationMinutesInResponse:
    """Tests for aggregation_minutes field in responses"""

    @pytest.mark.asyncio
    async def test_generation_history_includes_aggregation_minutes(self, async_client_extended):
        """Response should include aggregation_minutes field"""
        response = await async_client_extended.get("/api/region/NSW/generation/history?hours=720")
        assert response.status_code == 200
        data = response.json()
        assert "aggregation_minutes" in data

    @pytest.mark.asyncio
    async def test_generation_history_auto_aggregation_30d(self, async_client_extended):
        """30-day query should auto-aggregate to hourly (60 min)"""
        response = await async_client_extended.get("/api/region/NSW/generation/history?hours=720")
        data = response.json()
        assert data["aggregation_minutes"] == 60

    @pytest.mark.asyncio
    async def test_generation_history_auto_aggregation_90d(self, async_client_extended):
        """90-day query should auto-aggregate to daily (1440 min)"""
        response = await async_client_extended.get("/api/region/NSW/generation/history?hours=2160")
        data = response.json()
        assert data["aggregation_minutes"] == 1440

    @pytest.mark.asyncio
    async def test_generation_history_auto_aggregation_365d(self, async_client_extended):
        """365-day query should auto-aggregate to weekly (10080 min)"""
        response = await async_client_extended.get("/api/region/NSW/generation/history?hours=8760")
        data = response.json()
        assert data["aggregation_minutes"] == 10080

    @pytest.mark.asyncio
    async def test_price_history_includes_aggregation_minutes(self, async_client_extended):
        """Extended price response should include aggregation_minutes"""
        response = await async_client_extended.get("/api/region/NSW/prices/history?hours=720")
        assert response.status_code == 200
        data = response.json()
        assert "aggregation_minutes" in data

    @pytest.mark.asyncio
    async def test_price_history_auto_aggregation_30d(self, async_client_extended):
        """30-day price query should auto-aggregate to hourly"""
        response = await async_client_extended.get("/api/region/NSW/prices/history?hours=720")
        data = response.json()
        assert data["aggregation_minutes"] == 60


class TestExplicitAggregationParameter:
    """Tests for explicit aggregation parameter override"""

    @pytest.mark.asyncio
    async def test_generation_history_explicit_aggregation(self, async_client_extended):
        """Explicit aggregation should override auto-calculation"""
        response = await async_client_extended.get("/api/region/NSW/generation/history?hours=720&aggregation=30")
        assert response.status_code == 200
        data = response.json()
        assert data["aggregation_minutes"] == 30

    @pytest.mark.asyncio
    async def test_aggregation_validation_min(self, async_client_extended):
        """Aggregation below 5 minutes should be rejected"""
        response = await async_client_extended.get("/api/region/NSW/generation/history?hours=24&aggregation=3")
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_aggregation_validation_max(self, async_client_extended):
        """Aggregation above 10080 minutes should be rejected"""
        response = await async_client_extended.get("/api/region/NSW/generation/history?hours=24&aggregation=20000")
        assert response.status_code == 422


class TestDataPointReduction:
    """Tests to verify aggregation reduces data point counts"""

    @pytest.mark.asyncio
    async def test_30d_query_reasonable_data_points(self, async_client_extended):
        """30-day query should return reasonable number of data points"""
        response = await async_client_extended.get("/api/region/NSW/generation/history?hours=720")
        data = response.json()

        if data["count"] > 0:
            # With hourly aggregation for 7 days of test data
            # Should have <= 168 hours * num_fuel_types
            # Our test data has 4 fuel types, so max ~672 records
            assert data["count"] <= 1000

    @pytest.mark.asyncio
    async def test_90d_query_fewer_data_points(self, async_client_extended):
        """90-day query should have fewer points than 30-day (higher aggregation)"""
        response_30d = await async_client_extended.get("/api/region/NSW/generation/history?hours=720")
        response_90d = await async_client_extended.get("/api/region/NSW/generation/history?hours=2160")

        data_30d = response_30d.json()
        data_90d = response_90d.json()

        if data_30d["count"] > 0 and data_90d["count"] > 0:
            # 90-day should have same or fewer points (daily vs hourly)
            # Both are looking at same 7-day test data
            assert data_90d["count"] <= data_30d["count"]


class TestExtendedRangeResponseFormat:
    """Tests for response format with extended ranges"""

    @pytest.mark.asyncio
    async def test_generation_history_response_structure(self, async_client_extended):
        """Extended generation response should have correct structure"""
        response = await async_client_extended.get("/api/region/NSW/generation/history?hours=720")
        assert response.status_code == 200
        data = response.json()

        assert "region" in data
        assert "data" in data
        assert "count" in data
        assert "hours" in data
        assert "aggregation_minutes" in data
        assert "message" in data

    @pytest.mark.asyncio
    async def test_price_history_response_structure(self, async_client_extended):
        """Extended price response should have correct structure"""
        response = await async_client_extended.get("/api/region/NSW/prices/history?hours=720")
        assert response.status_code == 200
        data = response.json()

        assert "region" in data
        assert "data" in data
        assert "count" in data
        assert "hours" in data
        assert "price_type" in data
        assert "aggregation_minutes" in data
        assert "message" in data

    @pytest.mark.asyncio
    async def test_generation_data_records_have_expected_fields(self, async_client_extended):
        """Generation history records should have expected fields"""
        response = await async_client_extended.get("/api/region/NSW/generation/history?hours=168")
        data = response.json()

        if data["count"] > 0:
            record = data["data"][0]
            assert "period" in record
            assert "fuel_source" in record
            assert "generation_mw" in record
            assert "sample_count" in record

    @pytest.mark.asyncio
    async def test_price_data_records_have_expected_fields(self, async_client_extended):
        """Price history records should have expected fields"""
        response = await async_client_extended.get("/api/region/NSW/prices/history?hours=720")
        data = response.json()

        if data["count"] > 0:
            record = data["data"][0]
            assert "settlementdate" in record
            assert "price" in record
            assert "totaldemand" in record


class TestAllRegionsExtendedSupport:
    """Tests that all regions support extended time ranges"""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("region", ["NSW", "VIC", "QLD", "SA", "TAS"])
    async def test_all_regions_accept_720_hours(self, async_client_extended, region):
        """All regions should accept 30-day queries"""
        response = await async_client_extended.get(f"/api/region/{region}/generation/history?hours=720")
        assert response.status_code == 200
        data = response.json()
        assert data["region"] == region

    @pytest.mark.asyncio
    @pytest.mark.parametrize("region", ["NSW", "VIC", "QLD", "SA", "TAS"])
    async def test_all_regions_price_history_720(self, async_client_extended, region):
        """All regions should accept 30-day price queries"""
        response = await async_client_extended.get(f"/api/region/{region}/prices/history?hours=720")
        assert response.status_code == 200


class TestBackwardsCompatibility:
    """Tests to ensure existing short-range queries still work"""

    @pytest.mark.asyncio
    async def test_existing_7d_query_still_works(self, async_client_extended):
        """Existing 7-day queries should continue to work"""
        response = await async_client_extended.get("/api/region/NSW/generation/history?hours=168")
        assert response.status_code == 200
        data = response.json()
        assert data["hours"] == 168
        # Should still use 30-min aggregation for 7d
        assert data["aggregation_minutes"] == 30

    @pytest.mark.asyncio
    async def test_existing_24h_query_still_works(self, async_client_extended):
        """Existing 24-hour queries should work with raw data"""
        response = await async_client_extended.get("/api/region/NSW/generation/history?hours=24")
        assert response.status_code == 200
        data = response.json()
        assert data["hours"] == 24
        # Should use 5-min (raw) aggregation for 24h
        assert data["aggregation_minutes"] == 5

    @pytest.mark.asyncio
    async def test_existing_price_history_works(self, async_client_extended):
        """Existing price history endpoints should work unchanged"""
        response = await async_client_extended.get("/api/region/NSW/prices/history?hours=24")
        assert response.status_code == 200
        data = response.json()
        assert data["hours"] == 24
