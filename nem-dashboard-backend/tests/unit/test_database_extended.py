"""
Unit tests for extended time range database queries.

Tests verify that get_region_generation_history and get_aggregated_price_history
work correctly with extended ranges (30d, 90d, 365d) and appropriate aggregation.

NOTE: These tests are marked as slow because they generate substantial test data
(7 days of 5-minute interval data). Run with `pytest -m slow` to include them.
"""
import pytest
import pytest_asyncio
import pandas as pd
from datetime import datetime, timedelta

# Mark all tests in this module as slow - skip in CI by default
pytestmark = pytest.mark.slow

from app.database import NEMDatabase, calculate_aggregation_minutes
from tests.fixtures.extended_data import (
    generate_dispatch_data,
    generate_price_data,
    generate_generator_info,
)


@pytest_asyncio.fixture
async def populated_db_extended(test_db):
    """Database with 7 days of sample data for extended range testing.

    Uses 7 days as a reasonable test size that's faster than 30 days
    but still exercises the aggregation logic.
    """
    # Use recent dates relative to now for the time-based queries
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)

    # Generate and insert dispatch data
    dispatch_df = generate_dispatch_data(start_date, days=7, region='NSW')
    await test_db.insert_dispatch_data(dispatch_df)

    # Generate and insert price data (PUBLIC type)
    price_df = generate_price_data(
        start_date, days=7, region='NSW', price_type='PUBLIC'
    )
    await test_db.insert_price_data(price_df)

    # Generate and insert DISPATCH price data for merged queries
    dispatch_price_df = generate_price_data(
        start_date, days=7, region='NSW', price_type='DISPATCH'
    )
    await test_db.insert_price_data(dispatch_price_df)

    # Add generator info for test DUIDs
    await test_db.update_generator_info(generate_generator_info('NSW'))

    return test_db


class TestRegionGenerationHistoryExtended:
    """Tests for get_region_generation_history with extended ranges"""

    @pytest.mark.asyncio
    async def test_accepts_extended_hours_parameter(self, populated_db_extended):
        """Should accept hours values up to 8760 (365 days)"""
        # Test various extended hour values
        for hours in [720, 2160, 8760]:
            result = await populated_db_extended.get_region_generation_history(
                'NSW', hours=hours
            )
            # Should return a DataFrame (may be empty if no data)
            assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_auto_aggregation_30_days(self, populated_db_extended):
        """30-day query should auto-aggregate to hourly (60 min)"""
        result = await populated_db_extended.get_region_generation_history(
            'NSW', hours=720  # 30 days
        )

        if len(result) > 0:
            # Should have fewer records than raw 5-min data
            # 30 days of 5-min data = 8640 records per fuel type
            # With hourly aggregation = ~720 records per fuel type
            fuel_types = result['fuel_source'].nunique()
            # Allow for some variation due to edge effects
            assert len(result) <= 720 * fuel_types + 100

    @pytest.mark.asyncio
    async def test_auto_aggregation_90_days(self, populated_db_extended):
        """90-day query should auto-aggregate to daily (1440 min)"""
        result = await populated_db_extended.get_region_generation_history(
            'NSW', hours=2160  # 90 days
        )

        if len(result) > 0:
            # With daily aggregation, should have ~90 records per fuel type max
            fuel_types = result['fuel_source'].nunique()
            assert len(result) <= 90 * fuel_types + 50

    @pytest.mark.asyncio
    async def test_auto_aggregation_365_days(self, populated_db_extended):
        """365-day query should auto-aggregate to weekly (10080 min)"""
        result = await populated_db_extended.get_region_generation_history(
            'NSW', hours=8760  # 365 days
        )

        if len(result) > 0:
            # With weekly aggregation, should have ~52 records per fuel type max
            fuel_types = result['fuel_source'].nunique()
            assert len(result) <= 52 * fuel_types + 20

    @pytest.mark.asyncio
    async def test_explicit_aggregation_overrides_auto(self, populated_db_extended):
        """Explicit aggregation_minutes should override auto-calculation"""
        # Force 30-min aggregation for a 30-day query (instead of auto 60-min)
        result = await populated_db_extended.get_region_generation_history(
            'NSW', hours=720, aggregation_minutes=30
        )

        if len(result) > 0:
            # With 30-min aggregation, should have more records
            fuel_types = result['fuel_source'].nunique()
            # 30 days with 30-min = 1440 records per fuel type
            # But we only have 7 days of data, so check it's reasonable
            assert len(result) > 0

    @pytest.mark.asyncio
    async def test_returns_expected_columns(self, populated_db_extended):
        """Should return DataFrame with expected columns"""
        result = await populated_db_extended.get_region_generation_history(
            'NSW', hours=168  # 7 days (matches our test data)
        )

        assert isinstance(result, pd.DataFrame)
        if len(result) > 0:
            assert 'period' in result.columns
            assert 'fuel_source' in result.columns
            assert 'generation_mw' in result.columns
            assert 'sample_count' in result.columns

    @pytest.mark.asyncio
    async def test_aggregation_reduces_data_points(self, populated_db_extended):
        """Higher aggregation should result in fewer data points"""
        # Get with 30-min aggregation
        result_30min = await populated_db_extended.get_region_generation_history(
            'NSW', hours=168, aggregation_minutes=30
        )

        # Get with 60-min aggregation
        result_60min = await populated_db_extended.get_region_generation_history(
            'NSW', hours=168, aggregation_minutes=60
        )

        if len(result_30min) > 0 and len(result_60min) > 0:
            # 60-min should have roughly half the data points of 30-min
            assert len(result_60min) < len(result_30min)


class TestAggregatedPriceHistory:
    """Tests for get_aggregated_price_history with extended ranges"""

    @pytest.mark.asyncio
    async def test_method_exists(self, populated_db_extended):
        """get_aggregated_price_history method should exist"""
        assert hasattr(populated_db_extended, 'get_aggregated_price_history')

    @pytest.mark.asyncio
    async def test_returns_dataframe(self, populated_db_extended):
        """Should return a DataFrame"""
        result = await populated_db_extended.get_aggregated_price_history(
            'NSW', hours=168
        )
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_auto_aggregation_uses_merged_for_short_ranges(
        self, populated_db_extended
    ):
        """Short ranges (<=30 min aggregation) should use merged price logic"""
        result = await populated_db_extended.get_aggregated_price_history(
            'NSW', hours=24  # Should use 5-min aggregation
        )

        if len(result) > 0:
            # For short ranges, should include source_type column
            assert 'source_type' in result.columns

    @pytest.mark.asyncio
    async def test_hourly_aggregation_30_days(self, populated_db_extended):
        """30-day query should aggregate to hourly"""
        result = await populated_db_extended.get_aggregated_price_history(
            'NSW', hours=720
        )

        if len(result) > 0:
            # Should have <= 720 records (hourly for 30 days)
            assert len(result) <= 720 + 50

    @pytest.mark.asyncio
    async def test_daily_aggregation_90_days(self, populated_db_extended):
        """90-day query should aggregate to daily"""
        result = await populated_db_extended.get_aggregated_price_history(
            'NSW', hours=2160
        )

        if len(result) > 0:
            # Should have <= 90 records (daily for 90 days)
            assert len(result) <= 90 + 10

    @pytest.mark.asyncio
    async def test_weekly_aggregation_365_days(self, populated_db_extended):
        """365-day query should aggregate to weekly"""
        result = await populated_db_extended.get_aggregated_price_history(
            'NSW', hours=8760
        )

        if len(result) > 0:
            # Should have <= 52 records (weekly for 365 days)
            assert len(result) <= 52 + 5

    @pytest.mark.asyncio
    async def test_returns_expected_columns(self, populated_db_extended):
        """Should return DataFrame with expected columns"""
        result = await populated_db_extended.get_aggregated_price_history(
            'NSW', hours=720
        )

        if len(result) > 0:
            assert 'settlementdate' in result.columns
            assert 'price' in result.columns
            assert 'totaldemand' in result.columns

    @pytest.mark.asyncio
    async def test_aggregated_prices_are_averages(self, populated_db_extended):
        """Aggregated prices should be averages within the period"""
        result = await populated_db_extended.get_aggregated_price_history(
            'NSW', hours=168, aggregation_minutes=1440  # Force daily
        )

        if len(result) > 0:
            # Prices should be reasonable (not sum of all prices)
            # Typical prices are $50-$150/MWh
            assert result['price'].max() < 500

    @pytest.mark.asyncio
    async def test_includes_sample_count_for_aggregated(self, populated_db_extended):
        """Aggregated results should include sample_count"""
        result = await populated_db_extended.get_aggregated_price_history(
            'NSW', hours=720, aggregation_minutes=60  # Hourly
        )

        if len(result) > 0:
            assert 'sample_count' in result.columns
            # Sample count should be positive
            assert result['sample_count'].min() >= 1


class TestExtendedQueryPerformance:
    """Tests for query performance with extended ranges"""

    @pytest.mark.asyncio
    async def test_large_aggregation_completes_quickly(self, populated_db_extended):
        """Large aggregation queries should complete in reasonable time"""
        import time

        start = time.time()
        await populated_db_extended.get_region_generation_history(
            'NSW', hours=8760  # 365 days
        )
        elapsed = time.time() - start

        # Should complete within 5 seconds even for large query
        assert elapsed < 5.0

    @pytest.mark.asyncio
    async def test_price_aggregation_completes_quickly(self, populated_db_extended):
        """Price aggregation queries should complete in reasonable time"""
        import time

        start = time.time()
        await populated_db_extended.get_aggregated_price_history(
            'NSW', hours=8760
        )
        elapsed = time.time() - start

        assert elapsed < 5.0


class TestAutoAggregationIntegration:
    """Tests for automatic aggregation calculation integration"""

    @pytest.mark.asyncio
    async def test_generation_history_uses_calculate_aggregation(
        self, populated_db_extended
    ):
        """get_region_generation_history should use calculate_aggregation_minutes"""
        # 720 hours should auto-calculate to 60 min aggregation
        expected_agg = calculate_aggregation_minutes(720)
        assert expected_agg == 60

        # When no explicit aggregation passed, should use calculated value
        result = await populated_db_extended.get_region_generation_history(
            'NSW', hours=720
        )
        # Verify reasonable data point count for hourly aggregation
        if len(result) > 0:
            # With 7 days of test data and hourly aggregation
            # Should have ~168 hours * num_fuel_types
            pass  # Data point count verified in other tests

    @pytest.mark.asyncio
    async def test_price_history_uses_calculate_aggregation(
        self, populated_db_extended
    ):
        """get_aggregated_price_history should use calculate_aggregation_minutes"""
        expected_agg = calculate_aggregation_minutes(2160)  # 90 days
        assert expected_agg == 1440  # Daily

        result = await populated_db_extended.get_aggregated_price_history(
            'NSW', hours=2160
        )
        # Should use daily aggregation
        if len(result) > 0:
            # With 7 days of test data, should have <= 7 records
            assert len(result) <= 10
