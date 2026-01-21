"""
Unit tests for aggregation calculation functions.

These tests verify the calculate_aggregation_minutes function which determines
the appropriate aggregation interval based on the requested time range.

Aggregation Strategy:
- < 48h: 5 min (raw data)
- 48h - 7d: 30 min
- 7d - 30d: 60 min (hourly)
- 30d - 90d: 1440 min (daily)
- > 90d: 10080 min (weekly)
"""
import pytest


class TestCalculateAggregationMinutes:
    """Tests for calculate_aggregation_minutes function"""

    def test_under_48h_returns_5min(self):
        """Under 48h should use raw 5-min data"""
        from app.database import calculate_aggregation_minutes

        assert calculate_aggregation_minutes(1) == 5
        assert calculate_aggregation_minutes(6) == 5
        assert calculate_aggregation_minutes(12) == 5
        assert calculate_aggregation_minutes(24) == 5
        assert calculate_aggregation_minutes(47) == 5

    def test_boundary_48h_returns_30min(self):
        """Exactly 48h should use 30-min aggregation"""
        from app.database import calculate_aggregation_minutes

        assert calculate_aggregation_minutes(48) == 30

    def test_48h_to_7d_returns_30min(self):
        """48h to 7d (168h) should use 30-min aggregation"""
        from app.database import calculate_aggregation_minutes

        assert calculate_aggregation_minutes(48) == 30
        assert calculate_aggregation_minutes(72) == 30
        assert calculate_aggregation_minutes(96) == 30
        assert calculate_aggregation_minutes(120) == 30
        assert calculate_aggregation_minutes(168) == 30

    def test_boundary_7d_returns_30min(self):
        """Exactly 7d (168h) should still use 30-min aggregation"""
        from app.database import calculate_aggregation_minutes

        assert calculate_aggregation_minutes(168) == 30

    def test_over_7d_to_30d_returns_60min(self):
        """7d to 30d (169h-720h) should use hourly aggregation"""
        from app.database import calculate_aggregation_minutes

        assert calculate_aggregation_minutes(169) == 60
        assert calculate_aggregation_minutes(240) == 60  # 10 days
        assert calculate_aggregation_minutes(336) == 60  # 14 days
        assert calculate_aggregation_minutes(504) == 60  # 21 days
        assert calculate_aggregation_minutes(720) == 60  # 30 days

    def test_boundary_30d_returns_60min(self):
        """Exactly 30d (720h) should use hourly aggregation"""
        from app.database import calculate_aggregation_minutes

        assert calculate_aggregation_minutes(720) == 60

    def test_over_30d_to_90d_returns_daily(self):
        """30d to 90d (721h-2160h) should use daily aggregation"""
        from app.database import calculate_aggregation_minutes

        assert calculate_aggregation_minutes(721) == 1440
        assert calculate_aggregation_minutes(1000) == 1440
        assert calculate_aggregation_minutes(1440) == 1440  # 60 days
        assert calculate_aggregation_minutes(2000) == 1440
        assert calculate_aggregation_minutes(2160) == 1440  # 90 days

    def test_boundary_90d_returns_daily(self):
        """Exactly 90d (2160h) should use daily aggregation"""
        from app.database import calculate_aggregation_minutes

        assert calculate_aggregation_minutes(2160) == 1440

    def test_over_90d_returns_weekly(self):
        """Over 90d (>2160h) should use weekly aggregation"""
        from app.database import calculate_aggregation_minutes

        assert calculate_aggregation_minutes(2161) == 10080
        assert calculate_aggregation_minutes(4320) == 10080  # 180 days
        assert calculate_aggregation_minutes(6000) == 10080
        assert calculate_aggregation_minutes(8760) == 10080  # 365 days

    def test_365d_returns_weekly(self):
        """365d (8760h) should use weekly aggregation"""
        from app.database import calculate_aggregation_minutes

        assert calculate_aggregation_minutes(8760) == 10080

    def test_returns_integer(self):
        """Should always return an integer"""
        from app.database import calculate_aggregation_minutes

        for hours in [1, 48, 169, 721, 2161, 8760]:
            result = calculate_aggregation_minutes(hours)
            assert isinstance(result, int), f"Expected int for {hours}h, got {type(result)}"


class TestAggregationDataPoints:
    """Tests to verify expected data point counts for each aggregation level"""

    def test_5min_aggregation_data_points(self):
        """5-min aggregation should give reasonable data points"""
        # 24h * 60min / 5min = 288 points
        from app.database import calculate_aggregation_minutes

        hours = 24
        agg = calculate_aggregation_minutes(hours)
        expected_points = (hours * 60) / agg
        assert expected_points == 288

    def test_30min_aggregation_data_points(self):
        """30-min aggregation for 7d should give ~336 points"""
        from app.database import calculate_aggregation_minutes

        hours = 168  # 7 days
        agg = calculate_aggregation_minutes(hours)
        expected_points = (hours * 60) / agg
        assert expected_points == 336

    def test_hourly_aggregation_data_points(self):
        """Hourly aggregation for 30d should give ~720 points"""
        from app.database import calculate_aggregation_minutes

        hours = 720  # 30 days
        agg = calculate_aggregation_minutes(hours)
        expected_points = (hours * 60) / agg
        assert expected_points == 720

    def test_daily_aggregation_data_points(self):
        """Daily aggregation for 90d should give ~90 points"""
        from app.database import calculate_aggregation_minutes

        hours = 2160  # 90 days
        agg = calculate_aggregation_minutes(hours)
        expected_points = (hours * 60) / agg
        assert expected_points == 90

    def test_weekly_aggregation_data_points(self):
        """Weekly aggregation for 365d should give ~52 points"""
        from app.database import calculate_aggregation_minutes

        hours = 8760  # 365 days
        agg = calculate_aggregation_minutes(hours)
        expected_points = (hours * 60) / agg
        assert expected_points == pytest.approx(52.14, rel=0.01)
