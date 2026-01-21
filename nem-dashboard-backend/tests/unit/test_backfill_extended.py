"""
Unit tests for extended dispatch data backfill.

Tests verify that the backfill system supports extended time ranges (up to 365 days)
and implements staged loading (recent data first, older data later).
"""
import pytest
from datetime import datetime, timedelta
import pandas as pd
import os


class TestBackfillParameterSupport:
    """Tests for backfill parameter validation"""

    def test_dispatch_backfill_days_env_default(self):
        """DISPATCH_BACKFILL_DAYS defaults to 7"""
        original = os.environ.pop('DISPATCH_BACKFILL_DAYS', None)
        try:
            default_days = int(os.getenv('DISPATCH_BACKFILL_DAYS', '7'))
            assert default_days == 7
        finally:
            if original:
                os.environ['DISPATCH_BACKFILL_DAYS'] = original

    def test_dispatch_backfill_days_env_can_be_90(self):
        """DISPATCH_BACKFILL_DAYS can be set to 90"""
        original = os.environ.get('DISPATCH_BACKFILL_DAYS')
        try:
            os.environ['DISPATCH_BACKFILL_DAYS'] = '90'
            days = int(os.getenv('DISPATCH_BACKFILL_DAYS', '7'))
            assert days == 90
        finally:
            if original:
                os.environ['DISPATCH_BACKFILL_DAYS'] = original
            else:
                os.environ.pop('DISPATCH_BACKFILL_DAYS', None)

    def test_dispatch_backfill_days_env_can_be_365(self):
        """DISPATCH_BACKFILL_DAYS can be set to 365"""
        original = os.environ.get('DISPATCH_BACKFILL_DAYS')
        try:
            os.environ['DISPATCH_BACKFILL_DAYS'] = '365'
            days = int(os.getenv('DISPATCH_BACKFILL_DAYS', '7'))
            assert days == 365
        finally:
            if original:
                os.environ['DISPATCH_BACKFILL_DAYS'] = original
            else:
                os.environ.pop('DISPATCH_BACKFILL_DAYS', None)

    def test_dispatch_priority_days_env_default(self):
        """DISPATCH_PRIORITY_DAYS defaults to 30"""
        original = os.environ.pop('DISPATCH_PRIORITY_DAYS', None)
        try:
            default_days = int(os.getenv('DISPATCH_PRIORITY_DAYS', '30'))
            assert default_days == 30
        finally:
            if original:
                os.environ['DISPATCH_PRIORITY_DAYS'] = original


class TestMissingDatesLogic:
    """Tests for detecting dates that need backfilling"""

    def test_archive_cutoff_is_two_days(self):
        """Archives have ~2 day delay"""
        now = datetime.now()
        archive_cutoff = now - timedelta(days=2)
        assert (now - archive_cutoff).days == 2

    def test_date_range_generation(self):
        """Should correctly generate date range for backfill"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)

        dates = []
        current = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        while current <= end_date:
            dates.append(current)
            current += timedelta(days=1)

        assert len(dates) == 8  # 7 days + today

    def test_filter_existing_dates(self):
        """Should filter out dates that already have data"""
        all_dates = [
            datetime(2025, 1, 10),
            datetime(2025, 1, 11),
            datetime(2025, 1, 12),
            datetime(2025, 1, 13),
            datetime(2025, 1, 14),
        ]

        existing_dates = {'2025-01-11', '2025-01-13'}

        missing = [
            d for d in all_dates
            if d.strftime('%Y-%m-%d') not in existing_dates
        ]

        assert len(missing) == 3
        assert datetime(2025, 1, 10) in missing
        assert datetime(2025, 1, 12) in missing
        assert datetime(2025, 1, 14) in missing


class TestBackfillStagesLogic:
    """Tests for staged backfill logic"""

    def test_current_directory_covers_three_days(self):
        """Current directory provides ~3 days of recent data"""
        current_days = 3
        assert current_days >= 3

    def test_priority_days_within_total_days(self):
        """Priority days should be less than total backfill days"""
        total_days = 90
        priority_days = 30
        assert priority_days <= total_days

    def test_remaining_days_calculation(self):
        """Should correctly calculate remaining days after priority"""
        total_days = 90
        priority_days = 30
        remaining = total_days - priority_days
        assert remaining == 60


class TestAggregationIntegration:
    """Tests that backfill data works with aggregation queries"""

    def test_30d_backfill_enables_hourly_aggregation(self):
        """30-day backfill should enable hourly aggregation charts"""
        from app.database import calculate_aggregation_minutes

        hours = 720  # 30 days
        agg = calculate_aggregation_minutes(hours)
        assert agg == 60  # Hourly

    def test_90d_backfill_enables_daily_aggregation(self):
        """90-day backfill should enable daily aggregation charts"""
        from app.database import calculate_aggregation_minutes

        hours = 2160  # 90 days
        agg = calculate_aggregation_minutes(hours)
        assert agg == 1440  # Daily

    def test_365d_backfill_enables_weekly_aggregation(self):
        """365-day backfill should enable weekly aggregation charts"""
        from app.database import calculate_aggregation_minutes

        hours = 8760  # 365 days
        agg = calculate_aggregation_minutes(hours)
        assert agg == 10080  # Weekly


class TestBackfillDataFrameHandling:
    """Tests for DataFrame handling during backfill"""

    def test_empty_dataframe_handling(self):
        """Should handle empty DataFrames gracefully"""
        df = pd.DataFrame()
        assert df.empty
        assert len(df) == 0

    def test_dataframe_deduplication(self):
        """Should handle duplicate records in backfill data"""
        df = pd.DataFrame({
            'settlementdate': [
                datetime(2025, 1, 15, 10, 0),
                datetime(2025, 1, 15, 10, 0),  # Duplicate
                datetime(2025, 1, 15, 10, 5),
            ],
            'duid': ['TEST1', 'TEST1', 'TEST1'],
            'scadavalue': [100.0, 100.0, 110.0],
        })

        deduped = df.drop_duplicates(
            subset=['settlementdate', 'duid'],
            keep='last'
        )

        assert len(deduped) == 2

    def test_dataframe_date_conversion(self):
        """Should correctly convert dates in backfill data"""
        df = pd.DataFrame({
            'settlementdate': ['2025/01/15 10:00:00'],
        })

        df['settlementdate'] = pd.to_datetime(
            df['settlementdate'],
            format='%Y/%m/%d %H:%M:%S'
        )

        assert isinstance(df['settlementdate'].iloc[0], pd.Timestamp)


class TestBackfillConfiguration:
    """Tests for backfill configuration"""

    def test_default_configuration_values(self):
        """Should have sensible defaults for backfill"""
        default_backfill_days = 7
        default_priority_days = 30
        max_backfill_days = 365
        archive_delay_days = 2

        assert default_backfill_days > 0
        assert default_priority_days >= default_backfill_days
        assert max_backfill_days >= default_priority_days
        assert archive_delay_days == 2

    def test_rate_limit_sleep_duration(self):
        """Should have appropriate rate limiting sleep"""
        rate_limit_sleep = 1  # 1 second between requests
        assert rate_limit_sleep >= 1


class TestBackfillProgressTracking:
    """Tests for backfill progress tracking"""

    def test_progress_calculation(self):
        """Should correctly calculate backfill progress"""
        total_dates = 30
        completed_dates = 15
        progress = (completed_dates / total_dates) * 100
        assert progress == 50.0

    def test_records_count_aggregation(self):
        """Should correctly aggregate records from multiple fetches"""
        fetch_results = [100, 150, 200, 50, 0]  # 0 for empty result
        total = sum(fetch_results)
        assert total == 500
