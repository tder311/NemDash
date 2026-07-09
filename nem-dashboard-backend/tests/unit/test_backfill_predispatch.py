"""Unit tests for the MMS predispatch price backfill (pure parts only, no network/DB)."""

from datetime import date

import pandas as pd
import pytest

from scripts.backfill_predispatch import (
    archive_url,
    build_month_dataframe,
    last_n_complete_months,
    parse_predispatch_csv,
    select_thinned_runs,
    seqno_to_run_datetime,
)


class TestArchiveUrl:
    def test_matches_verified_pattern(self):
        url = archive_url(2026, 5)
        assert url == (
            "https://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/2026/"
            "MMSDM_2026_05/MMSDM_Historical_Data_SQLLoader/DATA/"
            "PUBLIC_ARCHIVE%23PREDISPATCHPRICE%23FILE01%23202605010000.zip"
        )

    def test_zero_pads_single_digit_month(self):
        url = archive_url(2025, 7)
        assert "MMSDM_2025_07" in url
        assert "%23202507010000.zip" in url


class TestLastNCompleteMonths:
    def test_excludes_current_incomplete_month(self):
        months = last_n_complete_months(3, today=date(2026, 7, 9))
        assert months == [(2026, 4), (2026, 5), (2026, 6)]

    def test_oldest_first(self):
        months = last_n_complete_months(12, today=date(2026, 7, 1))
        assert months[0] == (2025, 7)
        assert months[-1] == (2026, 6)

    def test_crosses_year_boundary(self):
        months = last_n_complete_months(2, today=date(2026, 1, 15))
        assert months == [(2025, 11), (2025, 12)]


class TestSeqnoToRunDatetime:
    def test_run_one_is_trading_day_0400(self):
        seqno = pd.Series(["2026050101"])
        result = seqno_to_run_datetime(seqno)
        assert result.iloc[0] == pd.Timestamp("2026-05-01 04:00:00")

    def test_run_forty_eight_rolls_into_next_day(self):
        # Verified against a real archive: seqno 2026053148 -> LASTCHANGED
        # "2026/06/01 03:31:57", interval_datetime "2026/06/01 04:00:00".
        seqno = pd.Series(["2026053148"])
        result = seqno_to_run_datetime(seqno)
        assert result.iloc[0] == pd.Timestamp("2026-06-01 03:30:00")

    def test_consecutive_runs_step_thirty_minutes(self):
        seqno = pd.Series(["2026050101", "2026050102", "2026050103"])
        result = seqno_to_run_datetime(seqno)
        diffs = result.diff().dropna()
        assert (diffs == pd.Timedelta(minutes=30)).all()

    def test_run_datetime_is_thirty_min_before_interval(self):
        # DATETIME (interval, periodid=1) is always run_datetime + 30min in the archive.
        seqno = pd.Series(["2026050105"])
        run_dt = seqno_to_run_datetime(seqno).iloc[0]
        interval_dt = pd.Timestamp("2026-05-01 06:30:00")
        assert interval_dt - run_dt == pd.Timedelta(minutes=30)


class TestSelectThinnedRuns:
    def _half_hourly_day(self, day="2026-05-01"):
        return pd.Series(pd.date_range(f"{day} 00:00", periods=48, freq="30min"))

    def test_keeps_four_runs_for_one_day(self):
        runs = self._half_hourly_day()
        mask = select_thinned_runs(runs)
        kept = runs[mask]
        assert len(kept) == 4

    def test_keeps_runs_nearest_target_hours(self):
        runs = self._half_hourly_day()
        mask = select_thinned_runs(runs, target_hours=(4, 10, 16, 22))
        kept = sorted(runs[mask])
        assert kept == [
            pd.Timestamp("2026-05-01 04:00"),
            pd.Timestamp("2026-05-01 10:00"),
            pd.Timestamp("2026-05-01 16:00"),
            pd.Timestamp("2026-05-01 22:00"),
        ]

    def test_scales_across_multiple_days(self):
        two_days = pd.concat(
            [self._half_hourly_day("2026-05-01"), self._half_hourly_day("2026-05-02")],
            ignore_index=True,
        )
        mask = select_thinned_runs(two_days)
        assert mask.sum() == 8

    def test_nearest_available_when_exact_hour_missing(self):
        # No run at exactly 10:00 -> nearest of 09:45/10:15 should be picked, not dropped.
        runs = pd.Series(
            [pd.Timestamp("2026-05-01 04:00"), pd.Timestamp("2026-05-01 09:45"), pd.Timestamp("2026-05-01 16:00")]
        )
        mask = select_thinned_runs(runs, target_hours=(4, 10, 16, 22))
        assert mask.sum() == 3
        assert runs[mask].tolist() == runs.tolist()


SAMPLE_CSV = (
    'C,SETP.WORLD,DVD_PREDISPATCHPRICE,AEMO,PUBLIC,2026/06/09,12:01:50,1,MONTHLY_ARCHIVE,1\n'
    'I,PREDISPATCH,REGION_PRICES,2,PREDISPATCHSEQNO,RUNNO,REGIONID,PERIODID,INTERVENTION,RRP,EEP,'
    'LASTCHANGED,DATETIME\n'
    'D,PREDISPATCH,REGION_PRICES,2,2026050101,1,NSW1,01,0,56.06,0,'
    '"2026/05/01 04:01:52","2026/05/01 04:30:00"\n'
    'D,PREDISPATCH,REGION_PRICES,2,2026050101,1,VIC1,01,0,-2.33,0,'
    '"2026/05/01 04:01:52","2026/05/01 04:30:00"\n'
    # Intervention row for the same run/region should be dropped.
    'D,PREDISPATCH,REGION_PRICES,2,2026050101,1,NSW1,01,1,999.0,0,'
    '"2026/05/01 04:01:52","2026/05/01 04:30:00"\n'
    # Non-NEM region should be dropped.
    'D,PREDISPATCH,REGION_PRICES,2,2026050101,1,NEM1,01,0,10.0,0,'
    '"2026/05/01 04:01:52","2026/05/01 04:30:00"\n'
    'C,"END OF REPORT",5\n'
)


class TestParsePredispatchCsv:
    def test_parses_expected_rows(self):
        df = parse_predispatch_csv(SAMPLE_CSV)
        assert len(df) == 2
        assert set(df["regionid"]) == {"NSW1", "VIC1"}

    def test_drops_intervention_rows(self):
        df = parse_predispatch_csv(SAMPLE_CSV)
        assert 999.0 not in df["rrp"].tolist()

    def test_drops_non_nem_regions(self):
        df = parse_predispatch_csv(SAMPLE_CSV)
        assert "NEM1" not in set(df["regionid"])

    def test_parses_values(self):
        df = parse_predispatch_csv(SAMPLE_CSV)
        row = df[df["regionid"] == "NSW1"].iloc[0]
        assert row["seqno"] == "2026050101"
        assert row["rrp"] == pytest.approx(56.06)
        assert row["interval_datetime"] == pd.Timestamp("2026-05-01 04:30:00")

    def test_no_i_rows_returns_empty(self):
        df = parse_predispatch_csv("C,header only\n")
        assert df.empty
        assert list(df.columns) == ["seqno", "interval_datetime", "regionid", "rrp"]


class TestBuildMonthDataframe:
    def test_adds_run_datetime_and_thins(self):
        df = build_month_dataframe(SAMPLE_CSV)
        assert list(df.columns) == ["run_datetime", "interval_datetime", "regionid", "rrp"]
        assert len(df) == 2
        assert (df["run_datetime"] == pd.Timestamp("2026-05-01 04:00:00")).all()

    def test_empty_csv_returns_empty_frame(self):
        df = build_month_dataframe("C,header only\n")
        assert df.empty
        assert list(df.columns) == ["run_datetime", "interval_datetime", "regionid", "rrp"]
