"""Unit tests for the pure parts of scripts/validate_unit_inference.py (no network/DB)."""

import pandas as pd
import pytest

from scripts.validate_unit_inference import (
    aggregate_realised_to_30min,
    compute_validation_metrics,
    parse_next_day_dispatch_csv,
)

# Real header verified against a downloaded PUBLIC_NEXT_DAY_DISPATCH_*.zip (2026-07-09 file).
SAMPLE_NEXT_DAY_DISPATCH_CSV = (
    'C,NEMP.WORLD,NEXT_DAY_DISPATCH,AEMO,PUBLIC,2026/07/10,04:10:00,1,NEXT_DAY_DISPATCH,1\n'
    'I,DISPATCH,UNIT_SOLUTION,6,SETTLEMENTDATE,RUNNO,DUID,TRADETYPE,DISPATCHINTERVAL,INTERVENTION,'
    'CONNECTIONPOINTID,DISPATCHMODE,AGCSTATUS,INITIALMW,TOTALCLEARED\n'
    'D,DISPATCH,UNIT_SOLUTION,6,"2026/07/09 04:05:00",1,BAYSW1,0,20260709001,0,NBAYSW,0,1,350.1,351.0\n'
    'D,DISPATCH,UNIT_SOLUTION,6,"2026/07/09 04:10:00",1,BAYSW1,0,20260709002,0,NBAYSW,0,1,351.0,352.5\n'
    'D,DISPATCH,UNIT_SOLUTION,6,"2026/07/09 04:15:00",1,BAYSW1,0,20260709003,0,NBAYSW,0,1,352.5,353.0\n'
    'D,DISPATCH,UNIT_SOLUTION,6,"2026/07/09 04:20:00",1,BAYSW1,0,20260709004,0,NBAYSW,0,1,353.0,354.0\n'
    'D,DISPATCH,UNIT_SOLUTION,6,"2026/07/09 04:25:00",1,BAYSW1,0,20260709005,0,NBAYSW,0,1,354.0,355.0\n'
    'D,DISPATCH,UNIT_SOLUTION,6,"2026/07/09 04:30:00",1,BAYSW1,0,20260709006,0,NBAYSW,0,1,355.0,356.0\n'
    # Intervention row should be dropped.
    'D,DISPATCH,UNIT_SOLUTION,6,"2026/07/09 04:30:00",1,BAYSW1,0,20260709006,1,NBAYSW,0,1,999.0,999.0\n'
    # A DUID not in the target set should be dropped entirely.
    'D,DISPATCH,UNIT_SOLUTION,6,"2026/07/09 04:05:00",1,OTHERDUID,0,20260709001,0,X,0,1,10.0,10.0\n'
    'C,"END OF REPORT",9\n'
)


class TestParseNextDayDispatchCsv:
    def test_keeps_only_target_duids(self):
        df = parse_next_day_dispatch_csv(SAMPLE_NEXT_DAY_DISPATCH_CSV, {"BAYSW1"})
        assert set(df["duid"]) == {"BAYSW1"}
        assert len(df) == 6

    def test_drops_intervention_rows(self):
        df = parse_next_day_dispatch_csv(SAMPLE_NEXT_DAY_DISPATCH_CSV, {"BAYSW1"})
        assert 999.0 not in df["totalcleared"].tolist()

    def test_columns(self):
        df = parse_next_day_dispatch_csv(SAMPLE_NEXT_DAY_DISPATCH_CSV, {"BAYSW1"})
        assert list(df.columns) == ["settlementdate", "duid", "initialmw", "totalcleared"]

    def test_no_matching_duid_returns_empty(self):
        df = parse_next_day_dispatch_csv(SAMPLE_NEXT_DAY_DISPATCH_CSV, {"NOT_PRESENT"})
        assert df.empty


class TestAggregateRealisedTo30Min:
    def test_ceils_five_minute_settlements_into_the_ending_half_hour(self):
        df = parse_next_day_dispatch_csv(SAMPLE_NEXT_DAY_DISPATCH_CSV, {"BAYSW1"})
        out = aggregate_realised_to_30min(df)
        # All six 5-min rows (04:05..04:30) end within the (04:00, 04:30] half hour.
        assert len(out) == 1
        assert out.iloc[0]["interval_datetime"] == pd.Timestamp("2026-07-09 04:30:00")
        assert out.iloc[0]["totalcleared"] == pytest.approx((351 + 352.5 + 353 + 354 + 355 + 356) / 6)

    def test_groups_separately_per_duid(self):
        df = pd.DataFrame([
            {"settlementdate": pd.Timestamp("2026-07-09 04:05:00"), "duid": "A", "initialmw": 1.0, "totalcleared": 10.0},
            {"settlementdate": pd.Timestamp("2026-07-09 04:05:00"), "duid": "B", "initialmw": 1.0, "totalcleared": 20.0},
        ])
        out = aggregate_realised_to_30min(df)
        assert len(out) == 2
        assert set(out["duid"]) == {"A", "B"}


class TestComputeValidationMetrics:
    def test_perfect_agreement_gives_zero_mae_and_corr_one(self):
        inferred = pd.DataFrame([
            {"interval_datetime": pd.Timestamp("2026-07-09 04:30"), "duid": "A", "mw_inferred": 10.0},
            {"interval_datetime": pd.Timestamp("2026-07-09 05:00"), "duid": "A", "mw_inferred": 20.0},
            {"interval_datetime": pd.Timestamp("2026-07-09 05:30"), "duid": "A", "mw_inferred": 30.0},
        ])
        realised = pd.DataFrame([
            {"interval_datetime": pd.Timestamp("2026-07-09 04:30"), "duid": "A", "totalcleared": 10.0},
            {"interval_datetime": pd.Timestamp("2026-07-09 05:00"), "duid": "A", "totalcleared": 20.0},
            {"interval_datetime": pd.Timestamp("2026-07-09 05:30"), "duid": "A", "totalcleared": 30.0},
        ])
        out = compute_validation_metrics(inferred, realised)
        row = out[out["duid"] == "A"].iloc[0]
        assert row["n"] == 3
        assert row["corr"] == pytest.approx(1.0)
        assert row["mae"] == pytest.approx(0.0)

    def test_unmatched_intervals_are_excluded(self):
        inferred = pd.DataFrame([
            {"interval_datetime": pd.Timestamp("2026-07-09 04:30"), "duid": "A", "mw_inferred": 10.0},
            {"interval_datetime": pd.Timestamp("2026-07-09 06:00"), "duid": "A", "mw_inferred": 999.0},
        ])
        realised = pd.DataFrame([
            {"interval_datetime": pd.Timestamp("2026-07-09 04:30"), "duid": "A", "totalcleared": 10.0},
        ])
        out = compute_validation_metrics(inferred, realised)
        assert out[out["duid"] == "A"].iloc[0]["n"] == 1

    def test_empty_inputs_returns_empty(self):
        out = compute_validation_metrics(pd.DataFrame(columns=["interval_datetime", "duid", "mw_inferred"]),
                                          pd.DataFrame(columns=["interval_datetime", "duid", "totalcleared"]))
        assert out.empty
