"""Unit tests for the pure parts of scripts/validate_joint_inference.py (no network/DB)."""

import pandas as pd
import pytest

from scripts.validate_joint_inference import (
    aggregate_bounds_to_30min,
    compute_calibration_errors,
    filter_forward_window,
    summarise_calibration,
)

RUN = pd.Timestamp("2026-07-09 07:00:00")


class TestFilterForwardWindow:
    def test_keeps_only_first_forward_hours(self):
        df = pd.DataFrame({
            "run_datetime": [RUN] * 4,
            "interval_datetime": [
                RUN,  # not strictly after the run
                RUN + pd.Timedelta(minutes=30),
                RUN + pd.Timedelta(hours=4),  # boundary, inclusive
                RUN + pd.Timedelta(hours=4, minutes=30),  # beyond window
            ],
            "x": [1, 2, 3, 4],
        })
        out = filter_forward_window(df, max_lead_hours=4)
        assert list(out["x"]) == [2, 3]


class TestComputeCalibrationErrors:
    def _lhs(self, cid, lhs):
        return pd.DataFrame([{
            "run_datetime": RUN, "interval_datetime": RUN + pd.Timedelta(minutes=30),
            "constraintid": cid, "lhs": lhs,
        }])

    def test_zero_duid_constraint_reconstruction_error(self):
        lhs = self._lhs("IC_ONLY", 25.0)
        terms = pd.DataFrame([
            {"constraintid": "IC_ONLY", "term_type": "interconnector", "term_id": "IC1", "factor": 2.0},
            {"constraintid": "IC_ONLY", "term_type": "interconnector", "term_id": "IC2", "factor": -1.0},
        ])
        flows = pd.DataFrame([
            {"run_datetime": RUN, "interval_datetime": RUN + pd.Timedelta(minutes=30),
             "interconnectorid": "IC1", "mwflow": 10.0},
            {"run_datetime": RUN, "interval_datetime": RUN + pd.Timedelta(minutes=30),
             "interconnectorid": "IC2", "mwflow": -3.0},
        ])
        out = compute_calibration_errors(lhs, terms, flows)
        # known = 2*10 + (-1)*(-3) = 23 => error = 25 - 23 = 2
        assert len(out) == 1
        assert out.iloc[0]["reconstruction_error"] == pytest.approx(2.0)

    def test_constraint_with_duid_terms_is_not_scored(self):
        lhs = self._lhs("HAS_DUID", 25.0)
        terms = pd.DataFrame([
            {"constraintid": "HAS_DUID", "term_type": "duid", "term_id": "A", "factor": 1.0},
            {"constraintid": "HAS_DUID", "term_type": "interconnector", "term_id": "IC1", "factor": 1.0},
        ])
        flows = pd.DataFrame([
            {"run_datetime": RUN, "interval_datetime": RUN + pd.Timedelta(minutes=30),
             "interconnectorid": "IC1", "mwflow": 10.0},
        ])
        out = compute_calibration_errors(lhs, terms, flows)
        assert out.empty

    def test_missing_flow_excludes_instance(self):
        lhs = self._lhs("IC_ONLY", 25.0)
        terms = pd.DataFrame([
            {"constraintid": "IC_ONLY", "term_type": "interconnector", "term_id": "IC1", "factor": 1.0},
        ])
        flows = pd.DataFrame(columns=["run_datetime", "interval_datetime", "interconnectorid", "mwflow"])
        out = compute_calibration_errors(lhs, terms, flows)
        assert out.empty


class TestAggregateBoundsTo30Min:
    def test_max_maxavail_per_halfhour_period_ending(self):
        bids = pd.DataFrame({
            "settlementdate": [
                pd.Timestamp("2026-07-09 07:05:00"),
                pd.Timestamp("2026-07-09 07:30:00"),
                pd.Timestamp("2026-07-09 07:35:00"),
            ],
            "duid": ["A", "A", "A"],
            "maxavail": [100.0, 120.0, 90.0],
        })
        out = aggregate_bounds_to_30min(bids)
        by_interval = out.set_index("interval_datetime")["maxavail"]
        # 07:05 and 07:30 both ceil to the 07:30 period-ending bucket.
        assert by_interval[pd.Timestamp("2026-07-09 07:30:00")] == 120.0
        assert by_interval[pd.Timestamp("2026-07-09 08:00:00")] == 90.0


class TestSummariseCalibration:
    def test_distribution_stats(self):
        calibration = pd.DataFrame({"reconstruction_error": [0.0, 0.5, -2.0, 10.0]})
        out = summarise_calibration(calibration)
        assert out["n"] == 4
        assert out["median_abs_error_mw"] == pytest.approx(1.25)
        assert out["max_abs_error_mw"] == pytest.approx(10.0)
        assert out["share_within_1mw"] == pytest.approx(0.5)

    def test_empty_input(self):
        out = summarise_calibration(pd.DataFrame(columns=["reconstruction_error"]))
        assert out == {"n": 0}
