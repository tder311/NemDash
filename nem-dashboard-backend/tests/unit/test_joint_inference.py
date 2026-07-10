"""Unit tests for solve_unit_generation (joint least-squares unit backsolve)."""

import numpy as np
import pandas as pd
import pytest

from app.joint_inference import (
    OUTPUT_COLUMNS,
    SERIES_COLUMNS,
    TRACKING_COLUMNS,
    TRACKING_CORR_THRESHOLD,
    aggregate_realised_30min,
    build_paired_series,
    compute_unit_tracking,
    select_short_lead_latest_run,
    solve_unit_generation,
)

RUN = pd.Timestamp("2026-07-09 10:00:00")
IVL = pd.Timestamp("2026-07-09 10:30:00")


def _lhs(rows):
    return pd.DataFrame(rows, columns=["run_datetime", "interval_datetime", "constraintid", "lhs"])


def _terms(rows):
    return pd.DataFrame(rows, columns=["constraintid", "term_type", "term_id", "factor"])


def _ic(rows):
    return pd.DataFrame(rows, columns=["run_datetime", "interval_datetime", "interconnectorid", "mwflow"])


def _region(rows):
    return pd.DataFrame(rows, columns=["run_datetime", "interval_datetime", "regionid", "demand"])


def _bounds(rows):
    return pd.DataFrame(rows, columns=["duid", "maxavail"])


def _duid_term(cid, duid, factor):
    return {"constraintid": cid, "term_type": "duid", "term_id": duid, "factor": factor}


class TestExactRecovery:
    def test_well_posed_noiseless_system_recovers_known_generation(self):
        # True g: A=30, B=50, C=20. Three independent constraints pin them exactly.
        lhs = _lhs([
            {"run_datetime": RUN, "interval_datetime": IVL, "constraintid": "C1", "lhs": 30.0},
            {"run_datetime": RUN, "interval_datetime": IVL, "constraintid": "C2", "lhs": 80.0},
            {"run_datetime": RUN, "interval_datetime": IVL, "constraintid": "C3", "lhs": 70.0},
        ])
        terms = _terms([
            _duid_term("C1", "A", 1.0),
            _duid_term("C2", "A", 1.0), _duid_term("C2", "B", 1.0),
            _duid_term("C3", "B", 1.0), _duid_term("C3", "C", 1.0),
        ])

        out = solve_unit_generation(lhs, terms, _ic([]), _region([]))

        recovered = out.set_index("duid")["mw_inferred"]
        assert recovered["A"] == pytest.approx(30.0, abs=1e-6)
        assert recovered["B"] == pytest.approx(50.0, abs=1e-6)
        assert recovered["C"] == pytest.approx(20.0, abs=1e-6)
        assert (out["quality"] == "good").all()
        assert (out["system_residual"] < 1e-6).all()
        assert list(out.columns) == OUTPUT_COLUMNS


class TestBoundsRespected:
    def test_upper_bound_clamps_solution(self):
        lhs = _lhs([
            {"run_datetime": RUN, "interval_datetime": IVL, "constraintid": "C1", "lhs": 100.0},
        ])
        terms = _terms([_duid_term("C1", "A", 1.0)])
        bounds = _bounds([{"duid": "A", "maxavail": 40.0}])

        out = solve_unit_generation(lhs, terms, _ic([]), _region([]), bounds=bounds)

        assert out.iloc[0]["mw_inferred"] <= 40.0 + 1e-9
        assert out.iloc[0]["mw_inferred"] == pytest.approx(40.0, abs=1e-6)

    def test_per_interval_bounds_apply_to_matching_interval_only(self):
        ivl2 = pd.Timestamp("2026-07-09 11:00:00")
        lhs = _lhs([
            {"run_datetime": RUN, "interval_datetime": IVL, "constraintid": "C1", "lhs": 100.0},
            {"run_datetime": RUN, "interval_datetime": ivl2, "constraintid": "C1", "lhs": 100.0},
        ])
        terms = _terms([_duid_term("C1", "A", 1.0)])
        bounds = pd.DataFrame(
            [{"interval_datetime": IVL, "duid": "A", "maxavail": 40.0}],
            columns=["interval_datetime", "duid", "maxavail"],
        )

        out = solve_unit_generation(lhs, terms, _ic([]), _region([]), bounds=bounds)

        by_interval = out.set_index("interval_datetime")["mw_inferred"]
        # IVL is capped at its MAXAVAIL; ivl2 has no bound row so it gets the large default cap.
        assert by_interval[IVL] == pytest.approx(40.0, abs=1e-6)
        assert by_interval[ivl2] == pytest.approx(100.0, abs=1e-6)

    def test_solution_is_non_negative(self):
        lhs = _lhs([
            {"run_datetime": RUN, "interval_datetime": IVL, "constraintid": "C1", "lhs": -50.0},
        ])
        terms = _terms([_duid_term("C1", "A", 1.0)])

        out = solve_unit_generation(lhs, terms, _ic([]), _region([]))
        assert out.iloc[0]["mw_inferred"] >= -1e-9


class TestRankDeficiency:
    def test_fixed_sum_pair_unidentifiable_pinned_unit_good(self):
        # C1 pins only the sum A+B; C2 pins C alone. A and B are not separable.
        lhs = _lhs([
            {"run_datetime": RUN, "interval_datetime": IVL, "constraintid": "C1", "lhs": 100.0},
            {"run_datetime": RUN, "interval_datetime": IVL, "constraintid": "C2", "lhs": 40.0},
        ])
        terms = _terms([
            _duid_term("C1", "A", 1.0), _duid_term("C1", "B", 1.0),
            _duid_term("C2", "C", 1.0),
        ])

        out = solve_unit_generation(lhs, terms, _ic([]), _region([]))
        quality = out.set_index("duid")["quality"]

        assert quality["A"] == "unidentifiable"
        assert quality["B"] == "unidentifiable"
        assert quality["C"] == "good"
        # Unidentifiable units still appear with an MW value for downstream rendering.
        assert out["mw_inferred"].notna().all()


class TestKnownTermSubstitution:
    def test_interconnector_value_moved_to_rhs(self):
        lhs = _lhs([
            {"run_datetime": RUN, "interval_datetime": IVL, "constraintid": "C1", "lhs": 50.0},
        ])
        terms = _terms([
            _duid_term("C1", "A", 2.0),
            {"constraintid": "C1", "term_type": "interconnector", "term_id": "IC1", "factor": -1.0},
        ])
        ic = _ic([{"run_datetime": RUN, "interval_datetime": IVL, "interconnectorid": "IC1", "mwflow": 10.0}])

        out = solve_unit_generation(lhs, terms, ic, _region([]))

        # b = 50 - (-1*10) = 60; 2*A = 60 => A = 30.
        assert out.iloc[0]["mw_inferred"] == pytest.approx(30.0, abs=1e-6)

    def test_missing_interconnector_flow_drops_that_constraint(self):
        lhs = _lhs([
            {"run_datetime": RUN, "interval_datetime": IVL, "constraintid": "C1", "lhs": 100.0},
            {"run_datetime": RUN, "interval_datetime": IVL, "constraintid": "C2", "lhs": 50.0},
        ])
        terms = _terms([
            _duid_term("C1", "A", 1.0),
            {"constraintid": "C1", "term_type": "interconnector", "term_id": "IC1", "factor": 1.0},
            _duid_term("C2", "B", 1.0),
            {"constraintid": "C2", "term_type": "interconnector", "term_id": "IC2", "factor": 1.0},
        ])
        # Flow present only for IC1; C2's IC2 is unresolvable so C2 (and unit B) is dropped.
        ic = _ic([{"run_datetime": RUN, "interval_datetime": IVL, "interconnectorid": "IC1", "mwflow": 30.0}])

        out = solve_unit_generation(lhs, terms, ic, _region([]))

        assert set(out["duid"]) == {"A"}
        assert out.iloc[0]["mw_inferred"] == pytest.approx(70.0, abs=1e-6)


class TestRegionTermExclusion:
    def test_region_term_with_no_demand_drops_constraint(self):
        lhs = _lhs([
            {"run_datetime": RUN, "interval_datetime": IVL, "constraintid": "C1", "lhs": 50.0},
            {"run_datetime": RUN, "interval_datetime": IVL, "constraintid": "C2", "lhs": 100.0},
        ])
        terms = _terms([
            _duid_term("C1", "A", 1.0),
            _duid_term("C2", "B", 1.0),
            {"constraintid": "C2", "term_type": "region", "term_id": "NSW1", "factor": 1.0},
        ])
        # No region demand supplied => C2 dropped, A still solved from C1.
        out = solve_unit_generation(lhs, terms, _ic([]), _region([]))

        assert set(out["duid"]) == {"A"}
        assert out.iloc[0]["mw_inferred"] == pytest.approx(50.0, abs=1e-6)

    def test_region_demand_substituted_when_supplied(self):
        lhs = _lhs([
            {"run_datetime": RUN, "interval_datetime": IVL, "constraintid": "C1", "lhs": 100.0},
        ])
        terms = _terms([
            _duid_term("C1", "A", 1.0),
            {"constraintid": "C1", "term_type": "region", "term_id": "NSW1", "factor": 0.5},
        ])
        region = _region([{"run_datetime": RUN, "interval_datetime": IVL, "regionid": "NSW1", "demand": 40.0}])

        out = solve_unit_generation(lhs, terms, _ic([]), region)

        # b = 100 - 0.5*40 = 80; A = 80.
        assert out.iloc[0]["mw_inferred"] == pytest.approx(80.0, abs=1e-6)


class TestEmptyInputs:
    def test_empty_lhs_returns_empty_with_columns(self):
        out = solve_unit_generation(_lhs([]), _terms([]), _ic([]), _region([]))
        assert out.empty
        assert list(out.columns) == OUTPUT_COLUMNS

    def test_all_nan_lhs_returns_empty(self):
        lhs = _lhs([
            {"run_datetime": RUN, "interval_datetime": IVL, "constraintid": "C1", "lhs": np.nan},
        ])
        terms = _terms([_duid_term("C1", "A", 1.0)])
        out = solve_unit_generation(lhs, terms, _ic([]), _region([]))
        assert out.empty
        assert list(out.columns) == OUTPUT_COLUMNS

    def test_n_equations_counts_constraints_per_unit(self):
        lhs = _lhs([
            {"run_datetime": RUN, "interval_datetime": IVL, "constraintid": "C1", "lhs": 30.0},
            {"run_datetime": RUN, "interval_datetime": IVL, "constraintid": "C2", "lhs": 80.0},
        ])
        terms = _terms([
            _duid_term("C1", "A", 1.0),
            _duid_term("C2", "A", 1.0), _duid_term("C2", "B", 1.0),
        ])
        out = solve_unit_generation(lhs, terms, _ic([]), _region([]))
        n_by_duid = out.set_index("duid")["n_equations"]
        assert n_by_duid["A"] == 2
        assert n_by_duid["B"] == 1


def _inferred_row(run, ivl, duid, mw, quality="good", n_eq=3):
    return {
        "run_datetime": run, "interval_datetime": ivl, "duid": duid, "mw_inferred": mw,
        "quality": quality, "n_equations": n_eq, "system_residual": 0.0,
    }


class TestShortLeadLatestRunSelection:
    def test_picks_latest_run_within_lead_window(self):
        ivl = pd.Timestamp("2026-07-09 11:00:00")
        inferred = pd.DataFrame([
            _inferred_row(pd.Timestamp("2026-07-09 09:00:00"), ivl, "A", 10.0),
            _inferred_row(pd.Timestamp("2026-07-09 10:00:00"), ivl, "A", 20.0),
        ])
        out = select_short_lead_latest_run(inferred)
        assert len(out) == 1
        assert out.iloc[0]["mw_inferred"] == 20.0

    def test_boundary_lead_exactly_at_limit_is_included(self):
        run = pd.Timestamp("2026-07-09 09:00:00")
        ivl = run + pd.Timedelta(hours=2)  # exactly SHORT_LEAD_HOURS
        inferred = pd.DataFrame([_inferred_row(run, ivl, "A", 10.0)])
        out = select_short_lead_latest_run(inferred)
        assert len(out) == 1

    def test_drops_rows_beyond_lead_window(self):
        run = pd.Timestamp("2026-07-09 05:00:00")
        ivl = pd.Timestamp("2026-07-09 11:00:00")  # 6h lead
        inferred = pd.DataFrame([_inferred_row(run, ivl, "A", 10.0)])
        assert select_short_lead_latest_run(inferred).empty

    def test_drops_non_positive_lead(self):
        run = pd.Timestamp("2026-07-09 11:00:00")
        inferred = pd.DataFrame([_inferred_row(run, run, "A", 10.0)])  # lead == 0
        assert select_short_lead_latest_run(inferred).empty

    def test_empty_input_returns_empty(self):
        out = select_short_lead_latest_run(pd.DataFrame(columns=OUTPUT_COLUMNS))
        assert out.empty


class TestAggregateRealised30min:
    def test_ceils_to_30min_period_ending_mean(self):
        dispatch = pd.DataFrame([
            {"settlementdate": pd.Timestamp("2026-07-09 10:05:00"), "duid": "A", "scadavalue": 10.0},
            {"settlementdate": pd.Timestamp("2026-07-09 10:25:00"), "duid": "A", "scadavalue": 20.0},
        ])
        out = aggregate_realised_30min(dispatch)
        assert len(out) == 1
        assert out.iloc[0]["interval_datetime"] == pd.Timestamp("2026-07-09 10:30:00")
        assert out.iloc[0]["mw_realised"] == pytest.approx(15.0)

    def test_empty_input_returns_empty_with_columns(self):
        out = aggregate_realised_30min(pd.DataFrame(columns=["settlementdate", "duid", "scadavalue"]))
        assert out.empty
        assert list(out.columns) == ["interval_datetime", "duid", "mw_realised"]


class TestComputeUnitTracking:
    def _series(self, duid, mws, mws_realised):
        run = pd.Timestamp("2026-07-09 10:00:00")
        ivls = [pd.Timestamp("2026-07-09 10:30:00") + pd.Timedelta(minutes=30 * i) for i in range(len(mws))]
        inferred = pd.DataFrame([_inferred_row(run, ivl, duid, mw) for ivl, mw in zip(ivls, mws)])
        realised = pd.DataFrame({"interval_datetime": ivls, "duid": duid, "mw_realised": mws_realised})
        return inferred, realised

    def test_corr_and_mae_correctness(self):
        mws, mws_realised = [10.0, 20.0, 30.0, 40.0], [12.0, 18.0, 33.0, 38.0]
        inferred, realised = self._series("A", mws, mws_realised)

        out = compute_unit_tracking(inferred, realised)

        row = out.iloc[0]
        expected_corr = pd.Series(mws).corr(pd.Series(mws_realised))
        expected_mae = (pd.Series(mws) - pd.Series(mws_realised)).abs().mean()
        assert row["duid"] == "A"
        assert row["n"] == 4
        assert row["corr"] == pytest.approx(expected_corr)
        assert row["mae"] == pytest.approx(expected_mae)
        assert row["quality"] == "good"
        assert row["median_n_equations"] == 3
        assert row["tracking"] == (expected_corr >= TRACKING_CORR_THRESHOLD)

    def test_anti_correlated_unit_is_not_tracking(self):
        inferred, realised = self._series("B", [10.0, 20.0, 30.0, 40.0], [40.0, 30.0, 20.0, 10.0])
        out = compute_unit_tracking(inferred, realised)
        assert not out.iloc[0]["tracking"]
        assert out.iloc[0]["corr"] < 0

    def test_constant_series_has_nan_corr_not_tracking_and_sorts_last(self):
        # Correlation is undefined for a zero-variance series; such a unit must not be trusted.
        varying_inf, varying_real = self._series("A", [10.0, 20.0, 30.0, 40.0], [12.0, 18.0, 33.0, 38.0])
        const_inf, const_real = self._series("CONST", [50.0, 50.0, 50.0, 50.0], [50.0, 50.0, 50.0, 50.0])
        inferred = pd.concat([varying_inf, const_inf], ignore_index=True)
        realised = pd.concat([varying_real, const_real], ignore_index=True)

        out = compute_unit_tracking(inferred, realised)

        const_row = out[out["duid"] == "CONST"].iloc[0]
        assert pd.isna(const_row["corr"])
        assert not const_row["tracking"]
        assert const_row["mae"] == pytest.approx(0.0)
        # NaN corr sorts after every real correlation.
        assert out.iloc[-1]["duid"] == "CONST"

    def test_empty_overlap_returns_empty_frame_with_columns(self):
        inferred = pd.DataFrame([
            _inferred_row(pd.Timestamp("2026-07-09 10:00:00"), pd.Timestamp("2026-07-09 10:30:00"), "A", 10.0),
        ])
        realised = pd.DataFrame({
            "interval_datetime": [pd.Timestamp("2026-07-10 10:30:00")], "duid": ["A"], "mw_realised": [5.0],
        })
        out = compute_unit_tracking(inferred, realised)
        assert out.empty
        assert list(out.columns) == TRACKING_COLUMNS

    def test_empty_inputs_return_empty_frame(self):
        out = compute_unit_tracking(pd.DataFrame(columns=OUTPUT_COLUMNS), pd.DataFrame(columns=["interval_datetime", "duid", "mw_realised"]))
        assert out.empty
        assert list(out.columns) == TRACKING_COLUMNS


class TestBuildPairedSeries:
    def test_outer_join_keeps_gaps_as_nan(self):
        run = pd.Timestamp("2026-07-09 10:00:00")
        ivl1 = pd.Timestamp("2026-07-09 10:30:00")
        ivl2 = pd.Timestamp("2026-07-09 11:00:00")
        inferred = pd.DataFrame([_inferred_row(run, ivl1, "A", 10.0)])
        realised = pd.DataFrame({"interval_datetime": [ivl1, ivl2], "duid": ["A", "A"], "mw_realised": [12.0, 22.0]})

        out = build_paired_series(inferred, realised)

        assert len(out) == 2
        row2 = out[out["interval_datetime"] == ivl2].iloc[0]
        assert pd.isna(row2["mw_inferred"])
        assert row2["mw_realised"] == 22.0
        assert list(out.columns) == SERIES_COLUMNS

    def test_empty_inputs_return_empty_with_columns(self):
        out = build_paired_series(
            pd.DataFrame(columns=OUTPUT_COLUMNS),
            pd.DataFrame(columns=["interval_datetime", "duid", "mw_realised"]),
        )
        assert out.empty
        assert list(out.columns) == SERIES_COLUMNS
