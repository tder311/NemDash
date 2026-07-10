"""Unit tests for solve_unit_generation (joint least-squares unit backsolve)."""

import numpy as np
import pandas as pd
import pytest

from app.joint_inference import OUTPUT_COLUMNS, solve_unit_generation

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
