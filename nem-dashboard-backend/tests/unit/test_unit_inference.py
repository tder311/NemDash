"""Unit tests for infer_unit_generation (single-unknown-DUID constraint backsolving)."""

import pandas as pd
import pytest

from app.unit_inference import infer_unit_generation

RUN = pd.Timestamp("2026-07-09 10:00:00")
IVL = pd.Timestamp("2026-07-09 10:30:00")
IVL2 = pd.Timestamp("2026-07-09 11:00:00")


def _constraints(rows):
    return pd.DataFrame(rows, columns=["run_datetime", "interval_datetime", "constraintid", "lhs"])


def _interconnectors(rows):
    return pd.DataFrame(rows, columns=["run_datetime", "interval_datetime", "interconnectorid", "mwflow"])


def _terms(rows):
    return pd.DataFrame(rows, columns=["constraintid", "version", "term_type", "term_id", "factor"])


class TestSingleSolvableConstraint:
    def test_solves_mw_with_interconnector_substitution(self):
        constraints = _constraints([
            {"run_datetime": RUN, "interval_datetime": IVL, "constraintid": "C1", "lhs": 100.0},
        ])
        interconnectors = _interconnectors([
            {"run_datetime": RUN, "interval_datetime": IVL, "interconnectorid": "IC1", "mwflow": 30.0},
        ])
        terms = _terms([
            {"constraintid": "C1", "version": 1, "term_type": "duid", "term_id": "DUID_A", "factor": 1.0},
            {"constraintid": "C1", "version": 1, "term_type": "interconnector", "term_id": "IC1", "factor": 1.0},
        ])

        out = infer_unit_generation(constraints, interconnectors, terms)

        assert len(out) == 1
        row = out.iloc[0]
        assert row["duid"] == "DUID_A"
        assert row["constraintid"] == "C1"
        assert row["mw_inferred"] == pytest.approx(70.0)  # (100 - 1*30) / 1
        assert row["n_terms"] == 2
        assert list(out.columns) == [
            "run_datetime", "interval_datetime", "duid", "mw_inferred", "constraintid", "n_terms",
        ]

    def test_handles_negative_factor_correctly(self):
        constraints = _constraints([
            {"run_datetime": RUN, "interval_datetime": IVL, "constraintid": "C1", "lhs": 50.0},
        ])
        interconnectors = _interconnectors([
            {"run_datetime": RUN, "interval_datetime": IVL, "interconnectorid": "IC1", "mwflow": 10.0},
        ])
        terms = _terms([
            {"constraintid": "C1", "version": 1, "term_type": "duid", "term_id": "DUID_A", "factor": 2.0},
            {"constraintid": "C1", "version": 1, "term_type": "interconnector", "term_id": "IC1", "factor": -1.0},
        ])

        out = infer_unit_generation(constraints, interconnectors, terms)

        # lhs = 2*mw + (-1)*10 => mw = (50 - (-10)) / 2 = 30
        assert out.iloc[0]["mw_inferred"] == pytest.approx(30.0)


class TestTwoUnknownDuidsExcluded:
    def test_constraint_with_two_duid_terms_is_excluded(self):
        constraints = _constraints([
            {"run_datetime": RUN, "interval_datetime": IVL, "constraintid": "C2", "lhs": 100.0},
        ])
        interconnectors = _interconnectors([])
        terms = _terms([
            {"constraintid": "C2", "version": 1, "term_type": "duid", "term_id": "DUID_A", "factor": 1.0},
            {"constraintid": "C2", "version": 1, "term_type": "duid", "term_id": "DUID_B", "factor": 1.0},
        ])

        out = infer_unit_generation(constraints, interconnectors, terms)
        assert out.empty


class TestRegionTermExcluded:
    def test_constraint_with_region_term_is_fully_excluded(self):
        constraints = _constraints([
            {"run_datetime": RUN, "interval_datetime": IVL, "constraintid": "C3", "lhs": 100.0},
        ])
        interconnectors = _interconnectors([])
        terms = _terms([
            {"constraintid": "C3", "version": 1, "term_type": "duid", "term_id": "DUID_A", "factor": 1.0},
            {"constraintid": "C3", "version": 1, "term_type": "region", "term_id": "NSW1", "factor": 1.0},
        ])

        out = infer_unit_generation(constraints, interconnectors, terms)
        assert out.empty


class TestInterconnectorSubstitution:
    def test_missing_mwflow_excludes_only_that_run_interval(self):
        constraints = _constraints([
            {"run_datetime": RUN, "interval_datetime": IVL, "constraintid": "C1", "lhs": 100.0},
            {"run_datetime": RUN, "interval_datetime": IVL2, "constraintid": "C1", "lhs": 90.0},
        ])
        # mwflow only present for IVL, not IVL2.
        interconnectors = _interconnectors([
            {"run_datetime": RUN, "interval_datetime": IVL, "interconnectorid": "IC1", "mwflow": 20.0},
        ])
        terms = _terms([
            {"constraintid": "C1", "version": 1, "term_type": "duid", "term_id": "DUID_A", "factor": 1.0},
            {"constraintid": "C1", "version": 1, "term_type": "interconnector", "term_id": "IC1", "factor": 1.0},
        ])

        out = infer_unit_generation(constraints, interconnectors, terms)

        assert len(out) == 1
        assert out.iloc[0]["interval_datetime"] == IVL
        assert out.iloc[0]["mw_inferred"] == pytest.approx(80.0)

    def test_multiple_interconnector_terms_all_substituted(self):
        constraints = _constraints([
            {"run_datetime": RUN, "interval_datetime": IVL, "constraintid": "C1", "lhs": 100.0},
        ])
        interconnectors = _interconnectors([
            {"run_datetime": RUN, "interval_datetime": IVL, "interconnectorid": "IC1", "mwflow": 20.0},
            {"run_datetime": RUN, "interval_datetime": IVL, "interconnectorid": "IC2", "mwflow": 5.0},
        ])
        terms = _terms([
            {"constraintid": "C1", "version": 1, "term_type": "duid", "term_id": "DUID_A", "factor": 1.0},
            {"constraintid": "C1", "version": 1, "term_type": "interconnector", "term_id": "IC1", "factor": 1.0},
            {"constraintid": "C1", "version": 1, "term_type": "interconnector", "term_id": "IC2", "factor": 2.0},
        ])

        out = infer_unit_generation(constraints, interconnectors, terms)

        # known_sum = 1*20 + 2*5 = 30 => mw = 100 - 30 = 70
        assert out.iloc[0]["mw_inferred"] == pytest.approx(70.0)


class TestNaNLhsExcluded:
    def test_nan_lhs_rows_are_unusable(self):
        constraints = _constraints([
            {"run_datetime": RUN, "interval_datetime": IVL, "constraintid": "C1", "lhs": float("nan")},
        ])
        interconnectors = _interconnectors([])
        terms = _terms([
            {"constraintid": "C1", "version": 1, "term_type": "duid", "term_id": "DUID_A", "factor": 1.0},
        ])

        out = infer_unit_generation(constraints, interconnectors, terms)
        assert out.empty


class TestMultipleConstraintsSameDuid:
    def test_multiple_constraints_inferring_same_duid_are_all_kept(self):
        constraints = _constraints([
            {"run_datetime": RUN, "interval_datetime": IVL, "constraintid": "C1", "lhs": 100.0},
            {"run_datetime": RUN, "interval_datetime": IVL, "constraintid": "C4", "lhs": 50.0},
        ])
        interconnectors = _interconnectors([])
        terms = _terms([
            {"constraintid": "C1", "version": 1, "term_type": "duid", "term_id": "DUID_A", "factor": 1.0},
            {"constraintid": "C4", "version": 1, "term_type": "duid", "term_id": "DUID_A", "factor": 1.0},
        ])

        out = infer_unit_generation(constraints, interconnectors, terms)

        assert len(out) == 2
        assert set(out["constraintid"]) == {"C1", "C4"}
        assert (out["duid"] == "DUID_A").all()


class TestEmptyInputs:
    def test_empty_constraints_returns_empty_with_columns(self):
        out = infer_unit_generation(
            _constraints([]), _interconnectors([]), _terms([]),
        )
        assert out.empty
        assert list(out.columns) == [
            "run_datetime", "interval_datetime", "duid", "mw_inferred", "constraintid", "n_terms",
        ]

    def test_no_duid_terms_for_constraint_returns_empty(self):
        constraints = _constraints([
            {"run_datetime": RUN, "interval_datetime": IVL, "constraintid": "C1", "lhs": 100.0},
        ])
        interconnectors = _interconnectors([
            {"run_datetime": RUN, "interval_datetime": IVL, "interconnectorid": "IC1", "mwflow": 20.0},
        ])
        terms = _terms([
            {"constraintid": "C1", "version": 1, "term_type": "interconnector", "term_id": "IC1", "factor": 1.0},
        ])

        out = infer_unit_generation(constraints, interconnectors, terms)
        assert out.empty
