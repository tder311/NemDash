"""Unit tests for the price forecaster.

All pure (no database). They lean on a synthetic price+PASA frame with a known
signal so we can assert the model actually learns rather than just runs.
"""

import numpy as np
import pandas as pd
import pytest

from app.forecaster import (
    HORIZON_INTERVALS,
    PD_FEATURES,
    combine_forward_pasa,
    PASA_FEATURES,
    REGIONS,
    PriceForecaster,
    _pasa_derived_features,
    _region_one_hot,
    assemble_features,
    build_calendar_features,
    dedup_pasa_runs,
    merge_price_pasa,
    predispatch_window_features,
    select_runs_at_lead,
    to_30min_price,
    walk_forward_validate,
)

CAP = {"NSW1": 12000.0, "QLD1": 9000.0, "VIC1": 8000.0, "SA1": 3500.0, "TAS1": 2000.0}


def _synthetic_merged(n_days: int = 30, regions=("NSW1", "SA1"), seed: int = 0) -> pd.DataFrame:
    """price+PASA frame with a deliberate signal.

    Price = base + time-of-day shape + utilisation**3 lift, plus a large spike
    when surplusreserve goes negative (LOR-style scarcity).
    """
    rng = np.random.default_rng(seed)
    intervals = pd.date_range("2025-01-01", periods=n_days * 48, freq="30min")
    rows = []
    for region in regions:
        cap = CAP[region]
        for ts in intervals:
            sp = ts.hour * 2 + ts.minute // 30  # settlement period 0..47
            shape = 0.6 + 0.25 * np.sin(2 * np.pi * (sp - 14) / 48)
            if 34 <= sp <= 39:  # evening peak
                shape += 0.15
            demand50 = cap * shape * (0.9 + 0.1 * rng.random())
            spread = cap * 0.04 * (0.5 + rng.random())
            reservereq = cap * 0.10
            avail = cap * (0.82 + 0.18 * rng.random())
            capreq = demand50 + reservereq
            surplus = avail - capreq
            util = demand50 / avail

            price = 30 + 80 * util**3 + 20 * np.sin(2 * np.pi * (sp - 36) / 48)
            if surplus < 0:
                price += 2000 * (-surplus / cap)
            price += rng.normal(0, 5)

            rows.append(
                {
                    "interval_datetime": ts,
                    "region": region,
                    "price": price,
                    "demand10": demand50 - spread / 2,
                    "demand50": demand50,
                    "demand90": demand50 + spread / 2,
                    "reservereq": reservereq,
                    "aggregatecapacityavailable": avail,
                    "aggregatepasaavailability": avail * 0.98,
                    "surplusreserve": surplus,
                }
            )
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- #
# Feature engineering
# --------------------------------------------------------------------------- #


def test_calendar_features_shapes_and_ranges():
    df = _synthetic_merged(n_days=3, regions=("NSW1",))
    feats = build_calendar_features(df["interval_datetime"], df["region"])
    assert len(feats) == len(df)
    for col in ("tod_sin", "tod_cos", "doy_sin", "doy_cos"):
        assert feats[col].between(-1.0, 1.0).all()
    # Jan 2025: 1st is a Wed; weekend flag must be 0/1 only.
    assert set(feats["is_weekend"].unique()) <= {0, 1}


def test_region_one_hot_is_exclusive():
    region = pd.Series(["NSW1", "SA1", "TAS1"])
    oh = _region_one_hot(region)
    assert list(oh.columns) == [f"region_{r}" for r in REGIONS]
    assert (oh.sum(axis=1) == 1).all()  # exactly one region set per row


def test_pasa_derived_features_present_and_safe():
    df = _synthetic_merged(n_days=2, regions=("SA1",))
    out = _pasa_derived_features(df)
    assert {"demand_spread", "capacity_margin", "utilisation"} <= set(out.columns)
    # spread is demand90 - demand10, must be non-negative.
    assert (out["demand_spread"] >= 0).all()

    # divide-by-zero must yield NaN, not raise.
    zero = pd.DataFrame(
        {"demand50": [100.0], "demand90": [110.0], "demand10": [90.0],
         "aggregatecapacityavailable": [0.0]}
    )
    safe = _pasa_derived_features(zero)
    assert np.isnan(safe["utilisation"].iloc[0])


def test_assemble_features_drops_null_target_and_excludes_price():
    df = _synthetic_merged(n_days=3, regions=("NSW1", "SA1"))
    df.loc[df.index[:5], "price"] = np.nan  # 5 unusable rows
    X, y, names = assemble_features(df, include_target=True)
    assert len(X) == len(y) == len(df) - 5
    assert "price" not in names
    # raw PASA cols and the derived cols both flow through.
    assert set(PASA_FEATURES) <= set(names)
    assert {"demand_spread", "utilisation"} <= set(names)


# --------------------------------------------------------------------------- #
# Joins / leakage guards
# --------------------------------------------------------------------------- #


def test_dedup_pasa_runs_drops_future_and_keeps_latest_valid():
    interval = "2025-01-15 18:00:00"
    base = {c: 1.0 for c in PASA_FEATURES}
    pasa = pd.DataFrame(
        [
            {"run_datetime": "2025-01-14 06:00:00", "interval_datetime": interval, "regionid": "NSW1", **base},
            {"run_datetime": "2025-01-15 12:00:00", "interval_datetime": interval, "regionid": "NSW1", **base},  # latest valid
            {"run_datetime": "2025-01-16 06:00:00", "interval_datetime": interval, "regionid": "NSW1", **base},  # future -> drop
        ]
    )
    out = dedup_pasa_runs(pasa)
    assert len(out) == 1
    assert out["run_datetime"].iloc[0] == pd.Timestamp("2025-01-15 12:00:00")


def test_merge_price_pasa_inner_joins_on_interval_region():
    df = _synthetic_merged(n_days=2, regions=("NSW1", "SA1"))
    price = df[["interval_datetime", "region", "price"]].rename(
        columns={"interval_datetime": "settlementdate"}
    )
    pasa = df[["interval_datetime", "region"] + PASA_FEATURES].rename(
        columns={"region": "regionid"}
    )
    merged = merge_price_pasa(price, pasa)
    assert len(merged) == len(df)
    assert {"price", "region", "interval_datetime"} <= set(merged.columns)


def test_merge_price_pasa_normalises_region_suffix():
    """price_data.region ('NSW') must still join PASA regionid ('NSW1')."""
    price = pd.DataFrame(
        {
            "settlementdate": pd.to_datetime(["2025-01-15 18:00", "2025-01-15 18:30"]),
            "region": ["NSW", "NSW"],  # no trailing '1', as stored in price_data
            "price": [100.0, 120.0],
        }
    )
    pasa = pd.DataFrame(
        {
            "interval_datetime": pd.to_datetime(["2025-01-15 18:00", "2025-01-15 18:30"]),
            "regionid": ["NSW1", "NSW1"],
            **{c: [1.0, 1.0] for c in PASA_FEATURES},
        }
    )
    merged = merge_price_pasa(price, pasa)
    assert len(merged) == 2  # would be 0 without region normalisation
    assert set(merged["region"]) == {"NSW1"}


# --------------------------------------------------------------------------- #
# Model
# --------------------------------------------------------------------------- #

FAST = {"n_estimators": 80, "max_depth": 4, "learning_rate": 0.1, "n_jobs": 1}


def test_model_learns_signal_beats_mean_baseline():
    df = _synthetic_merged(n_days=40, regions=("NSW1", "SA1"), seed=1)
    df = df.sort_values("interval_datetime").reset_index(drop=True)
    X, y, _ = assemble_features(df)
    split = int(len(X) * 0.8)
    model = PriceForecaster(FAST).train(X.iloc[:split], y.iloc[:split])
    pred = model.predict(X.iloc[split:])
    truth = y.iloc[split:].values

    model_mae = np.mean(np.abs(pred - truth))
    baseline_mae = np.mean(np.abs(y.iloc[:split].mean() - truth))
    assert model_mae < 0.5 * baseline_mae  # must clearly beat predicting the mean


def test_save_load_roundtrip(tmp_path):
    df = _synthetic_merged(n_days=10, regions=("NSW1",))
    X, y, _ = assemble_features(df)
    model = PriceForecaster(FAST).train(X, y)
    path = tmp_path / "model.joblib"
    model.save(str(path))

    reloaded = PriceForecaster.load(str(path))
    np.testing.assert_allclose(model.predict(X), reloaded.predict(X), rtol=1e-5)
    assert reloaded.card.feature_names == model.card.feature_names


def test_predict_realigns_missing_columns():
    """A region absent at inference must not break column alignment."""
    df = _synthetic_merged(n_days=10, regions=("NSW1", "SA1"))
    X, y, _ = assemble_features(df)
    model = PriceForecaster(FAST).train(X, y)
    # Drop a one-hot column the model trained on; predict() should refill it.
    pred = model.predict(X.drop(columns=["region_SA1"]))
    assert len(pred) == len(X)


def test_walk_forward_validate_runs():
    df = _synthetic_merged(n_days=30, regions=("NSW1",), seed=2)
    X, y, _ = assemble_features(df)
    res = walk_forward_validate(X, y, df["interval_datetime"], n_splits=3, params=FAST)
    assert len(res["folds"]) >= 1
    assert np.isfinite(res["mae"])


def _runs_for_interval(interval, runs, region="NSW1"):
    base = {c: 1.0 for c in PASA_FEATURES}
    return pd.DataFrame(
        [{"run_datetime": r, "interval_datetime": interval, "regionid": region, **base} for r in runs]
    )


def test_select_runs_at_lead_picks_closest_to_target_and_drops_out_of_band():
    interval = pd.Timestamp("2025-01-15 18:00")
    pasa = _runs_for_interval(
        interval,
        [
            "2025-01-14 18:00",  # 24h  -> should win
            "2025-01-14 12:00",  # 30h  (in band)
            "2025-01-15 06:00",  # 12h  (in band edge)
            "2025-01-15 17:30",  # 0.5h -> out of band, dropped
        ],
    )
    out = select_runs_at_lead(pasa, target_lead_hours=24, tolerance_hours=12)
    assert len(out) == 1
    assert out["run_datetime"].iloc[0] == pd.Timestamp("2025-01-14 18:00")


def test_select_runs_at_lead_tiebreak_prefers_longer_lead():
    interval = pd.Timestamp("2025-01-15 18:00")
    # 27h and 21h are equidistant from 24h; the longer (earlier) lead should win.
    pasa = _runs_for_interval(interval, ["2025-01-14 15:00", "2025-01-14 21:00"])
    out = select_runs_at_lead(pasa, target_lead_hours=24, tolerance_hours=12)
    assert out["run_datetime"].iloc[0] == pd.Timestamp("2025-01-14 15:00")


def test_to_30min_price_is_period_ending_block_mean():
    # six 5-min RRPs ending 00:05..00:30 should average to the block ending 00:30
    ts = pd.date_range("2025-01-01 00:05", periods=6, freq="5min")
    df = pd.DataFrame({"settlementdate": ts, "region": ["NSW"] * 6,
                       "price": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]})
    out = to_30min_price(df)
    row = out[out["settlementdate"] == pd.Timestamp("2025-01-01 00:30")]
    assert len(row) == 1
    assert abs(row["price"].iloc[0] - 35.0) < 1e-9  # mean(10..60)
    assert set(out["region"]) == {"NSW1"}  # region normalised to regionid


# --------------------------------------------------------------------------- #
# combine_forward_pasa
# --------------------------------------------------------------------------- #


def _forward_rows(intervals, demand50=1.0, region="NSW1"):
    base = {c: 1.0 for c in PASA_FEATURES}
    base["demand50"] = demand50
    return pd.DataFrame(
        [
            {
                "run_datetime": pd.Timestamp("2025-01-15 00:00"),
                "interval_datetime": pd.Timestamp(t),
                "regionid": region,
                **base,
            }
            for t in intervals
        ]
    )


def test_combine_forward_pasa_drops_past_intervals():
    now = pd.Timestamp("2025-01-15 12:00")
    pd_pasa = _forward_rows(pd.date_range("2025-01-15 06:00", periods=24, freq="30min"))
    out = combine_forward_pasa(pd_pasa, pd.DataFrame(), now)
    assert out["interval_datetime"].min() >= now
    assert len(out) == 12  # 12:00..17:30 of the 06:00..17:30 run


def test_combine_forward_pasa_prefers_pd_on_overlap():
    now = pd.Timestamp("2025-01-15 12:00")
    ivals = pd.date_range("2025-01-15 12:00", periods=8, freq="30min")
    pd_pasa = _forward_rows(ivals, demand50=111.0)
    st_pasa = _forward_rows(ivals, demand50=999.0)
    out = combine_forward_pasa(pd_pasa, st_pasa, now)
    assert len(out) == 8
    assert (out["demand50"] == 111.0).all()


def test_combine_forward_pasa_stale_pd_still_covers_near_term_via_st():
    # Regression: a PD run that is entirely in the past must not blank out the
    # next 24h when ST rows cover it.
    now = pd.Timestamp("2025-01-15 12:00")
    pd_pasa = _forward_rows(pd.date_range("2025-01-13 00:00", periods=48, freq="30min"))
    st_pasa = _forward_rows(pd.date_range("2025-01-15 12:00", periods=96, freq="30min"), demand50=999.0)
    out = combine_forward_pasa(pd_pasa, st_pasa, now)
    assert out["interval_datetime"].iloc[0] == now
    assert len(out) == 96
    assert (out["demand50"] == 999.0).all()


def test_combine_forward_pasa_caps_horizon_and_sets_region():
    now = pd.Timestamp("2025-01-15 12:00")
    st_pasa = _forward_rows(pd.date_range("2025-01-15 12:00", periods=400, freq="30min"))
    out = combine_forward_pasa(pd.DataFrame(), st_pasa, now)
    assert len(out) == HORIZON_INTERVALS
    assert (out["region"] == "NSW1").all()


def test_combine_forward_pasa_empty_inputs():
    now = pd.Timestamp("2025-01-15 12:00")
    assert combine_forward_pasa(pd.DataFrame(), pd.DataFrame(), now).empty


def _pd_frame(rrps, region="VIC1", run="2026-07-07 12:00:00", start="2026-07-08 00:30:00"):
    """One PD run: consecutive 30-min intervals with the given rrps."""
    intervals = pd.date_range(start, periods=len(rrps), freq="30min")
    return pd.DataFrame({
        "run_datetime": pd.Timestamp(run),
        "interval_datetime": intervals,
        "regionid": region,
        "rrp": rrps,
    })


def test_pd_window_sustained_block_outscores_isolated_spike():
    # Same max price: one lone $17,000 interval vs a 6-hour block at $17,000.
    lone = _pd_frame([100.0] * 20 + [17000.0] + [100.0] * 20)
    block = _pd_frame([100.0] * 20 + [17000.0] * 12 + [100.0] * 9)
    f_lone = predispatch_window_features(lone)
    f_block = predispatch_window_features(block)
    assert f_block["pd_hours_above_5000"].iloc[0] == 6.0
    assert f_lone["pd_hours_above_5000"].iloc[0] == 0.5
    assert f_block["pd_longest_run_above_300"].iloc[0] == 6.0
    assert f_lone["pd_longest_run_above_300"].iloc[0] == 0.5
    assert f_block["pd_exceedance_sum"].iloc[0] > f_lone["pd_exceedance_sum"].iloc[0]


def test_pd_window_thresholds_and_pointwise():
    f = predispatch_window_features(_pd_frame([100.0, 400.0, 1500.0, 6000.0]))
    row = f.iloc[0]
    assert row["pd_rrp"] == 100.0
    assert row["pd_hours_above_300"] == 1.5   # 400, 1500, 6000
    assert row["pd_hours_above_1000"] == 1.0  # 1500, 6000
    assert row["pd_hours_above_5000"] == 0.5  # 6000
    # window features are shared across the day; pointwise differs per row
    assert f["pd_hours_above_300"].nunique() == 1
    assert f["pd_rrp"].tolist() == [100.0, 400.0, 1500.0, 6000.0]


def test_pd_window_groups_by_run_region_and_day():
    # Two runs for the same day must not bleed into each other.
    a = _pd_frame([17000.0] * 4, run="2026-07-07 12:00:00")
    b = _pd_frame([100.0] * 4, run="2026-07-08 06:00:00")
    f = predispatch_window_features(pd.concat([a, b], ignore_index=True))
    by_run = f.groupby("run_datetime")["pd_hours_above_5000"].max()
    assert by_run[pd.Timestamp("2026-07-07 12:00:00")] == 2.0
    assert by_run[pd.Timestamp("2026-07-08 06:00:00")] == 0.0


def test_pd_window_longest_run_broken_by_dip():
    # 3 above, 1 below, 2 above -> longest contiguous run is 3 intervals = 1.5h.
    f = predispatch_window_features(_pd_frame([500.0, 500.0, 500.0, 100.0, 500.0, 500.0]))
    assert f["pd_longest_run_above_300"].iloc[0] == 1.5
    assert set(PD_FEATURES) <= set(f.columns)
