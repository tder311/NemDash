"""Unit tests for the price forecaster.

All pure (no database). They lean on a synthetic price+PASA frame with a known
signal so we can assert the model actually learns rather than just runs.
"""

import numpy as np
import pandas as pd
import pytest

from app.forecaster import (
    HORIZON_INTERVALS,
    LEAD_BUCKETS,
    PD_FEATURES,
    combine_forward_pasa,
    compute_forecast_accuracy,
    PASA_FEATURES,
    REGIONS,
    PriceForecaster,
    _extend_forward_frame,
    _pasa_derived_features,
    _region_one_hot,
    assemble_features,
    build_calendar_features,
    dedup_pasa_runs,
    lead_envelope_hours,
    merge_price_pasa,
    pinball_loss,
    predict_intervals,
    predispatch_window_features,
    select_runs_at_lead,
    select_runs_at_leads,
    spike_recall,
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
    merged = pd.DataFrame(rows)
    merged["lead_hours"] = 24.0
    for col in PD_FEATURES:
        merged[col] = np.nan
    return merged


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


def test_assemble_features_includes_pd_and_lead():
    merged = _synthetic_merged(n_days=3)
    X, y, names = assemble_features(merged)
    for col in PD_FEATURES + ["lead_hours"]:
        assert col in names
    assert X["lead_hours"].eq(24.0).all()
    assert X["pd_rrp"].isna().all()  # NaN preserved, not zero-filled


def test_assemble_features_raises_without_lead_or_pd():
    merged = _synthetic_merged(n_days=2).drop(columns=["pd_rrp"])
    with pytest.raises(KeyError):
        assemble_features(merged)


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
    pasa["lead_hours"] = 24.0
    pasa["lead_bucket"] = 24.0
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
            "lead_hours": [24.0, 24.0],
            "lead_bucket": [24.0, 24.0],
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


def test_model_learns_pd_scarcity_signal():
    # Price spikes exactly when the PD window features say the day is tight;
    # a trained model must rank tight-day intervals above calm-day intervals.
    merged = _synthetic_merged(n_days=40, regions=("NSW1",), seed=3)
    rng = np.random.default_rng(3)
    days = pd.to_datetime(merged["interval_datetime"]).dt.date
    tight_days = set(rng.choice(sorted(set(days)), size=8, replace=False))
    tight = days.isin(tight_days).to_numpy()
    merged.loc[tight, "pd_hours_above_1000"] = 6.0
    merged.loc[tight, "pd_exceedance_sum"] = 40000.0
    merged.loc[tight, "price"] = merged.loc[tight, "price"] + 2000.0
    X, y, _ = assemble_features(merged)
    model = PriceForecaster({"n_estimators": 120}).train(X, y)
    preds = model.predict(X)
    assert preds[tight].mean() > preds[~tight].mean() + 500


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


def test_quantile_heads_train_predict_and_roundtrip(tmp_path):
    merged = _synthetic_merged(n_days=20, seed=1)
    X, y, _ = assemble_features(merged)
    model = PriceForecaster({"n_estimators": 80}).train(X, y)
    q = model.predict_quantiles(X)
    assert list(q.columns) == ["p10", "p90"]
    # a valid P90 sits above P10 for the overwhelming majority of rows
    assert (q["p90"] >= q["p10"]).mean() > 0.95
    assert model.card.quantile == [0.1, 0.9]
    path = str(tmp_path / "m.joblib")
    model.save(path)
    loaded = PriceForecaster.load(path)
    q2 = loaded.predict_quantiles(X)
    assert np.allclose(q["p90"].values, q2["p90"].values)


def test_predict_quantiles_raises_without_quantile_models():
    model = PriceForecaster()
    model.model = "sentinel"  # point model present, quantile heads absent
    with pytest.raises(RuntimeError):
        model.predict_quantiles(pd.DataFrame({"a": [1.0]}))


def test_p90_reacts_more_to_scarcity_than_p50():
    # Heavy-tailed target: on tight days price sometimes spikes 10x. P90 must
    # separate tight from calm days by more than the P50 does.
    merged = _synthetic_merged(n_days=40, regions=("NSW1",), seed=5)
    rng = np.random.default_rng(5)
    days = pd.to_datetime(merged["interval_datetime"]).dt.date
    tight_days = set(rng.choice(sorted(set(days)), size=10, replace=False))
    tight = days.isin(tight_days).to_numpy()
    merged.loc[tight, "pd_hours_above_1000"] = 6.0
    spike = tight & (rng.random(len(merged)) < 0.15)
    merged.loc[spike, "price"] = merged.loc[spike, "price"] * 10
    X, y, _ = assemble_features(merged)
    model = PriceForecaster({"n_estimators": 150}).train(X, y)
    q = model.predict_quantiles(X)
    p50 = model.predict(X)
    p90_gap = q["p90"][tight].mean() - q["p90"][~tight].mean()
    p50_gap = p50[tight].mean() - p50[~tight].mean()
    assert p90_gap > p50_gap > 0


def test_predict_intervals_includes_quantiles():
    merged = _synthetic_merged(n_days=10, regions=("NSW1",))
    X_cols = merged.drop(columns=["price"])
    model = PriceForecaster(FAST).train(*assemble_features(merged)[:2])
    out = predict_intervals(X_cols, model)
    assert {"interval_datetime", "predicted_price", "p10", "p90"} <= set(out[0])
    assert out[0]["p10"] is not None


def test_predict_intervals_none_quantiles_for_old_blob():
    merged = _synthetic_merged(n_days=10, regions=("NSW1",))
    model = PriceForecaster(FAST).train(*assemble_features(merged)[:2])
    model.quantile_models = {}  # simulate a blob saved before quantile heads
    out = predict_intervals(merged.drop(columns=["price"]), model)
    assert out[0]["p10"] is None and out[0]["p90"] is None


def test_walk_forward_validate_runs():
    df = _synthetic_merged(n_days=30, regions=("NSW1",), seed=2)
    X, y, _ = assemble_features(df)
    res = walk_forward_validate(X, y, df["interval_datetime"], n_splits=3, params=FAST)
    assert len(res["folds"]) >= 1
    assert np.isfinite(res["mae"])


def test_pinball_loss_asymmetry():
    y = np.array([100.0])
    lo = np.array([50.0])
    hi = np.array([150.0])
    # under-prediction hurts the P90 head 9x more than over-prediction
    assert pinball_loss(y, lo, 0.9) == pytest.approx(45.0)
    assert pinball_loss(y, hi, 0.9) == pytest.approx(5.0)


def test_spike_recall():
    y = np.array([50.0, 2000.0, 3000.0, 80.0])
    p90 = np.array([40.0, 600.0, 100.0, 90.0])
    assert spike_recall(y, p90) == pytest.approx(0.5)  # caught 1 of 2 spikes
    assert np.isnan(spike_recall(np.array([50.0, 60.0]), np.array([1.0, 2.0])))


def test_walk_forward_reports_spike_metrics():
    merged = _synthetic_merged(n_days=30, seed=2)
    X, y, _ = assemble_features(merged)
    order = merged["interval_datetime"]
    result = walk_forward_validate(X, y, order, n_splits=2, params={"n_estimators": 60})
    for key in ("pinball_p10", "pinball_p90", "spike_recall"):
        assert key in result
        assert key in result["folds"][0]


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


def _runs_frame(region="NSW1"):
    """One target interval, runs at leads 6h..7d (uses rrp as the payload col)."""
    interval = pd.Timestamp("2026-07-08 19:00:00")
    leads = [6, 18, 30, 90, 170]
    return pd.DataFrame({
        "run_datetime": [interval - pd.Timedelta(hours=h) for h in leads],
        "interval_datetime": interval,
        "regionid": region,
        "rrp": [float(h) for h in leads],
    })


def test_select_runs_at_leads_one_row_per_bucket():
    out = select_runs_at_leads(_runs_frame())
    assert set(out["lead_bucket"]) == {b for b, _ in LEAD_BUCKETS}
    # each bucket picked the causal run nearest its target lead
    picked = out.set_index("lead_bucket")["lead_hours"].to_dict()
    assert picked[12.0] == 6.0 and picked[24.0] == 18.0 and picked[168.0] == 170.0


def test_select_runs_at_leads_dedups_shared_runs():
    # Only one run exists; it can serve at most one bucket after dedup.
    interval = pd.Timestamp("2026-07-08 19:00:00")
    one = pd.DataFrame({
        "run_datetime": [interval - pd.Timedelta(hours=20)],
        "interval_datetime": interval, "regionid": "NSW1", "rrp": [1.0],
    })
    out = select_runs_at_leads(one)
    assert len(out) == 1 and out["lead_bucket"].iloc[0] == 24.0


def test_select_runs_at_leads_causal():
    out = select_runs_at_leads(_runs_frame())
    assert (out["lead_hours"] >= 0).all()
    assert "lead_dist" not in out.columns


def test_lead_envelope_hours_is_union_of_bucket_bands():
    # min(target - tolerance) .. max(target + tolerance) across all buckets.
    low, high = lead_envelope_hours([(12.0, 6.0), (24.0, 12.0), (168.0, 36.0)])
    assert low == 6.0 and high == 204.0


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


def test_extend_forward_frame_adds_lead_and_pd():
    forward = pd.DataFrame({
        "interval_datetime": pd.date_range("2026-07-08 12:30:00", periods=4, freq="30min"),
        "region": "VIC1",
        "run_datetime": pd.Timestamp("2026-07-08 06:00:00"),  # stale run: age != now-based lead
        **{c: 1000.0 for c in PASA_FEATURES},
    })
    pd_latest = _pd_frame([100.0, 5500.0, 5500.0, 5500.0], start="2026-07-08 12:30:00",
                          run="2026-07-08 11:00:00")
    out = _extend_forward_frame(forward, pd_latest)
    assert out["lead_hours"].iloc[0] == 6.5  # interval 12:30 - run_datetime 06:00, not now
    assert out["pd_hours_above_5000"].iloc[0] == 1.5
    empty = _extend_forward_frame(forward.copy(), pd.DataFrame())
    assert empty["pd_rrp"].isna().all()


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


def test_pd_window_groups_by_regionid_independently():
    # Same run, same day, two regions: one tight, one calm; must not bleed across regions.
    tight = _pd_frame([17000.0] * 4, region="VIC1")
    calm = _pd_frame([100.0] * 4, region="SA1")
    f = predispatch_window_features(pd.concat([tight, calm], ignore_index=True))
    by_region = f.groupby("regionid")["pd_hours_above_5000"].max()
    assert by_region["VIC1"] == 2.0
    assert by_region["SA1"] == 0.0


def test_pd_window_longest_run_broken_by_dip():
    # 3 above, 1 below, 2 above -> longest contiguous run is 3 intervals = 1.5h.
    f = predispatch_window_features(_pd_frame([500.0, 500.0, 500.0, 100.0, 500.0, 500.0]))
    assert f["pd_longest_run_above_300"].iloc[0] == 1.5
    assert set(PD_FEATURES) <= set(f.columns)


# --------------------------------------------------------------------------- #
# compute_forecast_accuracy
# --------------------------------------------------------------------------- #

BASE = pd.Timestamp("2026-07-01 12:00:00")


def _fc_row(lead_hours, p50, p10=None, p90=None, interval=BASE, region="NSW1"):
    return {
        "run_at": interval - pd.Timedelta(hours=lead_hours),
        "interval_datetime": interval,
        "region": region,
        "p50": p50,
        "p10": p10,
        "p90": p90,
    }


def _realised_row(price, interval=BASE, region="NSW1"):
    return {"settlementdate": interval, "region": region, "price": price}


def _bucket(result, hours):
    return next(b for b in result["buckets"] if b["lead_bucket_hours"] == hours)


def test_accuracy_all_buckets_present_even_when_empty():
    result = compute_forecast_accuracy(pd.DataFrame(), pd.DataFrame())
    assert {b["lead_bucket_hours"] for b in result["buckets"]} == {b for b, _ in LEAD_BUCKETS}
    assert all(b["n"] == 0 for b in result["buckets"])
    assert result["overall"]["n"] == 0
    assert np.isnan(result["overall"]["mae"])


def test_accuracy_known_mae():
    forecast = pd.DataFrame([_fc_row(24, p50=90.0, p10=50.0, p90=150.0)])
    realised = pd.DataFrame([_realised_row(100.0)])
    result = compute_forecast_accuracy(forecast, realised)
    bucket = _bucket(result, 24.0)
    assert bucket["n"] == 1
    assert bucket["mae"] == pytest.approx(10.0)
    assert result["overall"]["mae"] == pytest.approx(10.0)


def test_accuracy_coverage_hit_and_miss():
    forecast = pd.DataFrame([
        _fc_row(24, p50=90.0, p10=50.0, p90=150.0, interval=BASE),
        _fc_row(24, p50=20.0, p10=10.0, p90=30.0, interval=BASE + pd.Timedelta(minutes=30)),
    ])
    realised = pd.DataFrame([
        _realised_row(100.0, interval=BASE),  # inside [50, 150] -> covered
        _realised_row(50.0, interval=BASE + pd.Timedelta(minutes=30)),  # outside [10, 30] -> miss
    ])
    bucket = _bucket(compute_forecast_accuracy(forecast, realised), 24.0)
    assert bucket["coverage_n"] == 2
    assert bucket["p10_p90_coverage"] == pytest.approx(0.5)


def test_accuracy_spike_recall_one_caught_one_missed():
    forecast = pd.DataFrame([
        _fc_row(24, p50=500.0, p10=100.0, p90=600.0, interval=BASE),
        _fc_row(24, p50=200.0, p10=50.0, p90=300.0, interval=BASE + pd.Timedelta(minutes=30)),
    ])
    realised = pd.DataFrame([
        _realised_row(2000.0, interval=BASE),  # settled spike, p90=600 > 500 -> caught
        _realised_row(1500.0, interval=BASE + pd.Timedelta(minutes=30)),  # p90=300 < 500 -> missed
    ])
    bucket = _bucket(compute_forecast_accuracy(forecast, realised), 24.0)
    assert bucket["spike_n"] == 2
    assert bucket["spike_recall"] == pytest.approx(0.5)


def test_accuracy_null_quantile_row_counted_in_mae_excluded_from_coverage():
    forecast = pd.DataFrame([
        _fc_row(24, p50=80.0, p10=np.nan, p90=np.nan, interval=BASE),
        _fc_row(24, p50=90.0, p10=50.0, p90=150.0, interval=BASE + pd.Timedelta(minutes=30)),
    ])
    realised = pd.DataFrame([
        _realised_row(100.0, interval=BASE),  # |80 - 100| = 20, no quantiles
        _realised_row(100.0, interval=BASE + pd.Timedelta(minutes=30)),  # |90 - 100| = 10, covered
    ])
    bucket = _bucket(compute_forecast_accuracy(forecast, realised), 24.0)
    assert bucket["n"] == 2
    assert bucket["mae"] == pytest.approx(15.0)  # mean(20, 10)
    assert bucket["coverage_n"] == 1
    assert bucket["p10_p90_coverage"] == pytest.approx(1.0)


def test_accuracy_assigns_nearest_lead_bucket():
    intervals = {12.0: BASE, 24.0: BASE + pd.Timedelta(hours=1), 48.0: BASE + pd.Timedelta(hours=2),
                 168.0: BASE + pd.Timedelta(hours=3)}
    # Leads chosen closer to one bucket target than any neighbour.
    leads = {12.0: 8.0, 24.0: 20.0, 48.0: 60.0, 168.0: 150.0}
    forecast = pd.DataFrame([
        _fc_row(leads[target], p50=1.0, p10=0.0, p90=2.0, interval=interval)
        for target, interval in intervals.items()
    ])
    realised = pd.DataFrame([_realised_row(1.0, interval=interval) for interval in intervals.values()])
    result = compute_forecast_accuracy(forecast, realised)
    for target in intervals:
        assert _bucket(result, target)["n"] == 1
    for target, _tol in LEAD_BUCKETS:
        if target not in intervals:
            assert _bucket(result, target)["n"] == 0
    assert result["overall"]["n"] == 4


def test_accuracy_extreme_lead_lands_in_furthest_bucket():
    # 300h is outside every tolerance band but nearest to the 168h target.
    forecast = pd.DataFrame([_fc_row(300.0, p50=100.0, p10=50.0, p90=150.0)])
    realised = pd.DataFrame([_realised_row(100.0)])
    result = compute_forecast_accuracy(forecast, realised)
    assert _bucket(result, 168.0)["n"] == 1
    assert result["overall"]["n"] == 1


def test_accuracy_unsettled_forecast_dropped_by_join():
    # No realised price yet for this interval -> row has nothing to score against.
    forecast = pd.DataFrame([_fc_row(24, p50=90.0, p10=50.0, p90=150.0)])
    result = compute_forecast_accuracy(forecast, pd.DataFrame(columns=["settlementdate", "region", "price"]))
    assert result["overall"]["n"] == 0
