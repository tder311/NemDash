"""Day/week-ahead NEM price forecaster.

A gradient-boosted (XGBoost) model that predicts 30-minute regional reference
prices (RRP) up to 7 days ahead. The forward-looking signal comes from AEMO's
PASA forecasts (PD PASA near-term, ST PASA for the tail), which already live in
the ``pdpasa_data`` / ``stpasa_data`` tables: forecast demand (10/50/90),
reserve requirement, and available capacity per interval.

Design notes
------------
* The feature engineering and model wrapper are **pure** (DataFrame in,
  DataFrame/array out) so they can be unit-tested without a database. The async
  ``load_*`` helpers at the bottom are the only DB-aware code.
* Region is one-hot encoded (5 NEM regions) rather than training a model per
  region, so one model shares signal across regions while still specialising.
* The model predicts the P50 (point) price via ``predict()``, plus P10/P90
  quantile heads via ``predict_quantiles()`` for uncertainty bands.
* NEM prices can be negative (to -$1000) and spike to the market cap
  ($16,600+), so the target is modelled raw — XGBoost handles the range, with
  the documented caveat that extreme spikes are under-called.
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

REGIONS: List[str] = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]

# AEMO region id -> Australian state subdivision for public-holiday lookups.
REGION_TO_STATE: Dict[str, str] = {
    "NSW1": "NSW",
    "QLD1": "QLD",
    "VIC1": "VIC",
    "SA1": "SA",
    "TAS1": "TAS",
}

SETTLEMENT_MINUTES: int = 30
INTERVALS_PER_DAY: int = 24 * 60 // SETTLEMENT_MINUTES  # 48
HORIZON_DAYS: int = 7
HORIZON_INTERVALS: int = HORIZON_DAYS * INTERVALS_PER_DAY  # 336

# Forward-looking PASA columns available as features.
PASA_FEATURES: List[str] = [
    "demand10",
    "demand50",
    "demand90",
    "reservereq",
    "aggregatecapacityavailable",
    "aggregatepasaavailability",
    "surplusreserve",
]

# Predispatch (PD7Day) price features. Pointwise PD is unreliable; sustained
# high-price windows are the real spike signal, so most features are day-window aggregates.
SPIKE_THRESHOLD_LOW: float = 300.0
PD_FEATURES: List[str] = [
    "pd_rrp",
    "pd_hours_above_300",
    "pd_hours_above_1000",
    "pd_hours_above_5000",
    "pd_longest_run_above_300",
    "pd_exceedance_sum",
]

HOURS_PER_INTERVAL: float = SETTLEMENT_MINUTES / 60.0

# NEM market time is AEST (UTC+10, no DST) year-round; Brisbane matches exactly.
NEM_TZ = ZoneInfo("Australia/Brisbane")

TARGET: str = "price"
# PUBLIC is the 5-min RRP series with full history; aggregated to 30-min below.
# (TRADING is the same RRP but only retained for ~weeks; PUBLIC@:30 == TRADING.)
PRICE_TYPE: str = "PUBLIC"

# Pinball-loss alphas for the P10/P90 quantile heads trained alongside the P50.
QUANTILES: List[float] = [0.1, 0.9]


# --------------------------------------------------------------------------- #
# Feature engineering (pure)
# --------------------------------------------------------------------------- #


def _public_holiday_flags(timestamps: pd.Series, region: pd.Series) -> pd.Series:
    """Return 1 where the interval falls on a state public holiday, else 0.

    Falls back to all-zeros if the optional ``holidays`` package is missing, so
    the model still trains (just without the holiday feature signal).
    """
    try:
        import holidays as _holidays
    except ImportError:  # pragma: no cover - optional dependency
        return pd.Series(0, index=timestamps.index, dtype="int8")

    ts = pd.to_datetime(timestamps)
    years = sorted(ts.dt.year.dropna().unique().tolist())
    flags = np.zeros(len(ts), dtype="int8")
    for reg, state in REGION_TO_STATE.items():
        mask = (region == reg).to_numpy()
        if not mask.any():
            continue
        cal = _holidays.Australia(subdiv=state, years=years)
        dates = ts[mask].dt.date
        flags[mask] = dates.isin(set(cal.keys())).to_numpy().astype("int8")
    return pd.Series(flags, index=timestamps.index)



def build_calendar_features(timestamps: pd.Series, region: pd.Series) -> pd.DataFrame:
    """Cyclical + categorical calendar features for each interval.

    Uses sin/cos encodings so the model sees time-of-day and time-of-year as
    smooth cycles rather than arbitrary integers.
    """
    ts = pd.to_datetime(timestamps)
    minute_of_day = ts.dt.hour * 60 + ts.dt.minute
    settlement_period = minute_of_day // SETTLEMENT_MINUTES  # 0..47
    day_of_week = ts.dt.dayofweek  # 0=Mon
    day_of_year = ts.dt.dayofyear

    feats = pd.DataFrame(index=ts.index)
    feats["settlement_period"] = settlement_period
    feats["day_of_week"] = day_of_week
    feats["month"] = ts.dt.month
    feats["is_weekend"] = (day_of_week >= 5).astype("int8")
    feats["is_holiday"] = _public_holiday_flags(timestamps, region).values
    feats["tod_sin"] = np.sin(2 * np.pi * settlement_period / INTERVALS_PER_DAY)
    feats["tod_cos"] = np.cos(2 * np.pi * settlement_period / INTERVALS_PER_DAY)
    feats["doy_sin"] = np.sin(2 * np.pi * day_of_year / 365.25)
    feats["doy_cos"] = np.cos(2 * np.pi * day_of_year / 365.25)
    return feats


def _region_one_hot(region: pd.Series) -> pd.DataFrame:
    """Deterministic one-hot over the fixed region set (robust to absent regions)."""
    out = pd.DataFrame(index=region.index)
    for reg in REGIONS:
        out[f"region_{reg}"] = (region == reg).astype("int8")
    return out


def _pasa_derived_features(df: pd.DataFrame) -> pd.DataFrame:
    """Engineered scarcity signals derived from the raw PASA columns.

    Returns a DataFrame indexed like ``df``; each column becomes a model
    feature, added alongside the raw PASA columns:

    - ``demand_spread``   = demand90 - demand10 (forecast uncertainty)
    - ``capacity_margin`` = aggregatecapacityavailable - demand50 (MW headroom)
    - ``utilisation``     = demand50 / aggregatecapacityavailable (0->1 tightness;
      NaN where capacity is zero)
    """
    out = pd.DataFrame(index=df.index)
    out["demand_spread"] = df["demand90"] - df["demand10"]
    out["capacity_margin"] = df["aggregatecapacityavailable"] - df["demand50"]
    out["utilisation"] = df["demand50"] / df["aggregatecapacityavailable"].replace(0, np.nan)
    return out


def _vectorised_longest_run(above: pd.Series, group_id: pd.Series) -> pd.Series:
    """Per-row longest contiguous True run (in intervals) within its group.

    Block-id trick: a new block starts wherever ``above`` or ``group_id``
    changes vs. the previous row; block sizes then give run lengths per row.
    """
    change = above.ne(above.shift()) | group_id.ne(group_id.shift())
    block_id = change.cumsum()
    block_len = block_id.groupby(block_id).transform("size")
    len_if_above = block_len.where(above, 0)
    return len_if_above.groupby(group_id).transform("max")


def predispatch_window_features(pd_frame: pd.DataFrame) -> pd.DataFrame:
    """Add PD scarcity features, aggregated within (run, region, calendar day).

    Grouping by run keeps different forecast runs from bleeding into each other;
    the calendar-day window captures "how much of that day does this run think is tight".
    All aggregates are computed via vectorised groupby ops (no per-group Python
    callbacks) so this scales to large stored PD histories.
    """
    df = pd_frame.copy()
    df["interval_datetime"] = pd.to_datetime(df["interval_datetime"])
    df["run_datetime"] = pd.to_datetime(df["run_datetime"])
    df = df.sort_values(["run_datetime", "regionid", "interval_datetime"]).reset_index(drop=True)
    df["pd_rrp"] = df["rrp"].astype("float32")

    group_id = df.groupby(
        [df["run_datetime"], df["regionid"], df["interval_datetime"].dt.date]
    ).ngroup()

    indicators = pd.DataFrame({
        "pd_hours_above_300": (df["rrp"] > 300).astype("float32"),
        "pd_hours_above_1000": (df["rrp"] > 1000).astype("float32"),
        "pd_hours_above_5000": (df["rrp"] > 5000).astype("float32"),
        "pd_exceedance_sum": (df["rrp"] - SPIKE_THRESHOLD_LOW).clip(lower=0),
    })
    sums = indicators.groupby(group_id).transform("sum")
    for threshold in (300, 1000, 5000):
        col = f"pd_hours_above_{threshold}"
        df[col] = sums[col] * HOURS_PER_INTERVAL
    df["pd_exceedance_sum"] = sums["pd_exceedance_sum"]

    above = df["rrp"] > SPIKE_THRESHOLD_LOW
    df["pd_longest_run_above_300"] = (
        _vectorised_longest_run(above, group_id) * HOURS_PER_INTERVAL
    )

    df[PD_FEATURES] = df[PD_FEATURES].astype("float32")
    return df


def assemble_features(
    merged: pd.DataFrame,
    *,
    include_target: bool = True,
) -> Tuple[pd.DataFrame, Optional[pd.Series], List[str]]:
    """Build the model matrix from a price+PASA merged frame.

    Parameters
    ----------
    merged
        One row per (``interval_datetime``, ``region``) with the PASA feature
        columns present, and (when ``include_target``) a ``price`` column.
    include_target
        Whether to extract and return ``y`` (False for forward inference).

    Returns
    -------
    (X, y, feature_names)
        ``y`` is None when ``include_target`` is False.

    Loaders guarantee ``lead_hours`` and all ``PD_FEATURES`` columns are
    present (NaN = data unavailable); a missing column is a loader bug, so
    this raises ``KeyError`` rather than silently dropping the feature.
    """
    if "interval_datetime" not in merged or "region" not in merged:
        raise ValueError("merged frame must have 'interval_datetime' and 'region'")

    ts = merged["interval_datetime"]
    region = merged["region"]

    parts = [
        build_calendar_features(ts, region),
        _region_one_hot(region),
        merged[PASA_FEATURES].astype("float32"),
        _pasa_derived_features(merged).astype("float32"),
        merged[PD_FEATURES + ["lead_hours"]].astype("float32"),
    ]
    X = pd.concat(parts, axis=1).reset_index(drop=True)
    feature_names = list(X.columns)

    y: Optional[pd.Series] = None
    if include_target:
        if TARGET not in merged:
            raise ValueError(f"merged frame missing target column '{TARGET}'")
        y = merged[TARGET].astype("float32").reset_index(drop=True)
        # Drop rows with no realised price (cannot train on them).
        mask = y.notna()
        X, y = X.loc[mask].reset_index(drop=True), y.loc[mask].reset_index(drop=True)

    return X, y, feature_names


def dedup_pasa_runs(pasa: pd.DataFrame) -> pd.DataFrame:
    """Collapse PASA history to one forecast row per (interval, region).

    AEMO publishes many runs; for any target interval we keep the *latest run
    that does not look into the future of that interval* (``run_datetime <=
    interval_datetime``) so training never sees hindsight that wouldn't have
    been available when forecasting.
    """
    df = pasa.copy()
    df["run_datetime"] = pd.to_datetime(df["run_datetime"])
    df["interval_datetime"] = pd.to_datetime(df["interval_datetime"])
    df = df[df["run_datetime"] <= df["interval_datetime"]]
    df = df.sort_values("run_datetime")
    df = df.drop_duplicates(subset=["interval_datetime", "regionid"], keep="last")
    return df.reset_index(drop=True)


def to_regionid(region: pd.Series) -> pd.Series:
    """Normalise region labels to AEMO regionid form ('NSW' -> 'NSW1').

    The dashboard stores ``price_data.region`` without the trailing '1'
    ('NSW', 'VIC', ...), while the PASA tables use the full regionid
    ('NSW1', 'VIC1', ...). Canonical form is the regionid, so the price/PASA
    join lines up.
    """
    s = region.astype(str).str.upper().str.strip()
    return s.where(s.str.endswith("1"), s + "1")


def _causal_band_select(
    df: pd.DataFrame,
    target_lead_hours: float,
    tolerance_hours: float,
    prefer_longer: bool,
) -> pd.DataFrame:
    """Keep causal, in-band runs and the one closest to the target lead per (interval, region).

    ``df`` must already carry a numeric ``lead_hours`` column. Ties are broken
    by the longer lead if ``prefer_longer`` else the shorter lead. Adds a
    ``lead_dist`` column callers can use or drop.
    """
    band = df[(df["lead_hours"] >= 0) & ((df["lead_hours"] - target_lead_hours).abs() <= tolerance_hours)].copy()
    band["lead_dist"] = (band["lead_hours"] - target_lead_hours).abs()
    band = band.sort_values(["lead_dist", "lead_hours"], ascending=[True, not prefer_longer])
    return band.drop_duplicates(subset=["interval_datetime", "regionid"], keep="first")


def select_runs_at_lead(
    pasa: pd.DataFrame,
    target_lead_hours: float = 24.0,
    tolerance_hours: float = 12.0,
) -> pd.DataFrame:
    """Pick one PASA run per (interval, region) at a target forecast lead time.

    AEMO publishes many runs per interval; for training we keep the one made
    ~``target_lead_hours`` before the interval, so features match the lead time
    available at day-ahead inference (rather than the near-foresight latest run).

    Keeps only causal, in-band runs (``run <= interval`` and lead within
    ``tolerance_hours`` of target), then the run closest to the target lead per
    (interval, region). Ties favour the longer (earlier, more conservative) lead.

    ``pasa`` must have run_datetime, interval_datetime, regionid + PASA features.
    """
    df = pasa.copy()
    df["run_datetime"] = pd.to_datetime(df["run_datetime"])
    df["interval_datetime"] = pd.to_datetime(df["interval_datetime"])
    df["lead_hours"] = (df["interval_datetime"] - df["run_datetime"]).dt.total_seconds() / 3600
    df = _causal_band_select(df, target_lead_hours, tolerance_hours, prefer_longer=True)

    return df.reset_index(drop=True)


# (target_lead_hours, tolerance_hours) buckets spanning intraday to 7-day leads.
# Tolerances roughly half the gap to the neighbouring bucket so bands don't overlap much.
LEAD_BUCKETS: List[Tuple[float, float]] = [
    (12.0, 6.0),
    (24.0, 12.0),
    (48.0, 24.0),
    (96.0, 36.0),
    (168.0, 36.0),
]


def lead_envelope_hours(buckets: List[Tuple[float, float]] = LEAD_BUCKETS) -> Tuple[float, float]:
    """Union envelope (min, max lead hours) any bucket in ``buckets`` could select from.

    Used to SQL-prefilter stored PD history to only rows a lead bucket could ever pick.
    """
    lows = [target - tolerance for target, tolerance in buckets]
    highs = [target + tolerance for target, tolerance in buckets]
    return min(lows), max(highs)


def select_runs_at_leads(
    pasa: pd.DataFrame,
    buckets: List[Tuple[float, float]] = LEAD_BUCKETS,
) -> pd.DataFrame:
    """One row per (interval, region, lead bucket): the run nearest each target lead.

    Training across leads (with lead_hours as a feature) teaches the model how much to
    trust far-lead inputs, e.g. phantom VOLL a week out vs. real tightness at 12h.

    Ties within a bucket favour the shorter lead (unlike ``select_runs_at_lead``'s
    longer-lead tie-break); this only resolves exact ties, not general bucket contention.
    """
    df = pasa.copy()
    df["run_datetime"] = pd.to_datetime(df["run_datetime"])
    df["interval_datetime"] = pd.to_datetime(df["interval_datetime"])
    df["lead_hours"] = (df["interval_datetime"] - df["run_datetime"]).dt.total_seconds() / 3600

    frames = []
    for target, tolerance in buckets:
        bucket_df = _causal_band_select(df, target, tolerance, prefer_longer=False)
        bucket_df["lead_bucket"] = target
        frames.append(bucket_df)

    out = pd.concat(frames, ignore_index=True)
    # A run selected by several buckets keeps only its nearest bucket.
    out = out.sort_values("lead_dist").drop_duplicates(
        subset=["interval_datetime", "regionid", "run_datetime"], keep="first"
    )
    return out.drop(columns=["lead_dist"]).sort_values(
        ["regionid", "interval_datetime", "lead_bucket"]
    ).reset_index(drop=True)


def to_30min_price(price: pd.DataFrame) -> pd.DataFrame:
    """Aggregate 5-min RRP to the 30-min period-ending block mean, per region.

    NEM timestamps are period-ending, so the half-hour ending HH:30 is the mean
    of the 5-min intervals stamped HH:05..HH:30 — i.e. resample with the right
    edge closed and labelled. Returns columns: settlementdate, region, price.
    """
    df = price.rename(columns={"settlementdate": "ts"}).copy()
    df["ts"] = pd.to_datetime(df["ts"])
    df["region"] = to_regionid(df["region"])
    out = (
        df.set_index("ts")
        .groupby("region")["price"]
        .resample("30min", label="right", closed="right")
        .mean()
        .reset_index()
        .rename(columns={"ts": "settlementdate"})
        .dropna(subset=["price"])
    )
    return out


def merge_price_pasa(price: pd.DataFrame, pasa: pd.DataFrame) -> pd.DataFrame:
    """Inner-join realised 30-min prices to their PASA forecast features.

    Requires ``lead_hours``/``lead_bucket`` on ``pasa`` (raises ``KeyError`` otherwise)
    so ``load_training_frame`` can join PD features onto the same lead bucket.
    """
    p = price.rename(columns={"settlementdate": "interval_datetime"}).copy()
    p["interval_datetime"] = pd.to_datetime(p["interval_datetime"])
    p["region"] = to_regionid(p["region"])
    q = pasa.rename(columns={"regionid": "region"}).copy()
    q["interval_datetime"] = pd.to_datetime(q["interval_datetime"])
    q["region"] = to_regionid(q["region"])
    q_cols = ["interval_datetime", "region"] + PASA_FEATURES + ["lead_hours", "lead_bucket"]
    merged = p.merge(
        q[q_cols],
        on=["interval_datetime", "region"],
        how="inner",
    )
    return merged.sort_values(["region", "interval_datetime"]).reset_index(drop=True)


# --------------------------------------------------------------------------- #
# Model wrapper
# --------------------------------------------------------------------------- #


@dataclass
class ModelCard:
    """Lightweight provenance/metrics record saved alongside the booster."""

    trained_at: str = ""
    n_train_rows: int = 0
    regions: List[str] = field(default_factory=lambda: list(REGIONS))
    feature_names: List[str] = field(default_factory=list)
    target: str = TARGET
    price_type: str = PRICE_TYPE
    horizon_intervals: int = HORIZON_INTERVALS
    quantile: Optional[Any] = None  # [0.1, 0.9] once quantile heads exist
    metrics: Dict[str, Any] = field(default_factory=dict)
    params: Dict[str, Any] = field(default_factory=dict)


DEFAULT_PARAMS: Dict[str, Any] = {
    "n_estimators": 600,
    "max_depth": 6,
    "learning_rate": 0.03,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "min_child_weight": 5,
    "tree_method": "hist",
    "n_jobs": -1,
    "random_state": 42,
}


class PriceForecaster:
    """XGBoost wrapper for 30-min regional RRP forecasting."""

    def __init__(self, params: Optional[Dict[str, Any]] = None):
        self.params = {**DEFAULT_PARAMS, **(params or {})}
        self.model = None
        self.quantile_models: Dict[float, Any] = {}
        self.card = ModelCard(params=self.params)

    def train(self, X: pd.DataFrame, y: pd.Series) -> "PriceForecaster":
        from xgboost import XGBRegressor

        self.model = XGBRegressor(**self.params)
        self.model.fit(X, y)
        for q in QUANTILES:
            qm = XGBRegressor(
                **{**self.params, "objective": "reg:quantileerror", "quantile_alpha": q}
            )
            qm.fit(X, y)
            self.quantile_models[q] = qm
        self.card.quantile = list(QUANTILES)
        self.card.trained_at = datetime.utcnow().isoformat()
        self.card.n_train_rows = int(len(X))
        self.card.feature_names = list(X.columns)
        return self

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        if self.model is None:
            raise RuntimeError("model is not trained; call train() or load() first")
        # Align columns to training order, filling any missing with 0.
        if self.card.feature_names:
            X = X.reindex(columns=self.card.feature_names, fill_value=0)
        return self.model.predict(X)

    def predict_quantiles(self, X: pd.DataFrame) -> pd.DataFrame:
        """P10/P90 predictions from the independently-trained pinball-loss heads.

        Crossings (p10 > p90) are possible since the two objectives are fit
        separately and are not corrected here.
        """
        if not self.quantile_models:
            raise RuntimeError("no quantile heads; retrain with the current code")
        if self.card.feature_names:
            X = X.reindex(columns=self.card.feature_names, fill_value=0)
        return pd.DataFrame(
            {f"p{int(q * 100)}": self.quantile_models[q].predict(X) for q in QUANTILES},
            index=X.index,
        )

    def save(self, path: str) -> None:
        import joblib

        joblib.dump(
            {"model": self.model, "quantile_models": self.quantile_models, "card": asdict(self.card)},
            path,
        )

    @classmethod
    def load(cls, path: str) -> "PriceForecaster":
        import joblib

        blob = joblib.load(path)
        obj = cls(params=blob["card"].get("params"))
        obj.model = blob["model"]
        obj.quantile_models = blob.get("quantile_models", {})
        obj.card = ModelCard(**blob["card"])
        return obj

    def feature_importance(self, top: int = 15) -> List[Tuple[str, float]]:
        if self.model is None or not self.card.feature_names:
            return []
        imp = self.model.feature_importances_
        pairs = sorted(zip(self.card.feature_names, imp), key=lambda x: -x[1])
        return [(n, float(v)) for n, v in pairs[:top]]


# --------------------------------------------------------------------------- #
# Validation
# --------------------------------------------------------------------------- #


SPIKE_SETTLED_ABOVE: float = 1000.0
SPIKE_ALERT_ABOVE: float = 500.0


def pinball_loss(y_true: np.ndarray, y_pred: np.ndarray, q: float) -> float:
    """Quantile (pinball) loss — the metric quantile heads are trained on."""
    err = y_true - y_pred
    return float(np.mean(np.maximum(q * err, (q - 1) * err)))


def spike_recall(
    y_true: np.ndarray,
    p90: np.ndarray,
    settled_above: float = SPIKE_SETTLED_ABOVE,
    alert_above: float = SPIKE_ALERT_ABOVE,
) -> float:
    """Of blocks that settled above ``settled_above``, the fraction where P90 raised
    the alarm (> ``alert_above``). NaN when the window contains no spikes."""
    spikes = y_true > settled_above
    if not spikes.any():
        return float("nan")
    return float((p90[spikes] > alert_above).mean())


def _metrics(y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, float]:
    err = y_pred - y_true
    mae = float(np.mean(np.abs(err)))
    rmse = float(np.sqrt(np.mean(err**2)))
    # Spearman rank correlation: how well the *shape/ordering* is captured,
    # which is what matters for arbitrage / TB-spread value capture.
    if len(y_true) > 2 and np.std(y_true) > 0 and np.std(y_pred) > 0:
        ra = pd.Series(y_true).rank()
        rb = pd.Series(y_pred).rank()
        spearman = float(np.corrcoef(ra, rb)[0, 1])
    else:
        spearman = float("nan")
    return {"mae": mae, "rmse": rmse, "spearman": spearman, "n": int(len(y_true))}


def walk_forward_validate(
    X: pd.DataFrame,
    y: pd.Series,
    order: pd.Series,
    *,
    n_splits: int = 4,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Time-ordered walk-forward backtest.

    ``order`` is a sortable series (e.g. interval_datetime) used to keep the
    splits chronological — never train on the future, test on the past.
    """
    idx = np.argsort(order.values, kind="stable")
    Xo, yo = X.iloc[idx].reset_index(drop=True), y.iloc[idx].reset_index(drop=True)

    n = len(Xo)
    fold = n // (n_splits + 1)
    folds: List[Dict[str, float]] = []
    for k in range(1, n_splits + 1):
        train_end = fold * k
        test_end = fold * (k + 1) if k < n_splits else n
        if train_end < 10 or test_end - train_end < 5:
            continue
        m = PriceForecaster(params).train(Xo.iloc[:train_end], yo.iloc[:train_end])
        pred = m.predict(Xo.iloc[train_end:test_end])
        q = m.predict_quantiles(Xo.iloc[train_end:test_end])
        y_test = yo.iloc[train_end:test_end].values
        fold_metrics = _metrics(y_test, pred)
        fold_metrics["pinball_p10"] = pinball_loss(y_test, q["p10"].values, 0.1)
        fold_metrics["pinball_p90"] = pinball_loss(y_test, q["p90"].values, 0.9)
        fold_metrics["spike_recall"] = spike_recall(y_test, q["p90"].values)
        folds.append(fold_metrics)

    if not folds:
        return {"folds": [], "mae": float("nan"), "rmse": float("nan")}
    # Mean over folds that had spikes to test for; NaN (not a RuntimeWarning) when none did.
    spike_vals = [f["spike_recall"] for f in folds if not np.isnan(f["spike_recall"])]
    return {
        "folds": folds,
        "mae": float(np.mean([f["mae"] for f in folds])),
        "rmse": float(np.mean([f["rmse"] for f in folds])),
        "spearman": float(np.nanmean([f["spearman"] for f in folds])),
        "pinball_p10": float(np.mean([f["pinball_p10"] for f in folds])),
        "pinball_p90": float(np.mean([f["pinball_p90"] for f in folds])),
        "spike_recall": float(np.mean(spike_vals)) if spike_vals else float("nan"),
    }


# --------------------------------------------------------------------------- #
# Async DB I/O (the only database-aware code)
# --------------------------------------------------------------------------- #


async def _fetch_pasa_history(db, table: str, start: datetime, end: datetime) -> pd.DataFrame:
    """Pull raw PASA rows for a date window directly from the pool."""
    cols = ", ".join(["run_datetime", "interval_datetime", "regionid"] + PASA_FEATURES)
    sql = (
        f"SELECT {cols} FROM {table} "
        f"WHERE interval_datetime >= $1 AND interval_datetime <= $2"
    )
    async with db._pool.acquire() as conn:
        rows = await conn.fetch(sql, start, end)
    return pd.DataFrame([dict(r) for r in rows])


async def _fetch_predispatch_history(db, start: datetime, end: datetime) -> pd.DataFrame:
    """Pull stored PD7Day price rows for runs that can win a LEAD_BUCKETS band.

    Only rows whose lead (interval_datetime - run_datetime) can ever be selected
    by ``select_runs_at_leads`` are worth transferring. Filtering is done at
    *run* granularity (keep every row of any run with >=1 in-envelope row),
    not row granularity, so ``predispatch_window_features``'s day-window
    aggregates for surviving runs are computed on the full day, not truncated
    at the envelope edge.
    """
    low, high = lead_envelope_hours(LEAD_BUCKETS)
    async with db._pool.acquire() as conn:
        run_rows = await conn.fetch(
            "SELECT DISTINCT run_datetime FROM predispatch_price "
            "WHERE interval_datetime >= $1 AND interval_datetime <= $2 "
            "AND interval_datetime - run_datetime BETWEEN $3::interval AND $4::interval",
            start, end, timedelta(hours=low), timedelta(hours=high),
        )
        run_datetimes = [r["run_datetime"] for r in run_rows]
        if not run_datetimes:
            return pd.DataFrame()
        rows = await conn.fetch(
            "SELECT run_datetime, interval_datetime, regionid, rrp FROM predispatch_price "
            "WHERE interval_datetime >= $1 AND interval_datetime <= $2 "
            "AND run_datetime = ANY($3::timestamp[])",
            start, end, run_datetimes,
        )
    return pd.DataFrame([dict(r) for r in rows])


async def load_training_frame(db, start: datetime, end: datetime) -> pd.DataFrame:
    """Assemble the merged price+PASA+PD training frame for [start, end].

    Combines ST PASA (7-day) and PD PASA (2-day) history; selects one PASA run
    per (interval, region, lead bucket) across ``LEAD_BUCKETS`` so the model
    trains on the full spread of forecast leads it will see at inference. PD
    window features (computed on the full PD history first) are then joined
    onto the matching lead bucket; early history with no PD coverage gets NaN.
    """
    price = await db.get_price_history(start, end, region=None, price_type=PRICE_TYPE)
    if price is None or price.empty:
        return pd.DataFrame()
    price = to_30min_price(price)  # 5-min RRP -> 30-min period-ending block mean

    st = await _fetch_pasa_history(db, "stpasa_data", start, end)
    pd_ = await _fetch_pasa_history(db, "pdpasa_data", start, end)
    pasa = pd.concat([st, pd_], ignore_index=True) if not pd_.empty else st
    if pasa.empty:
        return pd.DataFrame()
    pasa = select_runs_at_leads(pasa)
    merged = merge_price_pasa(price, pasa)

    pd_hist = await _fetch_predispatch_history(db, start, end)
    if pd_hist.empty:
        for col in PD_FEATURES:
            merged[col] = np.nan
    else:
        pd_feat = predispatch_window_features(pd_hist)
        pd_sel = select_runs_at_leads(pd_feat).rename(columns={"regionid": "region"})
        pd_sel["region"] = to_regionid(pd_sel["region"])
        merged = merged.merge(
            pd_sel[["interval_datetime", "region", "lead_bucket"] + PD_FEATURES],
            on=["interval_datetime", "region", "lead_bucket"],
            how="left",
        )
    return merged.drop(columns=["lead_bucket"])


def nem_now() -> datetime:
    """Current wall-clock time on the NEM market clock (naive, like the data)."""
    return datetime.now(NEM_TZ).replace(tzinfo=None)


def combine_forward_pasa(
    pd_pasa: pd.DataFrame, st_pasa: pd.DataFrame, now: datetime
) -> pd.DataFrame:
    """Merge PD + ST PASA rows into the forward feature frame.

    Keeps future intervals only (``interval_datetime >= now``), prefers PD over
    ST where both cover an interval, and caps at ``HORIZON_INTERVALS``. Past
    rows are dropped so a stale run can neither eat into the 7-day window nor
    produce "forecasts" for intervals that have already settled.
    """
    frames = [f for f in (pd_pasa, st_pasa) if not f.empty]
    if not frames:
        return pd.DataFrame()
    pasa = pd.concat(frames, ignore_index=True)
    pasa["interval_datetime"] = pd.to_datetime(pasa["interval_datetime"])
    pasa = pasa[pasa["interval_datetime"] >= now]
    # PD rows come first in the concat, so keep="first" prefers PD on overlap.
    pasa = pasa.drop_duplicates("interval_datetime", keep="first")
    pasa = pasa.sort_values("interval_datetime").reset_index(drop=True)
    pasa["region"] = to_regionid(pasa["regionid"])
    return pasa.head(HORIZON_INTERVALS)


def _extend_forward_frame(forward: pd.DataFrame, pd_latest: pd.DataFrame) -> pd.DataFrame:
    """Add ``lead_hours`` (data age: interval_datetime - run_datetime) and PD window features.

    ``pd_latest`` must be a single run for a single region (guaranteed by
    ``get_latest_predispatch_price``).
    """
    forward = forward.copy()
    forward["interval_datetime"] = pd.to_datetime(forward["interval_datetime"])
    forward["lead_hours"] = (
        (forward["interval_datetime"] - pd.to_datetime(forward["run_datetime"])).dt.total_seconds() / 3600.0
    ).astype("float32")
    if pd_latest.empty:
        for col in PD_FEATURES:
            forward[col] = np.nan
    else:
        pd_feat = predispatch_window_features(pd_latest)
        forward = forward.merge(
            pd_feat[["interval_datetime"] + PD_FEATURES], on="interval_datetime", how="left"
        )
    return forward


async def load_forecast_inputs(db, region: str) -> pd.DataFrame:
    """Build the forward feature frame (now -> +7 days) for ``region``.

    Uses the freshest stored PASA forecast per future interval across *all*
    runs (PD preferred over ST on overlap), so a stale latest run — e.g. after
    the ingester has been down — cannot blank out the next 24h. Adds
    ``lead_hours`` and the latest stored PD run's window features. No price
    column — this is what we predict.
    """
    now = nem_now()
    pd_rows = await db.get_pasa_forward("pdpasa_data", region, now)
    st_rows = await db.get_pasa_forward("stpasa_data", region, now)
    forward = combine_forward_pasa(pd.DataFrame(pd_rows), pd.DataFrame(st_rows), now)
    if forward.empty:
        return forward
    pd_rows_latest = await db.get_latest_predispatch_price(region)
    pd_latest = pd.DataFrame(pd_rows_latest)
    return _extend_forward_frame(forward, pd_latest)


def default_model_path() -> str:
    """Where the trained model is read from / written to (override via env)."""
    return os.environ.get(
        "FORECAST_MODEL_PATH",
        os.path.join(os.path.dirname(__file__), "..", "models", "price_forecaster.joblib"),
    )


def predict_intervals(inputs: pd.DataFrame, model: "PriceForecaster") -> List[Dict[str, Any]]:
    """Pure prediction: PASA/PD input frame -> list of forecast dicts.

    ``p10``/``p90`` are ``None`` when ``model.quantile_models`` is empty (a
    blob saved before quantile heads existed), rather than raising.
    """
    X, _, _ = assemble_features(inputs, include_target=False)
    preds = model.predict(X)
    if model.quantile_models:
        q = model.predict_quantiles(X)
        p10 = [round(float(v), 2) for v in q["p10"]]
        p90 = [round(float(v), 2) for v in q["p90"]]
    else:
        p10 = [None] * len(preds)
        p90 = [None] * len(preds)
    intervals = pd.to_datetime(inputs["interval_datetime"]).dt.strftime("%Y-%m-%dT%H:%M:%S")
    return [
        {"interval_datetime": t, "predicted_price": round(float(p), 2), "p10": lo, "p90": hi}
        for t, p, lo, hi in zip(intervals, preds, p10, p90)
    ]


async def generate_forecast(db, region: str, model: "PriceForecaster") -> List[Dict[str, Any]]:
    """Predict the next horizon of 30-min prices for ``region`` and log it to history.

    Returns a list of ``{interval_datetime, predicted_price, p10, p90}`` dicts,
    empty if no forward PASA data is available yet. Row order matches the PASA
    inputs (``assemble_features`` does not reorder when ``include_target=False``).
    The history write is telemetry only: it must never fail the serve.
    """
    inputs = await load_forecast_inputs(db, region)
    if inputs.empty:
        return []
    data = predict_intervals(inputs, model)
    run_at = nem_now()
    rows = [
        {"run_at": run_at, "interval_datetime": datetime.fromisoformat(d["interval_datetime"]),
         "region": region, "p50": d["predicted_price"], "p10": d["p10"], "p90": d["p90"],
         "model_trained_at": model.card.trained_at or None}
        for d in data
    ]
    try:
        await db.insert_forecast_history(rows)
    except Exception:
        logger.warning("forecast_history write failed; serving forecast anyway", exc_info=True)
    return data


async def forecast_price_series(db, region: str, model: "PriceForecaster") -> pd.Series:
    """Return the model's 7-day forecast as a Series indexed by interval_datetime.

    Same predictions as ``generate_forecast`` but in pandas form (un-rounded),
    convenient for downstream numerics like the dispatch optimiser.
    """
    inputs = await load_forecast_inputs(db, region)
    if inputs.empty:
        return pd.Series(dtype=float)
    X, _, _ = assemble_features(inputs, include_target=False)
    preds = model.predict(X)
    idx = pd.to_datetime(inputs["interval_datetime"]).values
    return pd.Series(preds.astype(float), index=idx, name="price")


async def train_and_save(db, days: int = 365, out: Optional[str] = None) -> Dict[str, Any]:
    """Load the last ``days`` of data, walk-forward validate, train, and save.

    Returns ``{"model", "metrics", "n_rows", "trained_at", "path"}``. The CPU-
    bound fitting runs in a worker thread so it never blocks the event loop.
    Raises ValueError if the training window has no data.
    """
    out = out or default_model_path()
    end = datetime.now()
    start = end - timedelta(days=days)
    merged = await load_training_frame(db, start, end)
    if merged.empty:
        raise ValueError(f"No training data in the last {days} days.")

    def _fit() -> Tuple["PriceForecaster", Dict[str, Any], int]:
        X, y, _ = assemble_features(merged)
        order = merged.loc[merged[TARGET].notna(), "interval_datetime"].reset_index(drop=True)
        metrics = walk_forward_validate(X, y, order)
        model = PriceForecaster().train(X, y)
        model.card.metrics = metrics
        os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)
        model.save(out)
        return model, metrics, int(len(X))

    model, metrics, n_rows = await asyncio.to_thread(_fit)
    return {
        "model": model,
        "metrics": metrics,
        "n_rows": n_rows,
        "trained_at": model.card.trained_at,
        "path": out,
    }
