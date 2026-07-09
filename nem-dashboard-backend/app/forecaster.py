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
* v1 forecasts the P50 (point) price. Quantile heads (P10/P90) are a planned
  fast-follow; the model card records ``quantile=None`` to leave room for them.
* NEM prices can be negative (to -$1000) and spike to the market cap
  ($16,600+), so the target is modelled raw — XGBoost handles the range, with
  the documented caveat that extreme spikes are under-called.
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

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

# NEM market time is AEST (UTC+10, no DST) year-round; Brisbane matches exactly.
NEM_TZ = ZoneInfo("Australia/Brisbane")

TARGET: str = "price"
# PUBLIC is the 5-min RRP series with full history; aggregated to 30-min below.
# (TRADING is the same RRP but only retained for ~weeks; PUBLIC@:30 == TRADING.)
PRICE_TYPE: str = "PUBLIC"


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
    df = df[(df["lead_hours"] >= 0) & ((df["lead_hours"] - target_lead_hours).abs() <= tolerance_hours)]
    df["lead_dist"] = (df["lead_hours"] - target_lead_hours).abs()
    df = df.sort_values(["lead_dist", "lead_hours"], ascending=[True, False])
    df = df.drop_duplicates(subset=["interval_datetime", "regionid"], keep="first")

    return df.reset_index(drop=True)


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
    """Inner-join realised 30-min prices to their PASA forecast features."""
    p = price.rename(columns={"settlementdate": "interval_datetime"}).copy()
    p["interval_datetime"] = pd.to_datetime(p["interval_datetime"])
    p["region"] = to_regionid(p["region"])
    q = pasa.rename(columns={"regionid": "region"}).copy()
    q["interval_datetime"] = pd.to_datetime(q["interval_datetime"])
    q["region"] = to_regionid(q["region"])
    merged = p.merge(
        q[["interval_datetime", "region"] + PASA_FEATURES],
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
    quantile: Optional[float] = None  # None = P50 point forecast
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
        self.card = ModelCard(params=self.params)

    def train(self, X: pd.DataFrame, y: pd.Series) -> "PriceForecaster":
        from xgboost import XGBRegressor

        self.model = XGBRegressor(**self.params)
        self.model.fit(X, y)
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

    def save(self, path: str) -> None:
        import joblib

        joblib.dump({"model": self.model, "card": asdict(self.card)}, path)

    @classmethod
    def load(cls, path: str) -> "PriceForecaster":
        import joblib

        blob = joblib.load(path)
        obj = cls(params=blob["card"].get("params"))
        obj.model = blob["model"]
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
        folds.append(_metrics(yo.iloc[train_end:test_end].values, pred))

    if not folds:
        return {"folds": [], "mae": float("nan"), "rmse": float("nan")}
    return {
        "folds": folds,
        "mae": float(np.mean([f["mae"] for f in folds])),
        "rmse": float(np.mean([f["rmse"] for f in folds])),
        "spearman": float(np.nanmean([f["spearman"] for f in folds])),
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


async def load_training_frame(db, start: datetime, end: datetime) -> pd.DataFrame:
    """Assemble the merged price+PASA training frame for [start, end].

    Combines ST PASA (7-day) and PD PASA (2-day) history; PD takes precedence
    where both cover an interval (it is the more accurate near-term run).
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
    # Day-ahead lead selection: one run per interval at ~24h lead, so training
    # features match the lead time the model has when used day-ahead.
    pasa = select_runs_at_lead(pasa)

    return merge_price_pasa(price, pasa)


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


async def load_forecast_inputs(db, region: str) -> pd.DataFrame:
    """Build the forward feature frame (now -> +7 days) for ``region``.

    Uses the freshest stored PASA forecast per future interval across *all*
    runs (PD preferred over ST on overlap), so a stale latest run — e.g. after
    the ingester has been down — cannot blank out the next 24h. No price
    column — this is what we predict.
    """
    now = nem_now()
    pd_rows = await db.get_pasa_forward("pdpasa_data", region, now)
    st_rows = await db.get_pasa_forward("stpasa_data", region, now)
    return combine_forward_pasa(pd.DataFrame(pd_rows), pd.DataFrame(st_rows), now)


def default_model_path() -> str:
    """Where the trained model is read from / written to (override via env)."""
    return os.environ.get(
        "FORECAST_MODEL_PATH",
        os.path.join(os.path.dirname(__file__), "..", "models", "price_forecaster.joblib"),
    )


async def generate_forecast(db, region: str, model: "PriceForecaster") -> List[Dict[str, Any]]:
    """Predict the next horizon of 30-min prices for ``region``.

    Returns a list of ``{interval_datetime, predicted_price}`` dicts, empty if
    no forward PASA data is available yet. Row order matches the PASA inputs
    (``assemble_features`` does not reorder when ``include_target=False``).
    """
    inputs = await load_forecast_inputs(db, region)
    if inputs.empty:
        return []
    X, _, _ = assemble_features(inputs, include_target=False)
    preds = model.predict(X)
    intervals = pd.to_datetime(inputs["interval_datetime"]).dt.strftime("%Y-%m-%dT%H:%M:%S")
    return [
        {"interval_datetime": t, "predicted_price": round(float(p), 2)}
        for t, p in zip(intervals, preds)
    ]


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
