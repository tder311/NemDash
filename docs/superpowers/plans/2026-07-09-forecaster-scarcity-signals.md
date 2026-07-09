# Forecaster Scarcity Signals Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the NemDash price forecaster flag scarcity/spike risk by (A) feeding it window-based AEMO predispatch price features with lead-aware training, and (B) adding P10/P90 quantile heads with spike-aware validation, plus a `forecast_history` table for live scorecarding.

**Architecture:** All model logic lives in `nem-dashboard-backend/app/forecaster.py` (pure functions + `PriceForecaster` wrapper; async `load_*` helpers are the only DB-aware code). Data comes from existing tables `predispatch_price`, `pdpasa_data`, `stpasa_data`, `price_data` — no new ingestion. One new table `forecast_history`. Frontend band goes in `ForecastPage.js` (react-plotly.js).

**Tech Stack:** Python 3.11+, pandas, xgboost ≥2.0 (3.2.0 installed — `reg:quantileerror` available), asyncpg, FastAPI, pytest (asyncio_mode=auto), React + react-plotly.js.

**Spec:** `docs/superpowers/specs/2026-07-09-forecaster-scarcity-signals-design.md` (committed). Read it first.

## Global Constraints

- Work in the worktree `/Users/tomderrick/repos/NemDash/.claude/worktrees/forecaster-scarcity` (branch `feat/forecaster/scarcity-signals`). All paths below are relative to it.
- **NEVER set/export `DATABASE_URL` when running pytest** — the test_db fixture TRUNCATES whatever database it points at. Unit tests are pure; run them with no `DATABASE_URL` in the environment.
- Run backend tests from `nem-dashboard-backend/`: `python3 -m pytest tests/unit/test_forecaster.py -v` (system python3 = anaconda, has all deps).
- No local imports — all imports at top of module. No `hasattr`/`getattr`/defensive checks; loaders guarantee columns exist (NaN is a valid value, absence is not).
- Vectorized pandas only — no row iteration.
- Comments ≤2 lines, describe current intent, never history.
- Commit after each passing task with `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>` trailer; commit only files relevant to the task.
- Keep functions small (complexity ceiling ~12): extract helpers rather than nesting.

---

### Task 1: Predispatch window features (pure)

**Files:**
- Modify: `nem-dashboard-backend/app/forecaster.py` (add constants + `predispatch_window_features` after `_pasa_derived_features`)
- Test: `nem-dashboard-backend/tests/unit/test_forecaster.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: constants `PD_FEATURES: List[str]`, `SPIKE_THRESHOLD_LOW = 300.0`, and
  `predispatch_window_features(pd_frame: pd.DataFrame) -> pd.DataFrame` — input needs columns
  `run_datetime, interval_datetime, regionid, rrp` (any number of runs); returns a copy with the 6
  added feature columns, computed **within each (run_datetime, regionid, calendar-day)** group.

Predispatch prices are unreliable pointwise; sustained high-price windows are the signal. Features per interval, over its run's forecast of that calendar day:
`pd_rrp` (pointwise), `pd_hours_above_300/1000/5000` (breadth), `pd_longest_run_above_300` (contiguity, hours), `pd_exceedance_sum` (Σ max(rrp−300, 0)).

- [ ] **Step 1: Write the failing tests** (append to `tests/unit/test_forecaster.py`; add `predispatch_window_features`, `PD_FEATURES` to the module's existing `from app.forecaster import (...)` block)

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd nem-dashboard-backend && python3 -m pytest tests/unit/test_forecaster.py -k pd_window -v`
Expected: ImportError (`predispatch_window_features` not defined).

- [ ] **Step 3: Implement** (in `forecaster.py`: constants near `PASA_FEATURES`, function after `_pasa_derived_features`)

```python
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


def _longest_true_run(above: pd.Series) -> float:
    """Length (in intervals) of the longest contiguous True run."""
    if not above.any():
        return 0.0
    blocks = (~above).cumsum()[above]
    return float(blocks.value_counts().max())


def predispatch_window_features(pd_frame: pd.DataFrame) -> pd.DataFrame:
    """Add PD scarcity features, aggregated within (run, region, calendar day).

    Grouping by run keeps different forecast runs from bleeding into each other;
    the calendar-day window captures "how much of that day does this run think is tight".
    """
    df = pd_frame.copy()
    df["interval_datetime"] = pd.to_datetime(df["interval_datetime"])
    df["run_datetime"] = pd.to_datetime(df["run_datetime"])
    df = df.sort_values(["run_datetime", "regionid", "interval_datetime"]).reset_index(drop=True)
    df["pd_rrp"] = df["rrp"].astype("float32")

    keys = [df["run_datetime"], df["regionid"], df["interval_datetime"].dt.date]
    grouped = df.groupby(keys)["rrp"]
    for threshold in (300, 1000, 5000):
        counts = grouped.transform(lambda s, t=threshold: (s > t).sum())
        df[f"pd_hours_above_{threshold}"] = counts * HOURS_PER_INTERVAL
    df["pd_longest_run_above_300"] = (
        grouped.transform(lambda s: _longest_true_run(s > SPIKE_THRESHOLD_LOW)) * HOURS_PER_INTERVAL
    )
    df["pd_exceedance_sum"] = grouped.transform(
        lambda s: (s - SPIKE_THRESHOLD_LOW).clip(lower=0).sum()
    )
    df[PD_FEATURES] = df[PD_FEATURES].astype("float32")
    return df
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd nem-dashboard-backend && python3 -m pytest tests/unit/test_forecaster.py -v`
Expected: all PASS (new + pre-existing 19).

- [ ] **Step 5: Commit**

```bash
git add nem-dashboard-backend/app/forecaster.py nem-dashboard-backend/tests/unit/test_forecaster.py
git commit -m "feat: window-based predispatch scarcity features"
```

---

### Task 2: Multi-lead run selection

**Files:**
- Modify: `nem-dashboard-backend/app/forecaster.py` (add `LEAD_BUCKETS` + `select_runs_at_leads` directly after `select_runs_at_lead`)
- Test: `nem-dashboard-backend/tests/unit/test_forecaster.py`

**Interfaces:**
- Consumes: existing `select_runs_at_lead(pasa, target_lead_hours, tolerance_hours)` (keeps `lead_hours` col, drops nothing else; note it leaves a `lead_dist` col — drop it in the new fn).
- Produces: `LEAD_BUCKETS: List[Tuple[float, float]]` and
  `select_runs_at_leads(pasa: pd.DataFrame, buckets=LEAD_BUCKETS) -> pd.DataFrame` — one row per
  (interval, region, bucket) with `lead_hours` (actual) and `lead_bucket` (target) columns; rows
  where the same run served adjacent buckets are deduplicated.

Why: PD shows phantom VOLL 5–7 days out (units haven't bid). Training across leads with `lead_hours` as a feature lets the model learn lead-conditional trust — and fixes the same latent train/serve mismatch for PASA.

- [ ] **Step 1: Write the failing tests** (append; import `LEAD_BUCKETS`, `select_runs_at_leads`)

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd nem-dashboard-backend && python3 -m pytest tests/unit/test_forecaster.py -k at_leads -v`
Expected: ImportError.

- [ ] **Step 3: Implement**

```python
# (target_lead_hours, tolerance_hours) buckets spanning intraday to 7-day leads.
# Tolerances roughly half the gap to the neighbouring bucket so bands don't overlap much.
LEAD_BUCKETS: List[Tuple[float, float]] = [
    (12.0, 6.0),
    (24.0, 12.0),
    (48.0, 24.0),
    (96.0, 36.0),
    (168.0, 36.0),
]


def select_runs_at_leads(
    pasa: pd.DataFrame,
    buckets: List[Tuple[float, float]] = LEAD_BUCKETS,
) -> pd.DataFrame:
    """One row per (interval, region, lead bucket): the run nearest each target lead.

    Training across leads (with lead_hours as a feature) teaches the model how much to
    trust far-lead inputs — e.g. phantom VOLL a week out vs. real tightness at 12h.
    """
    frames = []
    for target, tolerance in buckets:
        sel = select_runs_at_lead(pasa, target_lead_hours=target, tolerance_hours=tolerance)
        sel["lead_bucket"] = target
        frames.append(sel)
    out = pd.concat(frames, ignore_index=True)
    # A run that serves several buckets would duplicate rows; keep its nearest bucket only.
    out = out.sort_values("lead_dist").drop_duplicates(
        subset=["interval_datetime", "regionid", "run_datetime"], keep="first"
    )
    return out.drop(columns=["lead_dist"]).sort_values(
        ["regionid", "interval_datetime", "lead_bucket"]
    ).reset_index(drop=True)
```

Note: `select_runs_at_lead` currently drops `lead_dist` implicitly? **Check the source** (`forecaster.py:232-258`) — it keeps `lead_hours` and `lead_dist` in the returned frame. If it drops them, adjust: recompute `lead_dist` locally before sorting.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd nem-dashboard-backend && python3 -m pytest tests/unit/test_forecaster.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add nem-dashboard-backend/app/forecaster.py nem-dashboard-backend/tests/unit/test_forecaster.py
git commit -m "feat: multi-lead PASA/PD run selection with lead_hours"
```

---

### Task 3: Wire PD + lead features into training and inference frames

**Files:**
- Modify: `nem-dashboard-backend/app/forecaster.py` — `assemble_features` (:156), `merge_price_pasa` (:283), `load_training_frame` (:457), `load_forecast_inputs` (:508), new `_fetch_predispatch_history`
- Test: `nem-dashboard-backend/tests/unit/test_forecaster.py`

**Interfaces:**
- Consumes: Task 1 `predispatch_window_features`/`PD_FEATURES`; Task 2 `select_runs_at_leads`.
- Produces: `assemble_features` now requires `lead_hours` + all `PD_FEATURES` columns on the merged
  frame (NaN allowed, absence is an error) and includes them in X. `load_training_frame` and
  `load_forecast_inputs` both guarantee those columns. Feature count grows by 7.

Key decisions:
- PD window features are computed on the **full PD history first** (within-run), *then* lead-selected — so a row's window aggregates always come from a single coherent run.
- Training merges PD onto PASA rows on `(interval_datetime, region, lead_bucket)`, how="left" — early history has no PD coverage → NaN, which XGBoost handles natively. Do **not** fill with 0 (0 means "confidently no scarcity"; NaN means "unknown").
- Inference: `lead_hours = (interval − now)`; PD features from the latest stored run via `db.get_latest_predispatch_price(region)` (returns `List[Dict]` incl. run_datetime, interval_datetime, regionid, rrp).

- [ ] **Step 1: Write the failing tests** (append; also update the existing `_synthetic_merged` helper's callers only if they break — they shouldn't; instead add NaN PD columns + lead_hours inside `_synthetic_merged` so existing model-learning tests keep passing)

In `_synthetic_merged`, after the frame is built, add:

```python
    merged["lead_hours"] = 24.0
    for col in PD_FEATURES:
        merged[col] = np.nan
```

New tests:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd nem-dashboard-backend && python3 -m pytest tests/unit/test_forecaster.py -k "assemble or learns_pd" -v`
Expected: FAIL — `assemble_features` neither includes nor requires the new columns.

- [ ] **Step 3: Implement**

In `assemble_features`, add to the `parts` list (after the `_pasa_derived_features` entry):

```python
        merged[PD_FEATURES + ["lead_hours"]].astype("float32"),
```

(pandas raises `KeyError` on missing columns — that's the fail-loud contract; document it in the docstring: "loaders guarantee these columns; NaN = data unavailable".)

`merge_price_pasa`: extend the selected column list so lead metadata survives the join:

```python
    q_cols = ["interval_datetime", "region"] + PASA_FEATURES + ["lead_hours", "lead_bucket"]
```

(`lead_bucket` is needed for the PD merge in `load_training_frame`, and is dropped before `assemble_features` sees it — it is a join key, not a feature.)

New DB fetch (next to `_fetch_pasa_history`):

```python
async def _fetch_predispatch_history(db, start: datetime, end: datetime) -> pd.DataFrame:
    """Pull stored PD7Day price rows (all runs) for a date window."""
    sql = (
        "SELECT run_datetime, interval_datetime, regionid, rrp FROM predispatch_price "
        "WHERE interval_datetime >= $1 AND interval_datetime <= $2"
    )
    async with db._pool.acquire() as conn:
        rows = await conn.fetch(sql, start, end)
    return pd.DataFrame([dict(r) for r in rows])
```

`load_training_frame` — replace the single-lead selection and add the PD join:

```python
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
```

`load_forecast_inputs` — after `combine_forward_pasa(...)` returns `pasa` (rename the local as needed):

```python
    forward = combine_forward_pasa(pd.DataFrame(pd_rows), pd.DataFrame(st_rows), now)
    if forward.empty:
        return forward
    forward["lead_hours"] = (
        (forward["interval_datetime"] - now).dt.total_seconds() / 3600.0
    ).astype("float32")
    pd_rows_latest = await db.get_latest_predispatch_price(region)
    pd_latest = pd.DataFrame(pd_rows_latest)
    if pd_latest.empty:
        for col in PD_FEATURES:
            forward[col] = np.nan
    else:
        pd_feat = predispatch_window_features(pd_latest)
        forward = forward.merge(
            pd_feat[["interval_datetime"] + PD_FEATURES], on="interval_datetime", how="left"
        )
    return forward
```

Also add a pure-function test for the forward path shape (no DB): build a small `combine_forward_pasa` output frame by hand, apply the same lead+PD-merge logic — **if this requires extracting the post-processing into a pure helper, do it**: `_extend_forward_frame(forward, pd_latest, now) -> pd.DataFrame` containing everything after `combine_forward_pasa`, and have `load_forecast_inputs` call it. Test:

```python
def test_extend_forward_frame_adds_lead_and_pd():
    now = pd.Timestamp("2026-07-08 12:00:00")
    forward = pd.DataFrame({
        "interval_datetime": pd.date_range("2026-07-08 12:30:00", periods=4, freq="30min"),
        "region": "VIC1",
        **{c: 1000.0 for c in PASA_FEATURES},
    })
    pd_latest = _pd_frame([100.0, 5500.0, 5500.0, 5500.0], start="2026-07-08 12:30:00",
                          run="2026-07-08 11:00:00")
    out = _extend_forward_frame(forward, pd_latest, now)
    assert out["lead_hours"].iloc[0] == 0.5
    assert out["pd_hours_above_5000"].iloc[0] == 1.5
    empty = _extend_forward_frame(forward.copy(), pd.DataFrame(), now)
    assert empty["pd_rrp"].isna().all()
```

- [ ] **Step 4: Run the full unit suite**

Run: `cd nem-dashboard-backend && python3 -m pytest tests/unit/test_forecaster.py -v`
Expected: all PASS (pre-existing tests updated only via `_synthetic_merged`).

Also run: `python3 -m pytest tests/unit/test_main.py -v` — the forecast endpoint tests mock the model/db; if any build feature frames by hand they may need the new columns. Fix forward.

- [ ] **Step 5: Commit**

```bash
git add nem-dashboard-backend/app/forecaster.py nem-dashboard-backend/tests/unit/test_forecaster.py
git commit -m "feat: PD window + lead features in training and inference frames"
```

---

### Task 4: Quantile heads (P10/P90)

**Files:**
- Modify: `nem-dashboard-backend/app/forecaster.py` — `PriceForecaster` (:333), `ModelCard` (:304)
- Test: `nem-dashboard-backend/tests/unit/test_forecaster.py`

**Interfaces:**
- Consumes: existing `PriceForecaster.train/predict/save/load`.
- Produces: `QUANTILES = [0.1, 0.9]`; `PriceForecaster.quantile_models: Dict[float, Any]` (empty dict
  before training); `train()` also fits the quantile boosters; `predict_quantiles(X) -> pd.DataFrame`
  with columns `p10`, `p90` (raises `RuntimeError` if quantile models absent — e.g. a blob saved by
  the old code); `save()`/`load()` round-trip them; `card.quantile` records `[0.1, 0.9]`.

Two separate boosters (not multi-alpha) — simpler predict shape, robust across xgboost versions. P10 ≤ P50 ≤ P90 is NOT enforced (independent objectives can cross); document in the `predict_quantiles` docstring.

- [ ] **Step 1: Write the failing tests**

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd nem-dashboard-backend && python3 -m pytest tests/unit/test_forecaster.py -k quantile -v`
Expected: AttributeError (`predict_quantiles` not defined).

- [ ] **Step 3: Implement**

```python
QUANTILES: List[float] = [0.1, 0.9]
```

In `PriceForecaster.__init__`: `self.quantile_models: Dict[float, Any] = {}`.

In `train()` (after fitting the point model):

```python
        for q in QUANTILES:
            qm = XGBRegressor(
                **{**self.params, "objective": "reg:quantileerror", "quantile_alpha": q}
            )
            qm.fit(X, y)
            self.quantile_models[q] = qm
        self.card.quantile = list(QUANTILES)
```

New method:

```python
    def predict_quantiles(self, X: pd.DataFrame) -> pd.DataFrame:
        """P10/P90 predictions. Crossings (p10 > p90) are possible and not corrected."""
        if not self.quantile_models:
            raise RuntimeError("no quantile heads; retrain with the current code")
        if self.card.feature_names:
            X = X.reindex(columns=self.card.feature_names, fill_value=0)
        return pd.DataFrame(
            {f"p{int(q * 100)}": self.quantile_models[q].predict(X) for q in QUANTILES},
            index=X.index,
        )
```

`save()`: dump `{"model": ..., "quantile_models": self.quantile_models, "card": ...}`.
`load()`: `obj.quantile_models = blob.get("quantile_models", {})` — `.get` is correct here (old blobs genuinely lack the key: conditional data, not a guaranteed contract).

`ModelCard.quantile` type widens: `quantile: Optional[Any] = None  # [0.1, 0.9] once quantile heads exist`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd nem-dashboard-backend && python3 -m pytest tests/unit/test_forecaster.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add nem-dashboard-backend/app/forecaster.py nem-dashboard-backend/tests/unit/test_forecaster.py
git commit -m "feat: P10/P90 quantile heads on the price forecaster"
```

---

### Task 5: Spike-aware validation metrics

**Files:**
- Modify: `nem-dashboard-backend/app/forecaster.py` — `_metrics` stays as-is; `walk_forward_validate` (:402) grows quantile metrics; new pure helpers `pinball_loss`, `spike_recall`
- Test: `nem-dashboard-backend/tests/unit/test_forecaster.py`

**Interfaces:**
- Consumes: Task 4 `predict_quantiles`.
- Produces: `pinball_loss(y_true, y_pred, q) -> float`; `spike_recall(y_true, p90, settled_above=1000.0, alert_above=500.0) -> float` (NaN when no spikes in the window); `walk_forward_validate` fold dicts and the aggregate gain keys `pinball_p10`, `pinball_p90`, `spike_recall`.

MAE/Spearman cannot detect the missed-spike failure mode — a model that never predicts spikes scores *better* on MAE. These metrics make the tail visible in every retrain log.

- [ ] **Step 1: Write the failing tests**

```python
def test_pinball_loss_asymmetry():
    y = np.array([100.0]); lo = np.array([50.0]); hi = np.array([150.0])
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd nem-dashboard-backend && python3 -m pytest tests/unit/test_forecaster.py -k "pinball or spike" -v`
Expected: ImportError.

- [ ] **Step 3: Implement**

```python
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
```

In `walk_forward_validate`'s fold loop, after the existing `_metrics` call (the fold's trained model `m` already has quantile heads from Task 4):

```python
        q = m.predict_quantiles(Xo.iloc[train_end:test_end])
        y_test = yo.iloc[train_end:test_end].values
        fold_metrics = _metrics(y_test, pred)
        fold_metrics["pinball_p10"] = pinball_loss(y_test, q["p10"].values, 0.1)
        fold_metrics["pinball_p90"] = pinball_loss(y_test, q["p90"].values, 0.9)
        fold_metrics["spike_recall"] = spike_recall(y_test, q["p90"].values)
        folds.append(fold_metrics)
```

And in the aggregate dict:

```python
        "pinball_p10": float(np.mean([f["pinball_p10"] for f in folds])),
        "pinball_p90": float(np.mean([f["pinball_p90"] for f in folds])),
        "spike_recall": float(np.nanmean([f["spike_recall"] for f in folds])),
```

(`np.nanmean` — folds without spikes are NaN by design. If every fold is NaN this warns and returns NaN; acceptable, the training log should show it.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd nem-dashboard-backend && python3 -m pytest tests/unit/test_forecaster.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add nem-dashboard-backend/app/forecaster.py nem-dashboard-backend/tests/unit/test_forecaster.py
git commit -m "feat: spike recall + pinball loss in walk-forward validation"
```

---

### Task 6: forecast_history table + p10/p90 in the forecast output

**Files:**
- Modify: `nem-dashboard-backend/app/database.py` — table DDL in `init_db` (after the `predispatch_price` block, ~:196) + `insert_forecast_history`
- Modify: `nem-dashboard-backend/app/forecaster.py` — `generate_forecast` (:530)
- Test: `nem-dashboard-backend/tests/unit/test_forecaster.py` (pure part), `nem-dashboard-backend/tests/unit/test_database.py` (mirror existing insert-test style there — read 2–3 neighbouring tests first and copy their mocking pattern)

**Interfaces:**
- Consumes: Task 4 `predict_quantiles`.
- Produces: table `forecast_history`; `NEMDatabase.insert_forecast_history(rows: List[Dict[str, Any]]) -> int`;
  `generate_forecast` items become `{interval_datetime, predicted_price, p10, p90}` (p10/p90 `None`
  when the loaded blob predates quantile heads) and it writes one history row per interval, never
  failing the serve on a history-write error.

- [ ] **Step 1: DDL + insert.** In `init_db`:

```sql
CREATE TABLE IF NOT EXISTS forecast_history (
    id BIGSERIAL PRIMARY KEY,
    run_at TIMESTAMP NOT NULL,
    interval_datetime TIMESTAMP NOT NULL,
    region TEXT NOT NULL,
    p50 REAL NOT NULL,
    p10 REAL,
    p90 REAL,
    model_trained_at TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (run_at, interval_datetime, region)
)
```

plus `CREATE INDEX IF NOT EXISTS idx_forecast_history_region_interval ON forecast_history(region, interval_datetime)`.

```python
    async def insert_forecast_history(self, rows: List[Dict[str, Any]]) -> int:
        """Persist a served forecast so misses are diagnosable and models scorecardable."""
        if not rows:
            return 0
        async with self._pool.acquire() as conn:
            await conn.executemany("""
                INSERT INTO forecast_history
                    (run_at, interval_datetime, region, p50, p10, p90, model_trained_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (run_at, interval_datetime, region) DO NOTHING
            """, [
                (r["run_at"], r["interval_datetime"], r["region"], r["p50"],
                 r["p10"], r["p90"], r["model_trained_at"])
                for r in rows
            ])
        return len(rows)
```

- [ ] **Step 2: Extract the pure prediction step.** So the new output shape is testable without a DB, split `generate_forecast`:

```python
def predict_intervals(inputs: pd.DataFrame, model: "PriceForecaster") -> List[Dict[str, Any]]:
    """Pure prediction: PASA/PD input frame -> list of forecast dicts."""
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
```

`generate_forecast` becomes load → `predict_intervals` → history write:

```python
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
```

(`forecaster.py` has no logger yet — add `logger = logging.getLogger(__name__)` + `import logging` at top. The bare `except Exception` is deliberate: history is telemetry, the serve must not fail on it.)

`forecast_price_series` also refactors onto the shared input-loading but keeps returning the P50 series — change its body to reuse `predict_intervals`? No — it needs un-rounded floats; leave its body as-is.

- [ ] **Step 3: Tests.**

```python
class _QuantlessModel(PriceForecaster):
    pass  # loaded old blob: quantile_models == {}


def test_predict_intervals_includes_quantiles():
    merged = _synthetic_merged(n_days=10, regions=("NSW1",))
    X_cols = merged.drop(columns=["price"])
    model = PriceForecaster({"n_estimators": 50}).train(*assemble_features(merged)[:2])
    out = predict_intervals(X_cols, model)
    assert {"interval_datetime", "predicted_price", "p10", "p90"} <= set(out[0])
    assert out[0]["p10"] is not None


def test_predict_intervals_none_quantiles_for_old_blob():
    merged = _synthetic_merged(n_days=10, regions=("NSW1",))
    model = PriceForecaster({"n_estimators": 50}).train(*assemble_features(merged)[:2])
    model.quantile_models = {}  # simulate a blob saved before quantile heads
    out = predict_intervals(merged.drop(columns=["price"]), model)
    assert out[0]["p10"] is None and out[0]["p90"] is None
```

For `insert_forecast_history`, mirror the mock-pool pattern used by neighbouring tests in `tests/unit/test_database.py` (read `test_database.py` first; follow its exact fixture style). Assert: empty list returns 0 without touching the pool; non-empty calls `executemany` once with 7-tuples.

- [ ] **Step 4: Run**

`cd nem-dashboard-backend && python3 -m pytest tests/unit/test_forecaster.py tests/unit/test_database.py tests/unit/test_main.py -v`
Expected: all PASS. `test_main.py` forecast-endpoint tests may need their mock `generate_forecast` return values updated to the new dict shape.

- [ ] **Step 5: Commit**

```bash
git add nem-dashboard-backend/app/forecaster.py nem-dashboard-backend/app/database.py \
        nem-dashboard-backend/tests/unit/test_forecaster.py nem-dashboard-backend/tests/unit/test_database.py \
        nem-dashboard-backend/tests/unit/test_main.py
git commit -m "feat: forecast_history table; p10/p90 in forecast output"
```

Note: `/api/forecast/prices` needs **no** model change — `PriceForecastResponse.data` is `List[Dict[str, Any]]`, the new keys pass through. Update the comment on `models.py:236` to `# [{interval_datetime, predicted_price, p10, p90}, ...]`.

---

### Task 7: Frontend P10–P90 band

**Files:**
- Modify: `nem-dashboard-frontend/src/components/ForecastPage.js` (forecast traces built around :132-151)

**Interfaces:**
- Consumes: `/api/forecast/prices` items now carrying `p10`/`p90` (possibly `null`).
- Produces: shaded band behind the existing "Model forecast" line; no band when quantiles are null.

- [ ] **Step 1: Implement.** Where `fx`/`fy` are built (~:132):

```js
  const hasBand = forecast.length > 0 && forecast[0].p10 != null;
  const fp10 = forecast.map((d) => d.p10);
  const fp90 = forecast.map((d) => d.p90);
```

Insert two traces **before** the 'Model forecast' trace in the plot's data array (order matters for `fill: 'tonexty'` and so the band renders under the line), included only `...(hasBand ? [bandTraces] : [])`:

```js
    {
      x: fx, y: fp90, mode: 'lines', line: { width: 0 },
      hoverinfo: 'skip', showlegend: false, name: 'P90',
    },
    {
      x: fx, y: fp10, mode: 'lines', line: { width: 0 },
      fill: 'tonexty', fillcolor: darkMode ? 'rgba(136,132,216,0.18)' : 'rgba(99,110,250,0.15)',
      hoverinfo: 'skip', showlegend: true, name: 'P10–P90 range',
    },
```

Match the file's existing trace style (check how 'Model forecast' at :151 is written — colors, hovertemplate conventions) before writing.

- [ ] **Step 2: Verify.** `cd nem-dashboard-frontend && CI=true npm test -- --watchAll=false` — expected: existing tests pass (band is additive). If a ForecastPage test snapshot exists and fails, update it deliberately.

- [ ] **Step 3: Commit**

```bash
git add nem-dashboard-frontend/src/components/ForecastPage.js
git commit -m "feat: P10-P90 uncertainty band on forecast chart"
```

---

### Task 8: Retrain, before/after metrics, ship

**Files:**
- Modify: `nem-dashboard-backend/models/price_forecaster.joblib` (retrained artefact)
- Maybe modify: `nem-dashboard-backend/scripts/train_forecaster.py` (only if its metric printout needs the new keys)

- [ ] **Step 1: Baseline metrics.** Record the committed model's card: `cd nem-dashboard-backend && python3 -c "from app.forecaster import PriceForecaster, default_model_path; import json; print(json.dumps(PriceForecaster.load(default_model_path()).card.metrics, indent=2))"`

- [ ] **Step 2: Copy env + retrain.** The worktree lacks the untracked `.env`: `cp /Users/tomderrick/repos/NemDash/nem-dashboard-backend/.env nem-dashboard-backend/.env`. Then `cd nem-dashboard-backend && python3 -m scripts.train_forecaster --days 365`. This reads the live DB (read-only) and overwrites `models/price_forecaster.joblib`. **Never** pass this DATABASE_URL to pytest.
- [ ] **Step 3: Sanity-check the new card.** Re-run the Step 1 command: metrics must now include `pinball_p10/p90` + `spike_recall`, feature_names must include the 7 new features. Note PD-history coverage: if `predispatch_price` only holds recent weeks, PD features are NaN for most training rows — record that in the PR body, it bounds how much lift A can show yet.
- [ ] **Step 4: Full test suite.** `cd nem-dashboard-backend && python3 -m pytest tests/unit -v` (no DATABASE_URL exported!). Frontend: `cd nem-dashboard-frontend && CI=true npm test -- --watchAll=false`.
- [ ] **Step 5: Commit model, push, draft PR.**

```bash
git add nem-dashboard-backend/models/price_forecaster.joblib
git commit -m "chore: retrain model with scarcity features + quantile heads"
git push -u origin feat/forecaster/scarcity-signals
gh pr create --draft --title "feat(forecaster): scarcity signals — PD window features, lead-aware training, P10/P90 heads" --body "<before/after metrics table + spec link>"
```

PR body must include: motivation (missed 2026-07-08 VIC1 $5,757 spike; AEMO day-ahead PD showed $412–691 evening), before/after walk-forward metrics, PD-history coverage caveat, and the note that the optimiser still consumes P50.
