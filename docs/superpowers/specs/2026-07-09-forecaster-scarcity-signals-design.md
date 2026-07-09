# Forecaster scarcity signals — design

**Date:** 2026-07-09
**Motivation:** The price forecaster missed the 2026-07-08 VIC1/SA1/TAS1 evening spike
(VIC1 $5,757/MWh in the 20:00 block) and a similar event in June. Diagnosis: the model
is a squared-loss P50 XGBoost whose only scarcity view is 24h-lead PASA aggregates —
it structurally averages spikes away, and it never sees AEMO's predispatch prices,
which showed VIC1 evenings at 3–7× normal levels day-ahead.

**Scope:** (A) window-based predispatch features, (B) P10/P90 quantile heads with
spike-aware validation, plus a `forecast_history` table. A two-stage spike-probability
model (C) and a bid-coverage feature from `bid_day_offer` are explicitly deferred until
A+B are in and measured.

## 1. Window-based predispatch features

Predispatch (PD7Day) prices are unreliable pointwise: one spiky interval 24h out means
little, but a sustained high-price block (e.g. 7 hours at VOLL) strongly raises the
probability that at least one interval in that window binds. Features are therefore
window aggregates, not pointwise prices.

New pure function `predispatch_window_features(pd_frame)` producing, per
(interval, region), computed over the interval's **calendar day** within a single run
(period-ending 00:00 edge case accepted; evening spikes, the motivating case, are unaffected):

| Feature | Definition |
|---|---|
| `pd_rrp` | pointwise PD RRP (weak corroborating feature) |
| `pd_hours_above_300` | hours of the trading day with PD RRP > $300 |
| `pd_hours_above_1000` | hours > $1,000 |
| `pd_hours_above_5000` | hours > $5,000 |
| `pd_longest_run_above_300` | longest contiguous run (hours) > $300 |
| `pd_exceedance_sum` | Σ max(RRP − 300, 0) over the trading day |

Data source: existing `predispatch_price` table
(`run_datetime, interval_datetime, regionid, rrp`, unique on all three). No schema
changes. Missing PD coverage → features NaN (XGBoost handles natively); rows are not
dropped.

## 2. Multi-lead training (phantom far-lead VOLL)

PD routinely shows VOLL 5–7 days out purely because most units have not yet bid —
far-lead cap prices are artifacts of an incomplete bid stack. The same
train/serve mismatch is already latent for PASA: training selects runs at ~24h lead,
but inference features for day-6 intervals come from ~6-day-lead runs.

Fix: **lead time becomes a feature and training spans leads.**

- Training builds rows at lead buckets ≈ {12h, 24h, 48h, 96h, 168h} per interval
  (generalising `select_runs_at_lead` to multiple targets), for both PASA and PD.
- New feature `lead_hours` (actual run→interval gap). The model learns
  lead-conditional trust: hours-above-VOLL at 12h lead is an alarm, at 144h lead noise.
- At inference each forward interval's lead is `interval − run_datetime` (the serving
  run's actual age, matching training), so stale runs get far-lead trust, not fresh-lead trust.
- Row count ~5× (~440k rows/yr, 5 regions) — trivial for `tree_method=hist`.

## 3. Quantile heads (P10/P90)

- `PriceForecaster` grows a quantile mode: one
  `XGBRegressor(objective="reg:quantileerror", quantile_alpha=[0.1, 0.9])` trained
  alongside the existing squared-loss P50; both stored in the same joblib blob, model
  card records the quantiles.
- P10 ≤ P50 ≤ P90 is **not** enforced (independent objectives can cross); documented,
  not corrected.
- Rationale: pinball loss rewards the P90 head for reacting to scarcity features that
  squared loss suppresses. P90 is the product's visible scarcity signal; nailing spike
  magnitude day-ahead is not an achievable target (AEMO's own day-ahead run peaked at
  $691 vs the realised $5,757).

## 4. Spike-aware validation

`walk_forward_validate` additionally reports, per fold and averaged:

- **Spike recall:** of test blocks that settled > $1,000, the fraction where P90 > $500.
- **Pinball loss** at 0.1 and 0.9.

Caveat recorded in the model card: with ~1 year of training data spikes are rare, so
spike recall is noisy — live measurement via `forecast_history` is the real read.

## 5. Forecast history table

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

Written fire-and-forget by `generate_forecast` after each serve. Evaluation endpoints
over it are a later follow-up.

## 6. API / frontend

- `/api/forecast/prices` items gain optional `p10`/`p90` (additive, non-breaking).
- Frontend price chart draws a shaded P10–P90 band around the forecast line.
- The dispatch optimiser keeps consuming P50; P90-aware dispatch is a separate,
  deliberate decision.

## 7. Testing & rollout

- TDD on pure parts: window features (sustained block must outscore an isolated spike
  of equal max), multi-lead selection (causality: run ≤ interval at every bucket),
  spike-recall metric, quantile plumbing.
- Retrain via existing `scripts/train_forecaster.py`; PR description includes
  before/after walk-forward metrics (MAE, Spearman, spike recall, pinball).
- Existing tests must keep passing; the saved-model format change is
  backwards-incompatible → retrain to pick up the new format locally. `models/*.joblib`
  is gitignored and never committed; old blobs degrade gracefully (p10/p90 None, band hidden).
