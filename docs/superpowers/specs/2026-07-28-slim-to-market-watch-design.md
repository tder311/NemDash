# NemDash slim-down: market watching only

**Date:** 2026-07-28
**Status:** Approved (option 1 architecture)

## Goal

NemDash becomes a concise market-watching app: live prices, historical
prices, generation, network, PASA, and actual participant bids. The
forward stack — price forecasting, dispatch optimisation, bid-strategy
generation, and unit-generation inference — moves to a separate app
(built later, outside this repo).

## Target architecture (option 1)

Two frontends, two backends, one Postgres:

- **NemDash** (this repo): ingestion + market-watching API + SPA.
  Remains the **sole Postgres writer** (all NEMWEB ingestion).
- **Forecasting app** (future, separate repo): own SPA + own FastAPI
  for forecast/optimiser/bidding endpoints. Reads the same Postgres
  directly; never writes; never imports NemDash code.

Rationale: the shared asset is the data, not the API. Direct SQL beats
paginated JSON for bulk training pulls, and CPU-heavy work (XGBoost
training, CBC LP solves) leaves the service that serves the 30-second
live-price loop. Upgrade path if reads ever contend: Postgres read
replica.

## Removals (delete only — git history is the archive)

### Backend (`nem-dashboard-backend`)

- Modules: `app/forecaster.py`, `app/optimiser.py`, `app/bid_bands.py`,
  `app/joint_inference.py`, `app/unit_inference.py`
- `app/main.py`: price-forecast endpoints (incl. train/hot-reload
  hooks), dispatch-optimise endpoint, parametric bid-bands endpoint,
  unit-inference units/series endpoints, and the `_forecaster`
  singleton wiring
- `app/data_ingester.py`: scheduled unit-inference solving and the
  `select_runs_at_leads` forecaster import
- `app/agent.py`: forecast/optimise/bid-curve tools and the
  `forecaster` parameter to `stream_chat`; live-data tools stay
- `app/database.py` / `app/models.py`: query helpers and models used
  only by removed features
- (Task 8) `app/main.py`: `/api/network/interconnectors` and
  `/api/network/constraints` endpoints and the `parse_constraint_id`
  import; `app/constraint_ids.py` deleted (sole importer was
  `main.py`); `app/database.py` read-only accessors
  `get_latest_predispatch_interconnectors` and
  `get_latest_predispatch_constraints` (sole callers were those
  endpoints); `app/models.py` `NetworkInterconnectorsResponse` /
  `NetworkConstraintsResponse` + their nested models. Ingestion stays:
  `insert_predispatch_interconnector`, `insert_predispatch_constraint`,
  `filter_binding_constraints` (still used by
  `scripts/backfill_predispatch.py`), and the
  `predispatch_interconnector`/`predispatch_constraint` table DDL —
  data keeps landing in Postgres for the future forecasting app.
- Scripts: `scripts/train_forecaster.py`,
  `scripts/validate_joint_inference.py`,
  `scripts/validate_unit_inference.py`; trained model artifacts under
  `data/` if present
- Tests: `test_forecaster`, `test_optimiser`, `test_bid_bands`,
  `test_joint_inference`, `test_unit_inference`, `test_validate_*`;
  prune departed cases from `test_main`, `test_agent`,
  `test_data_ingester`, `test_database`

### Frontend (`nem-dashboard-frontend`)

- Pages (+ CSS, tests, nav entries in `App.js`): `ForecastPage`,
  `DispatchPage`, `BidBandsPage`, `GenerationForecastPage`
- (Task 8) `NetworkPage` (+ CSS, tests, `network` nav entry in
  `App.js`): removed entirely — supersedes the earlier plan to trim it
  to unit-inference only. Interconnector-flow and predispatch-
  constraint ingestion continues unaffected; only the tab and its
  serving endpoints go.
- `ChatPage`: prune forecast-flavoured suggested prompts
- Prune related mocks in `src/mocks` and `api.js` helpers

### Repo

- Stale forward-stack spec/plan docs under `docs/superpowers/`
- README, CLAUDE.md, Makefile updated to match

## Stays

- All NEMWEB ingestion: dispatch SCADA, trading prices, PASA,
  predispatch, actual bids (`nem_bid_client`), price setter
- `/api/bid-bands` viewer endpoint (actual DUID bids → `BidBandPage`)
- Pages: Live Prices, State Detail, Market Metrics, PASA, Bid Bands
  viewer, Downloads, DB Health, Chat (trimmed) — Network moved to
  Removals (Task 8)
- Interconnector-flow and predispatch-constraint ingestion continues
  via the predispatch path: `data_ingester.py`'s
  `ingest_predispatch_data` and `scripts/backfill_predispatch.py` feed
  `insert_predispatch_interconnector` / `insert_predispatch_constraint`
  regardless of the Task 8 display removal — Postgres remains the
  shared data asset for the future forecasting app. The
  `ingest_constraint_equations` / `ingest_nemde_constraints` scripts
  that existed solely for the inference backsolve were already removed
  in Task 6, alongside the inference solver.

## Boundary rules

- Postgres tables owned by removed features: code deleted, tables left
  in place — no destructive prod migration.
- Keep `data_ingester.py` / `models.py` / `database.py` free of
  app-specific modelling logic so the forecasting app can read the same
  tables without importing NemDash.
- Single-writer discipline: only NemDash ingests/writes.

## Verification

- Backend pytest green (never against a live `DATABASE_URL` — the test
  fixture truncates it)
- Frontend jest green
- `scripts/dev.sh` boots; every remaining page loads and renders data
- No dangling imports/routes: grep for removed module and page names
  returns nothing outside git history
