# NemDash Slim-Down Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Delete the forward stack (price forecasting, dispatch optimisation, bid-strategy generation, unit-generation inference) from NemDash, leaving a concise market-watching app.

**Architecture:** Delete-only cleanup per `docs/superpowers/specs/2026-07-28-slim-to-market-watch-design.md`. NemDash keeps all NEMWEB ingestion (sole Postgres writer) and market-watching pages; a future separate app will host the forward stack, reading the same Postgres. One deletion cluster per commit, suites green after every task.

**Tech Stack:** FastAPI + asyncpg backend (pytest), React CRA frontend (jest), PuLP/XGBoost dependencies being removed.

## Global Constraints

- Delete only — no rewrites, no new features. Git history is the archive.
- Postgres tables owned by removed features: delete their `CREATE TABLE`/insert/read code, never issue `DROP TABLE`.
- `predispatch_constraint` table, its ingestion, `constraint_ids.py`, and `/api/network/constraints` STAY (NetworkPage binding constraints).
- `/api/bids/{duid}` + `/api/duids/search` + `db.get_bid_bands_for_duid` STAY (BidBandPage actual-bids viewer). `/api/bid-bands` (parametric strategy) GOES.
- Backend tests: run from `nem-dashboard-backend/` with `python -m pytest tests/unit -x -q`. NEVER export a live `DATABASE_URL` — the `test_db` fixture TRUNCATES the database it points at.
- Frontend tests: run from `nem-dashboard-frontend/` with `CI=true npm test -- --watchAll=false`.
- Commit messages: `type(scope): description` matching repo history (e.g. `chore(frontend): ...`).
- Worktree: all work in `/Users/tomderrick/repos/NemDash/.opencode/worktrees/slim-market-watch` on branch `chore/app/slim-to-market-watch`.

---

### Task 1: Frontend — remove forward-stack pages

**Files:**
- Delete: `nem-dashboard-frontend/src/components/ForecastPage.js` + `.css`
- Delete: `nem-dashboard-frontend/src/components/DispatchPage.js` + `.css`
- Delete: `nem-dashboard-frontend/src/components/BidBandsPage.js` + `.css` (plural = strategy; singular `BidBandPage` stays)
- Delete: `nem-dashboard-frontend/src/components/GenerationForecastPage.js` + `.css`
- Delete: `nem-dashboard-frontend/src/__tests__/components/GenerationForecastPage.test.js`
- Modify: `nem-dashboard-frontend/src/App.js`

**Interfaces:**
- Produces: `App.js` TABS array containing only: dashboard, metrics, chat, network, bids, downloads.

- [ ] **Step 1: Delete the eight page files and the test file** (`git rm`).

- [ ] **Step 2: Edit `App.js`** — remove import lines 7, 8, 9, 12 (`ForecastPage`, `DispatchPage`, `BidBandsPage`, `GenerationForecastPage`) and TABS entries at lines 17, 18, 19, 22 (`forecast`, `dispatch`, `bidbands`, `generation`).

- [ ] **Step 3: Grep for dangling references**

Run: `grep -rn 'ForecastPage\|DispatchPage\|BidBandsPage\|GenerationForecast' src/` — expect no hits outside `NetworkPage`-unrelated files; fix any found.

- [ ] **Step 4: Run jest** — `CI=true npm test -- --watchAll=false`. Expected: PASS (GenerationForecastPage suite gone, others untouched).

- [ ] **Step 5: Commit** — `chore(frontend): remove forecast, dispatch, bid-strategy and generation-forecast pages`

---

### Task 2: Frontend — trim NetworkPage and ChatPage copy

**Files:**
- Modify: `nem-dashboard-frontend/src/components/NetworkPage.js` (unit-inference section: state at ~188-247, fetches of `/api/network/unit-inference/*`, and its JSX panel)
- Modify: `nem-dashboard-frontend/src/__tests__/components/NetworkPage.test.js` (mocks at 68-71, 116; tests at 158-163, 203)
- Modify: `nem-dashboard-frontend/src/components/ChatPage.js` (lines 8-9, 164, 195)

**Interfaces:**
- Produces: NetworkPage rendering only interconnectors + binding constraints.

- [ ] **Step 1: Remove the unit-inference feature from `NetworkPage.js`** — the `UNIT_INFERENCE_DAYS` constant, unit-inference state/fetch hooks (calls to `/api/network/unit-inference/units` and `/series`), the heatmap-building helpers used only by that panel, and the panel's JSX. Keep interconnectors (`/api/network/interconnectors`) and constraints (`/api/network/constraints`) untouched.

- [ ] **Step 2: Prune `NetworkPage.test.js`** — delete unit-inference mock branches and the two unit-inference test cases; keep interconnector/constraint tests.

- [ ] **Step 3: Update `ChatPage.js` copy** — replace the two suggestion strings (lines 8-9) with:

```js
"What's the current price in NSW1?",
'How tight is the PASA outlook for SA1 this week?',
```

Line 164 → `<p>Ask the NemDash analyst about live prices, generation mix, and the PASA outlook.</p>`
Line 195 placeholder → `"Ask about prices, generation, or PASA…"`

- [ ] **Step 4: Run jest** — `CI=true npm test -- --watchAll=false`. Expected: PASS.

- [ ] **Step 5: Commit** — `chore(frontend): drop unit-inference panel from NetworkPage, retune chat copy`

---

### Task 3: Backend — strip forward-stack tools from the chat agent

**Files:**
- Modify: `nem-dashboard-backend/app/agent.py`
- Modify: `nem-dashboard-backend/app/main.py:1701-1732` (chat endpoint)
- Modify: `nem-dashboard-backend/tests/unit/test_agent.py`

**Interfaces:**
- Produces: `stream_chat(client, db, messages)` and `_execute_tool(db, name, args)` — no `forecaster` parameter. Task 4's chat endpoint edit consumes this signature.

- [ ] **Step 1: Edit `agent.py`** — remove from `TOOLS` the `get_price_forecast` (line ~103), `optimise_battery_dispatch` (~119), and `get_bid_bands` (~142) schemas; delete `_forecast_series` (~196) and the "forward-stack tools" branch of `_execute_tool` (~279-330s); drop the `forecaster` parameter from `_execute_tool` and `stream_chat`; remove `optimiser`/`bid_bands`/forecaster imports; rewrite the system prompt (~167-183) to drop forecast/dispatch/bid-band sentences, e.g. keep: analyst over live prices, generation, PASA; every figure from a tool.

- [ ] **Step 2: Edit `main.py` chat endpoint** — remove the `_get_forecaster()` call (~1717) and pass `stream_chat(client, db, messages)`.

- [ ] **Step 3: Prune `test_agent.py`** — delete `test_forecast_tools_blocked_without_model`, `test_get_price_forecast_emits_line_artifact`, `test_forward_stack_tool_no_forecast_data`, `test_optimise_battery_dispatch_emits_artifact`, `test_get_bid_bands_emits_table_artifact`, `test_get_bid_bands_day_offset_beyond_horizon`, `test_forecast_series_delegates_to_forecaster`; update remaining calls to the new `_execute_tool`/`stream_chat` signatures.

- [ ] **Step 4: Run pytest** — `python -m pytest tests/unit/test_agent.py tests/unit/test_main.py -x -q`. Expected: PASS.

- [ ] **Step 5: Commit** — `chore(backend): chat agent keeps live-data tools only`

---

### Task 4: Backend — remove forward-stack API endpoints

**Files:**
- Modify: `nem-dashboard-backend/app/main.py`
- Modify: `nem-dashboard-backend/app/models.py`
- Modify: `nem-dashboard-backend/tests/unit/test_main.py` (verify only — grep found no forward-stack endpoint tests)

**Interfaces:**
- Consumes: Task 3's `stream_chat(client, db, messages)`.
- Produces: `main.py` with no references to `forecaster`, `optimiser`, `bid_bands`, `joint_inference`, `unit_inference`.

- [ ] **Step 1: Delete endpoints in `main.py`** (routes + handler bodies): `/api/forecast/prices` (1149), `/api/forecast/accuracy` (1186), `/api/forecast/retrain` (1217), `/api/forecast/status` (1229), `/api/predispatch/prices` (1236 — sole consumer was ForecastPage; ingestion of predispatch prices stays), `/api/network/unit-inference/units` (1351), `/api/network/unit-inference/series` (1390), `/api/network/generation-forecast` (1438), `/api/optimise/dispatch` (1509), `/api/bid-bands` (1584). Keep `/api/network/constraints`, `/api/bids/{duid}`, `/api/duids/search`.

- [ ] **Step 2: Delete supporting wiring** — imports from `.joint_inference` (51), `.forecaster` (57), `.optimiser` (68), `.bid_bands` (69); the `_forecaster` singleton, `_get_forecaster()`, and retrain hot-reload block (~85-149).

- [ ] **Step 3: Delete now-unused response models in `models.py`** — `PriceForecastResponse`, `ForecastAccuracyResponse`, `UnitInferenceUnitsResponse`, `UnitInferenceSeriesResponse`, `GenerationForecastResponse` and any classes referenced only by them (`grep` each nested type before deleting). `BidBandResponse` stays.

- [ ] **Step 4: Grep** — `grep -n 'forecaster\|optimise\|bid_bands\|inference' app/main.py app/models.py` → no hits (except the actual-bids `get_bid_bands` handler name for `/api/bids/{duid}`, which may be renamed `get_actual_bids` for clarity).

- [ ] **Step 5: Run pytest** — `python -m pytest tests/unit/test_main.py -x -q`. Expected: PASS; if any test imports a deleted response model, delete that test with it.

- [ ] **Step 6: Commit** — `chore(backend): remove forecast, optimise, bid-strategy and inference endpoints`

---

### Task 5: Backend — make ingestion self-contained

**Files:**
- Modify: `nem-dashboard-backend/app/data_ingester.py`
- Modify: `nem-dashboard-backend/tests/unit/test_data_ingester.py`
- Modify: `nem-dashboard-backend/tests/unit/test_forecaster.py` (move 4 tests out before Task 6 deletes it)

**Interfaces:**
- Produces: `select_runs_at_leads(pasa, buckets=LEAD_BUCKETS)`, `LEAD_BUCKETS`, and `_causal_band_select` defined IN `data_ingester.py`; no `data_ingester` imports from `forecaster` or `joint_inference`.

- [ ] **Step 1: Move the PASA lead-selection cluster verbatim** from `app/forecaster.py` into `app/data_ingester.py` (above `thin_pasa_for_multilead_backfill`): `_causal_band_select` (def at forecaster.py:309), `LEAD_BUCKETS` (:355), `select_runs_at_leads` (:374). Do not move `select_runs_at_lead` or `lead_envelope_hours` (no surviving users). Remove the `from .forecaster import select_runs_at_leads` line.

- [ ] **Step 2: Remove inference from ingestion** — delete `_infer_unit_generation` (data_ingester.py:443-475) and its call site inside `ingest_predispatch_data`/`backfill_predispatch_data` (grep `_infer_unit_generation`), plus the `from .joint_inference import ...` line (18).

- [ ] **Step 3: Move tests** — relocate `test_select_runs_at_lead_picks_closest_to_target_and_drops_out_of_band`, `test_select_runs_at_lead_tiebreak_prefers_longer_lead`, `test_select_runs_at_leads_one_row_per_bucket`, `test_select_runs_at_leads_dedups_shared_runs` from `test_forecaster.py` into `test_data_ingester.py`, importing from `app.data_ingester`. Drop the two `select_runs_at_lead` (singular) tests if they only exercise the unmoved function — verify what each imports; the singular function is deleted, so its 2 tests are deleted.

- [ ] **Step 4: Prune `test_data_ingester.py`** of any `_infer_unit_generation`/joint-inference cases (grep `infer`).

- [ ] **Step 5: Run pytest** — `python -m pytest tests/unit/test_data_ingester.py -x -q`. Expected: PASS.

- [ ] **Step 6: Commit** — `chore(backend): ingestion owns PASA lead selection, drops inference step`

---

### Task 6: Backend — delete forward-stack modules, scripts, DB helpers, deps

**Files:**
- Delete: `app/forecaster.py`, `app/optimiser.py`, `app/bid_bands.py`, `app/joint_inference.py`, `app/unit_inference.py`
- Delete: `scripts/train_forecaster.py`, `scripts/validate_joint_inference.py`, `scripts/validate_unit_inference.py`, `scripts/ingest_constraint_equations.py`, `scripts/ingest_nemde_constraints.py` (both feed `constraint_equation_terms`, whose sole reader is `joint_inference.fetch_terms`)
- Delete: `tests/unit/test_forecaster.py`, `test_optimiser.py`, `test_bid_bands.py`, `test_joint_inference.py`, `test_unit_inference.py`, `test_validate_joint_inference.py`, `test_validate_unit_inference.py`
- Modify: `app/database.py`, `tests/unit/test_database.py`, `requirements.txt`

**Interfaces:**
- Consumes: Tasks 3-5 already removed every `app/` import of these modules.

- [ ] **Step 1: `git rm` the five modules, five scripts, seven test files.** Keep `scripts/backfill_predispatch.py` (feeds `predispatch_constraint` for NetworkPage).

- [ ] **Step 2: Delete orphaned `database.py` code** — `forecast_history` CREATE TABLE (~357+) and its index (~477), `constraint_equation_terms` CREATE TABLE/ALTER block (~263-330), `insert_constraint_equation_terms` (~1933), `get_latest_generation_forecast_rows` (~2056), `insert_forecast_history` (~2082), and any unit-inference read/write methods (`grep -n 'inference\|forecast_history\|constraint_equation' app/database.py` and delete each hit's method). KEEP: `insert_predispatch_constraint`, `get_latest_predispatch_constraints`, predispatch price inserts, `get_bid_bands_for_duid`.

- [ ] **Step 3: Prune `test_database.py`** — `test_insert_constraint_equation_terms` (x3, lines 878-904), `test_insert_forecast_history*` (x2, 1894-1905), plus any tests of methods deleted in Step 2.

- [ ] **Step 4: Prune `requirements.txt`** — remove `xgboost`, `scikit-learn`, `pulp`. For `scipy`: `grep -rn scipy app/ scripts/` — remove iff no surviving user. Keep `openai` (agent).

- [ ] **Step 5: Full backend suite** — `python -m pytest tests/unit -x -q`. Expected: PASS.

- [ ] **Step 6: Commit** — `chore(backend): delete forward-stack modules, scripts, orphaned db helpers and deps`

---

### Task 7: Docs, verification sweep, ship

**Files:**
- Delete: `docs/superpowers/specs/2026-07-09-forecaster-scarcity-signals-design.md`, `docs/superpowers/plans/2026-07-09-forecaster-scarcity-signals.md`
- Modify: `README.md` (only if it documents removed endpoints/pages — grep first; feature list is already market-watching-only)

**Interfaces:** none.

- [ ] **Step 1: Delete stale forward-stack docs; grep README/CLAUDE.md** — `grep -n 'forecast\|optimis\|bid.band\|inference' README.md CLAUDE.md Makefile` and update/remove stale mentions (ignore DB "constraint" hits).

- [ ] **Step 2: Repo-wide dangling-name sweep** — from repo root: `grep -rn 'forecaster\|optimiser\|bid_bands\|joint_inference\|unit_inference\|ForecastPage\|DispatchPage\|BidBandsPage\|GenerationForecast' --include='*.py' --include='*.js' nem-dashboard-backend nem-dashboard-frontend | grep -v node_modules | grep -v __pycache__` → zero hits (allow `select_runs_at_leads` docstring mentions inside data_ingester and the frontend `forecastdocs`-free chat copy).

- [ ] **Step 3: Both suites green** — backend `python -m pytest tests/unit -q`; frontend `CI=true npm test -- --watchAll=false`.

- [ ] **Step 4: Boot check** — `scripts/dev.sh` (repo root); confirm backend starts clean (no import errors in log) and frontend serves; spot-check `/health`, `/api/prices/latest`, `/api/network/constraints`.

- [ ] **Step 5: Commit docs** — `docs: drop stale forward-stack specs, align README`

- [ ] **Step 6: Push and open draft PR** — `git push -u origin chore/app/slim-to-market-watch`; `gh pr create --draft` titled `chore(app): slim NemDash to market watching only`. PR body: link the design spec; note (a) Postgres tables for removed features are intentionally left in place, (b) `/api/predispatch/prices` removed as its sole consumer was ForecastPage though ingestion continues, (c) Railway env vars for the forecaster model path (if any) can be deleted after merge.

---

### Task 8: Remove the Network tab end-to-end (scope addition, 2026-07-28)

Approved addition: the Network page goes too. Ingestion of interconnector
flows and predispatch constraints STAYS (sole-writer discipline — data keeps
landing in Postgres for future apps); only serving/display code goes.

**Files:**
- Delete: `nem-dashboard-frontend/src/components/NetworkPage.js` + `.css`, `nem-dashboard-frontend/src/__tests__/components/NetworkPage.test.js`
- Modify: `nem-dashboard-frontend/src/App.js` (remove NetworkPage import + `network` TAB — final tabs: dashboard, metrics, chat, bids, downloads)
- Modify: `nem-dashboard-backend/app/main.py` — delete `/api/network/interconnectors` and `/api/network/constraints` endpoints and the `parse_constraint_id` import
- Delete: `nem-dashboard-backend/app/constraint_ids.py` + its unit tests (verify no other importer first)
- Modify: `nem-dashboard-backend/app/models.py` — delete `NetworkInterconnectorsResponse`, `NetworkConstraintsResponse` + nested models only they use
- Modify: `nem-dashboard-backend/app/database.py` — delete read methods whose only callers were those endpoints (e.g. `get_latest_predispatch_constraints`, `filter_binding_constraints`, and the interconnector-flow read the endpoint used — grep each for other callers first). KEEP all inserts and the `predispatch_constraint`/interconnector table DDL.
- Modify: `nem-dashboard-backend/tests/unit/test_main.py` / `test_database.py` — delete tests of removed endpoints/methods only
- Modify: `docs/superpowers/specs/2026-07-28-slim-to-market-watch-design.md` — move Network serving from Stays to Removals (note: ingestion still stays); update README if it lists the Network feature

**Steps:** delete → grep sweep (`NetworkPage`, `network/interconnectors`, `network/constraints`, `constraint_ids`, `parse_constraint_id` → zero hits outside docs/git history) → backend `python -m pytest tests/unit -x -q` + frontend `CI=true npm test -- --watchAll=false` green → commit `chore(app): remove network tab and its serving endpoints`.

### Task 9: Ingestion worker split (scope addition, 2026-07-28)

Approved addition: continuous ingestion moves out of the API process into a
separate worker so ingestion never competes with request serving. Same repo,
same image — a second Railway service with a different start command.

**Files:**
- Create: `nem-dashboard-backend/run_worker.py` — standalone asyncio entry point: build `DataIngester` from the same env vars the API uses (reuse the existing construction/config logic — extract a tiny shared helper if needed rather than duplicating), `await initialize()`, then `run_continuous_ingestion(update_interval)` forever; clean shutdown on SIGTERM/SIGINT (`stop_continuous_ingestion` + `cleanup`).
- Modify: `nem-dashboard-backend/app/main.py` lifespan — stop launching `run_continuous_ingestion`; the API keeps constructing `DataIngester` so manual `/api/ingest/*` endpoints still work (known ceiling: manual backfills still burn API resources — note in code comment `# ponytail: manual backfills run in-process; move to worker if they ever bog down serving`).
- Modify: `nem-dashboard-backend/tests/unit/` — update any lifespan/ingestion-startup tests; add one unit test that `run_worker`'s loop wiring calls `run_continuous_ingestion` (mocked — no network/DB).
- Modify: `README.md` — architecture note: three deployables (ingestion worker = sole writer, NemDash API = reader, future forecasting API = reader), one Postgres.
- Modify: `docs/superpowers/specs/2026-07-28-slim-to-market-watch-design.md` — architecture section gains the worker.

**Steps:** implement → `python -m pytest tests/unit -x -q` green → sanity: `python -c "import run_worker"` and API boot without ingestion task → commit `feat(backend): split continuous ingestion into standalone worker process`.

**Deploy note (manual, post-merge):** create a second Railway service off the same repo/image with start command `python run_worker.py`; scheduler env vars move to it. Railway CLI config edits no-op — use the dashboard.

---

## Self-Review Notes

- Spec coverage: every Removals/Stays bullet maps to a task (pages→1-2, agent→3, endpoints→4, ingester→5, modules/scripts/db/deps→6, docs/verify→7). Constraint-ingestion iff-rule resolved: scripts go, `predispatch_constraint` stays.
- Order guarantees imports never dangle: `app/` references removed (3-5) before modules deleted (6).
- Line numbers are as-of `origin/main` @ 2ad0332; re-grep before each edit if drift.
