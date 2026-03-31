# Masters Golf Analytics — Dev Log

## Stack
- Python 3.12, dbt-duckdb 1.9.1, DuckDB 1.2.1, Streamlit 1.44.0
- DataGolf API (Scratch Plus annual, event_id=14 for Masters)
- Local DuckDB at data/masters.duckdb for dev
- MotherDuck (md:masters_golf) for prod — Phase 7
- dbt profile: masters_golf, dev target = local, prod target = MotherDuck

## Repo structure
- ingestion/    — DataGolf API client + DuckDB loader
- simulation/   — Monte Carlo engine (50k sims default, --n_sims flag)
- dbt/          — staging (views) → intermediate (views) → marts (tables)
- streamlit/    — 4-page app

## Phase status
- [x] Phase 1 — Repo + environment setup
- [x] Phase 2 — DataGolf ingestion ✓ (7 raw tables, 627 rounds, 543 pred_archive rows)
- [x] Phase 3 — dbt data model ✓ (14 models, 40 tests, 135-row mart_player_model_inputs)
- [x] Phase 4 — Simulation engine ✓ (93-player Masters field, 50k sims in 0.4s, win_pct sum=1.0)
- [ ] Phase 5 — Back-testing
- [ ] Phase 6 — Streamlit UI
- [ ] Phase 7 — MotherDuck + Streamlit Cloud deploy
- [ ] Phase 8 — Live tournament
- [ ] Phase 9 — Polish + launch

---

## Phase 2 — DataGolf ingestion context

### Endpoints to hit (in order)
- `get-player-list` → raw_player_list
- `preds/skill-ratings?display=value` → raw_skill_ratings
- `preds/approach-skill?period=l24` → raw_approach_skill
- `preds/get-dg-rankings` → raw_dg_rankings
- `field-updates?tour=pga` → raw_masters_field_2026
- `historical-raw-data/rounds` for tour=pga, event_id=14, 
  years 2019–2025 → raw_masters_rounds_hist
- `preds/pre-tournament-archive` for event_id=14, 
  years 2020–2025 → raw_pred_archive (back-testing)

### Key decisions
- Always join on datagolf_id — never on name strings
- 2020 Masters played in November (covid) — flag with 
  is_covid_year BOOLEAN in raw_masters_rounds_hist
- Rate limit: 45 req/min — add time.sleep(0.5) between calls
- Add retry logic with exponential backoff for 429 responses
- API key loaded from .env via python-dotenv, never hardcoded
- run_ingestion.py is the single entry point that calls all loaders

---

## Phase 3 — dbt model context

### Materialization
- staging: views (1:1 with raw, clean types + naming only)
- intermediate: views (derived features)
- marts: tables (final outputs, Streamlit reads from here)

### Staging models needed
- stg_player_list, stg_skill_ratings, stg_approach_skill
- stg_dg_rankings, stg_masters_field_2026
- stg_masters_rounds_hist — cast types, add is_covid_year flag, dedup
- stg_pred_archive

### Intermediate models needed
- int_augusta_career_sg — career SG at Augusta per player, 
  recency-weighted (last 2 years get 2x weight), 
  exclude is_covid_year=true from averages
- int_recent_trajectory — last 6 rounds SG vs prior 18 SG 
  = momentum delta, capped at ±0.8 strokes
- int_augusta_fit_score — weighted composite:
  sg_app * 0.38 + sg_putt * 0.28 + sg_ott * 0.20 
  + sg_arg * 0.10 + sg_accuracy * 0.04
- int_player_sigma — std dev of round scores from historical data,
  fallback to 3.0 if fewer than 10 rounds available

### Mart models needed
- mart_player_model_inputs — one row per player in 2026 field,
  all features joined, ready for simulation
- mart_simulation_results — loaded back from Python after sim
  (win_pct, top5_pct, top10_pct, top25_pct, mc_pct, mu, sigma)
- mart_backtest_comparison — your predictions vs DG archived 
  predictions, for back-test validation

### Tests to add in schema.yml
- not_null + unique on datagolf_id in all mart models
- not_null on win_pct, top5_pct, top10_pct in mart_simulation_results
- accepted_values on is_covid_year: [true, false]

---

## Phase 3 — dbt model context (post-build notes)

### Validation results (confirmed clean)
- `mart_player_model_inputs`: 135 rows, 0 null on datagolf_id / augusta_mu / player_sigma
- Mu range: -1.73 to +0.92, mean -0.05 (reasonable SG distribution)
- Sigma range: 1.55 to 3.82, mean 2.96 (3.0 fallback for players with <10 rounds)
- Debut players (augusta_rounds_played=0): sigma correctly fallback to 3.0

### Known limitations
- `int_recent_trajectory` now uses a real two-signal combined momentum_delta:
  0.6 × DG `timing_adjustment` (from player-decompositions, current event only) +
  0.4 × Augusta trend (recent 2024–2025 SG vs prior 2019–2023 SG, capped ±0.8).
  Players absent from the current event field get timing_adjustment = 0 (coalesce).
  For Phase 5 back-test years, only the Augusta trend component is available (DG
  player-decompositions has no historical archive).
- 6 players have null `sg_overall_rolling` (not in DG top 500): Campos, Sargent, Willett,
  Wise, Howell III, Russell — these coalesce to 0 in the mu formula, which underweights them
- SG component coverage in masters_rounds: `sg_total` available all years (2019–2025);
  `sg_ott`, `sg_app`, `sg_arg`, `sg_putt` only available from 2021 onwards
- Pred archive win_pct values are decimal (0–1 range), summing to ~1.0 per year

---

## Phase 4 — Simulation context (post-build notes)

### Field data fix — upcoming_pga parameter
DataGolf's `field-updates?tour=pga` only returns the CURRENT active PGA Tour event.
Before Masters week, this returns the Valero Texas Open (or whatever is in play).
Fix: use `field-updates?tour=upcoming_pga` to get next week's field.
`refresh_field.py` now auto-detects: tries current event first, falls back to upcoming
if the current event name doesn't contain "masters" or "augusta".
93 players loaded for 2026 Masters field (normal range: 89–93 players per year).

### First full sim results (50k sims, 2026-03-30)
- McIlroy leads win_pct at 5.9% — strong DG rating + Augusta fit
- Scheffler: 4.2% win but lowest MC% (7.2%) and highest top10% (46.6%) — model correctly
  captures his consistency; win% slightly suppressed because high mu raises
  a lot of players' relative chances
- Validation assertion passed: win_pct sum = 1.0000

### Known limitations (Phase 4)
- player_decompositions always reflects the CURRENT PGA Tour event (Valero), not the Masters.
  timing_adjustment signal will be refreshed during Masters week via `refresh_field.py`.
- Masters field loaded 10 days pre-tournament — re-run refresh on April 7–9 to pick up any
  late WDs or last-minute invites before running the final simulation.

---

## Phase 4 — Simulation context

### Composite mu formula
Augusta_mu =
  w1 * sg_overall_rolling      (weight: 0.40)
  + w2 * augusta_historical_sg  (weight: 0.30)
  + w3 * augusta_fit_score      (weight: 0.20)
  + w4 * recent_trajectory      (weight: 0.10)

### Simulation mechanics
- Default 50,000 simulations, --n_sims CLI flag for dev testing
- Draw shared field_difficulty per round: np.random.normal(0, 1.2)
  — this is the correlated outcomes fix, applied to all players
- Per-round score = player_mu + field_difficulty + 
  np.random.normal(0, player_sigma)
- Vectorize with NumPy — draw all random numbers upfront, 
  not inside loops (performance critical)

### Cut rule — Masters specific
- Top 50 + ties (find score at position 50, include everyone 
  at or better) OR within 10 strokes of 36-hole leader
- Both conditions are independent — player passes if EITHER is true
- MC'd players get total_score = float('inf')

### Validation assertion
- After sim, assert 0.98 <= sum(all win_pcts) <= 1.02
- If assertion fails, check for duplicate players or cut rule bug

### Output
- Write results back to mart_simulation_results in DuckDB
- Include mu and sigma columns alongside probabilities 
  for transparency in the UI

---

## Phase 5 — Back-testing context

### Methodology
- For each year 2019–2024 (2020 optional), use only data 
  available BEFORE that tournament started
- Pull DG archived predictions from stg_pred_archive 
  for the same years as comparison baseline
- Primary metric: Spearman rank correlation 
  (your rank vs actual finishing rank)
- Secondary metric: top-10 precision 
  (what % of your top-10 actually finished top-10)
- Brier score requires round-level data — skip, note as limitation
- Write results to mart_backtest_comparison

---

## Phase 6 — Streamlit UI context

### Pages (use st.navigation)
1. Pre-tournament rankings — main leaderboard table,
   win%/top5%/top10% probability bars, Augusta fit grade,
   vs-DG-model delta column, filter pills
2. Player deep dive — SG breakdown with Augusta weights labeled,
   Augusta profile (appearances, best finish, fit grade),
   model inputs panel (show augusta_historical_sg row dimmed
   if null — do NOT hide it), vs-DG comparison panel
3. What-if simulator — st.slider widgets per SG category,
   use pre-computed sensitivity table for live updates
   (NOT a full re-sim on every slider move),
   "Re-run simulation" button triggers real 5k sim
4. Back-test results — rank correlation table by year,
   methodology notes callout, honest limitation disclosure

### Performance
- @st.cache_data(ttl=300) on all DuckDB reads
- DuckDB connection string from st.secrets in prod,
  os.getenv in dev
- Never run dbt and Streamlit simultaneously in dev 
  (DuckDB write lock)

---

## Known decisions and constraints
- 2020 Masters (covid year) — flagged, excluded from
  career SG averages
- LIV players (Rahm, Koepka, DJ) — sparse recent SG,
  fall back to DG ranking + Augusta historical data
- DuckDB write lock — stop Streamlit before running
  dbt or simulation scripts
- DataGolf ToS — personal non-commercial use only
- Probabilities must sum to ~100% — validate after every sim run
- Player sigma fallback: if <10 historical rounds, use 3.0
- Late field additions (e.g., Houston Open winner earns invite):
  run `python -m ingestion.refresh_field` — refreshes only the 5
  current-state tables (field, skill_ratings, dg_rankings,
  approach_skill, player_decompositions), prints a field diff
  (ADDED/REMOVED players), then re-run
  `dbt build --select +mart_player_model_inputs`.
  New players with no DG data get augusta_mu ≈ 0, sigma = 3.0
  (neutral high-variance estimate via existing coalesce fallbacks).
  Note: DataGolf's field endpoint can lag 1–2 days after a Sunday win
  (confirmed: Woodland/Houston Open not reflected until Monday+).
- Field detection logic: `refresh_field.py` tries `field-updates?tour=pga` first;
  if the current event is not the Masters it auto-falls back to `tour=upcoming_pga`
  (next week's field). This allows loading the Masters field up to ~1 week early.

---

## Masters week checklist (April 6–7, before first round April 10)

1. Re-run field refresh — picks up any WDs or late invites since last ingestion:
   ```
   python -m ingestion.refresh_field
   ```
   Check diff output — e.g., Houston Open winner (Woodland) should appear as ADDED.

2. Rebuild mart with updated field:
   ```
   cd dbt && dbt build --select +mart_player_model_inputs
   ```

3. Run simulation:
   ```
   python -m simulation.simulator
   ```

4. Confirm win_pct sum ≈ 1.0 (assertion built into simulator).

5. Start Streamlit (stop dbt/sim first — DuckDB write lock):
   ```
   streamlit run streamlit/app.py
   ```