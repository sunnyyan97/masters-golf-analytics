# Masters Golf Analytics — Dev Log

## Stack
- Python 3.12, dbt-duckdb 1.9.1, DuckDB 1.2.1, Streamlit 1.44.0
- DataGolf API (Scratch Plus annual, event_id=14 for Masters)
- Local DuckDB at data/masters.duckdb for dev
- MotherDuck (md:masters_golf) for prod — Phase 7
- dbt profile: masters_golf, dev target = local, prod target = MotherDuck

## Repo structure
- ingestion/    — DataGolf API client + DuckDB loader
- simulation/   — Monte Carlo engine (100k sims default, --n_sims flag)
- dbt/          — staging (views) → intermediate (views) → marts (tables)
- streamlit/    — 4-page app

## Phase status
- [x] Phase 1 — Repo + environment setup
- [x] Phase 2 — DataGolf ingestion ✓ (7 raw tables, 627 rounds, 543 pred_archive rows)
- [x] Phase 3 — dbt data model ✓ (14 models, 40 tests, 135-row mart_player_model_inputs)
- [x] Phase 4 — Simulation engine ✓ (93-player Masters field, 100k sims default, win_pct sum=1.0)
- [ ] Phase 4b — Ridge regression weight derivation (INSERT BEFORE Phase 5)
- [ ] Phase 5 — Back-testing (3-way comparison: manual vs regression vs DataGolf)
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
  (win_pct, top5_pct, top10_pct, top25_pct, mc_pct, mu, sigma, model_type)
  NOTE: model_type column added in Phase 4b — 'manual' or 'regression'
  Both model runs write to the same table filtered by model_type
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

### First full sim results (50k sims, 2026-03-30; default bumped to 100k)
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

### Composite mu formula (manual weights — model_type='manual')
Augusta_mu =
  w1 * sg_overall_rolling      (weight: 0.40)
  + w2 * augusta_historical_sg  (weight: 0.30)
  + w3 * augusta_fit_score      (weight: 0.20)
  + w4 * recent_trajectory      (weight: 0.10)

### Simulation mechanics
- Default 100,000 simulations, --n_sims CLI flag for dev testing (e.g. --n_sims 1000)
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
- Include mu, sigma, and model_type columns alongside probabilities
- model_type column distinguishes 'manual' vs 'regression' runs
  (both coexist in the same table, filtered by model_type in queries)

---

## Phase 4b — Ridge regression weight derivation

### Purpose
Replace manually chosen mu formula weights with data-derived coefficients from a
ridge regression trained on historical Augusta finishing SG (2021–2024, years where
SG category data is available). This produces a model_type='regression' sim run
that runs alongside the existing model_type='manual' run for comparison in Phase 5.

### Why ridge regression (not plain linear regression)
- Small sample (~300–400 player-year observations after excluding covid + pre-2021 years)
- Ridge adds L2 penalty to prevent overfitting — required at this sample size
- RidgeCV auto-selects the best regularization strength via cross-validation
- Coefficients are still fully interpretable (unlike XGBoost)

### Step 1 — New dbt intermediate model
Create `dbt/models/intermediate/int_augusta_regression_inputs.sql`

This model produces one row per player per Augusta appearance with:
- Target variable: sg_total for that year's Masters (actual outcome)
- Features: sg_approach, sg_putting, sg_off_tee, sg_around_green
  (use stg_masters_rounds_hist SG categories — available 2021+ only)
- prior_augusta_sg: career avg SG at Augusta from all years BEFORE
  this one (window function, no data leakage)
- prior_appearances: count of Augusta starts before this year
- Exclude is_covid_year = true (2020)
- Exclude years before 2021 (SG category data not available pre-2021)
- Final training set will be ~2021–2024, approx 300–400 rows

IMPORTANT: prior_augusta_sg must use only data from years < current year
(window function ordered by year, rows unbounded preceding to 1 preceding).
This prevents data leakage — never use future results in a feature.

### Step 2 — New Python script
Create `simulation/derive_weights.py`

Steps inside the script:
1. Query int_augusta_regression_inputs from DuckDB into a pandas DataFrame
2. Features: ['sg_approach', 'sg_putting', 'sg_off_tee', 'sg_around_green',
              'prior_augusta_sg', 'prior_appearances']
3. Target: 'sg_total' (Augusta finishing SG for that year)
4. Standardize features with StandardScaler (required for ridge coefficients
   to be comparable across features with different scales)
5. Cross-validation strategy: LeaveOneGroupOut where groups = year
   (train on 3 years, test on 1 — avoids temporal leakage)
6. Fit RidgeCV with alphas=[0.1, 1.0, 10.0, 100.0]
7. Print leave-one-year-out R² mean and std — this is the honest
   validation metric to report (not training R²)
8. Convert standardized coefficients back to original scale for
   interpretability
9. Save weights to simulation/regression_weights.json including:
   - One coefficient per feature (original scale)
   - intercept
   - model_alpha (best alpha selected by CV)
   - cv_r2_mean and cv_r2_std (report these honestly)
   - n_training_samples

Dependencies to add to requirements.txt if not present:
  scikit-learn>=1.4.0

Run with: `python -m simulation.derive_weights`

### Step 3 — Update simulator.py to accept --model flag
Add `--model` CLI argument: choices=['manual', 'regression'], default='manual'

The simulation logic (cut rule, field difficulty, NumPy vectorization) is
IDENTICAL for both models — only how player_mu is computed differs:

manual:   mu = (0.40 * sg_overall) + (0.30 * augusta_hist_sg)
              + (0.20 * fit_score) + (0.10 * trajectory)

regression: mu = intercept
                + coef['sg_approach']       * player['sg_approach']
                + coef['sg_putting']        * player['sg_putting']
                + coef['sg_off_tee']        * player['sg_off_tee']
                + coef['sg_around_green']   * player['sg_around_green']
                + coef['prior_augusta_sg']  * player['augusta_hist_sg']
                + coef['prior_appearances'] * player['n_appearances']
                (load coefs from simulation/regression_weights.json)

Add a load_weights(model_type) helper function that returns the right
weights dict based on model_type argument.

mart_simulation_results already has a model_type column per Phase 4 output
notes above. Both runs write to the same table — one row per player per
model_type. Upsert/replace on (datagolf_id, model_type) to avoid duplicates.

### Step 4 — Run both models
After derive_weights.py completes successfully:

```bash
# Run manual model (existing, re-run to populate model_type column)
python -m simulation.simulator --model manual --n_sims 100000

# Run regression model
python -m simulation.simulator --model regression --n_sims 100000
```

Both runs must pass the win_pct sum assertion (0.98–1.02).

### Expected output from derive_weights.py
The script should print something like:
  Training data: ~320 player-years across 4 Masters (2021–2024)
  Best alpha: [some value from 0.1–100.0]
  Leave-one-year-out R²: 0.15–0.35 (+/- some std)
  Derived weights: [feature coefficients]

R² of 0.15–0.35 is EXPECTED and reasonable for golf — do not treat a low
R² as a failure. Golf is high-variance. The regression weights are still
more principled than manual weights even at low R².

### Validation checks after Phase 4b
- regression_weights.json exists in simulation/ directory
- mart_simulation_results has rows for BOTH model_type='manual' 
  and model_type='regression'
- Both model_type win_pct columns sum to ~1.0 independently
- Top 5 players differ slightly between the two models (expected)
- If regression and manual produce identical rankings, something is wrong

---

## Phase 5 — Back-testing context (3-way comparison)

### Overview
Phase 5 evaluates THREE models side by side for each historical Masters year:
  1. model_type='manual'     — your manually weighted composite mu
  2. model_type='regression' — ridge regression derived weights
  3. 'datagolf'              — DataGolf's archived pre-tournament predictions
                               (from stg_pred_archive)

The goal is a legitimate A/B test with a real external baseline, not just
self-validation. This is the primary analytical contribution of the project.

### Data available per year
- 2019: sg_total only (no SG category breakdown pre-2021)
         manual model can be back-tested; regression model CANNOT
         (regression requires SG category features which are null pre-2021)
         DataGolf predictions available if in pred_archive
- 2021: full SG categories available — all three models testable
- 2022: full SG categories available — all three models testable
- 2023: full SG categories available — all three models testable
- 2024: full SG categories available — all three models testable
- 2020: EXCLUDED (covid year, November conditions)

For 2019, only run manual vs DataGolf comparison. Note regression model
years = 2021–2024 in all reporting.

### Methodology — no data leakage rule
For each back-test year Y, only use data that would have been available
BEFORE the tournament started:
- sg_overall_rolling: use the most recent skill ratings from stg_skill_ratings
  as a proxy (current ratings are the best available approximation —
  note this as a limitation since you don't have point-in-time skill rating snapshots)
- augusta_hist_sg: use only rounds from years < Y
  (already enforced in int_augusta_regression_inputs via window function)
- prior_appearances: count of Augusta starts before year Y
- DataGolf predictions: pull from stg_pred_archive for that year
  (these are pre-tournament predictions, already point-in-time correct)

Limitation to document: skill ratings used for back-test years are current
(2026) ratings rather than true historical point-in-time ratings. This
slightly inflates apparent back-test performance for all models equally
and does not bias the comparison between models.

### Actual results to retrieve
For each back-test year, you need actual finishing positions.
Source: stg_masters_rounds_hist — aggregate sg_total per player per year,
rank players by total sg_total descending = actual finishing rank.
Players who missed the cut get rank = (field_size - n_cuts_made + position).

### Metrics to compute (per year, per model)
Primary:
- Spearman rank correlation: your predicted rank vs actual finishing rank
  Use scipy.stats.spearmanr(predicted_ranks, actual_ranks)

Secondary:
- Top-10 precision: what % of your predicted top-10 actually finished top-10
  = len(set(your_top10) ∩ set(actual_top10)) / 10

Optional (if time allows):
- Winner predicted rank: where did the actual winner appear in your model's ranking
  (lower = better; #1 is perfect)

### Output schema — mart_backtest_comparison
One row per (year, model) combination:

```
year            INT       -- 2019, 2021, 2022, 2023, 2024
model           VARCHAR   -- 'manual', 'regression', 'datagolf'
spearman_corr   FLOAT     -- primary metric
top10_precision FLOAT     -- secondary metric
winner_rank     INT       -- where actual winner ranked in predictions
n_players       INT       -- field size that year
notes           VARCHAR   -- e.g. 'regression not available (pre-2021 SG)'
```

Write via Python after computing metrics — same DuckDB connection pattern
as simulator.py uses for mart_simulation_results.

### Expected results
Regression will likely beat manual by Spearman +0.03 to +0.06 on average.
Neither model is expected to consistently beat DataGolf — matching or
beating DG on 2 of 4 years is a strong result worth highlighting.
Document honestly: DG has more data, more features, and years of tuning.

### Script to create
`simulation/backtest.py` — single script that:
1. Loops over years [2019, 2021, 2022, 2023, 2024]
2. For each year, retrieves actual finishing ranks from stg_masters_rounds_hist
3. For each model, reconstructs predicted rankings using historical inputs
4. Computes spearman_corr and top10_precision for each (year, model) pair
5. Writes all results to mart_backtest_comparison
6. Prints a summary table to stdout

Run with: `python -m simulation.backtest`

### Streamlit back-test page (feeds into Phase 6)
The back-test results page in Streamlit reads from mart_backtest_comparison
and displays:
- Summary metric cards: avg Spearman for each model across all years
- Year-by-year table: all three models side by side per year
- Methodology callout: honest disclosure of limitations
  (point-in-time ratings, small sample, no Brier score)
- Note: regression model years limited to 2021–2024

---

## Phase 6 — Streamlit UI context

### Pages (use st.navigation)
1. Pre-tournament rankings — main leaderboard table,
   win%/top5%/top10% probability bars, Augusta fit grade,
   vs-DG-model delta column, filter pills
   ADD: model toggle at top of page — 'Manual weights' vs 'Regression weights'
   toggle filters mart_simulation_results by model_type
   Default: show regression model (it's the more principled one)
   vs-DG-model delta column updates based on which model is selected
2. Player deep dive — SG breakdown with Augusta weights labeled,
   Augusta profile (appearances, best finish, fit grade),
   model inputs panel (show augusta_historical_sg row dimmed
   if null — do NOT hide it), vs-DG comparison panel
   ADD: show both model win% values side by side in the vs-DG panel
   e.g. "Manual: 5.9% | Regression: 6.4% | DataGolf: 5.1%"
3. What-if simulator — st.slider widgets per SG category,
   use pre-computed sensitivity table for live updates
   (NOT a full re-sim on every slider move),
   "Re-run simulation" button triggers real 5k sim
   Model toggle here too — sliders adjust whichever model is selected
4. Back-test results — 3-way comparison table (manual vs regression vs DG),
   avg Spearman metric cards at top, year-by-year breakdown,
   methodology notes callout, honest limitation disclosure
   Note regression years = 2021–2024 only

### Performance
- @st.cache_data(ttl=300) on all DuckDB reads
- DuckDB connection string from st.secrets in prod,
  os.getenv in dev
- Never run dbt and Streamlit simultaneously in dev 
  (DuckDB write lock)
- Model toggle should use st.session_state so selection persists
  across page navigation within the same session

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
- Player sigma fallback: if <8 historical rounds (i.e., <2 complete Masters appearances),
  use 3.0. Players with ≥8 rounds (2+ full appearances) use their computed stddev.
  Threshold chosen to capture multi-year Augusta consistency (e.g., Aberg: σ=1.996 from
  2 top-10s) while avoiding single-year noise (4-round estimates range 0.96–6.88, and
  6-round estimates like Kitayama's 4.885 inflate win% via artificial high-variance).
  σ=3.0 ≈ tour average — neutral assumption for players with insufficient Augusta data.
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
- Ridge regression back-test years: 2021–2024 only (SG category data
  not available pre-2021 in raw_masters_rounds_hist). Manual model
  back-tests 2019 + 2021–2024. Note this asymmetry in all reporting.
- mart_simulation_results model_type column: both 'manual' and 'regression'
  rows coexist. All queries must filter by model_type explicitly.
  Default display in Streamlit = 'regression'.

---

## Masters week checklist (April 6–7, before first round April 10)

1. Re-run field refresh — picks up any WDs or late invites since last ingestion:
   ```
   python -m ingestion.refresh_field
   ```
   Check diff output — e.g., late WDs or sponsor exemptions.
   **Important:** this also refreshes `player_decompositions` — after re-running,
   `timing_adjustment` will reflect Masters-specific DataGolf predictions rather than
   the prior week's PGA event. Re-run dbt and the simulator after this step.

2. Rebuild mart with updated field:
   ```
   cd dbt && dbt build --select +mart_player_model_inputs
   ```

3. Run both simulations:
   ```
   python -m simulation.simulator --model manual --n_sims 100000
   python -m simulation.simulator --model regression --n_sims 100000
   ```

4. Confirm win_pct sum ≈ 1.0 for BOTH model runs (assertion built into simulator).

5. Start Streamlit (stop dbt/sim first — DuckDB write lock):
   ```
   streamlit run streamlit/app.py
   ```