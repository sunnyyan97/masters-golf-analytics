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
- [ ] Phase 4a — Model fixes (driving distance + activity discount + history cap)
- [ ] Phase 4b — Ridge regression weight derivation
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
  exclude is_covid_year=true AND only use year >= 2019 (see Phase 4a)
- int_recent_trajectory — last 6 rounds SG vs prior 18 SG
  = momentum delta, capped at ±0.8 strokes
- int_augusta_fit_score — weighted composite (7 components, see Phase 4a):
  sg_app * 0.34 + sg_putt * 0.24 + sg_ott * 0.12 + sg_arg * 0.08
  + driving_dist_vs_avg * 0.14 + long_approach_sg * 0.06 + sg_accuracy * 0.02
- int_player_sigma — std dev of round scores from historical data,
  fallback to 3.0 if fewer than 8 rounds available
- int_driving_profile — driving distance features (see Phase 4a)

### Mart models needed
- mart_player_model_inputs — one row per player in 2026 field,
  all features joined, ready for simulation
  UPDATED in Phase 4a: joins int_driving_profile, exposes
  driving_dist_vs_avg, long_approach_sg, aug_driving_dist,
  dist_data_rounds, recent_starts, sg_overall_is_null
- mart_simulation_results — loaded back from Python after sim
  (win_pct, top5_pct, top10_pct, top25_pct, mc_pct, mu, sigma, model_type)
  NOTE: model_type column — 'manual' or 'regression'
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

### Known issues identified (fixed in Phase 4a)
- Russell Henley ranked #6 — model cannot see driving distance, rewards his SG
  categories without penalizing his short hitting, which hurts at Augusta specifically
- Tiger Woods ranked #14 — career Augusta SG averaged over 20+ years including
  2000s peak; recency weighting insufficient to overcome volume of elite historical rounds;
  no activity signal for his near-zero competitive starts in past 12 months
- Root causes: no driving distance feature, Augusta history not capped at 2019+,
  no activity/availability discount applied to inactive players
- All three issues addressed in Phase 4a before Phase 4b and Phase 5

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
  + w3 * augusta_fit_score      (weight: 0.20)  ← now 7-component, see Phase 4a
  + w4 * recent_trajectory      (weight: 0.10)
  then × activity_discount()    ← applied after assembly, see Phase 4a

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

## Phase 4a — Model fixes (driving distance + activity discount + history cap)

### Overview
Three targeted fixes to the manual model before building the ridge regression.
These fixes address sniff-test failures identified after first sim run:
Russell Henley at #6 (no distance penalty) and Tiger Woods at #14
(stale historical SG, no activity signal). All changes are in dbt and
the simulator — no new ingestion needed.

Do Phase 4a BEFORE Phase 4b. The ridge regression will incorporate
the same driving distance features, so building them in dbt first means
Phase 4b can simply reference the new columns already in mart_player_model_inputs.

Order of execution:
  1. Fix int_augusta_career_sg.sql (2019+ cap)
  2. Create int_driving_profile.sql
  3. Update int_augusta_fit_score.sql (7-component formula)
  4. Update mart_player_model_inputs.sql (new columns)
  5. Run dbt build --select +mart_player_model_inputs
  6. Update simulator.py (activity_discount function)
  7. Run sim with --n_sims 10000 and verify sniff test

### Fix 1 — Cap Augusta history at 2019+ (dbt, ~30 min)

Edit `dbt/models/intermediate/int_augusta_career_sg.sql`

Add year >= 2019 to the WHERE clause alongside the existing covid exclusion:
  WHERE year >= 2019
    AND is_covid_year = false

Rationale: Augusta rounds from the 2000s and early 2010s have minimal
predictive value for 2026. The course plays differently in the modern
power era. The 2019 cap retains Tiger's 2019 win (relevant) while
discarding his 2000s dominance (not relevant to 2026 conditions).
Phil Mickelson gets the same treatment — his recent Augusta record
is what matters, not his 2000s-era form.

Validation after this change:
  Run in Jupyter or DuckDB CLI:
  SELECT player_name, avg_sg, rounds_played
  FROM int_augusta_career_sg
  WHERE player_name ILIKE '%woods%'
  Expected: only 2019 and 2022–2024 rounds counted (Tiger's recent appearances)
  His avg_sg should drop significantly vs previous value

### Fix 2 — New dbt intermediate model: int_driving_profile (dbt, ~1-2 hrs)

Create `dbt/models/intermediate/int_driving_profile.sql`

This model produces driving distance signals per player using two sources:

Source A — raw_masters_rounds_hist (primary signal):
  Average driving distance at Augusta from historical rounds
  Filters: year >= 2019, is_covid_year = false, driving_dist IS NOT NULL
  Group by datagolf_id: avg(driving_dist), count(*) as dist_data_rounds
  If a player has no Augusta distance data: fall back to tour average

Source B — stg_approach_skill (secondary signal):
  SG on 200+ yard approach shots as a distance proxy
  Long hitters disproportionately face 200+ yard approaches
  Column name in stg_approach_skill: check actual column names for the
  200+ yard bucket — likely sg_200_plus or similar
  Fallback to 0.0 if this bucket is null

Derived column — driving_dist_vs_avg:
  career_aug_driving_dist minus the field average driving distance
  Field average computed from players with dist_data_rounds >= 4 only
  Positive = longer than field, negative = shorter than field
  Players with no Augusta distance data get driving_dist_vs_avg = 0.0 (neutral)

Output columns:
  datagolf_id
  aug_driving_dist        FLOAT  — raw yards (for display)
  driving_dist_vs_avg     FLOAT  — yards above/below field avg (for model)
  long_approach_sg        FLOAT  — 200+ yd bucket SG from approach_skill
  dist_data_rounds        INT    — number of Augusta rounds with distance data

IMPORTANT: Check stg_approach_skill column names first before writing the
join — run DESCRIBE stg_approach_skill in DuckDB to see actual column names.
The 200+ yard bucket may be named differently than sg_200_plus.

### Fix 3 — Update int_augusta_fit_score.sql to 7 components (dbt, ~30 min)

Edit `dbt/models/intermediate/int_augusta_fit_score.sql`

CURRENT formula (5 components):
  sg_app * 0.38 + sg_putt * 0.28 + sg_ott * 0.20
  + sg_arg * 0.10 + sg_accuracy * 0.04

UPDATED formula (7 components, weights sum to 1.0):
  sg_app              * 0.34
  + sg_putt           * 0.24
  + sg_ott            * 0.12
  + sg_arg            * 0.08
  + driving_dist_vs_avg * 0.14   ← primary distance signal
  + long_approach_sg  * 0.06     ← secondary distance proxy
  + sg_accuracy       * 0.02

Weight rationale:
  driving_dist_vs_avg 0.14 — Augusta punishes short hitters severely;
    longer approaches into the fastest greens in golf are exponentially harder
  long_approach_sg 0.06 — complementary skill signal: can the player execute
    from distance, not just reach it. Weighted less than raw distance.
  sg_ott trimmed 0.20→0.12 — distance now counted separately; the remaining
    sg_ott weight captures the non-distance component (trajectory, consistency)
  sg_accuracy 0.02 — near-zero because Augusta fairways are unusually wide
    (among widest in golf); missing the fairway is far less penalizing than
    at US Open venues. Accuracy matters little here.
  driving_dist_vs_avg weighted MORE than long_approach_sg per stated preference:
    direct measurement (yards) > skill proxy (approach SG bucket)

This model requires joining to int_driving_profile on datagolf_id.
Add: LEFT JOIN {{ ref('int_driving_profile') }} USING (datagolf_id)
Coalesce driving_dist_vs_avg to 0.0 and long_approach_sg to 0.0 for
players missing from int_driving_profile.

### Fix 4 — Update mart_player_model_inputs.sql (dbt, ~20 min)

Edit `dbt/models/marts/mart_player_model_inputs.sql`

Add LEFT JOIN to int_driving_profile on datagolf_id.

Add these new columns to the SELECT:
  d.aug_driving_dist                              — raw yards (display only)
  d.driving_dist_vs_avg                           — used in fit score
  d.long_approach_sg                              — used in fit score
  coalesce(d.dist_data_rounds, 0) as dist_data_rounds  — data quality flag

Add two columns needed by the activity discount in simulator.py:
  CASE WHEN sg_overall_rolling_raw IS NULL THEN true ELSE false END
    as sg_overall_is_null
  CASE WHEN sg_overall_rolling_raw IS NULL THEN 2 ELSE 15 END
    as recent_starts

CRITICAL: sg_overall_is_null must be derived from the RAW value before
any coalesce. If the model currently does coalesce(sg_overall_rolling, 0),
you need to capture the null flag before that coalesce happens. Add a CTE
or subquery that exposes the pre-coalesce value, or use a CASE statement
on the source table directly before coalescing.

Example pattern:
  WITH base AS (
    SELECT
      ...,
      sr.sg_total AS sg_overall_rolling_raw,
      coalesce(sr.sg_total, 0) AS sg_overall_rolling,
      ...
    FROM ...
  )
  SELECT
    ...,
    sg_overall_rolling,
    (sg_overall_rolling_raw IS NULL) AS sg_overall_is_null,
    CASE WHEN sg_overall_rolling_raw IS NULL THEN 2 ELSE 15 END AS recent_starts,
    ...
  FROM base

After editing: run dbt build --select +mart_player_model_inputs
Row count should still be 135. Check new columns are populated:
  SELECT player_name, sg_overall_is_null, recent_starts, driving_dist_vs_avg
  FROM mart_player_model_inputs
  WHERE player_name ILIKE '%woods%' OR player_name ILIKE '%henley%'
  Expected: Tiger sg_overall_is_null=true, recent_starts=2
  Expected: Henley sg_overall_is_null=false, driving_dist_vs_avg negative value

### Fix 5 — Activity discount in simulator.py (Python, ~1 hr)

Edit `simulation/simulator.py`

Add activity_discount() helper function:

```python
def activity_discount(player: dict) -> float:
    sg_null = player.get('sg_overall_is_null', False)
    recent_starts = player.get('recent_starts', 15)

    if sg_null or recent_starts < 4:
        return 0.60   # essentially inactive (Tiger situation)
    elif recent_starts < 8:
        return 0.82   # limited activity (some LIV players)
    else:
        return 1.00   # full-season active player, no change
```

Update compute_player_mu() to apply the discount after assembling base_mu:
```python
def compute_player_mu(player, weights, model_type):
    base_mu = _compute_base_mu(player, weights, model_type)
    discount = activity_discount(player)
    return base_mu * discount
```

Apply activity_discount to BOTH model_type='manual' AND model_type='regression'.
The discount is a data quality correction for inactive players, not a
model-specific feature — both models should treat inactivity the same way.

Discount multiplier applies to the complete assembled mu, not individual
components. A 0.60 discount on mu=+0.80 gives +0.48 — meaningful reduction
without zeroing out legitimate historical signal entirely.

### Validation after Phase 4a — sniff test
Run: python -m simulation.simulator --model manual --n_sims 10000

Check top 20 against all criteria:
  ✓ Scheffler and/or Rory in top 3
  ✓ No players outside world top 25 in the top 10
  ✓ Tiger Woods below position 20
  ✓ Russell Henley below position 20
  ✓ DeChambeau in top 5 (bomber, 2025 runner-up)
  ✓ Rahm in top 6 (Augusta history + power game)

If Tiger still too high after fixes:
  Check int_augusta_career_sg applied year >= 2019 filter correctly
  Query: SELECT player_name, avg_sg, rounds_played
         FROM int_augusta_career_sg WHERE player_name ILIKE '%woods%'

If Henley still too high:
  Check driving_dist_vs_avg is negative for him
  Query: SELECT player_name, driving_dist_vs_avg
         FROM mart_player_model_inputs ORDER BY driving_dist_vs_avg ASC LIMIT 15

If activity discount not applying to Tiger:
  Check sg_overall_is_null flag is true for him
  Query: SELECT player_name, sg_overall_is_null, recent_starts
         FROM mart_player_model_inputs WHERE player_name ILIKE '%woods%'

### Phase 4a completion checklist
- [ ] int_augusta_career_sg.sql updated with year >= 2019 filter
- [ ] int_driving_profile.sql created, dbt build passes clean
- [ ] int_augusta_fit_score.sql updated to 7-component formula
- [ ] mart_player_model_inputs.sql updated with all new columns (135 rows)
- [ ] simulator.py updated with activity_discount() applied to both models
- [ ] Sniff test passes (Tiger < #20, Henley < #20, Scheffler/Rory top 3)
- [ ] win_pct sum assertion still passes after all changes

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
  this one, year >= 2019 cap applies (window function, no data leakage)
- prior_appearances: count of Augusta starts before this year (2019+)
- driving_dist_vs_avg: career Augusta distance vs avg for rounds before year Y
  JOIN to int_driving_profile, apply same leakage-safe window as prior_augusta_sg
  Use driving distance from years < Y only (no future data)
- long_approach_sg: from int_driving_profile JOIN on datagolf_id
  Use current value as proxy (no historical point-in-time available — note limitation)
- Exclude is_covid_year = true (2020)
- Exclude years before 2021 (SG category data not available pre-2021)
- Final training set: ~2021–2024, approx 300–400 rows

IMPORTANT: prior_augusta_sg uses window function ordered by year,
rows unbounded preceding to 1 preceding — never use same year's result as feature.

### Step 2 — New Python script
Create `simulation/derive_weights.py`

Feature list (8 features total, includes driving distance from Phase 4a):
  ['sg_approach', 'sg_putting', 'sg_off_tee', 'sg_around_green',
   'prior_augusta_sg', 'prior_appearances',
   'driving_dist_vs_avg', 'long_approach_sg']

Steps:
1. Query int_augusta_regression_inputs from DuckDB into pandas DataFrame
2. StandardScaler on all 8 features (required for comparable coefficients)
3. LeaveOneGroupOut CV where groups = year (temporal leakage prevention)
4. RidgeCV with alphas=[0.1, 1.0, 10.0, 100.0]
5. Print leave-one-year-out R² mean and std
6. Convert standardized coefficients back to original scale
7. Save to simulation/regression_weights.json:
   - coefficients for all 8 features (original scale)
   - intercept, model_alpha, cv_r2_mean, cv_r2_std, n_training_samples

Dependencies: scikit-learn>=1.4.0 (add to requirements.txt if missing)

Run with: python -m simulation.derive_weights

### Step 3 — Update simulator.py --model flag

regression mu formula uses all 8 features from regression_weights.json:
  mu = intercept
       + coef['sg_approach']         * player['sg_approach']
       + coef['sg_putting']          * player['sg_putting']
       + coef['sg_off_tee']          * player['sg_off_tee']
       + coef['sg_around_green']     * player['sg_around_green']
       + coef['prior_augusta_sg']    * player['augusta_hist_sg']
       + coef['prior_appearances']   * player['n_appearances']
       + coef['driving_dist_vs_avg'] * player['driving_dist_vs_avg']
       + coef['long_approach_sg']    * player['long_approach_sg']
  then × activity_discount() — same as manual model

### Step 4 — Run both models
```bash
python -m simulation.simulator --model manual --n_sims 100000
python -m simulation.simulator --model regression --n_sims 100000
```

Both runs must pass win_pct sum assertion AND sniff test.

### Expected output from derive_weights.py
  Training data: ~320 player-years across 4 Masters (2021–2024)
  Best alpha: [some value 0.1–100.0]
  Leave-one-year-out R²: 0.15–0.35 (+/- std)
  driving_dist_vs_avg coefficient expected to be meaningfully positive
  (bombers dominate Augusta in modern era — data should confirm this)

R² 0.15–0.35 is EXPECTED — not a failure. Golf is high-variance.

### Validation checks after Phase 4b
- regression_weights.json contains all 8 feature keys including driving features
- mart_simulation_results has rows for both model_type='manual' and 'regression'
- Both pass win_pct sum assertion and sniff test
- Top 5 differs slightly between models (expected)
- Identical rankings between models = something is wrong

---

## Phase 5 — Back-testing context (3-way comparison)

### Overview
Phase 5 evaluates THREE models side by side for each historical Masters year:
  1. model_type='manual'     — manually weighted composite mu
  2. model_type='regression' — ridge regression derived weights
  3. 'datagolf'              — DataGolf archived pre-tournament predictions
                               (from stg_pred_archive)

### Data available per year
- 2019: sg_total only (no SG category breakdown pre-2021)
         manual model testable; regression model CANNOT (needs SG categories)
         DataGolf predictions available if in pred_archive
- 2021–2024: full SG categories — all three models testable
- 2020: EXCLUDED (covid year)
For 2019: only run manual vs DataGolf. Regression years = 2021–2024 only.

### Methodology — no data leakage rule
For each back-test year Y, use only data available BEFORE that tournament:
- sg_overall_rolling: current ratings as proxy (note as limitation)
- augusta_hist_sg: rounds from years < Y only (window function enforces this)
- driving_dist_vs_avg: Augusta distance from rounds before year Y
- activity_discount: apply same logic as live model
- DataGolf predictions: from stg_pred_archive (already point-in-time correct)

Limitation to document: current skill ratings used as proxy for historical
point-in-time ratings — biases all models equally, does not affect comparison.

### Actual results to retrieve
Source: stg_masters_rounds_hist — aggregate sg_total per player per year,
rank by sg_total descending = actual finishing rank.
Players who missed the cut: rank = field_size - n_cuts_made + MC_position.

### Metrics (per year, per model)
Primary: Spearman rank correlation (scipy.stats.spearmanr)
Secondary: top-10 precision = len(predicted_top10 ∩ actual_top10) / 10
Optional: winner_rank — position of actual winner in your model's ranking

### Output schema — mart_backtest_comparison
year, model, spearman_corr, top10_precision, winner_rank, n_players, notes

### Script to create
`simulation/backtest.py` — loops years [2019, 2021, 2022, 2023, 2024],
computes metrics for all three models per year, writes to mart_backtest_comparison,
prints summary table to stdout.

Run with: python -m simulation.backtest

### Expected results
Regression beats manual by Spearman +0.03 to +0.06 on average.
Matching or beating DG on 2 of 4 years = strong result.
Document honestly: DG has more data, more features, years of tuning.

---

## Phase 6 — Streamlit UI context

### Pages (use st.navigation)
1. Pre-tournament rankings — leaderboard table, win%/top5%/top10% bars,
   Augusta fit grade, vs-DG delta column, filter pills
   Model toggle: 'Manual weights' vs 'Regression weights'
   Default: regression model. vs-DG delta updates with toggle.
2. Player deep dive — SG breakdown with Augusta weights labeled,
   Augusta profile (appearances, best finish, fit grade),
   model inputs panel (show augusta_historical_sg dimmed if null — do NOT hide),
   vs-DG panel showing both model win% side by side
   e.g. "Manual: 5.9% | Regression: 6.4% | DataGolf: 5.1%"
   ADD: driving distance vs field avg in Augusta profile section
   e.g. "+18 yds vs field avg" for long hitters
3. What-if simulator — st.slider per SG category + driving distance slider,
   pre-computed sensitivity table for live updates (NOT full re-sim on slider),
   "Re-run simulation" button triggers real 5k sim
4. Back-test results — 3-way comparison table, avg Spearman metric cards,
   year-by-year breakdown, methodology callout, limitation disclosure
   Note: regression years = 2021–2024 only

### Performance
- @st.cache_data(ttl=300) on all DuckDB reads
- DuckDB connection from st.secrets in prod, os.getenv in dev
- Never run dbt and Streamlit simultaneously (DuckDB write lock)
- Model toggle uses st.session_state for cross-page persistence

---

## Known decisions and constraints
- 2020 Masters (covid year) — flagged, excluded from career SG averages
- Augusta history capped at 2019+ — rounds before 2019 excluded from
  int_augusta_career_sg and int_augusta_regression_inputs.
  Rationale: modern power era Augusta plays differently; pre-2019 rounds
  have minimal predictive value and unfairly inflate Tiger/Phil rankings.
- LIV players (Rahm, Koepka, DJ) — sparse recent SG,
  fall back to DG ranking + Augusta historical data
- DuckDB write lock — stop Streamlit before running dbt or simulation scripts
- DataGolf ToS — personal non-commercial use only
- Probabilities must sum to ~100% — validate after every sim run
- Player sigma fallback: if <8 historical rounds (<2 complete Masters appearances),
  use 3.0. Players with ≥8 rounds use computed stddev. σ=3.0 ≈ tour average.
- Activity discount applied in simulator.py to BOTH model types:
  sg_overall_is_null OR recent_starts < 4 → 0.60x multiplier on mu
  recent_starts < 8 → 0.82x multiplier on mu
  recent_starts >= 8 → 1.00x (no change)
  sg_overall_is_null is a proxy for inactivity — players not in DG top 500
  lack sufficient recent competitive rounds to generate a rating.
- Driving distance in fit score:
  driving_dist_vs_avg weighted 0.14 (primary — direct measurement, career Augusta yards)
  long_approach_sg weighted 0.06 (secondary — skill execution proxy, 200+ yd bucket)
  driving_dist_vs_avg > long_approach_sg weight per explicit design decision:
  direct measurement of distance outweighs the skill proxy.
  Fallback: players with no Augusta distance data get driving_dist_vs_avg = 0.0 (neutral).
- Late field additions: run `python -m ingestion.refresh_field`, then
  `dbt build --select +mart_player_model_inputs`. New players with no DG
  data get augusta_mu ≈ 0, sigma = 3.0. DataGolf field endpoint lags
  1–2 days after Sunday wins (confirmed: Woodland/Houston Open).
- Field detection: refresh_field.py tries tour=pga first, falls back to
  tour=upcoming_pga if current event is not the Masters.
- Ridge regression back-test years: 2021–2024 only. Manual model: 2019 + 2021–2024.
  Note this asymmetry in all reporting.
- mart_simulation_results model_type: 'manual' and 'regression' coexist.
  All queries must filter by model_type explicitly. Default Streamlit = 'regression'.
- Sniff test after every sim run:
  ✓ Scheffler and/or Rory top 3
  ✓ No world top-25 outsider in top 10
  ✓ Tiger Woods below position 20
  ✓ Russell Henley below position 20
  ✓ DeChambeau top 5 (bomber, 2025 runner-up)
  ✓ Rahm top 6 (Augusta history + power game)

---

## Masters week checklist (April 6–7, before first round April 10)

1. Re-run field refresh:
   ```
   python -m ingestion.refresh_field
   ```
   Check diff output. This also refreshes player_decompositions —
   timing_adjustment will reflect Masters-specific DG predictions after this.

2. Rebuild mart with updated field:
   ```
   cd dbt && dbt build --select +mart_player_model_inputs
   ```

3. Run both simulations:
   ```
   python -m simulation.simulator --model manual --n_sims 100000
   python -m simulation.simulator --model regression --n_sims 100000
   ```

4. Confirm win_pct sum ≈ 1.0 for BOTH model runs.

5. Run sniff test — confirm Tiger < #20, Henley < #20, Scheffler/Rory top 3.

6. Start Streamlit (stop dbt/sim first — DuckDB write lock):
   ```
   streamlit run streamlit/app.py
   ```