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
- [x] Phase 2 — DataGolf ingestion ✓
- [x] Phase 3 — dbt data model ✓
- [x] Phase 4 — Simulation engine ✓
- [x] Phase 4a — Model fixes ✓ (driving distance + activity discount + history cap)
- [x] Phase 4b — Ridge regression ✓
- [x] Phase 5 — Back-testing ✓
- [ ] Phase 6 — Streamlit UI  ← CURRENT
- [ ] Phase 7 — MotherDuck + Streamlit Cloud deploy
- [ ] Phase 8 — Live tournament
- [ ] Phase 9 — Polish + launch

---

## Architecture reference (completed phases)

### Data pipeline
- Ingestion entry point: `python -m ingestion.run_ingestion`
- Field refresh only: `python -m ingestion.refresh_field`
  (refreshes field, skill_ratings, dg_rankings, approach_skill,
  player_decompositions — auto-detects Masters vs current event,
  falls back to tour=upcoming_pga if needed)
- DataGolf: always join on datagolf_id — never name strings
- Rate limit: 45 req/min, time.sleep(0.5) between calls

### Raw tables (7 existing + 1 to add in Phase 6)
- raw_player_list, raw_skill_ratings, raw_approach_skill, raw_dg_rankings
- raw_masters_field_2026, raw_masters_rounds_hist, raw_pred_archive
- raw_current_dg_predictions — TO BE ADDED in Phase 6 Step 1
  (current 2026 pre-tournament win% from preds/pre-tournament endpoint)

### dbt materialization
- staging: views — stg_player_list, stg_skill_ratings, stg_approach_skill,
  stg_dg_rankings, stg_masters_field_2026, stg_masters_rounds_hist,
  stg_pred_archive, stg_player_decompositions
- intermediate: views — int_augusta_career_sg, int_recent_trajectory,
  int_augusta_fit_score, int_player_sigma, int_driving_profile,
  int_augusta_regression_inputs
- marts: tables — mart_player_model_inputs, mart_simulation_results,
  mart_backtest_comparison

### mart_player_model_inputs (93 rows — 2026 field)
Key columns: datagolf_id, player_name, sg_overall_rolling, augusta_hist_sg,
augusta_fit_score, momentum_delta, player_sigma, augusta_mu,
driving_dist_vs_avg, long_approach_sg, aug_driving_dist, dist_data_rounds,
sg_overall_is_null, recent_starts, n_appearances, dg_rank,
sg_app, sg_putt, sg_ott, sg_arg, sg_accuracy, country

### mart_simulation_results
Columns: datagolf_id, player_name, win_pct, top5_pct, top10_pct,
top25_pct, mc_pct, mu, sigma, model_type
model_type values: 'manual', 'regression', 'ensemble' — all three populated ✓
All three win_pct sums verified = 1.000
Default in Streamlit = 'ensemble'
Always filter by model_type explicitly in every query

### mart_backtest_comparison
Columns: year, model_type, spearman_corr, top10_precision, winner_rank,
n_players, notes

### rankings_cache.parquet
Written to data/ by simulator.py after every sim run.
Streamlit reads this file instead of connecting to DuckDB directly —
avoids write-lock conflicts when a Jupyter kernel has masters.duckdb open.
Location: data/rankings_cache.parquet
Columns: datagolf_id, player_name, country, country_code, dg_rank,
win_pct, top5_pct, top10_pct, mc_pct, augusta_fit_score,
dg_win_pct (from stg_player_decompositions.final_pred), model_type

---

## Model architecture

### Manual (model_type='manual')
Augusta_mu =
  0.40 × sg_overall_rolling
  + 0.30 × augusta_hist_sg
  + 0.20 × augusta_fit_score
  + 0.10 × momentum_delta
  × activity_discount()

Fit score formula (int_augusta_fit_score.sql — 7 components sum to 1.0):
  sg_app * 0.28 + sg_putt * 0.20 + sg_ott * 0.04 + sg_arg * 0.18
  + (driving_dist_vs_avg * 0.004) * 0.16   ← unit-converted yards→SG
  + long_approach_sg * 0.10
  + sg_accuracy * 0.04
NOTE: driving_dist_vs_avg is in raw yards — must multiply by 0.004 to
convert to SG-equivalent before applying the 0.16 weight.
Without this conversion the fit score is dominated by distance (yards >> SG units).

Augusta history: year >= 2019 only (2019+ cap in int_augusta_career_sg)
Excludes 2020 covid year throughout.

### Regression (model_type='regression')
Ridge regression trained on 2021–2025 Augusta rounds (LOGO CV, alpha=100).
Features: sg_approach, sg_putting, sg_off_tee, sg_around_green,
prior_augusta_sg, prior_appearances, driving_dist_vs_avg, long_approach_sg
Weights: simulation/regression_weights.json
CV predictions: simulation/cv_predictions.json (LOGO CV OOF for backtest)
× activity_discount() after mu assembly

### Ensemble (model_type='ensemble') ← DEFAULT
ensemble_mu = (manual_mu + regression_mu) / 2
Equal weighting — regression won 2021/2025, manual won 2022/2023/2024.
Full 100k sim pass using ensemble_mu — NOT averaging output probabilities.
Implemented in simulator.py run_simulation() function.

### Activity discount (all three model types)
- sg_overall_is_null OR recent_starts < 4 → 0.60× (inactive, e.g. Tiger)
- recent_starts < 8 → 0.82× (limited activity, some LIV players)
- recent_starts >= 8 → 1.00× (no change)

### Running simulations
```bash
python -m simulation.simulator --model manual --n_sims 100000
python -m simulation.simulator --model regression --n_sims 100000
python -m simulation.simulator --model ensemble --n_sims 100000
```
Each run writes to mart_simulation_results (deletes prior rows for that
model_type) and exports data/rankings_cache.parquet.

### Sniff test (run after every sim)
✓ Scheffler and/or Rory top 3
✓ No world top-25 outsider in top 10
✓ Tiger Woods below position 20
✓ Russell Henley below position 20
✓ DeChambeau top 5 (bomber, 2025 runner-up)
✓ Rahm top 6 (Augusta history + power game)

---

## Phase 5 — Backtest results (final, post fit-score fix)

| Model | Avg Spearman | Avg Top-10% | Years |
|---|---|---|---|
| DataGolf | 0.496 | 0.360 | 5 (2021–2025) |
| Regression | 0.330 | 0.320 | 5 (2021–2025) |
| Manual | 0.327 | 0.280 | 6 (2019–2025) |

Year-by-year highlights:
- Regression won: 2021 (0.348 vs 0.306), 2025 (0.332 vs 0.279)
- Manual won: 2022 (0.369 vs 0.263), 2023 (0.464 vs 0.438), 2024 (0.369 vs 0.266)
- Neither dominates → ensemble is the right default
- DataGolf gap (~0.166 Spearman) largely from point-in-time ratings limitation
- 2019 manual miss (Tiger #52) = known proxy limitation, documented
- Regression and manual essentially tied (+0.003 gap) — manual weight
  intuitions were well-calibrated by the data

### Augusta Fit grade thresholds (percentile-based, applied in Streamlit)
Assign grades using PERCENT_RANK() over augusta_fit_score DESC:
- A+ = pct_rank ≤ 0.10 (top 10%)
- A  = pct_rank ≤ 0.25 (top 25%)
- B+ = pct_rank ≤ 0.45 (top 45%)
- B  = pct_rank ≤ 0.65 (top 65%)
- C+ = pct_rank ≤ 0.80 (top 80%)
- C  = everything else

DO NOT use fixed numeric thresholds — the fit score range changes when
weights or data changes. Always use percentile rank.

Top fit scores (confirmed post-fix): Scheffler 0.480, Rahm 0.395,
Schauffele 0.366, Fitzpatrick 0.364, McIlroy 0.343

---

## Phase 6 — Streamlit UI context ← CURRENT PHASE

### Critical architecture note — parquet not DuckDB
Streamlit reads data/rankings_cache.parquet, NOT DuckDB directly.
This avoids write-lock conflicts when a Jupyter kernel has masters.duckdb open.
For pages that need data not in the parquet (backtest results, player detail),
open a fresh duckdb.connect() with read_only=True.
Never open a writable DuckDB connection from Streamlit.

```python
# Standard pattern for Streamlit DuckDB reads
import duckdb
conn = duckdb.connect('data/masters.duckdb', read_only=True)
df = conn.execute("SELECT ...").df()
conn.close()
```

### Step 1 — Ingest current 2026 DG predictions (do this FIRST)
The vs-DG column currently shows +0.0% for all players because
raw_current_dg_predictions does not exist yet. The pred_archive table
only contains historical years (2020–2025), not 2026.

Add to ingestion/run_ingestion.py (or a standalone script):
```python
# Endpoint: preds/pre-tournament?tour=pga&odds_format=percent
# Store as raw_current_dg_predictions with columns:
#   datagolf_id, player_name, win_pct, top5_pct, top10_pct, mc_pct, season=2026
```

After ingesting, create stg_current_dg_predictions.sql (staging view).
Then update rankings_cache.parquet export query in simulator.py to join
on stg_current_dg_predictions instead of stg_player_decompositions.final_pred
(current dg_win_pct in parquet comes from decompositions which is not
the same as pre-tournament win probability).

Verify the vs-DG column has real values (not all +0.0%) before building UI.

### Step 2 — Rebuild Page 1 with HTML rendering
Streamlit's native st.dataframe CANNOT render inline probability bars,
colored grade badges, or styled text. Must use HTML rendering via
st.components.v1.html() or st.markdown(unsafe_allow_html=True).

The current Page 1 draft uses st.dataframe — replace the table section
entirely with a custom HTML table. Do not attempt to patch the existing
st.dataframe approach.

Reference mockup (recreate this exactly):
- Dark header: "2026 Masters Tournament — Pre-Tournament Model"
  subtitle: "{n} simulations · Augusta fit model · Updated {date}"
  badge top-right: "Augusta National · Apr 9–12"
- Model toggle row: Manual / Regression / Ensemble (pills, Ensemble active by default)
- Filter pills: All players / Top 20 / Contenders (win >3%)
- Sort by: Win % (default)
- Table columns: # | Player (name + flag + country) | DG Rank | Win % (bar) |
  Top 5 % (bar) | Top 10 % (bar) | Make Cut | Augusta Fit (grade badge) | vs DG
- Probability bars: green for win%, blue for top5%, amber for top10%
  Bars are proportional to the max value in that column across visible rows
- Augusta Fit badge colors:
  A+ = dark green, A = green, B+ = teal, B = blue, C+ = amber, C = red/orange
  Use percentile rank thresholds defined in Phase 5 above
- vs DG column: your model win% minus DataGolf 2026 pre-tournament win%
  Green for positive (you're higher than DG), red for negative
  Shows +0.0% only if DG data genuinely not available for that player
- Footer: "Showing X of 93 players · Click any row for full player breakdown
  · vs DG Model = your win% minus DataGolf's pre-tournament win%"
- Click any row → navigate to Page 2 with that player selected

HTML implementation notes:
- Build the table as an f-string HTML template in Python
- Pass filtered/sorted DataFrame slice into the template
- Use st.components.v1.html(html_string, height=..., scrolling=True)
  OR st.markdown(html_string, unsafe_allow_html=True)
- Model toggle: use st.radio with horizontal=True or st.segmented_control
  Store in st.session_state['selected_model'], default 'ensemble'
- Filter pills: st.pills or st.radio horizontal
- Row click for navigation: embed onclick="..." in HTML rows that call
  a JS postMessage, OR use a separate st.selectbox for player selection
  (simpler and more reliable than JS onclick in Streamlit)

### Pages to build (all 4)

**Page 1 — Pre-tournament rankings** (rebuild from scratch with HTML)
See Step 2 above for full spec.

**Page 2 — Player deep dive**
- Player selector: st.selectbox at top populated from rankings data
  Pre-populate from st.session_state['selected_player'] if set from Page 1
- Probability pills: Win%, Top 5%, Top 10%, Make Cut%
  Large numbers, color-coded (green/blue/amber/gray)
- SG breakdown grid (5 cells):
  sg_approach, sg_putting, sg_ott (off the tee), sg_arg (around green), sg_total
  Each cell: category label, SG value (+/- colored), tour rank, Augusta weight %
  Augusta weights from the manual fit formula:
  approach=28%, putting=20%, OTT=4%, ARG=18%
- Augusta profile section (3 cells):
  Appearances (n_appearances), Best finish (hardcode from public records or
  pull from mart if available), Augusta Fit grade
  ADD: driving distance vs field avg: f"+{driving_dist_vs_avg:.1f} yds vs field avg"
- Model inputs panel:
  Show all 4 mu components with weights:
  SG Overall (0.40), Augusta Hist SG (0.30), Augusta Fit Score (0.20), Trajectory (0.10)
  Show actual values for each component
  augusta_hist_sg row: if null/zero, show dimmed — do NOT hide
- vs DG comparison panel:
  Manual: X% | Regression: Y% | Ensemble: Z% | DataGolf: W%
  All four values side by side

**Page 3 — What-if simulator**
- Player selector (same as Page 2)
- Sliders for each SG category: sg_approach, sg_putting, sg_ott, sg_arg
- Slider for driving distance adjustment (yards)
- Display: current probabilities vs adjusted probabilities side by side
- "Re-run" button triggers a 5k sim with adjusted inputs
- Use pre-computed sensitivity table for instant feedback before re-run
  (sensitivity = partial derivative of win% with respect to each feature,
  approximate as (win%_at_baseline+0.1 - win%_at_baseline) / 0.1)

**Page 4 — Back-test results**
- Read from mart_backtest_comparison via read_only DuckDB connection
- Metric cards at top: avg Spearman for each of 3 models
  DataGolf: 0.496 | Regression: 0.330 | Manual: 0.327
- Year-by-year table: one row per year, 3 model columns
- Winner rank column: where did actual winner appear in each model's ranking
- Methodology callout box (use st.info or styled HTML):
  · Point-in-time ratings limitation (2026 ratings used as proxy for all years)
  · Momentum delta set to 0.0 for all backtest years (no historical decompositions)
  · Regression uses LOGO CV out-of-fold predictions (genuinely out-of-sample)
  · Small sample (5-6 tournaments) — directional not definitive
  · Regression years = 2021–2025 only (SG categories unavailable pre-2021)
  · DataGolf gap largely from data advantages (shot-level, point-in-time ratings)

### Performance requirements
- Read rankings_cache.parquet with pd.read_parquet() for Page 1
- Read-only DuckDB connections for Pages 2, 3, 4
- @st.cache_data(ttl=300) on all data reads
- Model toggle: st.session_state['selected_model'], default 'ensemble'
- Player selection: st.session_state['selected_player']
- Never open writable DuckDB connection from Streamlit

---

## Known decisions and constraints
- 2020 Masters (covid) — excluded from all career SG averages
- Augusta history capped at 2019+ — pre-2019 rounds excluded
- driving_dist_vs_avg is in raw yards — multiply by 0.004 before applying
  fit score weight (critical — without this the score is pure distance ranking)
- 6 players null sg_overall_rolling: Campos, Sargent, Willett, Wise,
  Howell III, Russell — coalesce to 0, get activity discount applied
- SG categories (sg_app etc.) only available 2021+ in rounds history
- LIV players — sparse recent SG, fall back to DG ranking + Augusta history
- DuckDB write lock — Streamlit uses read_only=True; never writable connection
- DataGolf ToS — personal non-commercial use only
- Fit grade thresholds: percentile-based, NOT fixed numeric cutoffs
- pred_archive only has historical years (2020–2025) NOT 2026
  Must ingest preds/pre-tournament for current 2026 DG predictions
- rankings_cache.parquet written after every simulator run
  Streamlit reads this file for Page 1 — ensure it exists before starting app

---

## Masters week checklist (April 6–7, before first round April 10)

1. Re-run field refresh:
   ```bash
   python -m ingestion.refresh_field
   ```
   Check diff for WDs or late invites.
   Refreshes player_decompositions — timing_adjustment reflects Masters DG predictions.

2. Rebuild mart:
   ```bash
   cd dbt && dbt build --select +mart_player_model_inputs
   ```

3. Re-ingest current DG 2026 predictions (updates vs-DG column):
   ```bash
   python -m ingestion.ingest_current_dg_predictions
   ```

4. Run all three simulations:
   ```bash
   python -m simulation.simulator --model manual --n_sims 100000
   python -m simulation.simulator --model regression --n_sims 100000
   python -m simulation.simulator --model ensemble --n_sims 100000
   ```

5. Confirm win_pct sum ≈ 1.0 for all three. Run sniff test.

6. Start Streamlit (read_only DuckDB — no write lock issue):
   ```bash
   streamlit run streamlit/app.py
   ```