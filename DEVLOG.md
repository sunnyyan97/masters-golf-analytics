# Masters Golf Analytics — Dev Log

## Stack
- Python 3.12, dbt-duckdb 1.9.1, DuckDB 1.2.1, Streamlit 1.44.0
- DataGolf API (Scratch Plus subscription, event_id=14 for Masters)
- Local DuckDB for dev, MotherDuck for prod

## Repo structure
- ingestion/    — DataGolf API client + DuckDB loader
- simulation/   — Monte Carlo engine (50k sims)
- dbt/          — staging → intermediate → marts pipeline
- streamlit/    — 4-page app (rankings, player detail, what-if, backtest)

## Phase status
- [x] Phase 1 — Repo + environment setup
- [ ] Phase 2 — DataGolf ingestion
- [ ] Phase 3 — dbt data model
- [ ] Phase 4 — Simulation engine
- [ ] Phase 5 — Back-testing
- [ ] Phase 6 — Streamlit UI
- [ ] Phase 7 — MotherDuck + Streamlit Cloud deploy
- [ ] Phase 8 — Live tournament
- [ ] Phase 9 — Polish + launch

## Known issues / decisions
- 2020 Masters (covid year) — flag with is_covid_year, exclude from career SG averages
- LIV players have sparse recent SG — fall back to DG ranking + Augusta history
- DuckDB write lock — never run dbt and Streamlit simultaneously in dev

## Current focus
Phase 2 — DataGolf ingestion layer