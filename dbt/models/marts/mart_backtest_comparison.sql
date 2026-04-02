-- Back-test results written by simulation/backtest.py (Phase 5).
-- One row per year per model_type ('manual', 'regression', 'datagolf').
-- Run: python -m simulation.backtest
select
    cast(null as integer)  as year,
    cast(null as varchar)  as model_type,
    cast(null as double)   as spearman_corr,
    cast(null as double)   as top10_precision,
    cast(null as integer)  as winner_rank,
    cast(null as integer)  as n_players,
    cast(null as varchar)  as notes
where false
