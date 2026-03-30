-- Two complementary momentum signals combined into momentum_delta:
--
-- 1. timing_adjustment (DG player-decompositions, current event):
--    DataGolf's model adjustment for recent tour form vs. long-term baseline.
--    Captures general hot/cold streaks across all recent PGA events.
--    Available only for the current event field — coalesces to 0 for absent players.
--
-- 2. augusta_trend (computed from stg_masters_rounds):
--    Recent Augusta SG (2024–2025) minus prior Augusta SG (2019–2023).
--    Captures venue-specific improvement or decline at Augusta National.
--    Requires ≥1 season in each window; coalesces to 0 otherwise (debut/single-visit players).
--    Capped at ±0.8 strokes to limit influence of small sample outliers.
--
-- Combined: momentum_delta = 0.6 * timing_adjustment + 0.4 * augusta_trend

with augusta_recent as (
    select
        datagolf_id,
        avg(sg_total) as avg_sg_recent
    from {{ ref('stg_masters_rounds') }}
    where season >= 2024
      and not is_covid_year
      and sg_total is not null
    group by datagolf_id
),
augusta_prior as (
    select
        datagolf_id,
        avg(sg_total) as avg_sg_prior
    from {{ ref('stg_masters_rounds') }}
    where season < 2024
      and not is_covid_year
      and sg_total is not null
    group by datagolf_id
),
augusta_trend as (
    select
        coalesce(r.datagolf_id, p.datagolf_id) as datagolf_id,
        case
            when r.datagolf_id is not null and p.datagolf_id is not null
            then greatest(-0.8, least(0.8, r.avg_sg_recent - p.avg_sg_prior))
            else 0.0
        end as augusta_trend
    from augusta_recent r
    full outer join augusta_prior p on r.datagolf_id = p.datagolf_id
),
all_players as (
    select datagolf_id from {{ ref('stg_skill_ratings') }}
)
select
    ap.datagolf_id,
    0.6 * coalesce(d.timing_adjustment, 0.0)
    + 0.4 * coalesce(at.augusta_trend, 0.0)    as momentum_delta
from all_players ap
left join {{ ref('stg_player_decompositions') }} d  on ap.datagolf_id = d.datagolf_id
left join augusta_trend at                          on ap.datagolf_id = at.datagolf_id
