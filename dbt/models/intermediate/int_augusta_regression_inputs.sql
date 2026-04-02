-- Training data for ridge regression: one row per player per Augusta appearance (2021+).
--
-- Features are genuinely pre-tournament (no temporal leakage):
--   dg_pred_win_pct → DataGolf's actual pre-tournament win probability from stg_pred_archive,
--     joined on datagolf_id + season. This replaces the previous approach of using 2026
--     static skill ratings (sg_app etc.) as a proxy across all training years, which was
--     temporally contaminated. Players not in pred_archive get 0.0.
--   prior_augusta_sg, prior_appearances → leakage-safe window functions on rounds.
--   driving_dist_vs_avg → leakage-safe (only rounds from seasons < Y).
--   long_approach_sg → current value from int_driving_profile (acknowledged proxy).
--
-- Target: sg_total (average strokes-gained for that year's Masters).

with rounds as (
    select
        datagolf_id,
        player_name,
        season,
        sg_total
    from {{ ref('stg_masters_rounds') }}
    where not is_covid_year
      and sg_total is not null
),

player_year_sg as (
    select
        datagolf_id,
        player_name,
        season,
        avg(sg_total) as sg_total
    from rounds
    group by datagolf_id, player_name, season
),

with_prior as (
    select
        *,
        avg(sg_total) over (
            partition by datagolf_id
            order by season
            rows between unbounded preceding and 1 preceding
        )                               as prior_augusta_sg_raw,
        count(*) over (
            partition by datagolf_id
            order by season
            rows between unbounded preceding and 1 preceding
        )                               as prior_appearances_raw
    from player_year_sg
),

-- Leakage-safe driving distance: for player P in year Y, use only seasons < Y
prior_driving as (
    select
        r1.datagolf_id,
        r1.season                  as target_season,
        avg(r2.driving_dist)       as prior_aug_dist,
        count(*)                   as prior_dist_rounds
    from (select distinct datagolf_id, season from rounds) r1
    join {{ ref('stg_masters_rounds') }} r2
        on  r1.datagolf_id = r2.datagolf_id
        and r2.season < r1.season
        and not r2.is_covid_year
        and r2.driving_dist is not null
    group by r1.datagolf_id, r1.season
),

-- Field average dist per target year (players with >= 4 prior dist rounds)
field_avg_by_year as (
    select target_season, avg(prior_aug_dist) as field_avg_dist
    from prior_driving
    where prior_dist_rounds >= 4
    group by target_season
)

select
    wp.datagolf_id,
    wp.player_name,
    wp.season,
    wp.sg_total,
    -- Genuine pre-tournament skill signal from DataGolf predictions archive
    coalesce(pa.win_pct, 0.0)                                               as dg_pred_win_pct,
    -- Leakage-safe prior Augusta signals
    coalesce(wp.prior_augusta_sg_raw, 0.0)                                  as prior_augusta_sg,
    coalesce(wp.prior_appearances_raw, 0)                                   as prior_appearances,
    -- Leakage-safe distance vs field avg; 0.0 for debut or no prior dist data
    coalesce(pd.prior_aug_dist - fa.field_avg_dist, 0.0)                    as driving_dist_vs_avg,
    -- Current long approach SG as proxy (no historical point-in-time available)
    coalesce(dp.long_approach_sg, 0.0)                                      as long_approach_sg
from with_prior wp
left join {{ ref('stg_pred_archive') }}    pa on wp.datagolf_id = pa.datagolf_id and wp.season = pa.season
left join prior_driving    pd on wp.datagolf_id = pd.datagolf_id and wp.season = pd.target_season
left join field_avg_by_year fa on wp.season = fa.target_season
left join {{ ref('int_driving_profile') }} dp on wp.datagolf_id = dp.datagolf_id
where wp.season >= 2021
