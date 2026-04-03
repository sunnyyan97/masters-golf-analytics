-- Training data for ridge regression: one row per player per Augusta appearance (2021+).
--
-- Features:
--   sg_approach, sg_putting, sg_off_tee, sg_around_green → current tour SG component
--     ratings from stg_skill_ratings (static 2026 snapshot used as proxy across all
--     training years — same acknowledged limitation as long_approach_sg).
--     Pre-2021 Augusta rounds have no SG category data (verified: 2019=0% coverage),
--     making leakage-safe prior-year Augusta SG categories nonviable.
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
    -- Current tour SG component ratings as proxy (static 2026 snapshot, same limitation
    -- as long_approach_sg). Nulls dropped in derive_weights.py via dropna.
    sr.sg_app                                                               as sg_approach,
    sr.sg_putt                                                              as sg_putting,
    sr.sg_ott                                                               as sg_off_tee,
    sr.sg_arg                                                               as sg_around_green,
    -- Leakage-safe prior Augusta signals
    coalesce(wp.prior_augusta_sg_raw, 0.0)                                  as prior_augusta_sg,
    coalesce(wp.prior_appearances_raw, 0)                                   as prior_appearances,
    -- Leakage-safe distance vs field avg; 0.0 for debut or no prior dist data
    coalesce(pd.prior_aug_dist - fa.field_avg_dist, 0.0)                    as driving_dist_vs_avg,
    -- Current long approach SG as proxy (no historical point-in-time available)
    coalesce(dp.long_approach_sg, 0.0)                                      as long_approach_sg
from with_prior wp
left join {{ ref('stg_skill_ratings') }}   sr on wp.datagolf_id = sr.datagolf_id
left join prior_driving    pd on wp.datagolf_id = pd.datagolf_id and wp.season = pd.target_season
left join field_avg_by_year fa on wp.season = fa.target_season
left join {{ ref('int_driving_profile') }} dp on wp.datagolf_id = dp.datagolf_id
where wp.season >= 2021
