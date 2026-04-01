with rounds as (
    select
        datagolf_id,
        player_name,
        season,
        sg_total,
        sg_ott,
        sg_app,
        sg_arg,
        sg_putt
    from {{ ref('stg_masters_rounds') }}
    where not is_covid_year
      and sg_total is not null
),

player_year_sg as (
    select
        datagolf_id,
        player_name,
        season,
        avg(sg_total) as sg_total,
        avg(sg_ott)   as sg_off_tee,
        avg(sg_app)   as sg_approach,
        avg(sg_arg)   as sg_around_green,
        avg(sg_putt)  as sg_putting
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
)

select
    datagolf_id,
    player_name,
    season,
    sg_total,
    sg_approach,
    sg_putting,
    sg_off_tee,
    sg_around_green,
    coalesce(prior_augusta_sg_raw, 0.0) as prior_augusta_sg,
    coalesce(prior_appearances_raw, 0)  as prior_appearances
from with_prior
where season >= 2021
