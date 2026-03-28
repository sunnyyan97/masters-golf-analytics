with stats as (
    select
        datagolf_id,
        stddev_samp(round_score) as raw_sigma,
        count(*)                 as rounds_available
    from {{ ref('stg_masters_rounds') }}
    where round_score is not null
    group by datagolf_id
)
select
    datagolf_id,
    coalesce(
        case when rounds_available >= 10 then raw_sigma end,
        3.0
    )               as player_sigma,
    rounds_available
from stats
