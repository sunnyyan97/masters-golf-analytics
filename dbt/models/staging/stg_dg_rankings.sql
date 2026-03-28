select
    cast(dg_id as integer)                            as datagolf_id,
    player_name,
    country,
    primary_tour,
    cast(datagolf_rank as integer)                    as datagolf_rank,
    try_cast(owgr_rank as integer)                    as owgr_rank,
    try_cast(dg_skill_estimate as double)             as sg_overall_rolling,
    am = 1                                            as is_amateur
from {{ source('raw', 'dg_rankings') }}
