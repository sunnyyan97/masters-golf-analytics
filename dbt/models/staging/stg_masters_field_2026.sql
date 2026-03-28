select
    cast(dg_id as integer)                           as datagolf_id,
    player_name,
    country,
    am = 1                                           as is_amateur,
    try_cast(dg_rank as integer)                     as dg_rank,
    try_cast(owgr_rank as integer)                   as owgr_rank
from {{ source('raw', 'masters_field_2026') }}
