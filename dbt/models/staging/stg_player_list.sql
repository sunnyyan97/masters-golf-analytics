select
    cast(dg_id as integer)                              as datagolf_id,
    player_name,
    country,
    country_code,
    amateur = 1                                          as is_amateur
from {{ source('raw', 'player_list') }}
