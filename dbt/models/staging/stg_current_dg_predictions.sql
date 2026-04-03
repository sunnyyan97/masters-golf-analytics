select
    cast(datagolf_id as integer)        as datagolf_id,
    player_name,
    try_cast(win_pct   as double)       as win_pct,
    try_cast(top5_pct  as double)       as top5_pct,
    try_cast(top10_pct as double)       as top10_pct,
    try_cast(mc_pct    as double)       as mc_pct,
    cast(season as integer)             as season
from {{ source('raw', 'current_dg_predictions') }}
