select
    cast(dg_id as integer)                              as datagolf_id,
    player_name,
    event_name,
    try_cast(timing_adjustment as double)               as timing_adjustment,
    try_cast(baseline_pred     as double)               as baseline_pred,
    try_cast(final_pred        as double)               as final_pred
from {{ source('raw', 'player_decompositions') }}
