select
    cast(dg_id as integer)                        as datagolf_id,
    player_name,
    cast(year as integer)                         as season,
    fin_text,
    try_cast(win as double)                       as win_pct,
    try_cast(top_3 as double)                     as top3_pct,
    try_cast(top_5 as double)                     as top5_pct,
    try_cast(top_10 as double)                    as top10_pct,
    try_cast(top_20 as double)                    as top20_pct,
    try_cast(top_30 as double)                    as top30_pct,
    try_cast(make_cut as double)                  as make_cut_pct,
    try_cast(first_round_leader as double)        as first_round_leader_pct
from {{ source('raw', 'pred_archive') }}
