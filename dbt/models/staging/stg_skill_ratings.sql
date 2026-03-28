select
    cast(dg_id as integer)           as datagolf_id,
    player_name,
    try_cast(sg_ott as double)       as sg_ott,
    try_cast(sg_app as double)       as sg_app,
    try_cast(sg_arg as double)       as sg_arg,
    try_cast(sg_putt as double)      as sg_putt,
    try_cast(sg_total as double)     as sg_total,
    try_cast(driving_acc as double)  as driving_acc,
    try_cast(driving_dist as double) as driving_dist
from {{ source('raw', 'skill_ratings') }}
