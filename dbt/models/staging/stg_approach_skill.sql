select
    cast(dg_id as integer)                                    as datagolf_id,
    player_name,
    try_cast("50_100_fw_sg_per_shot" as double)               as sg_50_100_fw,
    try_cast("100_150_fw_sg_per_shot" as double)              as sg_100_150_fw,
    try_cast("150_200_fw_sg_per_shot" as double)              as sg_150_200_fw,
    try_cast("over_200_fw_sg_per_shot" as double)             as sg_over_200_fw,
    try_cast("under_150_rgh_sg_per_shot" as double)           as sg_under_150_rgh,
    try_cast("over_150_rgh_sg_per_shot" as double)            as sg_over_150_rgh
from {{ source('raw', 'approach_skill') }}
