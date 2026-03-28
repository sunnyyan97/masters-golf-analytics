with base as (
    select
        cast(dg_id as integer)           as datagolf_id,
        player_name,
        cast(year as integer)            as season,
        cast(round_num as integer)       as round_num,
        fin_text,
        try_cast(score as double)        as round_score,
        try_cast(sg_total as double)     as sg_total,
        try_cast(sg_ott as double)       as sg_ott,
        try_cast(sg_app as double)       as sg_app,
        try_cast(sg_arg as double)       as sg_arg,
        try_cast(sg_putt as double)      as sg_putt,
        try_cast(driving_acc as double)  as driving_acc,
        try_cast(driving_dist as double) as driving_dist,
        cast(is_covid_year as boolean)   as is_covid_year,
        row_number() over (
            partition by
                cast(dg_id as integer),
                cast(year as integer),
                cast(round_num as integer)
            order by 1
        ) as rn
    from {{ source('raw', 'masters_rounds') }}
)
select * exclude (rn)
from base
where rn = 1
