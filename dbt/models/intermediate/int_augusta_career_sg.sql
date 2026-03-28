with weighted_rounds as (
    select
        datagolf_id,
        sg_total,
        sg_ott,
        sg_app,
        sg_arg,
        sg_putt,
        case when season >= 2023 then 2 else 1 end as weight
    from {{ ref('stg_masters_rounds') }}
    where not is_covid_year
      and sg_total is not null
)
select
    datagolf_id,
    sum(sg_total * weight) / sum(weight)                                                          as augusta_sg_total,
    sum(sg_ott  * weight) / nullif(sum(case when sg_ott  is not null then weight end), 0)         as augusta_sg_ott,
    sum(sg_app  * weight) / nullif(sum(case when sg_app  is not null then weight end), 0)         as augusta_sg_app,
    sum(sg_arg  * weight) / nullif(sum(case when sg_arg  is not null then weight end), 0)         as augusta_sg_arg,
    sum(sg_putt * weight) / nullif(sum(case when sg_putt is not null then weight end), 0)         as augusta_sg_putt,
    count(*)                                                                                       as rounds_played
from weighted_rounds
group by datagolf_id
