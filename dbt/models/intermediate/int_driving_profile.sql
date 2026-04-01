with aug_dist as (
    select
        datagolf_id,
        avg(driving_dist)  as aug_driving_dist,
        count(*)           as dist_data_rounds
    from {{ ref('stg_masters_rounds') }}
    where season >= 2019
      and not is_covid_year
      and driving_dist is not null
    group by datagolf_id
),

field_avg as (
    select avg(aug_driving_dist) as avg_dist
    from aug_dist
    where dist_data_rounds >= 4
),

approach as (
    select
        datagolf_id,
        sg_over_200_fw as long_approach_sg
    from {{ ref('stg_approach_skill') }}
)

select
    coalesce(d.datagolf_id, a.datagolf_id)                  as datagolf_id,
    d.aug_driving_dist,
    coalesce(d.aug_driving_dist - f.avg_dist, 0.0)          as driving_dist_vs_avg,
    coalesce(a.long_approach_sg, 0.0)                       as long_approach_sg,
    coalesce(d.dist_data_rounds, 0)                         as dist_data_rounds
from aug_dist d
full outer join approach a  using (datagolf_id)
cross join field_avg f
