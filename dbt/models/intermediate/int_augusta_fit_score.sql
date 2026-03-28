-- Weighted composite of current SG skill ratings.
-- Weights: sg_app 38%, sg_putt 28%, sg_ott 20%, sg_arg 10%, driving_acc 4%
-- driving_acc used as the accuracy component (from skill_ratings).
select
    datagolf_id,
    player_name,
    coalesce(sg_app,      0) * 0.38
    + coalesce(sg_putt,   0) * 0.28
    + coalesce(sg_ott,    0) * 0.20
    + coalesce(sg_arg,    0) * 0.10
    + coalesce(driving_acc, 0) * 0.04  as augusta_fit_score
from {{ ref('stg_skill_ratings') }}
