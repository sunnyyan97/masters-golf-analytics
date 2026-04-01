-- Weighted composite of current SG skill ratings + driving distance signals.
-- Weights: sg_app 34%, sg_putt 24%, driving_dist_vs_avg 20%, sg_arg 8%,
--          sg_ott 6%, long_approach_sg 6%, driving_acc 2% (sum = 1.00)
-- sg_ott trimmed further (0.12→0.06) as distance signal takes more of that component.
select
    s.datagolf_id,
    s.player_name,
    coalesce(s.sg_app,      0) * 0.34
    + coalesce(s.sg_putt,   0) * 0.24
    + coalesce(s.sg_ott,    0) * 0.06
    + coalesce(s.sg_arg,    0) * 0.08
    + coalesce(d.driving_dist_vs_avg, 0) * 0.20
    + coalesce(d.long_approach_sg,    0) * 0.06
    + coalesce(s.driving_acc, 0) * 0.02  as augusta_fit_score
from {{ ref('stg_skill_ratings') }} s
left join {{ ref('int_driving_profile') }} d using (datagolf_id)
