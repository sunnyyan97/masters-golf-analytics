-- Weighted composite of current SG skill ratings + driving distance signals.
-- Weights: sg_app 28%, sg_putt 20%, sg_arg 18%, driving_dist_vs_avg 16%,
--          long_approach_sg 10%, sg_accuracy 4%, sg_ott 4% (sum = 1.00)
-- sg_arg and long_approach_sg raised based on regression finding that around-green
-- play and long approach execution are the dominant Augusta predictors.
-- sg_ott trimmed further as distance is already captured separately.
select
    s.datagolf_id,
    s.player_name,
    coalesce(s.sg_app,                    0) * 0.28
    + coalesce(s.sg_putt,                 0) * 0.20
    + coalesce(s.sg_ott,                  0) * 0.04
    + coalesce(s.sg_arg,                  0) * 0.18
    + coalesce(d.driving_dist_vs_avg, 0) * 0.004 * 0.16
    + coalesce(d.long_approach_sg,        0) * 0.10
    + coalesce(s.driving_acc,             0) * 0.04  as augusta_fit_score
from {{ ref('stg_skill_ratings') }} s
left join {{ ref('int_driving_profile') }} d using (datagolf_id)