with field as (
    select * from {{ ref('stg_masters_field_2026') }}
),
rankings as (
    select
        datagolf_id,
        sg_overall_rolling                    as sg_overall_rolling_raw,
        coalesce(sg_overall_rolling, 0)       as sg_overall_rolling
    from {{ ref('stg_dg_rankings') }}
),
career_sg as (
    select * from {{ ref('int_augusta_career_sg') }}
),
trajectory as (
    select * from {{ ref('int_recent_trajectory') }}
),
fit as (
    select * from {{ ref('int_augusta_fit_score') }}
),
sigma as (
    select * from {{ ref('int_player_sigma') }}
),
driving as (
    select * from {{ ref('int_driving_profile') }}
),
skill as (
    select datagolf_id, sg_app, sg_putt, sg_ott, sg_arg
    from {{ ref('stg_skill_ratings') }}
)
select
    f.datagolf_id,
    f.player_name,
    f.country,
    f.is_amateur,
    f.dg_rank,
    f.owgr_rank,

    -- Current overall skill
    r.sg_overall_rolling,

    -- Current SG components (for regression model)
    coalesce(sk.sg_app,  0.0) as sg_app,
    coalesce(sk.sg_putt, 0.0) as sg_putt,
    coalesce(sk.sg_ott,  0.0) as sg_ott,
    coalesce(sk.sg_arg,  0.0) as sg_arg,

    -- Augusta career SG (null for debut players)
    c.augusta_sg_total,
    c.augusta_sg_ott,
    c.augusta_sg_app,
    c.augusta_sg_arg,
    c.augusta_sg_putt,
    coalesce(c.rounds_played,   0) as augusta_rounds_played,
    coalesce(c.seasons_played,  0) as augusta_seasons_played,

    -- Fit score and trajectory
    fit.augusta_fit_score,
    t.momentum_delta,

    -- Player volatility
    coalesce(s.player_sigma, 3.0)     as player_sigma,
    coalesce(s.rounds_available, 0)   as sigma_rounds_available,

    -- Driving distance signals
    d.aug_driving_dist,
    coalesce(d.driving_dist_vs_avg, 0.0) as driving_dist_vs_avg,
    coalesce(d.long_approach_sg,    0.0) as long_approach_sg,
    coalesce(d.dist_data_rounds,    0)   as dist_data_rounds,

    -- Activity discount inputs (derived from raw sg_overall_rolling before coalesce)
    (r.sg_overall_rolling_raw is null) as sg_overall_is_null,
    case when r.sg_overall_rolling_raw is null then 2 else 15 end as recent_starts,

    -- Composite mu (manual model)
    0.40 * coalesce(r.sg_overall_rolling, 0)
    + 0.30 * coalesce(c.augusta_sg_total, 0)
    + 0.20 * coalesce(fit.augusta_fit_score, 0)
    + 0.10 * coalesce(t.momentum_delta, 0)       as augusta_mu

from field f
left join rankings  r   on f.datagolf_id = r.datagolf_id
left join career_sg c   on f.datagolf_id = c.datagolf_id
left join trajectory t  on f.datagolf_id = t.datagolf_id
left join fit            on f.datagolf_id = fit.datagolf_id
left join sigma     s   on f.datagolf_id = s.datagolf_id
left join driving   d   on f.datagolf_id = d.datagolf_id
left join skill     sk  on f.datagolf_id = sk.datagolf_id
