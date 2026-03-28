with field as (
    select * from {{ ref('stg_masters_field_2026') }}
),
rankings as (
    select datagolf_id, sg_overall_rolling
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
)
select
    f.datagolf_id,
    f.player_name,
    f.country,
    f.is_amateur,
    f.dg_rank,
    f.owgr_rank,

    -- Current skill
    r.sg_overall_rolling,

    -- Augusta career SG (null for debut players)
    c.augusta_sg_total,
    c.augusta_sg_ott,
    c.augusta_sg_app,
    c.augusta_sg_arg,
    c.augusta_sg_putt,
    coalesce(c.rounds_played, 0) as augusta_rounds_played,

    -- Fit score and trajectory
    fit.augusta_fit_score,
    t.momentum_delta,

    -- Player volatility
    coalesce(s.player_sigma, 3.0)     as player_sigma,
    coalesce(s.rounds_available, 0)   as sigma_rounds_available,

    -- Composite mu
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
