-- DataGolf skill ratings only expose a current snapshot, not rolling windows.
-- Momentum delta is set to 0.0 (neutral) as a placeholder.
-- A proper trajectory signal requires the player-decompositions endpoint (future enhancement).
select
    datagolf_id,
    0.0::double as momentum_delta
from {{ ref('stg_skill_ratings') }}
