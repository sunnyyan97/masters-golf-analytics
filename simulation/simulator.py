"""
Monte Carlo simulation engine for the 2026 Masters.

Usage:
    python -m simulation.simulator                         # 100,000 sims (default)
    python -m simulation.simulator --n_sims 1000           # fast dev run
    python -m simulation.simulator --seed 42               # reproducible
    python -m simulation.simulator --model manual          # explicit model type
"""

import argparse
import json
import time
from pathlib import Path

import numpy as np
import pandas as pd

from ingestion.load_to_duckdb import get_connection
from simulation.model_inputs import load_inputs


def compute_regression_mu(df: pd.DataFrame) -> np.ndarray:
    """Compute mu using ridge regression weights from regression_weights.json."""
    weights_path = Path(__file__).parent / "regression_weights.json"
    with open(weights_path) as f:
        w = json.load(f)
    coef = w["coefficients"]
    intercept = w["intercept"]

    # Map training feature names → mart_player_model_inputs column names
    feature_col_map = {
        "sg_approach":         "sg_app",
        "sg_putting":          "sg_putt",
        "sg_off_tee":          "sg_ott",
        "sg_around_green":     "sg_arg",
        "prior_augusta_sg":    "augusta_sg_total",
        "prior_appearances":   "augusta_seasons_played",
        "driving_dist_vs_avg": "driving_dist_vs_avg",
        "long_approach_sg":    "long_approach_sg",
    }
    mu = np.full(len(df), intercept)
    for feat, col in feature_col_map.items():
        mu = mu + coef[feat] * df[col].fillna(0.0).to_numpy()
    return mu


def activity_discount(player: dict) -> float:
    """
    Returns a multiplier (0–1) applied to base mu to discount inactive players.

    sg_overall_is_null is a proxy for inactivity — players not in DG top 500
    lack sufficient recent competitive rounds to generate a rating.
    Applied to both model_type='manual' and model_type='regression'.
    """
    sg_null = player.get("sg_overall_is_null", False)
    recent_starts = player.get("recent_starts", 15)

    if sg_null or recent_starts < 4:
        return 0.60   # essentially inactive (Tiger situation)
    elif recent_starts < 8:
        return 0.82   # limited activity (some LIV players)
    else:
        return 1.00   # full-season active player, no change


def run_simulation(df: pd.DataFrame, n_sims: int = 50_000, seed=None,
                   model_type: str = "manual") -> pd.DataFrame:
    """
    Vectorised Monte Carlo simulation of the Masters Tournament.

    Draws all random numbers upfront — no Python loops over sims.

    Parameters
    ----------
    df         : DataFrame from mart_player_model_inputs (one row per player)
    n_sims     : number of tournament simulations
    seed       : optional RNG seed for reproducibility
    model_type : 'manual' or 'regression' — stored in output for mart writes

    Returns
    -------
    DataFrame with one row per player and columns:
        datagolf_id, player_name, win_pct, top5_pct, top10_pct,
        top25_pct, mc_pct, mu, sigma, model_type
    """
    # Select mu source based on model type
    # Select mu source based on model type
    if model_type == "regression":
        base_mu = compute_regression_mu(df)
    elif model_type == "ensemble":
        manual_mu = df["augusta_mu"].to_numpy()
        regression_mu = compute_regression_mu(df)
        base_mu = (manual_mu + regression_mu) / 2
    else:
        base_mu = df["augusta_mu"].to_numpy()

    # Apply activity discount to base mu before simulation
    discounts = df.apply(lambda r: activity_discount(r.to_dict()), axis=1).to_numpy()
    mu = base_mu * discounts  # (N,)

    # Sigma blending: pull player-specific sigma toward field mean to prevent
    # consistent players (low sigma) from having their win% suppressed unrealistically.
    # SIGMA_BLEND_ALPHA=0.6 means 60% player, 40% field mean.
    # SIGMA_FLOOR=2.5 ensures no player falls below this minimum.
    SIGMA_BLEND_ALPHA = 0.6
    SIGMA_FLOOR = 2.5
    sigma = df["player_sigma"].to_numpy()          # (N,)
    field_mean_sigma = sigma.mean()
    sigma = SIGMA_BLEND_ALPHA * sigma + (1 - SIGMA_BLEND_ALPHA) * field_mean_sigma
    sigma = np.maximum(sigma, SIGMA_FLOOR)
    N = len(mu)
    S = n_sims

    rng = np.random.default_rng(seed)

    # --- Draw all random numbers upfront ---
    # Shared field difficulty per round: same for all players in a given round/sim
    field_difficulty = rng.normal(0, 1.2, size=(S, 4))          # (S, 4)
    # Idiosyncratic noise scaled by each player's sigma
    noise = rng.standard_normal(size=(S, N, 4)) * sigma[np.newaxis, :, np.newaxis]

    # --- Round scores ---
    # mu is strokes GAINED (positive = better player = lower actual score).
    # Golf uses lowest score to win, so we negate mu:
    #   score[s, n, r] = -mu[n] + field_difficulty[s, r] + noise[s, n, r]
    scores = (
        -mu[np.newaxis, :, np.newaxis]          # (1, N, 1) — negate: higher SG → lower score
        + field_difficulty[:, np.newaxis, :]    # (S, 1, 4)
        + noise                                 # (S, N, 4)
    )  # shape: (S, N, 4)

    # --- 36-hole cut (after rounds 1 & 2) ---
    scores_36 = scores[:, :, 0] + scores[:, :, 1]   # (S, N)
    sorted_36 = np.sort(scores_36, axis=1)           # (S, N)

    cut_top50    = sorted_36[:, 49]                  # score at 50th place, (S,)
    cut_within10 = sorted_36[:, 0] + 10              # leader + 10 strokes, (S,)

    # Player passes if EITHER condition is met → threshold is the more permissive one
    cut_threshold = np.maximum(cut_top50, cut_within10)[:, np.newaxis]  # (S, 1)
    made_cut = scores_36 <= cut_threshold            # (S, N) boolean

    # --- 72-hole total (inf for MC'd players) ---
    scores_72 = scores.sum(axis=2)                   # (S, N)
    scores_72 = np.where(made_cut, scores_72, np.inf)

    # --- Win: player(s) with the lowest score in each sim ---
    min_score = scores_72.min(axis=1, keepdims=True)  # (S, 1)
    win_mask = scores_72 == min_score                  # (S, N)

    # --- Finishing rank (ordinal; inf players rank last automatically) ---
    ranks = np.argsort(np.argsort(scores_72, axis=1), axis=1) + 1  # (S, N)

    # --- Aggregate probabilities ---
    win_pct   = win_mask.mean(axis=0)          # (N,)
    top5_pct  = (ranks <= 5).mean(axis=0)
    top10_pct = (ranks <= 10).mean(axis=0)
    top25_pct = (ranks <= 25).mean(axis=0)
    mc_pct    = (~made_cut).mean(axis=0)

    # --- Validation ---
    total_win = win_pct.sum()
    assert 0.98 <= total_win <= 1.02, (
        f"win_pct sum = {total_win:.4f} — expected ~1.0. "
        "Check for duplicate players or cut rule bug."
    )

    return pd.DataFrame({
        "datagolf_id": df["datagolf_id"].to_numpy(),
        "player_name": df["player_name"].to_numpy(),
        "win_pct":     win_pct,
        "top5_pct":    top5_pct,
        "top10_pct":   top10_pct,
        "top25_pct":   top25_pct,
        "mc_pct":      mc_pct,
        "mu":          mu,
        "sigma":       sigma,
        "model_type":  model_type,
    })


_RANKINGS_PARQUET_QUERY = """
SELECT
    sim.datagolf_id,
    sim.player_name,
    inp.country,
    pl.country_code,
    inp.dg_rank,
    sim.win_pct,
    sim.top5_pct,
    sim.top10_pct,
    sim.mc_pct,
    inp.augusta_fit_score,
    cdg.win_pct / 100.0 AS dg_win_pct,
    sim.model_type
FROM main.mart_simulation_results sim
LEFT JOIN main.mart_player_model_inputs      inp ON sim.datagolf_id = inp.datagolf_id
LEFT JOIN main.stg_player_list               pl  ON sim.datagolf_id = pl.datagolf_id
LEFT JOIN main.stg_current_dg_predictions    cdg ON sim.datagolf_id = cdg.datagolf_id
"""


def _export_rankings_parquet(conn, db_path) -> None:
    """Export joined rankings data to a parquet file for Streamlit to consume.

    This avoids Streamlit needing a DuckDB connection (which conflicts with
    any open notebook kernel holding a write lock on the same file).
    """
    out_path = Path(db_path).parent / "rankings_cache.parquet"
    df = conn.execute(_RANKINGS_PARQUET_QUERY).df()
    df.to_parquet(out_path, index=False)


def write_results(results: pd.DataFrame, db_path=None) -> None:
    """
    Write simulation results to main.mart_simulation_results in DuckDB.

    Deletes existing rows for the given model_type then inserts fresh results,
    so manual and regression runs coexist in the same table.
    If the table doesn't exist or is missing the model_type column, recreates it.

    Also exports a combined parquet cache for Streamlit consumption.
    """
    conn = get_connection(db_path)
    model_type = results["model_type"].iloc[0]

    # Resolve db_path for parquet export (mirrors get_connection default)
    if db_path is None:
        db_path = Path(__file__).parent.parent / "data" / "masters.duckdb"

    # Check whether the table exists with the model_type column
    has_model_type = conn.execute("""
        SELECT count(*) FROM information_schema.columns
        WHERE table_name = 'mart_simulation_results'
          AND table_schema = 'main'
          AND column_name = 'model_type'
    """).fetchone()[0] > 0

    if not has_model_type:
        # Table missing or schema is stale — recreate from scratch
        conn.execute(
            "CREATE OR REPLACE TABLE main.mart_simulation_results AS SELECT * FROM results"
        )
    else:
        # Delete stale rows for this model_type, then insert
        conn.execute(
            "DELETE FROM main.mart_simulation_results WHERE model_type = ?",
            [model_type]
        )
        conn.execute("INSERT INTO main.mart_simulation_results SELECT * FROM results")

    _export_rankings_parquet(conn, db_path)
    conn.close()


def _print_leaderboard(results: pd.DataFrame) -> None:
    top10 = (
        results.sort_values("win_pct", ascending=False)
        .head(10)
        .reset_index(drop=True)
    )
    print(f"\n{'Rank':<5} {'Player':<25} {'Win%':>6} {'Top10%':>7} {'MC%':>6}")
    print("-" * 52)
    for i, row in top10.iterrows():
        print(
            f"{i+1:<5} {row['player_name']:<25} "
            f"{row['win_pct']*100:>5.1f}% "
            f"{row['top10_pct']*100:>6.1f}% "
            f"{row['mc_pct']*100:>5.1f}%"
        )
    print(f"\n  win_pct sum: {results['win_pct'].sum():.4f}")


def main():
    parser = argparse.ArgumentParser(description="Masters Monte Carlo simulator")
    parser.add_argument("--n_sims", type=int, default=100_000,
                        help="Number of simulations (default: 100,000)")
    parser.add_argument("--seed", type=int, default=None,
                        help="RNG seed for reproducibility (default: random)")
    parser.add_argument("--model", type=str, default="manual",
                        choices=["manual", "regression", "ensemble"],
                        help="Model type to run (default: manual)")
    args = parser.parse_args()

    print(f"Loading model inputs...", end=" ", flush=True)
    df = load_inputs()
    print(f"{len(df)} players")

    print(f"Running {args.n_sims:,} simulations [{args.model}]...", end=" ", flush=True)
    t0 = time.perf_counter()
    results = run_simulation(df, n_sims=args.n_sims, seed=args.seed,
                             model_type=args.model)
    elapsed = time.perf_counter() - t0
    print(f"done in {elapsed:.1f}s")

    write_results(results)
    print("Results written → main.mart_simulation_results")

    _print_leaderboard(results)


if __name__ == "__main__":
    main()
