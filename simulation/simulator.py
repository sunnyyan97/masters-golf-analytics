"""
Monte Carlo simulation engine for the 2026 Masters.

Usage:
    python -m simulation.simulator                  # 50,000 sims (default)
    python -m simulation.simulator --n_sims 1000    # fast dev run
    python -m simulation.simulator --seed 42        # reproducible
"""

import argparse
import time

import numpy as np
import pandas as pd

from ingestion.load_to_duckdb import get_connection
from simulation.model_inputs import load_inputs


def run_simulation(df: pd.DataFrame, n_sims: int = 50_000, seed=None) -> pd.DataFrame:
    """
    Vectorised Monte Carlo simulation of the Masters Tournament.

    Draws all random numbers upfront — no Python loops over sims.

    Parameters
    ----------
    df      : DataFrame from mart_player_model_inputs (one row per player)
    n_sims  : number of tournament simulations
    seed    : optional RNG seed for reproducibility

    Returns
    -------
    DataFrame with one row per player and columns:
        datagolf_id, player_name, win_pct, top5_pct, top10_pct,
        top25_pct, mc_pct, mu, sigma
    """
    mu = df["augusta_mu"].to_numpy()        # (N,)
    sigma = df["player_sigma"].to_numpy()   # (N,)
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
    })


def write_results(results: pd.DataFrame, db_path=None) -> None:
    """Write simulation results to main.mart_simulation_results in DuckDB."""
    conn = get_connection(db_path)
    conn.execute(
        "CREATE OR REPLACE TABLE main.mart_simulation_results AS SELECT * FROM results"
    )
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
    parser.add_argument("--n_sims", type=int, default=50_000,
                        help="Number of simulations (default: 50,000)")
    parser.add_argument("--seed", type=int, default=None,
                        help="RNG seed for reproducibility (default: random)")
    args = parser.parse_args()

    print(f"Loading model inputs...", end=" ", flush=True)
    df = load_inputs()
    print(f"{len(df)} players")

    print(f"Running {args.n_sims:,} simulations...", end=" ", flush=True)
    t0 = time.perf_counter()
    results = run_simulation(df, n_sims=args.n_sims, seed=args.seed)
    elapsed = time.perf_counter() - t0
    print(f"done in {elapsed:.1f}s")

    write_results(results)
    print("Results written → main.mart_simulation_results")

    _print_leaderboard(results)


if __name__ == "__main__":
    main()
