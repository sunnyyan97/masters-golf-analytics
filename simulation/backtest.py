"""
Phase 5 back-testing: compare manual, regression, and DataGolf models
against historical Masters outcomes (2019–2025).

Usage:
    python -m simulation.backtest
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

from ingestion.load_to_duckdb import get_connection


# ──────────────────────────────────────────────────────────────────────────────
# Actual results
# ──────────────────────────────────────────────────────────────────────────────

def get_actual_results(conn, year: int) -> pd.DataFrame:
    """Return players sorted by actual finishing order for the given year.

    Players who made the cut rank above those who didn't.
    Within cut-makers, higher sg_total total = lower score = better rank.
    Actual rank 1 = winner.
    """
    df = conn.execute("""
        SELECT datagolf_id, player_name,
               sum(sg_total)  AS total_sg,
               count(*)       AS rounds_played,
               (max(round_num) >= 3) AS made_cut
        FROM stg_masters_rounds
        WHERE season = ? AND sg_total IS NOT NULL
        GROUP BY datagolf_id, player_name
        ORDER BY made_cut DESC, total_sg DESC
    """, [year]).df()
    df["actual_rank"] = range(1, len(df) + 1)
    return df


# ──────────────────────────────────────────────────────────────────────────────
# Manual model
# ──────────────────────────────────────────────────────────────────────────────

def get_manual_predictions(conn, year: int, current_sg_overall: pd.DataFrame) -> pd.DataFrame:
    """Compute manual-model mu for every player who appeared in `year`.

    Formula (same weights as mart_player_model_inputs):
        mu = 0.40 * sg_overall_rolling   ← current proxy (acknowledged limitation)
           + 0.30 * prior_augusta_sg      ← leakage-safe
           + 0.20 * fit_score_proxy       ← leakage-safe driving + current skill
           + 0.10 * 0.0                   ← momentum_delta unavailable historically

    fit_score_proxy:
        sg_app*0.28 + sg_putt*0.20 + sg_ott*0.04 + sg_arg*0.18
        + driving_dist_vs_avg*0.16 + long_approach_sg*0.10 + driving_acc*0.04

    For 2019: int_augusta_regression_inputs only covers 2021+, so we build
    leakage-safe prior Augusta SG directly from stg_masters_rounds.
    driving_dist_vs_avg = 0.0 for all (no 2019 driving data).
    """
    if year < 2021:
        # Build features directly from stg_masters_rounds for pre-2021 years
        players = conn.execute("""
            SELECT DISTINCT datagolf_id FROM stg_masters_rounds WHERE season = ?
        """, [year]).df()

        prior_sg = conn.execute("""
            SELECT datagolf_id, avg(sg_total) AS prior_augusta_sg
            FROM stg_masters_rounds
            WHERE season < ? AND not is_covid_year AND sg_total IS NOT NULL
            GROUP BY datagolf_id
        """, [year]).df()

        reg_inputs = players.merge(prior_sg, on="datagolf_id", how="left")
        reg_inputs["driving_dist_vs_avg"] = 0.0
        reg_inputs["long_approach_sg"]    = 0.0
    else:
        reg_inputs = conn.execute("""
            SELECT datagolf_id, prior_augusta_sg, prior_appearances,
                   driving_dist_vs_avg, long_approach_sg
            FROM int_augusta_regression_inputs
            WHERE season = ?
        """, [year]).df()

    skill = conn.execute("""
        SELECT datagolf_id, sg_app, sg_putt, sg_ott, sg_arg, driving_acc
        FROM stg_skill_ratings
    """).df()

    df = reg_inputs.merge(skill, on="datagolf_id", how="left")
    df = df.merge(current_sg_overall, on="datagolf_id", how="left")

    # Fill nulls with 0
    for col in ["sg_app", "sg_putt", "sg_ott", "sg_arg", "driving_acc",
                "driving_dist_vs_avg", "long_approach_sg", "prior_augusta_sg",
                "sg_overall_rolling"]:
        df[col] = df[col].fillna(0.0)

    fit_score = (
        df["sg_app"]              * 0.28
        + df["sg_putt"]           * 0.20
        + df["sg_ott"]            * 0.04
        + df["sg_arg"]            * 0.18
        + df["driving_dist_vs_avg"] * 0.16
        + df["long_approach_sg"]  * 0.10
        + df["driving_acc"]       * 0.04
    )

    mu = (
        0.40 * df["sg_overall_rolling"]
        + 0.30 * df["prior_augusta_sg"]
        + 0.20 * fit_score
        # momentum_delta = 0.0 (no historical data)
    )

    # Activity discount: players not in DG top-500 (sg_overall_is_null proxy)
    sg_null = df["sg_overall_rolling"] == 0.0
    discount = np.where(sg_null, 0.60, 1.00)
    mu = mu * discount

    return df[["datagolf_id"]].assign(pred_signal=mu)


# ──────────────────────────────────────────────────────────────────────────────
# Regression model
# ──────────────────────────────────────────────────────────────────────────────

def get_regression_predictions(cv_rows: list[dict], year: int) -> pd.DataFrame:
    """Return LOGO CV out-of-fold predictions for the given year."""
    rows = [r for r in cv_rows if r["season"] == year]
    if not rows:
        return pd.DataFrame(columns=["datagolf_id", "pred_signal"])
    df = pd.DataFrame(rows)
    return df.rename(columns={"cv_pred_mu": "pred_signal"})[["datagolf_id", "pred_signal"]]


# ──────────────────────────────────────────────────────────────────────────────
# DataGolf model
# ──────────────────────────────────────────────────────────────────────────────

def get_datagolf_predictions(conn, year: int) -> pd.DataFrame:
    """Return DataGolf pred_archive win_pct as the rank signal."""
    df = conn.execute("""
        SELECT datagolf_id, win_pct AS pred_signal
        FROM stg_pred_archive
        WHERE season = ?
    """, [year]).df()
    return df[["datagolf_id", "pred_signal"]]


# ──────────────────────────────────────────────────────────────────────────────
# Metrics
# ──────────────────────────────────────────────────────────────────────────────

def compute_metrics(pred_df: pd.DataFrame, actual_df: pd.DataFrame):
    """
    pred_df  must have: datagolf_id, pred_rank (1 = best predicted)
    actual_df must have: datagolf_id, actual_rank (1 = winner)

    Returns: (spearman_corr, top10_precision, winner_pred_rank, n_players)
    """
    merged = pred_df.merge(actual_df[["datagolf_id", "actual_rank"]], on="datagolf_id")
    if len(merged) < 5:
        return None, None, None, len(merged)

    spearman, _ = spearmanr(merged["pred_rank"], merged["actual_rank"])

    pred_top10   = set(merged.nsmallest(10, "pred_rank")["datagolf_id"])
    actual_top10 = set(actual_df.head(10)["datagolf_id"])
    top10_precision = len(pred_top10 & actual_top10) / 10

    winner_id  = actual_df.iloc[0]["datagolf_id"]
    winner_row = merged[merged["datagolf_id"] == winner_id]
    winner_pred_rank = int(winner_row["pred_rank"].iloc[0]) if len(winner_row) else None

    return spearman, top10_precision, winner_pred_rank, len(merged)


# ──────────────────────────────────────────────────────────────────────────────
# Write results
# ──────────────────────────────────────────────────────────────────────────────

def write_results(conn, rows: list[dict]) -> None:
    results = pd.DataFrame(rows)
    conn.execute("DELETE FROM mart_backtest_comparison")
    conn.execute("INSERT INTO mart_backtest_comparison SELECT * FROM results")


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main():
    conn = get_connection()

    # Check available tables for diagnostics
    tables = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
    if "stg_masters_rounds" not in tables:
        raise RuntimeError(
            "stg_masters_rounds not found. Run `dbt build` from the dbt/ directory first."
        )

    # Current DG skill ratings (used as proxy for all manual-model years)
    current_sg_overall = conn.execute("""
        SELECT datagolf_id, coalesce(sg_overall_rolling, 0.0) AS sg_overall_rolling
        FROM stg_dg_rankings
    """).df()

    # LOGO CV predictions for regression model
    cv_path = Path(__file__).parent / "cv_predictions.json"
    with open(cv_path) as f:
        cv_rows = json.load(f)

    # Years to evaluate per model
    manual_years     = [2019, 2021, 2022, 2023, 2024, 2025]
    regression_years = [2021, 2022, 2023, 2024, 2025]
    datagolf_years   = [2021, 2022, 2023, 2024, 2025]

    all_years = sorted(set(manual_years + regression_years + datagolf_years))

    output_rows = []

    print(f"\n{'Year':<6} {'Model':<12} {'Spearman':>9} {'Top10%':>7} {'Winner_rank':>12} {'N':>5}  Notes")
    print("-" * 72)

    for year in all_years:
        actual = get_actual_results(conn, year)
        if actual.empty:
            print(f"{year}  (no actual data — skipping)")
            continue

        year_results = {}

        # ── Manual ──────────────────────────────────────────────────────────
        if year in manual_years:
            try:
                pred = get_manual_predictions(conn, year, current_sg_overall)
                pred["pred_rank"] = pred["pred_signal"].rank(ascending=False).astype(int)
                spearman, top10_prec, win_rank, n = compute_metrics(pred, actual)
                notes = "sg_overall=current proxy; no momentum"
                if year == 2019:
                    notes += "; no driving dist"
                year_results["manual"] = (spearman, top10_prec, win_rank, n, notes)
            except Exception as e:
                year_results["manual"] = (None, None, None, 0, str(e))

        # ── Regression ──────────────────────────────────────────────────────
        if year in regression_years:
            try:
                pred = get_regression_predictions(cv_rows, year)
                if pred.empty:
                    year_results["regression"] = (None, None, None, 0, "no CV predictions")
                else:
                    pred["pred_rank"] = pred["pred_signal"].rank(ascending=False).astype(int)
                    spearman, top10_prec, win_rank, n = compute_metrics(pred, actual)
                    year_results["regression"] = (spearman, top10_prec, win_rank, n, "LOGO CV OOF")
            except Exception as e:
                year_results["regression"] = (None, None, None, 0, str(e))

        # ── DataGolf ────────────────────────────────────────────────────────
        if year in datagolf_years:
            try:
                pred = get_datagolf_predictions(conn, year)
                if pred.empty:
                    year_results["datagolf"] = (None, None, None, 0, "no pred_archive data")
                else:
                    pred["pred_rank"] = pred["pred_signal"].rank(ascending=False).astype(int)
                    spearman, top10_prec, win_rank, n = compute_metrics(pred, actual)
                    year_results["datagolf"] = (spearman, top10_prec, win_rank, n, "pred_archive win_pct")
            except Exception as e:
                year_results["datagolf"] = (None, None, None, 0, str(e))

        for model_type, (spearman, top10_prec, win_rank, n, notes) in year_results.items():
            sp_str   = f"{spearman:+.3f}" if spearman is not None else "  N/A"
            t10_str  = f"{top10_prec:.1f}" if top10_prec is not None else "N/A"
            wrk_str  = str(win_rank) if win_rank is not None else "N/A"
            print(f"{year:<6} {model_type:<12} {sp_str:>9} {t10_str:>7} {wrk_str:>12}  {n:>4}  {notes}")

            output_rows.append({
                "year":            year,
                "model_type":      model_type,
                "spearman_corr":   float(spearman) if spearman is not None else None,
                "top10_precision": float(top10_prec) if top10_prec is not None else None,
                "winner_rank":     int(win_rank) if win_rank is not None else None,
                "n_players":       int(n),
                "notes":           notes,
            })

    # ── Summary ──────────────────────────────────────────────────────────────
    print("\n── Average Spearman by model ──────────────────────────────────────")
    summary = pd.DataFrame(output_rows)
    for model_type, grp in summary.groupby("model_type"):
        avg_sp  = grp["spearman_corr"].mean()
        avg_t10 = grp["top10_precision"].mean()
        print(f"  {model_type:<12} avg_spearman={avg_sp:+.3f}  avg_top10={avg_t10:.2f}  n_years={len(grp)}")

    # ── Persist ──────────────────────────────────────────────────────────────
    write_results(conn, output_rows)
    print(f"\nWrote {len(output_rows)} rows → mart_backtest_comparison")
    conn.close()


if __name__ == "__main__":
    main()
