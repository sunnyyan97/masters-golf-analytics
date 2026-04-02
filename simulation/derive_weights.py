"""
Derive ridge regression weights from historical Augusta data (2021–2025).
Writes coefficients to simulation/regression_weights.json.

Run with: python -m simulation.derive_weights
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import RidgeCV
from sklearn.model_selection import LeaveOneGroupOut
from sklearn.preprocessing import StandardScaler

from ingestion.load_to_duckdb import get_connection

FEATURES = [
    "dg_pred_win_pct",
    "prior_augusta_sg",
    "prior_appearances",
    "driving_dist_vs_avg",
    "long_approach_sg",
]
TARGET = "sg_total"
ALPHAS = [0.1, 1.0, 10.0, 100.0]


def load_training_data(db_path=None) -> pd.DataFrame:
    conn = get_connection(db_path)
    df = conn.execute("SELECT * FROM main.int_augusta_regression_inputs").df()
    conn.close()
    if df.empty:
        raise RuntimeError(
            "int_augusta_regression_inputs is empty — run "
            "`cd dbt && dbt build --select int_augusta_regression_inputs` first."
        )
    return df


def main():
    print("Loading training data...", end=" ", flush=True)
    df = load_training_data()
    print(f"{len(df)} rows")

    print("\nSeason distribution:")
    for season, count in df.groupby("season").size().items():
        print(f"  {season}: {count} players")

    # Drop rows missing any feature or target
    needed = FEATURES + [TARGET, "season"]
    before = len(df)
    df = df.dropna(subset=needed)
    dropped = before - len(df)
    if dropped:
        print(f"\nDropped {dropped} rows with nulls in features/target")

    print(f"\nTraining on {len(df)} complete rows across {df['season'].nunique()} seasons")

    X = df[FEATURES].to_numpy()
    y = df[TARGET].to_numpy()
    groups = df["season"].to_numpy()

    # Standardise features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Leave-one-year-out CV to select best alpha
    logo = LeaveOneGroupOut()
    alphas_r2 = {a: [] for a in ALPHAS}

    for alpha in ALPHAS:
        for train_idx, test_idx in logo.split(X_scaled, y, groups):
            from sklearn.linear_model import Ridge
            model = Ridge(alpha=alpha)
            model.fit(X_scaled[train_idx], y[train_idx])
            y_pred = model.predict(X_scaled[test_idx])
            # R² per fold
            ss_res = np.sum((y[test_idx] - y_pred) ** 2)
            ss_tot = np.sum((y[test_idx] - y[test_idx].mean()) ** 2)
            r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0
            alphas_r2[alpha].append(r2)

    print("\nLeave-one-year-out R² by alpha:")
    best_alpha = None
    best_mean = -np.inf
    for alpha in ALPHAS:
        r2s = alphas_r2[alpha]
        mean_r2 = np.mean(r2s)
        std_r2 = np.std(r2s)
        fold_str = "  ".join(f"{r:.3f}" for r in r2s)
        print(f"  alpha={alpha:>6}: mean={mean_r2:+.3f}  std={std_r2:.3f}  folds=[{fold_str}]")
        if mean_r2 > best_mean:
            best_mean = mean_r2
            best_alpha = alpha
            best_std = std_r2

    print(f"\nBest alpha: {best_alpha}  (mean R²={best_mean:+.3f}, std={best_std:.3f})")

    # Refit on all data with best alpha
    from sklearn.linear_model import Ridge
    final_model = Ridge(alpha=best_alpha)
    final_model.fit(X_scaled, y)

    # Convert standardised coefficients → original scale
    coef_orig = final_model.coef_ / scaler.scale_
    intercept_orig = final_model.intercept_ - np.dot(coef_orig, scaler.mean_)

    print("\nFeature coefficients (original scale, sorted by |coef|):")
    coef_pairs = sorted(zip(FEATURES, coef_orig), key=lambda x: abs(x[1]), reverse=True)
    for feat, coef in coef_pairs:
        print(f"  {feat:<25} {coef:+.4f}")
    print(f"  {'intercept':<25} {intercept_orig:+.4f}")

    # Save dg_pred_win_pct distribution stats so the simulator can normalize
    # dg_overall_skill (in SG units) to the same scale at prediction time.
    pred_win_mean = float(df["dg_pred_win_pct"].mean())
    pred_win_std  = float(df["dg_pred_win_pct"].std())

    # Save weights
    weights = {
        "intercept": float(intercept_orig),
        "coefficients": {feat: float(coef) for feat, coef in zip(FEATURES, coef_orig)},
        "model_alpha": float(best_alpha),
        "cv_r2_mean": float(best_mean),
        "cv_r2_std": float(best_std),
        "n_training_samples": int(len(df)),
        "training_seasons": sorted(df["season"].unique().tolist()),
        "features": FEATURES,
        "dg_pred_win_pct_mean": pred_win_mean,
        "dg_pred_win_pct_std":  pred_win_std,
    }

    out_path = Path(__file__).parent / "regression_weights.json"
    with open(out_path, "w") as f:
        json.dump(weights, f, indent=2)

    print(f"\nWeights saved → {out_path}")


if __name__ == "__main__":
    main()
