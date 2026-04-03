"""
Ingest current 2026 DataGolf pre-tournament predictions.

Usage:
    python -m ingestion.ingest_current_dg_predictions
"""
from ingestion.datagolf_client import DataGolfClient
from ingestion.load_to_duckdb import get_connection, load_current_dg_predictions

SEASON = 2026


def main():
    client = DataGolfClient()
    conn = get_connection()

    print("Fetching current DG pre-tournament predictions...", end=" ", flush=True)
    resp = client.get_pre_tournament_predictions(tour="pga", odds_format="percent")

    # Live endpoint returns the same structure as the archive endpoint
    rows = resp.get("baseline_history_fit", [])
    if not rows:
        # Fallback key used by some endpoint versions
        rows = resp.get("baseline", [])

    if not rows:
        print(f"\nERROR: no player rows found. Response keys: {list(resp.keys())}")
        conn.close()
        return

    records = []
    for r in rows:
        records.append({
            "datagolf_id": r["dg_id"],
            "player_name":  r["player_name"],
            "win_pct":      r.get("win"),
            "top5_pct":     r.get("top_5"),
            "top10_pct":    r.get("top_10"),
            "mc_pct":       r.get("make_cut"),
            "season":       SEASON,
        })

    n = load_current_dg_predictions(conn, records)
    print(f"{n:,} rows → raw.current_dg_predictions")

    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
