"""
refresh_field.py — Refresh the 5 current-state tables without touching historical data.

Run this when the Masters field changes (e.g., a new winner earns an invitation).
Refreshes: masters_field_2026, skill_ratings, dg_rankings, approach_skill,
           player_decompositions.
Skips:     masters_rounds (7-year historical — never changes)
           pred_archive   (archived predictions — never changes)

After running, rebuild the mart:
    cd dbt && dbt build --select +mart_player_model_inputs
"""

from ingestion.datagolf_client import DataGolfClient
from ingestion.load_to_duckdb import (
    get_connection,
    load_approach_skill,
    load_dg_rankings,
    load_masters_field_2026,
    load_player_decompositions,
    load_skill_ratings,
)

MASTERS_KEYWORDS = ("masters", "augusta")


def _is_masters(event_name: str) -> bool:
    return any(kw in event_name.lower() for kw in MASTERS_KEYWORDS)


def _snapshot_field(conn) -> dict[int, str]:
    """Return {datagolf_id: player_name} for the current field table."""
    try:
        rows = conn.execute(
            "SELECT dg_id, player_name FROM raw.masters_field_2026"
        ).fetchall()
        return {int(r[0]): r[1] for r in rows}
    except Exception:
        return {}


def _print_diff(before: dict[int, str], after: dict[int, str]) -> None:
    added = {k: v for k, v in after.items() if k not in before}
    removed = {k: v for k, v in before.items() if k not in after}

    if not added and not removed:
        print("\nField diff: no changes.")
        return

    if added:
        print(f"\nField diff — ADDED ({len(added)}):")
        for dg_id, name in sorted(added.items(), key=lambda x: x[1]):
            print(f"  + {name} (datagolf_id={dg_id})")
    if removed:
        print(f"\nField diff — REMOVED ({len(removed)}):")
        for dg_id, name in sorted(removed.items(), key=lambda x: x[1]):
            print(f"  - {name} (datagolf_id={dg_id})")


def main():
    client = DataGolfClient()
    conn = get_connection()

    # Snapshot field before overwriting
    before = _snapshot_field(conn)

    # --- Refresh current-state tables ---
    print("Fetching DG rankings...", end=" ", flush=True)
    n = load_dg_rankings(conn, client.get_dg_rankings()["rankings"])
    print(f"{n:,} rows → raw.dg_rankings")

    print("Fetching skill ratings...", end=" ", flush=True)
    n = load_skill_ratings(conn, client.get_skill_ratings()["players"])
    print(f"{n:,} rows → raw.skill_ratings")

    print("Fetching approach skill...", end=" ", flush=True)
    n = load_approach_skill(conn, client.get_approach_skill()["data"])
    print(f"{n:,} rows → raw.approach_skill")

    print("Fetching 2026 Masters field...", end=" ", flush=True)
    field_resp = client.get_field_updates(tour="pga")
    event_name = field_resp.get("event_name", "")
    if not _is_masters(event_name):
        print(f"\n  Current event: '{event_name}' — not the Masters. Trying upcoming field...")
        field_resp = client.get_upcoming_field(tour="pga")
        event_name = field_resp.get("event_name", "")
        if not _is_masters(event_name):
            print(f"  Upcoming event: '{event_name}' — Masters field not yet available.")
            print("  Skipping field update. Existing raw.masters_field_2026 unchanged.")
            field_rows = []
        else:
            print(f"  Found upcoming Masters field: '{event_name}'")
            field_rows = field_resp.get("field", [])
    else:
        field_rows = field_resp.get("field", [])
    if field_rows:
        n = load_masters_field_2026(conn, field_rows)
        print(f"{n:,} rows → raw.masters_field_2026 (event: {event_name})")
    else:
        print("0 rows loaded (field not yet available)")

    print("Fetching player decompositions (current event):", end=" ", flush=True)
    decomp_resp = client.get_player_decompositions(tour="pga")
    decomp_rows = [
        {
            "dg_id": p["dg_id"],
            "player_name": p["player_name"],
            "event_name": decomp_resp.get("event_name"),
            "timing_adjustment": p.get("timing_adjustment"),
            "baseline_pred": p.get("baseline_pred"),
            "final_pred": p.get("final_pred"),
        }
        for p in decomp_resp.get("players", [])
    ]
    n = load_player_decompositions(conn, decomp_rows)
    print(f"{n} rows → raw.player_decompositions")

    # Snapshot field after and print diff
    after_rows = conn.execute(
        "SELECT dg_id, player_name FROM raw.masters_field_2026"
    ).fetchall()
    after = {int(r[0]): r[1] for r in after_rows}
    _print_diff(before, after)

    conn.close()
    print("\nRefresh complete.")
    print("Next step: cd dbt && dbt build --select +mart_player_model_inputs")


if __name__ == "__main__":
    main()
