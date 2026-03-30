from ingestion.datagolf_client import DataGolfClient, MASTERS_EVENT_ID
from ingestion.load_to_duckdb import (
    COVID_YEAR,
    get_connection,
    load_approach_skill,
    load_dg_rankings,
    load_masters_field_2026,
    load_masters_rounds,
    load_player_decompositions,
    load_player_list,
    load_pred_archive,
    load_skill_ratings,
)

MASTERS_YEARS = (2019, 2020, 2021, 2022, 2023, 2024, 2025)
MASTERS_EVENT_ID_OVERRIDES = {2021: 536}  # "The Masters #2" in DataGolf's 2021 calendar
PRED_ARCHIVE_YEARS = range(2020, 2026)  # 6 years for back-testing


def main():
    client = DataGolfClient()
    conn = get_connection()

    # --- Static / current snapshots ---
    print("Fetching player list...", end=" ", flush=True)
    n = load_player_list(conn, client.get_player_list())
    print(f"{n:,} rows → raw.player_list")

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
    field_rows = field_resp.get("field", [])
    n = load_masters_field_2026(conn, field_rows)
    print(f"{n:,} rows → raw.masters_field_2026")

    # --- Historical Masters rounds (with is_covid_year flag) ---
    # Flatten per-round nested dicts: each scores row has round_1..round_4 as dicts
    # containing score, sg_total, sg_ott, sg_app, sg_arg, sg_putt, driving_acc, etc.
    print("\nFetching Masters rounds (2019–2025):")
    all_rounds = []
    for year in MASTERS_YEARS:
        event_id = MASTERS_EVENT_ID_OVERRIDES.get(year, MASTERS_EVENT_ID)
        resp = client.get_historical_rounds(event_id, year)
        rows = resp.get("scores", [])
        year_count = 0
        for row in rows:
            for round_num in [1, 2, 3, 4]:
                rnd = row.get(f"round_{round_num}")
                if rnd is None:
                    continue
                flat_row = {
                    "dg_id": row["dg_id"],
                    "player_name": row["player_name"],
                    "fin_text": row.get("fin_text"),
                    "year": year,
                    "is_covid_year": year == COVID_YEAR,
                    "round_num": round_num,
                    **rnd,
                }
                all_rounds.append(flat_row)
                year_count += 1
        print(f"  {year}: {year_count} round rows")
    n = load_masters_rounds(conn, all_rounds)
    print(f"  → raw.masters_rounds: {n:,} rows total")

    # --- Pre-tournament prediction archive (back-testing) ---
    print("\nFetching prediction archive (2020–2025):")
    all_preds = []
    for year in PRED_ARCHIVE_YEARS:
        event_id = MASTERS_EVENT_ID_OVERRIDES.get(year, MASTERS_EVENT_ID)
        resp = client.get_pre_tournament_archive(event_id, year)
        rows = resp.get("baseline_history_fit", [])
        for row in rows:
            row["year"] = year
        all_preds.extend(rows)
        print(f"  {year}: {len(rows)} rows")
    n = load_pred_archive(conn, all_preds)
    print(f"  → raw.pred_archive: {n:,} rows total")

    # --- Player decompositions (current event — general momentum signal) ---
    print("\nFetching player decompositions (current event):", end=" ", flush=True)
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

    conn.close()
    print("\nIngestion complete.")


if __name__ == "__main__":
    main()
