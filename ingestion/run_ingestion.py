from ingestion.datagolf_client import DataGolfClient, MASTERS_EVENT_ID
from ingestion.load_to_duckdb import (
    COVID_YEAR,
    get_connection,
    load_approach_skill,
    load_dg_rankings,
    load_masters_field_2026,
    load_masters_rounds,
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
    print("\nFetching Masters rounds (2019–2025):")
    all_rounds = []
    for year in MASTERS_YEARS:
        event_id = MASTERS_EVENT_ID_OVERRIDES.get(year, MASTERS_EVENT_ID)
        resp = client.get_historical_rounds(event_id, year)
        rows = resp.get("scores", [])
        for row in rows:
            row["is_covid_year"] = year == COVID_YEAR
        all_rounds.extend(rows)
        print(f"  {year}: {len(rows)} rows")
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

    conn.close()
    print("\nIngestion complete.")


if __name__ == "__main__":
    main()
