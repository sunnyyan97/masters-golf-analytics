"""
Comprehensive pipeline validation for Masters Golf Analytics — Phases 1–4.

Run as:
    python -m validation.validate

Prints PASS/FAIL for each check. Exits with code 1 if any check fails.
Re-run on Masters week after the final field refresh to confirm everything is clean.
"""

import importlib
import subprocess
import sys
from pathlib import Path

from ingestion.load_to_duckdb import get_connection

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_PASSED: list[str] = []
_FAILED: list[str] = []


def _check(name: str, condition: bool, detail: str = "") -> None:
    if condition:
        _PASSED.append(name)
        print(f"  PASS  {name}")
    else:
        _FAILED.append(name)
        suffix = f" — {detail}" if detail else ""
        print(f"  FAIL  {name}{suffix}")


def _section(title: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


def _scalar(conn, sql: str):
    return conn.execute(sql).fetchone()[0]


# ---------------------------------------------------------------------------
# Phase 1 — Environment
# ---------------------------------------------------------------------------

def validate_environment() -> None:
    _section("Phase 1 — Environment")

    # Python version
    v = sys.version_info
    _check("Python ≥ 3.12", v >= (3, 12), f"got {v.major}.{v.minor}")

    # Key packages importable
    for pkg in ["numpy", "pandas", "duckdb", "dbt", "streamlit", "dotenv", "requests"]:
        try:
            importlib.import_module(pkg)
            ok = True
        except ImportError:
            ok = False
        _check(f"Package importable: {pkg}", ok)

    # .env + API key
    env_path = Path("../.env") if not Path(".env").exists() else Path(".env")
    # Walk up to find .env relative to project root
    root = Path(__file__).resolve().parent.parent
    env_file = root / ".env"
    _check(".env file exists", env_file.exists(), f"looked at {env_file}")

    if env_file.exists():
        content = env_file.read_text()
        api_key_set = any(
            line.startswith("DATAGOLF_API_KEY=") and len(line.split("=", 1)[1].strip()) > 0
            for line in content.splitlines()
        )
        _check("DATAGOLF_API_KEY set in .env", api_key_set)

    # DuckDB file
    db_path = root / "data" / "masters.duckdb"
    _check("data/masters.duckdb exists", db_path.exists(), f"looked at {db_path}")


# ---------------------------------------------------------------------------
# Phase 2 — Raw tables (ingestion)
# ---------------------------------------------------------------------------

MASTERS_MUST_HAVE = ["Scheffler", "McIlroy", "Schauffele", "Rahm", "DeChambeau"]

RAW_TABLE_ROW_RANGES = {
    "player_list":          (1_000, None),
    "dg_rankings":          (400,   600),
    "skill_ratings":        (300,   600),
    "approach_skill":       (300,   600),
    "masters_field_2026":   (85,    100),   # Masters ~89–93; Valero ~130 → would FAIL
    "masters_rounds":       (1_500, None),
    "pred_archive":         (400,   None),
    "player_decompositions":(85,    150),
}


def validate_raw_tables(conn) -> None:
    _section("Phase 2 — Raw tables (ingestion)")


    # 5 — all 8 tables exist
    existing = {
        r[0] for r in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'raw'"
        ).fetchall()
    }
    for tbl in RAW_TABLE_ROW_RANGES:
        _check(f"raw.{tbl} exists", tbl in existing)

    # 6 — row counts
    for tbl, (lo, hi) in RAW_TABLE_ROW_RANGES.items():
        if tbl not in existing:
            continue
        n = _scalar(conn, f"SELECT COUNT(*) FROM raw.{tbl}")
        lo_ok = n >= lo
        hi_ok = (hi is None) or (n <= hi)
        detail = f"got {n:,}, expected {lo}–{hi if hi else '∞'}"
        _check(f"raw.{tbl} row count in range", lo_ok and hi_ok, detail)

    # 7 — Masters field: known players present
    if "masters_field_2026" in existing:
        names = {r[0] for r in conn.execute(
            "SELECT player_name FROM raw.masters_field_2026"
        ).fetchall()}
        for surname in MASTERS_MUST_HAVE:
            found = any(surname.lower() in n.lower() for n in names)
            _check(f"Masters field contains {surname}", found,
                   "wrong field loaded (Valero?)" if not found else "")

    # 8 — masters_rounds year coverage (2020 optional)
    if "masters_rounds" in existing:
        years_present = {r[0] for r in conn.execute(
            "SELECT DISTINCT year FROM raw.masters_rounds"
        ).fetchall()}
        for yr in [2019, 2021, 2022, 2023, 2024, 2025]:
            _check(f"masters_rounds has year {yr}", yr in years_present)
        # 2020 is optional but note it
        has_2020 = 2020 in years_present
        print(f"         (2020 COVID year present: {'yes' if has_2020 else 'no — excluded as expected'})")

    # 9 — COVID flag correctness
    if "masters_rounds" in existing:
        bad_flag = _scalar(conn,
            "SELECT COUNT(*) FROM raw.masters_rounds WHERE year = 2020 AND is_covid_year = FALSE"
        )
        _check("masters_rounds 2020 rows flagged is_covid_year=TRUE", bad_flag == 0,
               f"{bad_flag} rows with wrong flag")

        non_covid_bad = _scalar(conn,
            "SELECT COUNT(*) FROM raw.masters_rounds WHERE year != 2020 AND is_covid_year = TRUE"
        )
        _check("masters_rounds non-2020 rows have is_covid_year=FALSE", non_covid_bad == 0,
               f"{non_covid_bad} rows with wrong flag")

    # 10 — pred_archive year coverage (all 6 years needed for Phase 5)
    if "pred_archive" in existing:
        pa_years = {r[0] for r in conn.execute(
            "SELECT DISTINCT year FROM raw.pred_archive"
        ).fetchall()}
        for yr in [2020, 2021, 2022, 2023, 2024, 2025]:
            _check(f"pred_archive has year {yr}", yr in pa_years,
                   "required for Phase 5 back-test")

    # 11 — no null datagolf_id in any raw table
    id_col = {
        "player_list": "dg_id",
        "dg_rankings": "dg_id",
        "skill_ratings": "dg_id",
        "approach_skill": "dg_id",
        "masters_field_2026": "dg_id",
        "masters_rounds": "dg_id",
        "pred_archive": "dg_id",
        "player_decompositions": "dg_id",
    }
    for tbl, col in id_col.items():
        if tbl not in existing:
            continue
        nulls = _scalar(conn, f"SELECT COUNT(*) FROM raw.{tbl} WHERE {col} IS NULL")
        _check(f"raw.{tbl} no null {col}", nulls == 0, f"{nulls} null rows")


# ---------------------------------------------------------------------------
# Phase 3 — dbt models
# ---------------------------------------------------------------------------

def _run_dbt_test() -> None:
    # 12 — run dbt test (before DuckDB connection is opened to avoid write-lock conflict)
    dbt_bin = Path(sys.executable).parent / "dbt"
    dbt_dir = Path(__file__).resolve().parent.parent / "dbt"
    result = subprocess.run(
        [str(dbt_bin), "test", "--quiet"],
        cwd=str(dbt_dir),
        capture_output=True,
        text=True,
    )
    passed = result.returncode == 0
    summary = ""
    for line in (result.stdout + result.stderr).splitlines():
        if "Completed" in line or "PASS=" in line or "ERROR=" in line:
            summary = line.strip()
    _check("dbt test — all tests pass", passed, summary or result.stderr[:200])


def validate_dbt_data(conn) -> None:
    # 13 — mart_player_model_inputs row count
    mart_rows = _scalar(conn, "SELECT COUNT(*) FROM main.mart_player_model_inputs")
    _check("mart_player_model_inputs row count 85–100", 85 <= mart_rows <= 100,
           f"got {mart_rows} (Masters field should be ~89–93; if 130 → Valero loaded)")

    # 14 — no nulls on key columns
    for col in ["datagolf_id", "player_name", "augusta_mu", "player_sigma"]:
        nulls = _scalar(conn,
            f"SELECT COUNT(*) FROM main.mart_player_model_inputs WHERE {col} IS NULL"
        )
        _check(f"mart_player_model_inputs.{col} has no nulls", nulls == 0, f"{nulls} null rows")

    # 15 — mu range
    mu_min, mu_max = conn.execute(
        "SELECT MIN(augusta_mu), MAX(augusta_mu) FROM main.mart_player_model_inputs"
    ).fetchone()
    _check("augusta_mu within -5.0 to +5.0", -5.0 <= mu_min and mu_max <= 5.0,
           f"range: {mu_min:.3f} to {mu_max:.3f}")

    # 16 — sigma range + debut-player fallback spot-check
    sigma_min, sigma_max = conn.execute(
        "SELECT MIN(player_sigma), MAX(player_sigma) FROM main.mart_player_model_inputs"
    ).fetchone()
    _check("player_sigma within 1.0 to 6.0", 1.0 <= sigma_min and sigma_max <= 6.0,
           f"range: {sigma_min:.3f} to {sigma_max:.3f}")

    # sigma=3.0 fallback applies to players with < 10 Augusta rounds (not just debuts)
    sigma_3_count = _scalar(conn,
        "SELECT COUNT(*) FROM main.mart_player_model_inputs WHERE player_sigma = 3.0"
    )
    cols = {r[0] for r in conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'mart_player_model_inputs'"
    ).fetchall()}
    if "augusta_rounds_played" in cols:
        under_10_count = _scalar(conn,
            "SELECT COUNT(*) FROM main.mart_player_model_inputs "
            "WHERE augusta_rounds_played < 10"
        )
        # sigma=3.0 ⊆ <10 rounds: all sigma=3.0 players must have <10 rounds,
        # but a player with <10 rounds may have a computed sigma from those few rounds
        bad = _scalar(conn,
            "SELECT COUNT(*) FROM main.mart_player_model_inputs "
            "WHERE player_sigma = 3.0 AND augusta_rounds_played >= 10"
        )
        _check("all sigma=3.0 players have < 10 Augusta rounds (correct fallback)",
               bad == 0,
               f"{bad} sigma=3.0 players have ≥10 rounds (unexpected)")
    else:
        print(f"         (sigma=3.0 rows: {sigma_3_count} — column augusta_rounds_played not in mart)")

    # 17 — Scheffler mu in top 5
    top5_mu = {r[0] for r in conn.execute(
        "SELECT player_name FROM main.mart_player_model_inputs "
        "ORDER BY augusta_mu DESC LIMIT 5"
    ).fetchall()}
    scheffler_in_top5 = any("Scheffler" in n for n in top5_mu)
    _check("Scheffler augusta_mu is top-5 (world #1 sanity)", scheffler_in_top5,
           f"top-5 mu players: {sorted(top5_mu)}")


# ---------------------------------------------------------------------------
# Phase 4 — Simulation results
# ---------------------------------------------------------------------------

TOP_PLAYERS = ["Scheffler", "McIlroy", "Schauffele", "Rahm", "DeChambeau"]


def validate_simulation(conn) -> None:
    _section("Phase 4 — Simulation results")

    # 18 — mart_simulation_results exists and row count matches mart_player_model_inputs
    sim_tables = {r[0] for r in conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchall()}
    sim_exists = "mart_simulation_results" in sim_tables
    _check("mart_simulation_results exists", sim_exists)
    if not sim_exists:
        print("         (skipping remaining Phase 4 checks)")
        return

    sim_rows = _scalar(conn, "SELECT COUNT(*) FROM main.mart_simulation_results")
    mart_rows = _scalar(conn, "SELECT COUNT(*) FROM main.mart_player_model_inputs")
    _check("mart_simulation_results row count matches mart_player_model_inputs",
           sim_rows == mart_rows, f"sim={sim_rows}, mart={mart_rows}")

    # 19 — win_pct sum
    win_sum = _scalar(conn, "SELECT SUM(win_pct) FROM main.mart_simulation_results")
    _check("win_pct sum within 0.98–1.02", 0.98 <= win_sum <= 1.02,
           f"sum = {win_sum:.4f}")

    # 20 — no extreme win_pct
    max_win = _scalar(conn, "SELECT MAX(win_pct) FROM main.mart_simulation_results")
    max_player = conn.execute(
        "SELECT player_name FROM main.mart_simulation_results ORDER BY win_pct DESC LIMIT 1"
    ).fetchone()[0]
    _check("no single player win_pct > 25%", max_win <= 0.25,
           f"{max_player} has {max_win*100:.1f}%")

    # 21 — Scheffler mc_pct < 15%
    scheffler_mc = conn.execute(
        "SELECT mc_pct FROM main.mart_simulation_results "
        "WHERE player_name LIKE '%Scheffler%'"
    ).fetchone()
    if scheffler_mc:
        _check("Scheffler mc_pct < 15%", scheffler_mc[0] < 0.15,
               f"got {scheffler_mc[0]*100:.1f}%")
    else:
        _check("Scheffler mc_pct < 15%", False, "Scheffler not found in results")

    # 22 — at least 3 of top 5 players in top 10 by win_pct
    top10_names = [r[0] for r in conn.execute(
        "SELECT player_name FROM main.mart_simulation_results "
        "ORDER BY win_pct DESC LIMIT 10"
    ).fetchall()]
    elite_in_top10 = sum(
        1 for surname in TOP_PLAYERS
        if any(surname.lower() in n.lower() for n in top10_names)
    )
    _check(f"≥3 of {TOP_PLAYERS} in top-10 by win_pct",
           elite_in_top10 >= 3,
           f"found {elite_in_top10}/5 — top-10: {top10_names[:5]}...")

    # 23 — average mc_pct between 25–55%
    avg_mc = _scalar(conn, "SELECT AVG(mc_pct) FROM main.mart_simulation_results")
    _check("average mc_pct between 25–55% (historical Masters ~40%)",
           0.25 <= avg_mc <= 0.55, f"avg mc_pct = {avg_mc*100:.1f}%")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 60)
    print("  Masters Golf Analytics — Phase 1–4 Validation")
    print("=" * 60)

    validate_environment()

    # Run dbt test BEFORE opening the DuckDB connection.
    # DuckDB allows only one read-write connection at a time; if we hold one open
    # while dbt test runs its own connection, dbt gets a write-lock error.
    _section("Phase 3 — dbt models")
    _run_dbt_test()

    conn = get_connection()
    try:
        validate_raw_tables(conn)
        validate_dbt_data(conn)
        validate_simulation(conn)
    finally:
        conn.close()

    # Summary
    total = len(_PASSED) + len(_FAILED)
    print(f"\n{'=' * 60}")
    if _FAILED:
        print(f"  {len(_FAILED)} of {total} checks FAILED:")
        for name in _FAILED:
            print(f"    ✗ {name}")
        print("\n  Fix the above before proceeding to Phase 5.")
        sys.exit(1)
    else:
        print(f"  All {total} checks passed. Pipeline is clean — ready for Phase 5.")
    print("=" * 60)


if __name__ == "__main__":
    main()
