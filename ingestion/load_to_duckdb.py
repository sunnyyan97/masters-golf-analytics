from pathlib import Path

import duckdb
import pandas as pd
from dotenv import load_dotenv

DB_PATH = Path(__file__).parent.parent / "data" / "masters.duckdb"
COVID_YEAR = 2020


def get_connection(db_path=None) -> duckdb.DuckDBPyConnection:
    load_dotenv()
    conn = duckdb.connect(str(db_path or DB_PATH))
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    return conn


def _write_table(conn, table: str, records: list[dict]) -> int:
    """Replace raw.{table} entirely from records. Returns row count."""
    if not records:
        return 0
    df = pd.DataFrame(records)
    # Object-dtype columns can contain mixed types (e.g. "MC"/"WD" strings and
    # integer positions) that DuckDB can't unify. Cast them all to string,
    # keeping NaN as NULL.
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].where(df[col].isna(), df[col].astype(str))
    conn.execute(f"CREATE OR REPLACE TABLE raw.{table} AS SELECT * FROM df")
    return len(df)


def load_player_list(conn, records: list[dict]) -> int:
    return _write_table(conn, "player_list", records)


def load_dg_rankings(conn, records: list[dict]) -> int:
    return _write_table(conn, "dg_rankings", records)


def load_skill_ratings(conn, records: list[dict]) -> int:
    return _write_table(conn, "skill_ratings", records)


def load_approach_skill(conn, records: list[dict]) -> int:
    return _write_table(conn, "approach_skill", records)


def load_masters_field_2026(conn, records: list[dict]) -> int:
    return _write_table(conn, "masters_field_2026", records)


def load_masters_rounds(conn, records: list[dict]) -> int:
    """Records must already have is_covid_year injected before calling."""
    return _write_table(conn, "masters_rounds", records)


def load_pred_archive(conn, records: list[dict]) -> int:
    return _write_table(conn, "pred_archive", records)


def load_player_decompositions(conn, records: list[dict]) -> int:
    return _write_table(conn, "player_decompositions", records)
