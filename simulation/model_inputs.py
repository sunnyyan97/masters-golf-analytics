import pandas as pd

from ingestion.load_to_duckdb import get_connection


def load_inputs(db_path=None) -> pd.DataFrame:
    """Load mart_player_model_inputs from DuckDB. Returns one row per field player."""
    conn = get_connection(db_path)
    df = conn.execute("SELECT * FROM main.mart_player_model_inputs").df()
    conn.close()
    if df.empty:
        raise RuntimeError(
            "mart_player_model_inputs is empty — run "
            "`cd dbt && dbt build --select +mart_player_model_inputs` first."
        )
    return df
