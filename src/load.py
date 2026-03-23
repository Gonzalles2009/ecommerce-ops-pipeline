"""Load: write transformed DataFrames into DuckDB warehouse."""

from pathlib import Path

import duckdb
import pandas as pd

DB_PATH = Path(__file__).parent.parent / "data" / "warehouse.duckdb"


def get_connection() -> duckdb.DuckDBPyConnection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH))


def load_table(con: duckdb.DuckDBPyConnection, name: str, df: pd.DataFrame) -> int:
    con.execute(f"DROP TABLE IF EXISTS {name}")
    con.execute(f"CREATE TABLE {name} AS SELECT * FROM df")
    count = con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
    return count


def load_all(tables: dict[str, pd.DataFrame]) -> dict[str, int]:
    con = get_connection()
    counts = {}
    for name, df in tables.items():
        counts[name] = load_table(con, name, df)
    con.close()
    return counts
