# pipeline/rare_species_map/duckdb_utils.py

from __future__ import annotations

import duckdb

from rare_species_map.config import PIPELINE_ROOT


def get_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()

    con.execute("PRAGMA threads=8")
    con.execute("PRAGMA memory_limit='16GB'")

    extension_dir = PIPELINE_ROOT / ".duckdb" / "extensions"
    extension_dir.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET extension_directory='{extension_dir.as_posix()}'")

    con.execute("INSTALL h3 FROM community")
    con.execute("LOAD h3")

    return con
