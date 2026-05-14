from __future__ import annotations

import duckdb

from rare_species_map.config import PIPELINE_ROOT


def get_connection(
    threads: int = 8,
    memory_limit: str = "16GB",
) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()

    con.execute(f"PRAGMA threads={threads}")
    con.execute(f"PRAGMA memory_limit='{memory_limit}'")

    extension_dir = PIPELINE_ROOT / ".duckdb" / "extensions"
    extension_dir.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET extension_directory='{extension_dir.as_posix()}'")

    con.execute("INSTALL h3 FROM community")
    con.execute("LOAD h3")

    return con
