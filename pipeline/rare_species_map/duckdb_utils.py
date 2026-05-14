from __future__ import annotations

import os

import duckdb

from rare_species_map.config import PIPELINE_ROOT


def get_default_thread_count() -> int:
    configured_threads = os.environ.get("RARE_SPECIES_DUCKDB_THREADS")
    if configured_threads is not None:
        return int(configured_threads)

    return os.cpu_count() or 8


def get_connection(
    threads: int | None = None,
    memory_limit: str = "16GB",
) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()

    thread_count = threads if threads is not None else get_default_thread_count()
    con.execute(f"PRAGMA threads={thread_count}")
    con.execute(f"PRAGMA memory_limit='{memory_limit}'")

    extension_dir = PIPELINE_ROOT / ".duckdb" / "extensions"
    extension_dir.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET extension_directory='{extension_dir.as_posix()}'")

    try:
        con.execute("LOAD h3")
    except duckdb.Error:
        con.execute("INSTALL h3 FROM community")
        con.execute("LOAD h3")

    return con
