# pipeline/scripts/02_compute_species_occupancy.py

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PIPELINE_ROOT = Path(__file__).resolve().parents[1]
if str(PIPELINE_ROOT) not in sys.path:
    sys.path.insert(0, str(PIPELINE_ROOT))

from rare_species_map.config import DATA_PROCESSED, H3_OCCUPANCY_RESOLUTION
from rare_species_map.duckdb_utils import get_connection


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute species occupancy and rarity from filtered observations"
    )

    parser.add_argument(
        "--input",
        default=str(DATA_PROCESSED / "observations_filtered.parquet"),
        help="Input filtered observations parquet path",
    )

    parser.add_argument(
        "--output",
        default=str(DATA_PROCESSED / "species_occupancy.parquet"),
        help="Output species occupancy parquet path",
    )

    return parser.parse_args()


def build_query(input_path: Path, output_path: Path) -> str:
    return f"""
    COPY (
        WITH species_occupancy AS (
            SELECT
                speciesKey,
                min(species) AS species,
                COUNT(*) AS n_observations,
                COUNT(DISTINCT h3_resLow) AS occupancy
            FROM parquet_scan('{input_path.as_posix()}')
            WHERE
                speciesKey IS NOT NULL
                AND h3_resLow IS NOT NULL
            GROUP BY speciesKey
        )

        SELECT
            speciesKey,
            species,
            n_observations,
            occupancy,
            1.0 / (occupancy::DOUBLE)^0.25 AS rarity
        FROM species_occupancy
    )
    TO '{output_path.as_posix()}'
    (
        FORMAT PARQUET,
        COMPRESSION ZSTD,
        ROW_GROUP_SIZE 100000
    )
    """


def print_stats(output_path: Path) -> None:
    con = get_connection()

    stats_query = f"""
    SELECT
        COUNT(*) AS n_species,
        MIN(occupancy) AS min_occupancy,
        AVG(occupancy) AS avg_occupancy,
        MAX(occupancy) AS max_occupancy,
        MIN(rarity) AS min_rarity,
        AVG(rarity) AS avg_rarity,
        MAX(rarity) AS max_rarity
    FROM parquet_scan('{output_path.as_posix()}')
    """

    stats = con.execute(stats_query).fetchone()

    print()
    print("=== Species occupancy statistics ===")
    print(f"H3 resolution  : {H3_OCCUPANCY_RESOLUTION}")
    print(f"Species        : {stats[0]:,}")
    print(f"Min occupancy  : {stats[1]:,}")
    print(f"Avg occupancy  : {stats[2]:,.2f}")
    print(f"Max occupancy  : {stats[3]:,}")
    print(f"Min rarity     : {stats[4]:.8f}")
    print(f"Avg rarity     : {stats[5]:.8f}")
    print(f"Max rarity     : {stats[6]:.8f}")

    con.close()


def main() -> None:
    args = parse_args()

    input_path = Path(args.input).resolve()
    output_path = Path(args.output).resolve()

    if not input_path.exists():
        raise FileNotFoundError(f"Input parquet not found: {input_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    print()
    print("=== Species Occupancy Pipeline ===")
    print(f"Input  : {input_path}")
    print(f"Output : {output_path}")
    print()

    con = get_connection()
    query = build_query(input_path=input_path, output_path=output_path)

    print("Running DuckDB query...")
    print()

    con.execute(query)
    con.close()

    print("Species occupancy generation completed.")

    print_stats(output_path)


if __name__ == "__main__":
    main()
