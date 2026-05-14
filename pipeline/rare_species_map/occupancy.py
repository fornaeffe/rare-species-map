from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple

from rare_species_map.config import DATA_PROCESSED, H3_OCCUPANCY_RESOLUTION
from rare_species_map.duckdb_utils import get_connection


@dataclass(frozen=True, slots=True)
class SpeciesOccupancyConfig:
    input_path: Path = DATA_PROCESSED / "observations_filtered.parquet"
    output_path: Path = DATA_PROCESSED / "species_occupancy.parquet"
    h3_resolution: int = H3_OCCUPANCY_RESOLUTION


class SpeciesOccupancyStats(NamedTuple):
    n_species: int
    min_occupancy: int
    avg_occupancy: float
    max_occupancy: int
    min_rarity: float
    avg_rarity: float
    max_rarity: float


def build_species_occupancy_query(
    input_path: Path,
    output_path: Path,
    h3_resolution: int = H3_OCCUPANCY_RESOLUTION,
) -> str:
    return f"""
    COPY (
        WITH species_occupancy AS (
            SELECT
                speciesKey,
                min(species) AS species,
                COUNT(*) AS n_observations,
                COUNT(DISTINCT h3_res{h3_resolution}) AS occupancy
            FROM parquet_scan('{input_path.as_posix()}')
            WHERE
                speciesKey IS NOT NULL
                AND h3_res{h3_resolution} IS NOT NULL
            GROUP BY speciesKey
        )

        SELECT
            speciesKey,
            species,
            n_observations,
            occupancy,
            1.0 / sqrt(occupancy::DOUBLE) AS rarity
        FROM species_occupancy
    )
    TO '{output_path.as_posix()}'
    (
        FORMAT PARQUET,
        COMPRESSION ZSTD,
        ROW_GROUP_SIZE 100000
    )
    """


def compute_species_occupancy(config: SpeciesOccupancyConfig) -> None:
    input_path = config.input_path.resolve()
    output_path = config.output_path.resolve()

    if not input_path.exists():
        raise FileNotFoundError(f"Input parquet not found: {input_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    con = get_connection()
    try:
        con.execute(
            build_species_occupancy_query(
                input_path=input_path,
                output_path=output_path,
                h3_resolution=config.h3_resolution,
            )
        )
    finally:
        con.close()


def get_species_occupancy_stats(output_path: Path) -> SpeciesOccupancyStats:
    con = get_connection()
    try:
        stats = con.execute(
            f"""
            SELECT
                COUNT(*) AS n_species,
                MIN(occupancy) AS min_occupancy,
                AVG(occupancy) AS avg_occupancy,
                MAX(occupancy) AS max_occupancy,
                MIN(rarity) AS min_rarity,
                AVG(rarity) AS avg_rarity,
                MAX(rarity) AS max_rarity
            FROM parquet_scan('{output_path.resolve().as_posix()}')
            """
        ).fetchone()
    finally:
        con.close()

    if stats is None:
        raise ValueError(f"No statistics available for {output_path}")

    return SpeciesOccupancyStats(
        n_species=int(stats[0]),
        min_occupancy=int(stats[1]),
        avg_occupancy=float(stats[2]),
        max_occupancy=int(stats[3]),
        min_rarity=float(stats[4]),
        avg_rarity=float(stats[5]),
        max_rarity=float(stats[6]),
    )
