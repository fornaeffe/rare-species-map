from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple

from rare_species_map.config import (
    DATA_PROCESSED,
    DATA_RAW,
    H3_VISUALIZATION_RESOLUTIONS,
)
from rare_species_map.duckdb_utils import get_connection
from rare_species_map.schemas import CSV_COLUMNS


@dataclass(frozen=True, slots=True)
class FilterObservationsConfig:
    input_path: Path = DATA_RAW
    output_path: Path = DATA_PROCESSED / "observations_filtered.parquet"
    encoding: str = "auto"
    country_code: str | None = None
    h3_resolutions: tuple[int, ...] = tuple(H3_VISUALIZATION_RESOLUTIONS)


class FilteredObservationStats(NamedTuple):
    n_observations: int
    n_species: int


def detect_encoding(input_path: Path, requested_encoding: str = "auto") -> str:
    if requested_encoding != "auto":
        return requested_encoding

    with input_path.open("rb") as file:
        first_bytes = file.read(4)

    if first_bytes.startswith(b"\xff\xfe") or first_bytes.startswith(b"\xfe\xff"):
        return "utf-16"

    if b"\x00" in first_bytes:
        return "utf-16"

    return "utf-8"


def build_h3_columns(resolutions: tuple[int, ...]) -> str:
    return ",\n".join(
        f"""
        h3_latlng_to_cell(
            decimalLatitude,
            decimalLongitude,
            {resolution}
        ) AS h3_res{resolution}
        """.strip()
        for resolution in resolutions
    )


def sql_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_optional_filters(country_code: str | None) -> str:
    if country_code is None:
        return ""

    normalized_country_code = country_code.strip().upper()
    if not normalized_country_code:
        return ""

    return f"\n            AND countryCode = {sql_string_literal(normalized_country_code)}"


def build_filter_query(
    input_path: Path,
    output_path: Path,
    encoding: str,
    country_code: str | None = None,
    h3_resolutions: tuple[int, ...] = tuple(H3_VISUALIZATION_RESOLUTIONS),
) -> str:
    h3_columns = build_h3_columns(h3_resolutions)
    optional_filters = build_optional_filters(country_code)

    return f"""
    COPY (
        SELECT
            gbifID,
            speciesKey,
            species,

            decimalLatitude,
            decimalLongitude,
            coordinateUncertaintyInMeters,

            taxonRank,
            recordedBy,

            {h3_columns}

        FROM read_csv(
            '{input_path.as_posix()}',
            delim='\\t',
            header=true,
            columns={dict(CSV_COLUMNS)},
            auto_detect=false,
            encoding='{encoding}',
            strict_mode=false,
            ignore_errors=true
        )

        WHERE
            speciesKey IS NOT NULL

            AND recordedBy IS NOT NULL

            AND taxonRank = 'SPECIES'

            AND decimalLatitude IS NOT NULL
            AND decimalLongitude IS NOT NULL

            AND decimalLatitude BETWEEN -90 AND 90
            AND decimalLongitude BETWEEN -180 AND 180
            {optional_filters}
    )
    TO '{output_path.as_posix()}'
    (
        FORMAT PARQUET,
        COMPRESSION ZSTD,
        ROW_GROUP_SIZE 100000
    )
    """


def filter_observations_to_parquet(config: FilterObservationsConfig) -> None:
    input_path = config.input_path.resolve()
    output_path = config.output_path.resolve()
    encoding = detect_encoding(input_path, config.encoding)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    con = get_connection()
    try:
        con.execute(
            build_filter_query(
                input_path=input_path,
                output_path=output_path,
                encoding=encoding,
                country_code=config.country_code,
                h3_resolutions=config.h3_resolutions,
            )
        )
    finally:
        con.close()


def get_filtered_observation_stats(output_path: Path) -> FilteredObservationStats:
    con = get_connection()
    try:
        stats = con.execute(
            f"""
            SELECT
                COUNT(*) AS n_observations,
                COUNT(DISTINCT speciesKey) AS n_species
            FROM parquet_scan('{output_path.resolve().as_posix()}')
            """
        ).fetchone()
    finally:
        con.close()

    if stats is None:
        raise ValueError(f"No statistics available for {output_path}")

    return FilteredObservationStats(
        n_observations=int(stats[0]),
        n_species=int(stats[1]),
    )
