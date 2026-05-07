# pipeline/scripts/01_filter_to_parquet.py

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PIPELINE_ROOT = Path(__file__).resolve().parents[1]
if str(PIPELINE_ROOT) not in sys.path:
    sys.path.insert(0, str(PIPELINE_ROOT))

from rare_species_map.config import (
    DATA_PROCESSED,
    H3_OCCUPANCY_RESOLUTION,
    H3_VISUALIZATION_RESOLUTION,
    MAX_COORDINATE_UNCERTAINTY,
    DEFAULT_COUNTRY,
)
from rare_species_map.duckdb_utils import get_connection


CSV_COLUMNS = {
    "gbifID": "BIGINT",
    "datasetKey": "VARCHAR",
    "occurrenceID": "VARCHAR",
    "kingdom": "VARCHAR",
    "phylum": "VARCHAR",
    "class": "VARCHAR",
    "order": "VARCHAR",
    "family": "VARCHAR",
    "genus": "VARCHAR",
    "species": "VARCHAR",
    "infraspecificEpithet": "VARCHAR",
    "taxonRank": "VARCHAR",
    "scientificName": "VARCHAR",
    "verbatimScientificName": "VARCHAR",
    "verbatimScientificNameAuthorship": "VARCHAR",
    "countryCode": "VARCHAR",
    "locality": "VARCHAR",
    "stateProvince": "VARCHAR",
    "occurrenceStatus": "VARCHAR",
    "individualCount": "BIGINT",
    "publishingOrgKey": "VARCHAR",
    "decimalLatitude": "DOUBLE",
    "decimalLongitude": "DOUBLE",
    "coordinateUncertaintyInMeters": "DOUBLE",
    "coordinatePrecision": "DOUBLE",
    "elevation": "DOUBLE",
    "elevationAccuracy": "DOUBLE",
    "depth": "DOUBLE",
    "depthAccuracy": "DOUBLE",
    "eventDate": "VARCHAR",
    "day": "BIGINT",
    "month": "BIGINT",
    "year": "BIGINT",
    "taxonKey": "BIGINT",
    "speciesKey": "BIGINT",
    "basisOfRecord": "VARCHAR",
    "institutionCode": "VARCHAR",
    "collectionCode": "VARCHAR",
    "catalogNumber": "VARCHAR",
    "recordNumber": "VARCHAR",
    "identifiedBy": "VARCHAR",
    "dateIdentified": "VARCHAR",
    "license": "VARCHAR",
    "rightsHolder": "VARCHAR",
    "recordedBy": "VARCHAR",
    "typeStatus": "VARCHAR",
    "establishmentMeans": "VARCHAR",
    "lastInterpreted": "VARCHAR",
    "mediaType": "VARCHAR",
    "issue": "VARCHAR",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Filter GBIF TSV and export optimized Parquet"
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Path to GBIF TSV file",
    )

    parser.add_argument(
        "--output",
        default=str(DATA_PROCESSED / "observations_filtered.parquet"),
        help="Output parquet path",
    )

    parser.add_argument(
        "--country",
        default=DEFAULT_COUNTRY,
        help=f"Country filter (default: {DEFAULT_COUNTRY})",
    )

    parser.add_argument(
        "--uncertainty",
        type=float,
        default=MAX_COORDINATE_UNCERTAINTY,
        help="Maximum coordinate uncertainty in meters",
    )

    parser.add_argument(
        "--encoding",
        default="auto",
        help="Input text encoding (default: auto)",
    )

    return parser.parse_args()


def detect_encoding(input_path: Path, requested_encoding: str) -> str:
    if requested_encoding != "auto":
        return requested_encoding

    with input_path.open("rb") as file:
        first_bytes = file.read(4)

    if first_bytes.startswith(b"\xff\xfe") or first_bytes.startswith(b"\xfe\xff"):
        return "utf-16"

    if b"\x00" in first_bytes:
        return "utf-16"

    return "utf-8"


def build_query(
    input_path: Path,
    output_path: Path,
    country: str,
    uncertainty: float,
    encoding: str,
) -> str:
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
            occurrenceStatus,
            basisOfRecord,

            eventDate,
            year,
            countryCode,
            issue,

            h3_latlng_to_cell(
                decimalLatitude,
                decimalLongitude,
                {H3_OCCUPANCY_RESOLUTION}
            ) AS h3_res7,

            h3_latlng_to_cell(
                decimalLatitude,
                decimalLongitude,
                {H3_VISUALIZATION_RESOLUTION}
            ) AS h3_res8

        FROM read_csv(
            '{input_path.as_posix()}',
            delim='\\t',
            header=true,
            columns={CSV_COLUMNS},
            auto_detect=false,
            encoding='{encoding}',
            strict_mode=false,
            ignore_errors=true
        )

        WHERE
            speciesKey IS NOT NULL

            AND taxonRank = 'SPECIES'

            AND decimalLatitude IS NOT NULL
            AND decimalLongitude IS NOT NULL

            AND decimalLatitude BETWEEN -90 AND 90
            AND decimalLongitude BETWEEN -180 AND 180

            AND occurrenceStatus = 'PRESENT'

            AND basisOfRecord = 'HUMAN_OBSERVATION'

            AND countryCode = '{country}'

            AND (
                coordinateUncertaintyInMeters IS NULL
                OR coordinateUncertaintyInMeters <= {uncertainty}
            )
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
        COUNT(*) AS n_observations,
        COUNT(DISTINCT speciesKey) AS n_species,
        COUNT(DISTINCT h3_res8) AS n_cells
    FROM parquet_scan('{output_path.as_posix()}')
    """

    stats = con.execute(stats_query).fetchone()

    print()
    print("=== Dataset statistics ===")
    print(f"Observations : {stats[0]:,}")
    print(f"Species      : {stats[1]:,}")
    print(f"H3 cells     : {stats[2]:,}")

    con.close()


def main() -> None:
    args = parse_args()

    input_path = Path(args.input).resolve()
    output_path = Path(args.output).resolve()
    encoding = detect_encoding(input_path, args.encoding)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    print()
    print("=== GBIF Filter Pipeline ===")
    print(f"Input  : {input_path}")
    print(f"Output : {output_path}")
    print(f"Encoding: {encoding}")
    print()

    con = get_connection()

    query = build_query(
        input_path=input_path,
        output_path=output_path,
        country=args.country,
        uncertainty=args.uncertainty,
        encoding=encoding,
    )

    print("Running DuckDB query...")
    print()

    con.execute(query)

    con.close()

    print("Parquet generation completed.")

    print_stats(output_path)


if __name__ == "__main__":
    main()
