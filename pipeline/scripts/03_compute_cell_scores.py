# pipeline/scripts/03_compute_cell_scores.py

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PIPELINE_ROOT = Path(__file__).resolve().parents[1]
if str(PIPELINE_ROOT) not in sys.path:
    sys.path.insert(0, str(PIPELINE_ROOT))

from rare_species_map.config import DATA_PROCESSED, H3_VISUALIZATION_RESOLUTION
from rare_species_map.duckdb_utils import get_connection


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute H3 cell rarity residual scores from observations and species rarity"
    )

    parser.add_argument(
        "--observations",
        default=str(DATA_PROCESSED / "observations_filtered.parquet"),
        help="Input filtered observations parquet path",
    )

    parser.add_argument(
        "--species-occupancy",
        default=str(DATA_PROCESSED / "species_occupancy.parquet"),
        help="Input species occupancy parquet path",
    )

    parser.add_argument(
        "--output",
        default=str(DATA_PROCESSED / "cell_scores.parquet"),
        help="Output H3 cell scores parquet path",
    )

    return parser.parse_args()


def build_query(
    observations_path: Path,
    species_occupancy_path: Path,
    output_path: Path,
) -> str:
    return f"""
    COPY (
        WITH observations AS (
            SELECT
                h3_res8,
                speciesKey
            FROM parquet_scan('{observations_path.as_posix()}')
            WHERE
                h3_res8 IS NOT NULL
                AND speciesKey IS NOT NULL
        ),

        observations_by_cell AS (
            SELECT
                h3_res8,
                COUNT(*) AS count_observations
            FROM observations
            GROUP BY h3_res8
        ),

        species_by_cell AS (
            SELECT DISTINCT
                h3_res8,
                speciesKey
            FROM observations
        ),

        rarity_by_cell AS (
            SELECT
                species_by_cell.h3_res8,
                COUNT(*) AS count_species,
                SUM(species_occupancy.rarity) AS sum_rarity
            FROM species_by_cell
            INNER JOIN parquet_scan('{species_occupancy_path.as_posix()}') AS species_occupancy
                ON species_by_cell.speciesKey = species_occupancy.speciesKey
            GROUP BY species_by_cell.h3_res8
        ),

        cell_metrics AS (
            SELECT
                observations_by_cell.h3_res8,
                observations_by_cell.count_observations,
                rarity_by_cell.count_species,
                rarity_by_cell.sum_rarity,
                ln(observations_by_cell.count_observations::DOUBLE) AS log_count_observations,
                ln(rarity_by_cell.sum_rarity) AS log_sum_rarity
            FROM observations_by_cell
            INNER JOIN rarity_by_cell
                ON observations_by_cell.h3_res8 = rarity_by_cell.h3_res8
            WHERE
                observations_by_cell.count_observations > 0
                AND rarity_by_cell.sum_rarity > 0
        ),

        regression AS (
            SELECT
                regr_intercept(log_sum_rarity, log_count_observations) AS intercept,
                regr_slope(log_sum_rarity, log_count_observations) AS slope,
                corr(log_sum_rarity, log_count_observations) AS correlation,
                COUNT(*) AS regression_n_cells
            FROM cell_metrics
        )

        SELECT
            cell_metrics.h3_res8,
            cell_metrics.count_observations,
            cell_metrics.count_species,
            cell_metrics.sum_rarity,
            cell_metrics.log_count_observations,
            cell_metrics.log_sum_rarity,
            regression.intercept AS regression_intercept,
            regression.slope AS regression_slope,
            regression.correlation AS regression_correlation,
            regression.regression_n_cells,
            (
                regression.intercept
                + regression.slope * cell_metrics.log_count_observations
            ) AS expected_log_sum_rarity,
            (
                cell_metrics.log_sum_rarity
                - (
                    regression.intercept
                    + regression.slope * cell_metrics.log_count_observations
                )
            ) AS rarity_score
        FROM cell_metrics
        CROSS JOIN regression
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
        COUNT(*) AS n_cells,
        MIN(count_observations) AS min_observations,
        AVG(count_observations) AS avg_observations,
        MAX(count_observations) AS max_observations,
        MIN(sum_rarity) AS min_sum_rarity,
        AVG(sum_rarity) AS avg_sum_rarity,
        MAX(sum_rarity) AS max_sum_rarity,
        MIN(rarity_score) AS min_score,
        AVG(rarity_score) AS avg_score,
        MAX(rarity_score) AS max_score,
        any_value(regression_intercept) AS regression_intercept,
        any_value(regression_slope) AS regression_slope,
        any_value(regression_correlation) AS regression_correlation,
        any_value(regression_n_cells) AS regression_n_cells
    FROM parquet_scan('{output_path.as_posix()}')
    """

    stats = con.execute(stats_query).fetchone()

    print()
    print("=== H3 cell score statistics ===")
    print(f"H3 resolution         : {H3_VISUALIZATION_RESOLUTION}")
    print(f"Cells                 : {stats[0]:,}")
    print(f"Min observations      : {stats[1]:,}")
    print(f"Avg observations      : {stats[2]:,.2f}")
    print(f"Max observations      : {stats[3]:,}")
    print(f"Min sum rarity        : {stats[4]:.8f}")
    print(f"Avg sum rarity        : {stats[5]:.8f}")
    print(f"Max sum rarity        : {stats[6]:.8f}")
    print(f"Min rarity score      : {stats[7]:.8f}")
    print(f"Avg rarity score      : {stats[8]:.8f}")
    print(f"Max rarity score      : {stats[9]:.8f}")
    print()
    print("=== Log-log regression ===")
    print("Model                 : log(sum_rarity) ~ log(count_observations)")
    print(f"Intercept             : {stats[10]:.8f}")
    print(f"Slope                 : {stats[11]:.8f}")
    print(f"Correlation           : {stats[12]:.8f}")
    print(f"Regression cells      : {stats[13]:,}")

    con.close()


def main() -> None:
    args = parse_args()

    observations_path = Path(args.observations).resolve()
    species_occupancy_path = Path(args.species_occupancy).resolve()
    output_path = Path(args.output).resolve()

    if not observations_path.exists():
        raise FileNotFoundError(f"Observations parquet not found: {observations_path}")

    if not species_occupancy_path.exists():
        raise FileNotFoundError(
            f"Species occupancy parquet not found: {species_occupancy_path}"
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)

    print()
    print("=== H3 Cell Score Pipeline ===")
    print(f"Observations      : {observations_path}")
    print(f"Species occupancy : {species_occupancy_path}")
    print(f"Output            : {output_path}")
    print()

    con = get_connection()
    query = build_query(
        observations_path=observations_path,
        species_occupancy_path=species_occupancy_path,
        output_path=output_path,
    )

    print("Running DuckDB query...")
    print()

    con.execute(query)
    con.close()

    print("H3 cell score generation completed.")

    print_stats(output_path)


if __name__ == "__main__":
    main()
