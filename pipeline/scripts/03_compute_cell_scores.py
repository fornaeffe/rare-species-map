# pipeline/scripts/03_compute_cell_scores.py

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
import time

import numpy as np

PIPELINE_ROOT = Path(__file__).resolve().parents[1]
if str(PIPELINE_ROOT) not in sys.path:
    sys.path.insert(0, str(PIPELINE_ROOT))

from rare_species_map.config import DATA_PROCESSED, GENERATED_JSONS, H3_VISUALIZATION_RESOLUTIONS
from rare_species_map.duckdb_utils import get_connection


DEFAULT_DIAGNOSTICS_DIR = DATA_PROCESSED / "diagnostics" / "cell_scores"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute H3 cell rarity residual scores using GAM smooth spline"
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
        default=str(DATA_PROCESSED),
        help="Output H3 cell scores parquet path",
    )

    parser.add_argument(
        "--diagnostics-output-dir",
        default=str(DEFAULT_DIAGNOSTICS_DIR),
        help="Output directory for GAM diagnostic plots",
    )

    parser.add_argument(
        "--diagnostics-sample-size",
        type=int,
        default=200_000,
        help="Maximum number of H3 cells to sample for diagnostic plots",
    )

    parser.add_argument(
        "--no-diagnostics",
        action="store_true",
        help="Skip diagnostic plot generation",
    )

    return parser.parse_args()


def build_aggregation_query(
    res: int,
    observations_path: Path,
    species_occupancy_path: Path,
) -> str:
    """
    Build DuckDB query to aggregate cell statistics.

    Returns a query that selects all cells with their metrics.
    This is used to extract data for Python-based GAM fitting.
    """
    return f"""
    WITH valid_observers AS (
        SELECT recordedBy
        FROM parquet_scan('{observations_path.as_posix()}')
        WHERE recordedBy IS NOT NULL
        GROUP BY recordedBy
        HAVING MIN(h3_res{res}) != MAX(h3_res{res})
    ),
    
    observations AS (
        SELECT
            o.h3_res{res},
            o.speciesKey,
            o.recordedBy
        FROM parquet_scan('{observations_path.as_posix()}') o
        SEMI JOIN valid_observers v
            ON o.recordedBy = v.recordedBy
    ),

    observer_counts_by_cell AS (
        SELECT
            h3_res{res},
            recordedBy,
            COUNT(*) AS obs_by_observer
        FROM observations
        GROUP BY h3_res{res}, recordedBy
    ),

    cell_metrics AS (
        SELECT
            h3_res{res},
            COUNT(*) AS count_observations,
            COUNT(DISTINCT speciesKey) AS count_species,
            COUNT(DISTINCT recordedBy) AS count_observers
        FROM observations
        GROUP BY h3_res{res}
    ),

    regression_data AS (
        SELECT
            h3_res{res},
            ln(count_observations) AS log_count_observations,
            ln(count_species) AS log_count_species,
        FROM cell_metrics
    ),

    regression AS (
        SELECT
            regr_intercept(log_count_species, log_count_observations) AS intercept,
            regr_slope(log_count_species, log_count_observations) AS slope
        FROM regression_data
    ),

    shannon_by_cell AS (
        SELECT
            o.h3_res{res},
            -SUM( (o.obs_by_observer::DOUBLE / cm.count_observations) * LN(o.obs_by_observer::DOUBLE / cm.count_observations) ) AS shannon_H
        FROM observer_counts_by_cell o
        JOIN cell_metrics cm
            ON o.h3_res{res} = cm.h3_res{res}
        GROUP BY o.h3_res{res}
    ),

    neff_by_cell AS (
        SELECT
            h3_res{res},
            shannon_H,
            EXP(shannon_H) AS neff_observers
        FROM shannon_by_cell
    ),

    species_by_cell_and_observer AS (
        SELECT DISTINCT
            h3_res{res},
            speciesKey,
            recordedBy
        FROM observations
    ),

    mean_rarity_by_cell_and_observer AS (
        SELECT
            s.h3_res{res},
            s.recordedBy,
            AVG(so.rarity) AS mean_rarity
        FROM species_by_cell_and_observer s
        INNER JOIN parquet_scan('{species_occupancy_path.as_posix()}') so
            ON s.speciesKey = so.speciesKey
        GROUP BY s.h3_res{res}, s.recordedBy
    ),

    mean_rarity_by_observer AS (
        SELECT
            recordedBy,
            AVG(mean_rarity) AS mean_rarity
        FROM mean_rarity_by_cell_and_observer
        GROUP BY recordedBy
    ),

    residual_rarity_by_cell_and_observer AS (
        SELECT
            m.h3_res{res},
            m.recordedBy,
            m.mean_rarity - o.mean_rarity AS residual_rarity
        FROM mean_rarity_by_cell_and_observer m
        INNER JOIN mean_rarity_by_observer o
            ON m.recordedBy = o.recordedBy
    ),

    residual_rarity_by_cell AS (
        SELECT
            h3_res{res},
            AVG(residual_rarity) AS mean_residual_rarity
        FROM residual_rarity_by_cell_and_observer
        GROUP BY h3_res{res}
    )

    SELECT
        r.h3_res{res},
        r.mean_residual_rarity AS rarity_zscore,
        cm.count_species,
        cm.count_observations,
        cm.count_observers,
        ( 1 - EXP( - n.neff_observers / 4 ) ) * ( 1 - EXP( - cm.count_observations / 40 ) ) AS confidence_scores,
        ( rd.log_count_species - (rd.log_count_observations * reg.slope + reg.intercept) ) AS species_vs_observations
    FROM residual_rarity_by_cell r
    JOIN neff_by_cell n
        ON r.h3_res{res} = n.h3_res{res}
    JOIN cell_metrics cm
        ON r.h3_res{res} = cm.h3_res{res}
    JOIN regression_data rd
         ON r.h3_res{res} = rd.h3_res{res}
    CROSS JOIN regression reg
    ORDER BY r.h3_res{res}
    """


def fetch_cell_data(
    res: int,
    observations_path: Path,
    species_occupancy_path: Path,
) -> tuple[list[int], np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """
    Fetch aggregated cell statistics from DuckDB.

    Returns:
        (h3_indices, count_observations, count_species, sum_rarity, confidence_scores)
    """
    con = get_connection()
    query = build_aggregation_query(res, observations_path, species_occupancy_path)

    start = time.monotonic()
    result = con.execute(query).fetchall()
    elapsed = time.monotonic() - start

    print()
    print(f"  Query executed in {elapsed:.1f} seconds.")

    con.close()

    if not result:
        raise ValueError("No cells found in aggregation query")

    h3_indices: list[int] = []
    rarity_zscore: list[float] = []
    count_species: list[int] = []
    count_observations: list[int] = []
    count_observers: list[int] = []
    confidence_scores: list[float] = []
    species_vs_observations: list[float] = []

    for row in result:
        h3_indices.append(row[0])
        rarity_zscore.append(row[1])
        count_species.append(row[2])
        count_observations.append(row[3])
        count_observers.append(row[4])
        confidence_scores.append(row[5])
        species_vs_observations.append(row[6])
    return (
        h3_indices,
        np.array(rarity_zscore, dtype=np.float64),
        np.array(count_species, dtype=np.float64),
        np.array(count_observations, dtype=np.float64),
        np.array(count_observers, dtype=np.float64),
        np.array(confidence_scores, dtype=np.float64),
        np.array(species_vs_observations, dtype=np.float64)
    )


def export_scores_to_parquet(
    res: int,
    output_path: Path,
    h3_indices: list[int],
    rarity_zscore: np.ndarray,
    count_species: np.ndarray,
    count_observations: np.ndarray,
    count_observers: np.ndarray,
    confidence_scores: np.ndarray,
    species_vs_observations: np.ndarray,
) -> None:
    """
    Export scores to Parquet file.

    """
    try:
        import pyarrow as pa
    except ImportError as exc:
        raise RuntimeError("Exporting to Parquet requires pyarrow") from exc


    # Create PyArrow table
    table = pa.table(
        {
            f"h3_res{res}": pa.array(h3_indices, type=pa.uint64()),
            "rarity_zscore": pa.array(rarity_zscore, type=pa.float64()),
            "count_species": pa.array(count_species, type=pa.uint64()),
            "count_observations": pa.array(count_observations, type=pa.uint64()),
            "count_observers": pa.array(count_observers, type=pa.uint64()),
            "confidence_scores": pa.array(confidence_scores, type=pa.float64()),
            "species_vs_observations": pa.array(species_vs_observations, type=pa.float64()),
        }
    )

    # Write to Parquet with compression
    import pyarrow.parquet as pq

    output_path.mkdir(parents=True, exist_ok=True)
    file_path = output_path / f"cell_scores{res}.parquet"
    pq.write_table(table, file_path, compression="zstd", row_group_size=100000)



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

    for res in H3_VISUALIZATION_RESOLUTIONS:
        # Fetch aggregated cell data from DuckDB
        print(f"Fetching cell aggregates from DuckDB, resolution {res}...")
        h3_indices, rarity_zscore, count_species, count_observations, count_observers, confidence_scores, species_vs_observations = fetch_cell_data(
            res=res,
            observations_path=observations_path,
            species_occupancy_path=species_occupancy_path,
        )
        print(f"  Cells loaded: {len(h3_indices):,}")

        # Export results to Parquet
        print("Exporting results to Parquet...")
        export_scores_to_parquet(
            res=res,
            output_path=output_path,
            h3_indices=h3_indices,
            rarity_zscore=rarity_zscore,
            count_observations=count_observations,
            count_species=count_species,
            count_observers=count_observers,
            confidence_scores=confidence_scores,
            species_vs_observations=species_vs_observations,
        )
        print(f"  Written to: {output_path}")

        print()
        print(f" Rarity z-score quantile 0.025: {np.quantile(rarity_zscore, 0.025):.4f}")
        print(f" Rarity z-score quantile 0.975: {np.quantile(rarity_zscore, 0.975):.4f}")
        print(f" Count observations quantile 0.975: {np.quantile(count_observations, 0.975):.0f}")
        print(f" Count species quantile 0.975: {np.quantile(count_species, 0.975):.0f}")
        print(f" Count observers quantile 0.975: {np.quantile(count_observers, 0.975):.0f}")
        print(f" Confidence scores quantile 0.025: {np.quantile(confidence_scores, 0.025):.4f}")
        print(f" Confidence scores quantile 0.975: {np.quantile(confidence_scores, 0.975):.4f}")
        print(f" Species vs observations quantile 0.025: {np.quantile(species_vs_observations, 0.025):.4f}")
        print(f" Species vs observations quantile 0.975: {np.quantile(species_vs_observations, 0.975):.4f}")

        data = {
            "rarity_quantiles": [np.quantile(rarity_zscore, 0.025), np.quantile(rarity_zscore, 0.5),  np.quantile(rarity_zscore, 0.75), np.quantile(rarity_zscore, 0.975)],
            "count_observations_quantiles": [np.quantile(count_observations, 0.025), np.quantile(count_observations, 0.5), np.quantile(count_observations, 0.975)],
            "count_species_quantiles": [np.quantile(count_species, 0.025), np.quantile(count_species, 0.5), np.quantile(count_species, 0.975)],
            "count_observers_quantiles": [np.quantile(count_observers, 0.025), np.quantile(count_observers, 0.5), np.quantile(count_observers, 0.975)],
            "confidence_scores_quantiles": [np.quantile(confidence_scores, 0.025), np.quantile(confidence_scores, 0.5), np.quantile(confidence_scores, 0.975)],
            "species_vs_observations_quantiles": [np.quantile(species_vs_observations, 0.025), np.quantile(species_vs_observations, 0.25), np.quantile(species_vs_observations, 0.5), np.quantile(species_vs_observations, 0.75), np.quantile(species_vs_observations, 0.975)],
        }

        with open(GENERATED_JSONS / f"cell_scores_summary{res}.json", "w") as f:
            json.dump(data, f)

    print()
    print("H3 cell score generation completed.")



if __name__ == "__main__":
    main()
