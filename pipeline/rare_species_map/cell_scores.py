from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple

import h3
import numpy as np
import numpy.typing as npt

from rare_species_map.config import (
    DATA_PROCESSED,
    GENERATED_JSONS,
    H3_VISUALIZATION_RESOLUTIONS,
)
from rare_species_map.duckdb_utils import get_connection


DEFAULT_DIAGNOSTICS_DIR = DATA_PROCESSED / "diagnostics" / "cell_scores"


@dataclass(frozen=True, slots=True)
class CellScoreConfig:
    observations_path: Path = DATA_PROCESSED / "observations_filtered.parquet"
    species_occupancy_path: Path = DATA_PROCESSED / "species_occupancy.parquet"
    output_dir: Path = DATA_PROCESSED
    summary_output_dir: Path = GENERATED_JSONS
    diagnostics_output_dir: Path = DEFAULT_DIAGNOSTICS_DIR
    diagnostics_sample_size: int = 200_000
    write_diagnostics: bool = True
    h3_resolutions: tuple[int, ...] = tuple(H3_VISUALIZATION_RESOLUTIONS)


class CellScoreArrays(NamedTuple):
    h3_indices: list[int]
    rarity_zscore: npt.NDArray[np.float64]
    count_species: npt.NDArray[np.float64]
    count_observations: npt.NDArray[np.float64]
    count_observers: npt.NDArray[np.float64]
    confidence_scores: npt.NDArray[np.float64]


class CellScoreQuantiles(NamedTuple):
    rarity_quantiles: list[float]
    count_observations_quantiles: list[float]
    count_species_quantiles: list[float]
    count_observers_quantiles: list[float]
    confidence_scores_quantiles: list[float]


def build_aggregation_query(
    resolution: int,
    observations_path: Path,
    species_occupancy_path: Path,
) -> str:
    hex_edge_length = h3.average_hexagon_edge_length(resolution, unit="m")

    return f"""
    WITH valid_observers AS (
        SELECT recordedBy
        FROM parquet_scan('{observations_path.as_posix()}')
        WHERE recordedBy IS NOT NULL
        GROUP BY recordedBy
        HAVING MIN(h3_res{resolution}) != MAX(h3_res{resolution})
    ),

    observations AS (
        SELECT
            o.h3_res{resolution},
            o.speciesKey,
            o.recordedBy
        FROM parquet_scan('{observations_path.as_posix()}') o
        SEMI JOIN valid_observers v
            ON o.recordedBy = v.recordedBy
        WHERE o.coordinateUncertaintyInMeters <= {hex_edge_length}
    ),

    observer_counts_by_cell AS (
        SELECT
            h3_res{resolution},
            recordedBy,
            COUNT(*) AS obs_by_observer
        FROM observations
        GROUP BY h3_res{resolution}, recordedBy
    ),

    cell_metrics AS (
        SELECT
            h3_res{resolution},
            COUNT(*) AS count_observations,
            COUNT(DISTINCT speciesKey) AS count_species,
            COUNT(DISTINCT recordedBy) AS count_observers
        FROM observations
        GROUP BY h3_res{resolution}
    ),

    shannon_by_cell AS (
        SELECT
            o.h3_res{resolution},
            -SUM( (o.obs_by_observer::DOUBLE / cm.count_observations) * LN(o.obs_by_observer::DOUBLE / cm.count_observations) ) AS shannon_H
        FROM observer_counts_by_cell o
        JOIN cell_metrics cm
            ON o.h3_res{resolution} = cm.h3_res{resolution}
        GROUP BY o.h3_res{resolution}
    ),

    neff_by_cell AS (
        SELECT
            h3_res{resolution},
            shannon_H,
            EXP(shannon_H) AS neff_observers
        FROM shannon_by_cell
    ),

    species_by_cell_and_observer AS (
        SELECT DISTINCT
            h3_res{resolution},
            speciesKey,
            recordedBy
        FROM observations
    ),

    mean_rarity_by_cell_and_observer AS (
        SELECT
            s.h3_res{resolution},
            s.recordedBy,
            AVG(so.rarity) AS mean_rarity
        FROM species_by_cell_and_observer s
        INNER JOIN parquet_scan('{species_occupancy_path.as_posix()}') so
            ON s.speciesKey = so.speciesKey
        GROUP BY s.h3_res{resolution}, s.recordedBy
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
            m.h3_res{resolution},
            m.recordedBy,
            m.mean_rarity - o.mean_rarity AS residual_rarity
        FROM mean_rarity_by_cell_and_observer m
        INNER JOIN mean_rarity_by_observer o
            ON m.recordedBy = o.recordedBy
    ),

    residual_rarity_by_cell AS (
        SELECT
            h3_res{resolution},
            AVG(residual_rarity) AS mean_residual_rarity
        FROM residual_rarity_by_cell_and_observer
        GROUP BY h3_res{resolution}
    )

    SELECT
        r.h3_res{resolution},
        r.mean_residual_rarity AS rarity_zscore,
        cm.count_species,
        cm.count_observations,
        cm.count_observers,
        ( 1 - EXP( - n.neff_observers / 4 ) ) * ( 1 - EXP( - cm.count_observations / 40 ) ) AS confidence_scores
    FROM residual_rarity_by_cell r
    JOIN neff_by_cell n
        ON r.h3_res{resolution} = n.h3_res{resolution}
    JOIN cell_metrics cm
        ON r.h3_res{resolution} = cm.h3_res{resolution}
    ORDER BY r.h3_res{resolution}
    """


def fetch_cell_data(
    resolution: int,
    observations_path: Path,
    species_occupancy_path: Path,
) -> tuple[CellScoreArrays, float]:
    con = get_connection()
    query = build_aggregation_query(
        resolution=resolution,
        observations_path=observations_path,
        species_occupancy_path=species_occupancy_path,
    )

    start = time.monotonic()
    try:
        result = con.execute(query).fetchall()
    finally:
        con.close()
    elapsed_seconds = time.monotonic() - start

    if not result:
        raise ValueError("No cells found in aggregation query")

    h3_indices: list[int] = []
    rarity_zscore: list[float] = []
    count_species: list[int] = []
    count_observations: list[int] = []
    count_observers: list[int] = []
    confidence_scores: list[float] = []

    for row in result:
        h3_indices.append(int(row[0]))
        rarity_zscore.append(float(row[1]))
        count_species.append(int(row[2]))
        count_observations.append(int(row[3]))
        count_observers.append(int(row[4]))
        confidence_scores.append(float(row[5]))

    arrays = CellScoreArrays(
        h3_indices=h3_indices,
        rarity_zscore=np.array(rarity_zscore, dtype=np.float64),
        count_species=np.array(count_species, dtype=np.float64),
        count_observations=np.array(count_observations, dtype=np.float64),
        count_observers=np.array(count_observers, dtype=np.float64),
        confidence_scores=np.array(confidence_scores, dtype=np.float64),
    )
    return arrays, elapsed_seconds


def export_scores_to_parquet(
    resolution: int,
    output_dir: Path,
    cell_scores: CellScoreArrays,
) -> Path:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("Exporting to Parquet requires pyarrow") from exc

    table = pa.table(
        {
            f"h3_res{resolution}": pa.array(cell_scores.h3_indices, type=pa.uint64()),
            "rarity_zscore": pa.array(cell_scores.rarity_zscore, type=pa.float64()),
            "count_species": pa.array(cell_scores.count_species, type=pa.uint64()),
            "count_observations": pa.array(
                cell_scores.count_observations,
                type=pa.uint64(),
            ),
            "count_observers": pa.array(cell_scores.count_observers, type=pa.uint64()),
            "confidence_scores": pa.array(
                cell_scores.confidence_scores,
                type=pa.float64(),
            ),
        }
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"cell_scores{resolution}.parquet"
    pq.write_table(table, output_path, compression="zstd", row_group_size=100000)
    return output_path


def summarize_cell_scores(cell_scores: CellScoreArrays) -> CellScoreQuantiles:
    return CellScoreQuantiles(
        rarity_quantiles=[
            float(np.quantile(cell_scores.rarity_zscore, 0.025)),
            float(np.quantile(cell_scores.rarity_zscore, 0.5)),
            float(np.quantile(cell_scores.rarity_zscore, 0.75)),
            float(np.quantile(cell_scores.rarity_zscore, 0.975)),
        ],
        count_observations_quantiles=[
            float(np.quantile(cell_scores.count_observations, 0.025)),
            float(np.quantile(cell_scores.count_observations, 0.5)),
            float(np.quantile(cell_scores.count_observations, 0.975)),
        ],
        count_species_quantiles=[
            float(np.quantile(cell_scores.count_species, 0.025)),
            float(np.quantile(cell_scores.count_species, 0.5)),
            float(np.quantile(cell_scores.count_species, 0.975)),
        ],
        count_observers_quantiles=[
            float(np.quantile(cell_scores.count_observers, 0.025)),
            float(np.quantile(cell_scores.count_observers, 0.5)),
            float(np.quantile(cell_scores.count_observers, 0.975)),
        ],
        confidence_scores_quantiles=[
            float(np.quantile(cell_scores.confidence_scores, 0.025)),
            float(np.quantile(cell_scores.confidence_scores, 0.5)),
            float(np.quantile(cell_scores.confidence_scores, 0.975)),
        ],
    )


def write_cell_score_summary(
    resolution: int,
    summary_output_dir: Path,
    quantiles: CellScoreQuantiles,
) -> Path:
    summary_output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = summary_output_dir / f"cell_scores_summary{resolution}.json"
    summary_path.write_text(
        json.dumps(quantiles._asdict()),
        encoding="utf-8",
    )
    return summary_path


def compute_cell_scores(config: CellScoreConfig) -> dict[int, CellScoreQuantiles]:
    observations_path = config.observations_path.resolve()
    species_occupancy_path = config.species_occupancy_path.resolve()
    output_dir = config.output_dir.resolve()

    if not observations_path.exists():
        raise FileNotFoundError(f"Observations parquet not found: {observations_path}")

    if not species_occupancy_path.exists():
        raise FileNotFoundError(
            f"Species occupancy parquet not found: {species_occupancy_path}"
        )

    summaries: dict[int, CellScoreQuantiles] = {}
    for resolution in config.h3_resolutions:
        cell_scores, _elapsed_seconds = fetch_cell_data(
            resolution=resolution,
            observations_path=observations_path,
            species_occupancy_path=species_occupancy_path,
        )
        export_scores_to_parquet(
            resolution=resolution,
            output_dir=output_dir,
            cell_scores=cell_scores,
        )
        quantiles = summarize_cell_scores(cell_scores)
        write_cell_score_summary(
            resolution=resolution,
            summary_output_dir=config.summary_output_dir.resolve(),
            quantiles=quantiles,
        )
        summaries[resolution] = quantiles

    return summaries
