from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from rare_species_map.cell_scores import CellScoreConfig, compute_cell_scores
from rare_species_map.config import DATA_PROCESSED, DATA_RAW, DATA_TILES
from rare_species_map.filtering import (
    FilterObservationsConfig,
    filter_observations_to_parquet,
)
from rare_species_map.occupancy import (
    SpeciesOccupancyConfig,
    compute_species_occupancy,
)
from rare_species_map.tiles import TileFormat, TileGenerationConfig, generate_pmtiles


@dataclass(frozen=True, slots=True)
class PipelineConfig:
    input_path: Path = DATA_RAW
    observations_output_path: Path = DATA_PROCESSED / "observations_filtered.parquet"
    species_occupancy_output_path: Path = DATA_PROCESSED / "species_occupancy.parquet"
    cell_scores_output_dir: Path = DATA_PROCESSED
    cell_scores_summary_output_dir: Path = DATA_TILES
    diagnostics_output_dir: Path = DATA_PROCESSED / "diagnostics" / "cell_scores"
    diagnostics_sample_size: int = 200_000
    write_diagnostics: bool = True
    tiles_output_dir: Path = DATA_TILES
    encoding: str = "auto"
    start_at: int = 1
    stop_after: int = 4
    tile_format: TileFormat = "mvt"
    drop_rate: float | None = None
    coalesce: bool = False
    simplification: bool = True
    keep_geojsonseq: bool = False
    quiet_tiles: bool = False
    country_code: str | None = None


@dataclass(frozen=True, slots=True)
class PipelineStepResult:
    step: int
    label: str
    elapsed_seconds: float


ProgressCallback = Callable[[int, str], None]


def run_pipeline(
    config: PipelineConfig,
    progress: ProgressCallback | None = None,
) -> list[PipelineStepResult]:
    if config.stop_after < config.start_at:
        raise ValueError("stop_after must be greater than or equal to start_at")

    if config.start_at < 1 or config.stop_after > 4:
        raise ValueError("start_at and stop_after must be in the range 1-4")

    steps: dict[int, tuple[str, Callable[[], object]]] = {
        1: (
            "Filter raw GBIF TSV to Parquet",
            lambda: filter_observations_to_parquet(
                FilterObservationsConfig(
                    input_path=config.input_path,
                    output_path=config.observations_output_path,
                    encoding=config.encoding,
                    country_code=config.country_code,
                )
            ),
        ),
        2: (
            "Compute species occupancy",
            lambda: compute_species_occupancy(
                SpeciesOccupancyConfig(
                    input_path=config.observations_output_path,
                    output_path=config.species_occupancy_output_path,
                )
            ),
        ),
        3: (
            "Compute H3 cell scores",
            lambda: compute_cell_scores(
                CellScoreConfig(
                    observations_path=config.observations_output_path,
                    species_occupancy_path=config.species_occupancy_output_path,
                    output_dir=config.cell_scores_output_dir,
                    summary_output_dir=config.cell_scores_summary_output_dir,
                    diagnostics_output_dir=config.diagnostics_output_dir,
                    diagnostics_sample_size=config.diagnostics_sample_size,
                    write_diagnostics=config.write_diagnostics,
                )
            ),
        ),
        4: (
            "Generate PMTiles",
            lambda: generate_pmtiles(
                TileGenerationConfig(
                    input_dir=config.cell_scores_output_dir,
                    output_dir=config.tiles_output_dir,
                    tile_format=config.tile_format,
                    drop_rate=config.drop_rate,
                    coalesce=config.coalesce,
                    simplification=config.simplification,
                    keep_geojsonseq=config.keep_geojsonseq,
                    quiet=config.quiet_tiles,
                )
            ),
        ),
    }

    results: list[PipelineStepResult] = []
    for step in range(config.start_at, config.stop_after + 1):
        label, action = steps[step]
        if progress is not None:
            progress(step, label)
        start = time.monotonic()
        action()
        results.append(
            PipelineStepResult(
                step=step,
                label=label,
                elapsed_seconds=time.monotonic() - start,
            )
        )

    return results
