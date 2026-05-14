from __future__ import annotations

from typing import Any


__all__ = [
    "CellScoreConfig",
    "FilterObservationsConfig",
    "PipelineConfig",
    "SpeciesOccupancyConfig",
    "TileGenerationConfig",
    "compute_cell_scores",
    "compute_species_occupancy",
    "filter_observations_to_parquet",
    "generate_pmtiles",
    "run_pipeline",
]


def __getattr__(name: str) -> Any:
    if name in {"CellScoreConfig", "compute_cell_scores"}:
        from rare_species_map.cell_scores import CellScoreConfig, compute_cell_scores

        return {
            "CellScoreConfig": CellScoreConfig,
            "compute_cell_scores": compute_cell_scores,
        }[name]

    if name in {"FilterObservationsConfig", "filter_observations_to_parquet"}:
        from rare_species_map.filtering import (
            FilterObservationsConfig,
            filter_observations_to_parquet,
        )

        return {
            "FilterObservationsConfig": FilterObservationsConfig,
            "filter_observations_to_parquet": filter_observations_to_parquet,
        }[name]

    if name in {"SpeciesOccupancyConfig", "compute_species_occupancy"}:
        from rare_species_map.occupancy import (
            SpeciesOccupancyConfig,
            compute_species_occupancy,
        )

        return {
            "SpeciesOccupancyConfig": SpeciesOccupancyConfig,
            "compute_species_occupancy": compute_species_occupancy,
        }[name]

    if name in {"PipelineConfig", "run_pipeline"}:
        from rare_species_map.pipeline import PipelineConfig, run_pipeline

        return {
            "PipelineConfig": PipelineConfig,
            "run_pipeline": run_pipeline,
        }[name]

    if name in {"TileGenerationConfig", "generate_pmtiles"}:
        from rare_species_map.tiles import TileGenerationConfig, generate_pmtiles

        return {
            "TileGenerationConfig": TileGenerationConfig,
            "generate_pmtiles": generate_pmtiles,
        }[name]

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
