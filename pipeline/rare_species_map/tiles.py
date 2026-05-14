from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Mapping

from freestiler import freestile_file
import h3

from rare_species_map.config import (
    DATA_PROCESSED,
    DATA_TILES,
    H3_VISUALIZATION_RESOLUTIONS,
    H3_ZOOM_RANGES,
    PIPELINE_ROOT,
)
from rare_species_map.duckdb_utils import get_connection


DEFAULT_LAYER_NAME = "rare_species_cells"

TileFormat = Literal["mvt", "mlt"]


@dataclass(frozen=True, slots=True)
class TileGenerationConfig:
    input_dir: Path = DATA_PROCESSED
    output_dir: Path = DATA_TILES
    layer_name: str = DEFAULT_LAYER_NAME
    batch_size: int = 100_000
    tile_format: TileFormat = "mvt"
    drop_rate: float | None = None
    coalesce: bool = False
    simplification: bool = True
    keep_geojsonseq: bool = False
    geojsonseq_only: bool = False
    quiet: bool = False
    h3_resolutions: tuple[int, ...] = tuple(H3_VISUALIZATION_RESOLUTIONS)
    h3_zoom_ranges: tuple[tuple[int, int], ...] = tuple(
        (int(zoom_range[0]), int(zoom_range[1])) for zoom_range in H3_ZOOM_RANGES
    )


def h3_boundary_geojson(h3_cell: int) -> list[list[list[float]]]:
    """Convert an H3 cell boundary to GeoJSON coordinates."""
    h3_string = h3.int_to_str(h3_cell)
    boundary = h3.cell_to_boundary(h3_string)
    ring = [[lng, lat] for lat, lng in boundary]

    normalized_ring = [ring[0]]

    for index in range(1, len(ring)):
        previous_lng = normalized_ring[-1][0]
        current_lng = ring[index][0]
        current_lat = ring[index][1]
        lng_diff = current_lng - previous_lng

        if lng_diff > 180:
            normalized_ring.append([current_lng - 360, current_lat])
        elif lng_diff < -180:
            normalized_ring.append([current_lng + 360, current_lat])
        else:
            normalized_ring.append([current_lng, current_lat])

    normalized_ring.append(normalized_ring[0])
    return [normalized_ring]


def build_feature(row: Mapping[str, Any], resolution: int) -> dict[str, Any]:
    h3_cell = int(row[f"h3_res{resolution}"])

    return {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": h3_boundary_geojson(h3_cell),
        },
        "properties": {
            "h3": h3.int_to_str(h3_cell),
            "rarity_zscore": float(row["rarity_zscore"]),
            "count_species": int(row["count_species"]),
            "count_observations": int(row["count_observations"]),
            "count_observers": int(row["count_observers"]),
            "confidence_scores": float(row["confidence_scores"]),
        },
    }


def export_geojsonseq(
    input_path: Path,
    output_path: Path,
    resolution: int,
    batch_size: int,
) -> int:
    con = get_connection()

    query = f"""
    SELECT
        h3_res{resolution},
        rarity_zscore,
        count_species,
        count_observations,
        count_observers,
        confidence_scores
    FROM parquet_scan('{input_path.as_posix()}')
    WHERE
        h3_res{resolution} IS NOT NULL
        AND rarity_zscore IS NOT NULL
        AND count_species IS NOT NULL
        AND count_observations IS NOT NULL
        AND count_observers IS NOT NULL
        AND confidence_scores IS NOT NULL
    """

    try:
        reader = con.execute(query).to_arrow_reader(batch_size=batch_size)

        n_features = 0
        with output_path.open("w", encoding="utf-8", newline="\n") as file:
            for batch in reader:
                for row in batch.to_pylist():
                    feature = build_feature(row, resolution)
                    file.write(json.dumps(feature, separators=(",", ":")))
                    file.write("\n")
                    n_features += 1
    finally:
        con.close()

    return n_features


def configure_freestiler_duckdb_home() -> None:
    duckdb_home = PIPELINE_ROOT / ".duckdb_freestiler_home"
    duckdb_home.mkdir(parents=True, exist_ok=True)
    os.environ["HOME"] = str(duckdb_home)
    os.environ["USERPROFILE"] = str(duckdb_home)


def run_freestiler(
    geojsonseq_path: Path,
    output_path: Path,
    layer_name: str,
    min_zoom: int,
    max_zoom: int,
    tile_format: TileFormat,
    drop_rate: float | None,
    coalesce: bool,
    simplification: bool,
    quiet: bool,
) -> None:
    configure_freestiler_duckdb_home()

    freestile_file(
        geojsonseq_path,
        output_path,
        layer_name=layer_name,
        tile_format=tile_format,
        min_zoom=min_zoom,
        max_zoom=max_zoom,
        drop_rate=drop_rate,
        coalesce=coalesce,
        simplification=simplification,
        overwrite=True,
        quiet=quiet,
        engine="duckdb",
    )


def generate_pmtiles(config: TileGenerationConfig) -> dict[int, int]:
    if len(config.h3_resolutions) != len(config.h3_zoom_ranges):
        raise ValueError("h3_resolutions and h3_zoom_ranges must have the same length")

    if config.drop_rate is not None and config.drop_rate <= 0:
        raise ValueError("drop_rate must be greater than 0")

    feature_counts: dict[int, int] = {}
    for resolution, zoom_range in zip(config.h3_resolutions, config.h3_zoom_ranges):
        min_zoom, max_zoom = zoom_range
        input_path = config.input_dir.resolve() / f"cell_scores{resolution}.parquet"
        output_path = config.output_dir.resolve() / f"rare_species_cells{resolution}.pmtiles"

        if not input_path.exists():
            raise FileNotFoundError(f"Cell scores parquet not found: {input_path}")

        if min_zoom < 0 or max_zoom < min_zoom:
            raise ValueError("max_zoom must be greater than or equal to min_zoom")

        output_path.parent.mkdir(parents=True, exist_ok=True)

        temp_file = tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".geojsonseq",
            prefix=f"rare_species_cells_{resolution}_",
            dir=output_path.parent,
            delete=False,
        )
        temp_file.close()
        geojsonseq_path = Path(temp_file.name)

        try:
            n_features = export_geojsonseq(
                input_path=input_path,
                output_path=geojsonseq_path,
                resolution=resolution,
                batch_size=config.batch_size,
            )
            feature_counts[resolution] = n_features

            if config.geojsonseq_only:
                break

            run_freestiler(
                geojsonseq_path=geojsonseq_path,
                output_path=output_path,
                layer_name=config.layer_name,
                min_zoom=min_zoom,
                max_zoom=max_zoom,
                tile_format=config.tile_format,
                drop_rate=config.drop_rate,
                coalesce=config.coalesce,
                simplification=config.simplification,
                quiet=config.quiet,
            )
        finally:
            if not config.keep_geojsonseq:
                geojsonseq_path.unlink(missing_ok=True)

    return feature_counts
