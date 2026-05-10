# pipeline/scripts/04_generate_pmtiles.py

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

from freestiler import freestile_file
import h3

PIPELINE_ROOT = Path(__file__).resolve().parents[1]
if str(PIPELINE_ROOT) not in sys.path:
    sys.path.insert(0, str(PIPELINE_ROOT))

from rare_species_map.config import DATA_PROCESSED, DATA_TILES, H3_VISUALIZATION_RESOLUTIONS, H3_ZOOM_RANGES
from rare_species_map.duckdb_utils import get_connection


DEFAULT_LAYER_NAME = "rare_species_cells"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate PMTiles vector tiles from H3 cell score parquet"
    )

    parser.add_argument(
        "--input",
        default=str(DATA_PROCESSED),
        help="Input H3 cell scores parquet path",
    )

    parser.add_argument(
        "--output",
        default=str(DATA_TILES),
        help="Output PMTiles path",
    )

    parser.add_argument(
        "--layer",
        default=DEFAULT_LAYER_NAME,
        help=f"Vector tile layer name (default: {DEFAULT_LAYER_NAME})",
    )


    parser.add_argument(
        "--batch-size",
        type=int,
        default=100_000,
        help="DuckDB fetch batch size while exporting GeoJSONSeq",
    )

    parser.add_argument(
        "--tile-format",
        choices=("mvt", "mlt"),
        default="mvt",
        help="Tile encoding format. Use mvt for broad MapLibre compatibility.",
    )


    parser.add_argument(
        "--drop-rate",
        type=float,
        default=None,
        help="Optional exponential feature thinning rate at lower zooms.",
    )

    parser.add_argument(
        "--coalesce",
        action="store_true",
        help="Merge features with identical attributes within each tile",
    )

    parser.add_argument(
        "--no-simplification",
        action="store_true",
        help="Disable geometry snapping/simplification in freestiler",
    )

    parser.add_argument(
        "--keep-geojsonseq",
        action="store_true",
        help="Keep the temporary GeoJSONSeq file after PMTiles generation",
    )

    parser.add_argument(
        "--geojsonseq-only",
        action="store_true",
        help="Only export GeoJSONSeq and skip PMTiles generation",
    )

    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress freestiler progress output",
    )

    return parser.parse_args()


def h3_boundary_geojson(h3_cell: int) -> list[list[list[float]]]:
    h3_string = h3.int_to_str(h3_cell)
    boundary = h3.cell_to_boundary(h3_string)
    ring = [[lng, lat] for lat, lng in boundary]
    ring.append(ring[0])
    return [ring]


def build_feature(row: dict[str, Any], res: int) -> dict[str, Any]:
    h3_cell = int(row[f"h3_res{res}"])

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
            "species_vs_observations": float(row["species_vs_observations"])
        },
    }


def export_geojsonseq(input_path: Path, output_path: Path, res: int, batch_size: int) -> int:
    con = get_connection()

    query = f"""
    SELECT
        h3_res{res},
        rarity_zscore,
        count_species,
        count_observations,
        count_observers,
        confidence_scores,
        species_vs_observations
    FROM parquet_scan('{input_path.as_posix()}')
    WHERE
        h3_res{res} IS NOT NULL
        AND rarity_zscore IS NOT NULL
        AND count_species IS NOT NULL
        AND count_observations IS NOT NULL
        AND count_observers IS NOT NULL
        AND confidence_scores IS NOT NULL
        AND species_vs_observations IS NOT NULL
    """

    reader = con.execute(query).to_arrow_reader(batch_size=batch_size)

    n_features = 0
    with output_path.open("w", encoding="utf-8", newline="\n") as file:
        for batch in reader:
            rows = batch.to_pylist()
            for row in rows:
                feature = build_feature(row, res)
                file.write(json.dumps(feature, separators=(",", ":")))
                file.write("\n")
                n_features += 1

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
    tile_format: str,
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


def main() -> None:
    args = parse_args()

    print()
    print("=== PMTiles Generation Pipeline ===")

    for i in range(len(H3_VISUALIZATION_RESOLUTIONS)):
        res = H3_VISUALIZATION_RESOLUTIONS[i]
        min_zoom, max_zoom = H3_ZOOM_RANGES[i]

        input_path = Path(args.input).resolve() / f"cell_scores{res}.parquet"
        output_path = Path(args.output).resolve() / f"rare_species_cells{res}.pmtiles"

        if not input_path.exists():
            raise FileNotFoundError(f"Cell scores parquet not found: {input_path}")

        if min_zoom < 0 or max_zoom < min_zoom:
            raise ValueError("max-zoom must be greater than or equal to min-zoom")

        if args.drop_rate is not None and args.drop_rate <= 0:
            raise ValueError("--drop-rate must be greater than 0")

        output_path.parent.mkdir(parents=True, exist_ok=True)

        temp_file = tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".geojsonseq",
            prefix=f"rare_species_cells_{res}_",
            dir=output_path.parent,
            delete=False,
        )
        temp_file.close()
        geojsonseq_path = Path(temp_file.name)

        print()
        print(f"Input       : {input_path}")
        print(f"GeoJSONSeq  : {geojsonseq_path}")
        print(f"Output      : {output_path}")
        print(f"Layer       : {args.layer}")
        print(f"H3 res      : {res}")
        print(f"Zooms       : {min_zoom}-{max_zoom}")
        print(f"Tile format : {args.tile_format}")
        print()

        try:
            print("Exporting H3 cells to GeoJSONSeq...")
            n_features = export_geojsonseq(
                input_path=input_path,
                output_path=geojsonseq_path,
                res=res,
                batch_size=args.batch_size,
            )
            print(f"Exported features: {n_features:,}")
            print()

            if args.geojsonseq_only:
                print("GeoJSONSeq export completed.")
                return

            print("Running freestiler...")
            print()

            run_freestiler(
                geojsonseq_path=geojsonseq_path,
                output_path=output_path,
                layer_name=args.layer,
                min_zoom=min_zoom,
                max_zoom=max_zoom,
                tile_format=args.tile_format,
                drop_rate=args.drop_rate,
                coalesce=args.coalesce,
                simplification=not args.no_simplification,
                quiet=args.quiet,
            )

            print()
            print("PMTiles generation completed.")

        finally:
            if not args.keep_geojsonseq:
                geojsonseq_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
