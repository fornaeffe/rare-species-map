# pipeline/scripts/04_generate_pmtiles.py

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

import h3

PIPELINE_ROOT = Path(__file__).resolve().parents[1]
if str(PIPELINE_ROOT) not in sys.path:
    sys.path.insert(0, str(PIPELINE_ROOT))

from rare_species_map.config import DATA_PROCESSED, DATA_TILES, H3_VISUALIZATION_RESOLUTION
from rare_species_map.duckdb_utils import get_connection


DEFAULT_LAYER_NAME = "rare_species_cells"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate PMTiles vector tiles from H3 cell score parquet"
    )

    parser.add_argument(
        "--input",
        default=str(DATA_PROCESSED / "cell_scores.parquet"),
        help="Input H3 cell scores parquet path",
    )

    parser.add_argument(
        "--output",
        default=str(DATA_TILES / "rare_species_cells.pmtiles"),
        help="Output PMTiles path",
    )

    parser.add_argument(
        "--layer",
        default=DEFAULT_LAYER_NAME,
        help=f"Vector tile layer name (default: {DEFAULT_LAYER_NAME})",
    )

    parser.add_argument(
        "--min-zoom",
        type=int,
        default=0,
        help="Minimum tile zoom",
    )

    parser.add_argument(
        "--max-zoom",
        type=int,
        default=12,
        help="Maximum tile zoom",
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=100_000,
        help="DuckDB fetch batch size while exporting GeoJSONSeq",
    )

    parser.add_argument(
        "--tippecanoe",
        default=None,
        help="Path to tippecanoe executable. Defaults to resolving it from PATH.",
    )

    parser.add_argument(
        "--geojsonseq",
        default=None,
        help="Optional GeoJSONSeq output path. Useful for debugging or external tiling.",
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

    return parser.parse_args()


def h3_boundary_geojson(h3_cell: int) -> list[list[list[float]]]:
    h3_string = h3.int_to_str(h3_cell)
    boundary = h3.cell_to_boundary(h3_string)
    ring = [[lng, lat] for lat, lng in boundary]
    ring.append(ring[0])
    return [ring]


def build_feature(row: dict[str, Any]) -> dict[str, Any]:
    h3_cell = int(row["h3_res8"])

    return {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": h3_boundary_geojson(h3_cell),
        },
        "properties": {
            "h3": h3.int_to_str(h3_cell),
            "count_observations": int(row["count_observations"]),
            "count_species": int(row["count_species"]),
            "sum_rarity": float(row["sum_rarity"]),
            "rarity_score": float(row["rarity_score"]),
        },
    }


def export_geojsonseq(input_path: Path, output_path: Path, batch_size: int) -> int:
    con = get_connection()

    query = f"""
    SELECT
        h3_res8,
        count_observations,
        count_species,
        sum_rarity,
        rarity_score
    FROM parquet_scan('{input_path.as_posix()}')
    WHERE
        h3_res8 IS NOT NULL
        AND count_observations IS NOT NULL
        AND count_species IS NOT NULL
        AND sum_rarity IS NOT NULL
        AND rarity_score IS NOT NULL
    """

    reader = con.execute(query).to_arrow_reader(batch_size=batch_size)

    n_features = 0
    with output_path.open("w", encoding="utf-8", newline="\n") as file:
        for batch in reader:
            rows = batch.to_pylist()
            for row in rows:
                feature = build_feature(row)
                file.write(json.dumps(feature, separators=(",", ":")))
                file.write("\n")
                n_features += 1

    con.close()
    return n_features


def resolve_tippecanoe(explicit_path: str | None) -> str:
    if explicit_path is not None:
        path = Path(explicit_path)
        if path.exists():
            return str(path)
        raise FileNotFoundError(f"Tippecanoe executable not found: {path}")

    executable = shutil.which("tippecanoe")
    if executable is None:
        raise FileNotFoundError(
            "Tippecanoe was not found in PATH. Install tippecanoe or pass "
            "--tippecanoe with the executable path. On Windows, running this "
            "script from WSL is usually the easiest option."
        )

    return executable


def run_tippecanoe(
    tippecanoe_path: str,
    geojsonseq_path: Path,
    output_path: Path,
    layer_name: str,
    min_zoom: int,
    max_zoom: int,
) -> None:
    command = [
        tippecanoe_path,
        "--force",
        "--output",
        str(output_path),
        "--layer",
        layer_name,
        "--minimum-zoom",
        str(min_zoom),
        "--maximum-zoom",
        str(max_zoom),
        "--drop-densest-as-needed",
        "--extend-zooms-if-still-dropping",
        "-P",
        "--read-parallel",
        str(geojsonseq_path),
    ]

    subprocess.run(command, check=True)


def main() -> None:
    args = parse_args()

    input_path = Path(args.input).resolve()
    output_path = Path(args.output).resolve()

    if not input_path.exists():
        raise FileNotFoundError(f"Cell scores parquet not found: {input_path}")

    if args.min_zoom < 0 or args.max_zoom < args.min_zoom:
        raise ValueError("--max-zoom must be greater than or equal to --min-zoom")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    geojsonseq_path: Path
    using_temp_geojsonseq = args.geojsonseq is None
    if using_temp_geojsonseq:
        temp_file = tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".geojsonseq",
            prefix="rare_species_cells_",
            dir=output_path.parent,
            delete=False,
        )
        temp_file.close()
        geojsonseq_path = Path(temp_file.name)
    else:
        geojsonseq_path = Path(args.geojsonseq).resolve()
        geojsonseq_path.parent.mkdir(parents=True, exist_ok=True)

    print()
    print("=== PMTiles Generation Pipeline ===")
    print(f"Input       : {input_path}")
    print(f"GeoJSONSeq  : {geojsonseq_path}")
    print(f"Output      : {output_path}")
    print(f"Layer       : {args.layer}")
    print(f"H3 res      : {H3_VISUALIZATION_RESOLUTION}")
    print(f"Zooms       : {args.min_zoom}-{args.max_zoom}")
    print()

    try:
        print("Exporting H3 cells to GeoJSONSeq...")
        n_features = export_geojsonseq(
            input_path=input_path,
            output_path=geojsonseq_path,
            batch_size=args.batch_size,
        )
        print(f"Exported features: {n_features:,}")
        print()

        if args.geojsonseq_only:
            print("GeoJSONSeq export completed.")
            return

        tippecanoe_path = resolve_tippecanoe(args.tippecanoe)
        print(f"Running tippecanoe: {tippecanoe_path}")
        print()

        run_tippecanoe(
            tippecanoe_path=tippecanoe_path,
            geojsonseq_path=geojsonseq_path,
            output_path=output_path,
            layer_name=args.layer,
            min_zoom=args.min_zoom,
            max_zoom=args.max_zoom,
        )

        print()
        print("PMTiles generation completed.")

    finally:
        if using_temp_geojsonseq and not args.keep_geojsonseq:
            geojsonseq_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
