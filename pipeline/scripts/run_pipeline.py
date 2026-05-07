# pipeline/scripts/run_pipeline.py

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

PIPELINE_ROOT = Path(__file__).resolve().parents[1]
if str(PIPELINE_ROOT) not in sys.path:
    sys.path.insert(0, str(PIPELINE_ROOT))

from rare_species_map.config import (
    DATA_PROCESSED,
    DATA_TILES,
    DEFAULT_COUNTRY,
    MAX_COORDINATE_UNCERTAINTY,
)


SCRIPT_DIR = Path(__file__).resolve().parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the full rare species map tile generation pipeline"
    )

    parser.add_argument(
        "--input",
        default=None,
        help="Path to the raw GBIF TSV input file",
    )

    parser.add_argument(
        "--observations-output",
        default=str(DATA_PROCESSED / "observations_filtered.parquet"),
        help="Step 1 output parquet path",
    )

    parser.add_argument(
        "--species-occupancy-output",
        default=str(DATA_PROCESSED / "species_occupancy.parquet"),
        help="Step 2 output parquet path",
    )

    parser.add_argument(
        "--cell-scores-output",
        default=str(DATA_PROCESSED / "cell_scores.parquet"),
        help="Step 3 output parquet path",
    )

    parser.add_argument(
        "--diagnostics-output-dir",
        default=str(DATA_PROCESSED / "diagnostics" / "cell_scores"),
        help="Step 3 diagnostic plots output directory",
    )

    parser.add_argument(
        "--diagnostics-sample-size",
        type=int,
        default=200_000,
        help="Maximum number of H3 cells to sample for Step 3 diagnostic plots",
    )

    parser.add_argument(
        "--no-diagnostics",
        action="store_true",
        help="Pass --no-diagnostics to Step 3",
    )

    parser.add_argument(
        "--tiles-output",
        default=str(DATA_TILES / "rare_species_cells.pmtiles"),
        help="Step 4 output PMTiles path",
    )

    parser.add_argument(
        "--country",
        default=DEFAULT_COUNTRY,
        help=f"Country filter for Step 1 (default: {DEFAULT_COUNTRY})",
    )

    parser.add_argument(
        "--uncertainty",
        type=float,
        default=MAX_COORDINATE_UNCERTAINTY,
        help="Maximum coordinate uncertainty in meters for Step 1",
    )

    parser.add_argument(
        "--encoding",
        default="auto",
        help="Input text encoding for Step 1 (default: auto)",
    )

    parser.add_argument(
        "--start-at",
        type=int,
        choices=(1, 2, 3, 4),
        default=1,
        help="First step to run",
    )

    parser.add_argument(
        "--stop-after",
        type=int,
        choices=(1, 2, 3, 4),
        default=4,
        help="Last step to run",
    )

    parser.add_argument(
        "--min-zoom",
        type=int,
        default=0,
        help="Minimum tile zoom for Step 4",
    )

    parser.add_argument(
        "--max-zoom",
        type=int,
        default=12,
        help="Maximum tile zoom for Step 4",
    )

    parser.add_argument(
        "--tile-format",
        choices=("mvt", "mlt"),
        default="mvt",
        help="Tile encoding format for Step 4",
    )

    parser.add_argument(
        "--base-zoom",
        type=int,
        default=None,
        help="Step 4 base zoom. Defaults to max zoom in freestiler.",
    )

    parser.add_argument(
        "--drop-rate",
        type=float,
        default=None,
        help="Optional Step 4 exponential feature thinning rate",
    )

    parser.add_argument(
        "--coalesce",
        action="store_true",
        help="Pass --coalesce to Step 4",
    )

    parser.add_argument(
        "--no-simplification",
        action="store_true",
        help="Pass --no-simplification to Step 4",
    )

    parser.add_argument(
        "--keep-geojsonseq",
        action="store_true",
        help="Pass --keep-geojsonseq to Step 4",
    )

    parser.add_argument(
        "--geojsonseq",
        default=None,
        help="Optional Step 4 GeoJSONSeq output path",
    )

    parser.add_argument(
        "--quiet-tiles",
        action="store_true",
        help="Pass --quiet to Step 4 freestiler",
    )

    return parser.parse_args()


def as_path(path: str) -> Path:
    return Path(path).resolve()


def run_command(step: int, label: str, command: list[str]) -> None:
    print()
    print(f"=== Step {step}: {label} ===")
    print(" ".join(command))
    print(flush=True)

    start = time.monotonic()
    subprocess.run(command, check=True)
    elapsed = time.monotonic() - start

    print()
    print(f"Step {step} completed in {elapsed / 60:.1f} minutes.", flush=True)


def build_steps(args: argparse.Namespace) -> dict[int, tuple[str, list[str]]]:
    input_path = as_path(args.input) if args.input is not None else None
    observations_output = as_path(args.observations_output)
    species_occupancy_output = as_path(args.species_occupancy_output)
    cell_scores_output = as_path(args.cell_scores_output)
    diagnostics_output_dir = as_path(args.diagnostics_output_dir)
    tiles_output = as_path(args.tiles_output)

    step_1 = [
        sys.executable,
        str(SCRIPT_DIR / "01_filter_to_parquet.py"),
        "--input",
        str(input_path) if input_path is not None else "",
        "--output",
        str(observations_output),
        "--country",
        args.country,
        "--uncertainty",
        str(args.uncertainty),
        "--encoding",
        args.encoding,
    ]

    step_2 = [
        sys.executable,
        str(SCRIPT_DIR / "02_compute_species_occupancy.py"),
        "--input",
        str(observations_output),
        "--output",
        str(species_occupancy_output),
    ]

    step_3 = [
        sys.executable,
        str(SCRIPT_DIR / "03_compute_cell_scores.py"),
        "--observations",
        str(observations_output),
        "--species-occupancy",
        str(species_occupancy_output),
        "--output",
        str(cell_scores_output),
        "--diagnostics-output-dir",
        str(diagnostics_output_dir),
        "--diagnostics-sample-size",
        str(args.diagnostics_sample_size),
    ]

    if args.no_diagnostics:
        step_3.append("--no-diagnostics")

    step_4 = [
        sys.executable,
        str(SCRIPT_DIR / "04_generate_pmtiles.py"),
        "--input",
        str(cell_scores_output),
        "--output",
        str(tiles_output),
        "--min-zoom",
        str(args.min_zoom),
        "--max-zoom",
        str(args.max_zoom),
        "--tile-format",
        args.tile_format,
    ]

    if args.base_zoom is not None:
        step_4.extend(["--base-zoom", str(args.base_zoom)])

    if args.drop_rate is not None:
        step_4.extend(["--drop-rate", str(args.drop_rate)])

    if args.coalesce:
        step_4.append("--coalesce")

    if args.no_simplification:
        step_4.append("--no-simplification")

    if args.keep_geojsonseq:
        step_4.append("--keep-geojsonseq")

    if args.geojsonseq is not None:
        step_4.extend(["--geojsonseq", str(as_path(args.geojsonseq))])

    if args.quiet_tiles:
        step_4.append("--quiet")

    return {
        1: ("Filter raw GBIF TSV to Parquet", step_1),
        2: ("Compute species occupancy", step_2),
        3: ("Compute H3 cell scores", step_3),
        4: ("Generate PMTiles", step_4),
    }


def main() -> None:
    args = parse_args()

    if args.stop_after < args.start_at:
        raise ValueError("--stop-after must be greater than or equal to --start-at")

    if args.start_at == 1 and args.input is None:
        raise ValueError("--input is required when running Step 1")

    steps = build_steps(args)

    print()
    print("=== Rare Species Map Tile Pipeline ===")
    print(f"Running steps {args.start_at}-{args.stop_after}", flush=True)

    pipeline_start = time.monotonic()
    for step_number in range(args.start_at, args.stop_after + 1):
        label, command = steps[step_number]
        run_command(step_number, label, command)

    elapsed = time.monotonic() - pipeline_start

    print()
    print("Pipeline completed.")
    print(f"Total elapsed time: {elapsed / 60:.1f} minutes.")


if __name__ == "__main__":
    main()
