from __future__ import annotations

import argparse
import time
from pathlib import Path

from rare_species_map.config import DATA_PROCESSED, DATA_RAW, DATA_TILES


DEFAULT_LAYER_NAME = "rare_species_cells"


def resolved_path(path: str) -> Path:
    return Path(path).resolve()


def filter_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Filter GBIF TSV and export optimized Parquet"
    )
    parser.add_argument("--input", default=str(DATA_RAW), help="Path to GBIF TSV file")
    parser.add_argument(
        "--output",
        default=str(DATA_PROCESSED / "observations_filtered.parquet"),
        help="Output parquet path",
    )
    parser.add_argument(
        "--encoding",
        default="auto",
        help="Input text encoding (default: auto)",
    )
    parser.add_argument(
        "--country-code",
        default=None,
        help="Optional ISO 3166-1 alpha-2 countryCode filter, for example IT.",
    )
    return parser


def occupancy_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compute species occupancy and rarity from filtered observations"
    )
    parser.add_argument(
        "--input",
        default=str(DATA_PROCESSED / "observations_filtered.parquet"),
        help="Input filtered observations parquet path",
    )
    parser.add_argument(
        "--output",
        default=str(DATA_PROCESSED / "species_occupancy.parquet"),
        help="Output species occupancy parquet path",
    )
    return parser


def cell_scores_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compute H3 cell rarity residual scores"
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
        help="Output H3 cell scores parquet directory",
    )
    parser.add_argument(
        "--summary-output-dir",
        default=str(DATA_TILES),
        help="Output directory for cell score summary JSON files",
    )
    parser.add_argument(
        "--diagnostics-output-dir",
        default=str(DATA_PROCESSED / "diagnostics" / "cell_scores"),
        help="Output directory for diagnostic plots",
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
    return parser


def pmtiles_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate PMTiles vector tiles from H3 cell score parquet"
    )
    parser.add_argument(
        "--input",
        default=str(DATA_PROCESSED),
        help="Input H3 cell scores parquet directory",
    )
    parser.add_argument(
        "--output",
        default=str(DATA_TILES),
        help="Output PMTiles directory",
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
    return parser


def run_pipeline_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the full rare species map tile generation pipeline"
    )
    parser.add_argument(
        "--input",
        default=str(DATA_RAW),
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
        default=str(DATA_PROCESSED),
        help="Step 3 output parquet directory",
    )
    parser.add_argument(
        "--cell-scores-summary-output",
        default=str(DATA_TILES),
        help="Step 3 summary JSON output directory",
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
        default=str(DATA_TILES),
        help="Step 4 output PMTiles directory",
    )
    parser.add_argument(
        "--encoding",
        default="auto",
        help="Input text encoding for Step 1 (default: auto)",
    )
    parser.add_argument(
        "--country-code",
        default=None,
        help="Optional Step 1 ISO 3166-1 alpha-2 countryCode filter.",
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
        help="Deprecated compatibility option; zoom ranges come from config.py.",
    )
    parser.add_argument(
        "--max-zoom",
        type=int,
        default=12,
        help="Deprecated compatibility option; zoom ranges come from config.py.",
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
        help="Deprecated compatibility option; freestiler chooses its base zoom.",
    )
    parser.add_argument(
        "--drop-rate",
        type=float,
        default=None,
        help="Optional Step 4 exponential feature thinning rate",
    )
    parser.add_argument("--coalesce", action="store_true", help="Pass --coalesce to Step 4")
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
        help="Deprecated compatibility option; temporary GeoJSONSeq paths are generated.",
    )
    parser.add_argument(
        "--quiet-tiles",
        action="store_true",
        help="Pass --quiet to Step 4 freestiler",
    )
    return parser


def main_filter_to_parquet(argv: list[str] | None = None) -> None:
    args = filter_parser().parse_args(argv)

    from rare_species_map.filtering import (
        FilterObservationsConfig,
        detect_encoding,
        filter_observations_to_parquet,
        get_filtered_observation_stats,
    )

    input_path = resolved_path(args.input)
    output_path = resolved_path(args.output)
    encoding = detect_encoding(input_path, args.encoding)

    print()
    print("=== GBIF Filter Pipeline ===")
    print(f"Input  : {input_path}")
    print(f"Output : {output_path}")
    print(f"Encoding: {encoding}")
    if args.country_code is not None:
        print(f"Country: {args.country_code.strip().upper()}")
    print()
    print("Running DuckDB query...")
    print()

    filter_observations_to_parquet(
        FilterObservationsConfig(
            input_path=input_path,
            output_path=output_path,
            encoding=args.encoding,
            country_code=args.country_code,
        )
    )

    print("Parquet generation completed.")
    stats = get_filtered_observation_stats(output_path)
    print()
    print("=== Dataset statistics ===")
    print(f"Observations : {stats.n_observations:,}")
    print(f"Species      : {stats.n_species:,}")


def main_compute_species_occupancy(argv: list[str] | None = None) -> None:
    args = occupancy_parser().parse_args(argv)

    from rare_species_map.occupancy import (
        SpeciesOccupancyConfig,
        compute_species_occupancy,
        get_species_occupancy_stats,
    )

    input_path = resolved_path(args.input)
    output_path = resolved_path(args.output)

    print()
    print("=== Species Occupancy Pipeline ===")
    print(f"Input  : {input_path}")
    print(f"Output : {output_path}")
    print()
    print("Running DuckDB query...")
    print()

    config = SpeciesOccupancyConfig(input_path=input_path, output_path=output_path)
    compute_species_occupancy(config)

    print("Species occupancy generation completed.")
    stats = get_species_occupancy_stats(output_path)
    print()
    print("=== Species occupancy statistics ===")
    print(f"H3 resolution  : {config.h3_resolution}")
    print(f"Species        : {stats.n_species:,}")
    print(f"Min occupancy  : {stats.min_occupancy:,}")
    print(f"Avg occupancy  : {stats.avg_occupancy:,.2f}")
    print(f"Max occupancy  : {stats.max_occupancy:,}")
    print(f"Min rarity     : {stats.min_rarity:.8f}")
    print(f"Avg rarity     : {stats.avg_rarity:.8f}")
    print(f"Max rarity     : {stats.max_rarity:.8f}")


def main_compute_cell_scores(argv: list[str] | None = None) -> None:
    args = cell_scores_parser().parse_args(argv)

    from rare_species_map.cell_scores import (
        CellScoreConfig,
        export_scores_to_parquet,
        fetch_cell_data,
        summarize_cell_scores,
        write_cell_score_summary,
    )

    config = CellScoreConfig(
        observations_path=resolved_path(args.observations),
        species_occupancy_path=resolved_path(args.species_occupancy),
        output_dir=resolved_path(args.output),
        summary_output_dir=resolved_path(args.summary_output_dir),
        diagnostics_output_dir=resolved_path(args.diagnostics_output_dir),
        diagnostics_sample_size=args.diagnostics_sample_size,
        write_diagnostics=not args.no_diagnostics,
    )

    print()
    print("=== H3 Cell Score Pipeline ===")
    print(f"Observations      : {config.observations_path}")
    print(f"Species occupancy : {config.species_occupancy_path}")
    print(f"Output            : {config.output_dir}")
    print()

    if not config.observations_path.exists():
        raise FileNotFoundError(f"Observations parquet not found: {config.observations_path}")
    if not config.species_occupancy_path.exists():
        raise FileNotFoundError(
            f"Species occupancy parquet not found: {config.species_occupancy_path}"
        )

    for resolution in config.h3_resolutions:
        print(f"Fetching cell aggregates from DuckDB, resolution {resolution}...")
        cell_scores, elapsed = fetch_cell_data(
            resolution=resolution,
            observations_path=config.observations_path,
            species_occupancy_path=config.species_occupancy_path,
        )
        print()
        print(f"  Query executed in {elapsed:.1f} seconds.")
        print(f"  Cells loaded: {len(cell_scores.h3_indices):,}")
        print("Exporting results to Parquet...")
        output_path = export_scores_to_parquet(
            resolution=resolution,
            output_dir=config.output_dir,
            cell_scores=cell_scores,
        )
        print(f"  Written to: {output_path}")

        quantiles = summarize_cell_scores(cell_scores)
        write_cell_score_summary(
            resolution=resolution,
            summary_output_dir=config.summary_output_dir,
            quantiles=quantiles,
        )

        print()
        print(f" Rarity z-score quantile 0.025: {quantiles.rarity_quantiles[0]:.4f}")
        print(f" Rarity z-score quantile 0.975: {quantiles.rarity_quantiles[3]:.4f}")
        print(
            " Count observations quantile 0.975: "
            f"{quantiles.count_observations_quantiles[2]:.0f}"
        )
        print(
            f" Count species quantile 0.975: {quantiles.count_species_quantiles[2]:.0f}"
        )
        print(
            f" Count observers quantile 0.975: {quantiles.count_observers_quantiles[2]:.0f}"
        )
        print(
            " Confidence scores quantile 0.025: "
            f"{quantiles.confidence_scores_quantiles[0]:.4f}"
        )
        print(
            " Confidence scores quantile 0.975: "
            f"{quantiles.confidence_scores_quantiles[2]:.4f}"
        )

    print()
    print("H3 cell score generation completed.")


def main_generate_pmtiles(argv: list[str] | None = None) -> None:
    args = pmtiles_parser().parse_args(argv)

    from rare_species_map.tiles import (
        TileGenerationConfig,
        export_geojsonseq,
        run_freestiler,
    )

    config = TileGenerationConfig(
        input_dir=resolved_path(args.input),
        output_dir=resolved_path(args.output),
        layer_name=args.layer,
        batch_size=args.batch_size,
        tile_format=args.tile_format,
        drop_rate=args.drop_rate,
        coalesce=args.coalesce,
        simplification=not args.no_simplification,
        keep_geojsonseq=args.keep_geojsonseq,
        geojsonseq_only=args.geojsonseq_only,
        quiet=args.quiet,
    )

    if config.drop_rate is not None and config.drop_rate <= 0:
        raise ValueError("--drop-rate must be greater than 0")

    print()
    print("=== PMTiles Generation Pipeline ===")

    for resolution, zoom_range in zip(config.h3_resolutions, config.h3_zoom_ranges):
        min_zoom, max_zoom = zoom_range
        input_path = config.input_dir / f"cell_scores{resolution}.parquet"
        output_path = config.output_dir / f"rare_species_cells{resolution}.pmtiles"

        if not input_path.exists():
            raise FileNotFoundError(f"Cell scores parquet not found: {input_path}")
        if min_zoom < 0 or max_zoom < min_zoom:
            raise ValueError("max-zoom must be greater than or equal to min-zoom")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        import tempfile

        temp_file = tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".geojsonseq",
            prefix=f"rare_species_cells_{resolution}_",
            dir=output_path.parent,
            delete=False,
        )
        temp_file.close()
        geojsonseq_path = Path(temp_file.name)

        print()
        print(f"Input       : {input_path}")
        print(f"GeoJSONSeq  : {geojsonseq_path}")
        print(f"Output      : {output_path}")
        print(f"Layer       : {config.layer_name}")
        print(f"H3 res      : {resolution}")
        print(f"Zooms       : {min_zoom}-{max_zoom}")
        print(f"Tile format : {config.tile_format}")
        print()

        try:
            print("Exporting H3 cells to GeoJSONSeq...")
            n_features = export_geojsonseq(
                input_path=input_path,
                output_path=geojsonseq_path,
                resolution=resolution,
                batch_size=config.batch_size,
            )
            print(f"Exported features: {n_features:,}")
            print()

            if config.geojsonseq_only:
                print("GeoJSONSeq export completed.")
                break

            print("Running freestiler...")
            print()
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
            print()
            print("PMTiles generation completed.")
        finally:
            if not config.keep_geojsonseq:
                geojsonseq_path.unlink(missing_ok=True)


def main_run_pipeline(argv: list[str] | None = None) -> None:
    args = run_pipeline_parser().parse_args(argv)

    from rare_species_map.pipeline import PipelineConfig, run_pipeline

    config = PipelineConfig(
        input_path=resolved_path(args.input),
        observations_output_path=resolved_path(args.observations_output),
        species_occupancy_output_path=resolved_path(args.species_occupancy_output),
        cell_scores_output_dir=resolved_path(args.cell_scores_output),
        cell_scores_summary_output_dir=resolved_path(args.cell_scores_summary_output),
        diagnostics_output_dir=resolved_path(args.diagnostics_output_dir),
        diagnostics_sample_size=args.diagnostics_sample_size,
        write_diagnostics=not args.no_diagnostics,
        tiles_output_dir=resolved_path(args.tiles_output),
        encoding=args.encoding,
        start_at=args.start_at,
        stop_after=args.stop_after,
        tile_format=args.tile_format,
        drop_rate=args.drop_rate,
        coalesce=args.coalesce,
        simplification=not args.no_simplification,
        keep_geojsonseq=args.keep_geojsonseq,
        quiet_tiles=args.quiet_tiles,
        country_code=args.country_code,
    )

    print()
    print("=== Rare Species Map Tile Pipeline ===")
    print(f"Running steps {config.start_at}-{config.stop_after}", flush=True)

    pipeline_start = time.monotonic()

    def print_progress(step: int, label: str) -> None:
        print()
        print(f"=== Step {step}: {label} ===")
        print(flush=True)

    results = run_pipeline(config, progress=print_progress)

    for result in results:
        print()
        print(
            f"Step {result.step} completed in {result.elapsed_seconds / 60:.1f} minutes.",
            flush=True,
        )

    elapsed = time.monotonic() - pipeline_start
    print()
    print("Pipeline completed.")
    print(f"Total elapsed time: {elapsed / 60:.1f} minutes.")
