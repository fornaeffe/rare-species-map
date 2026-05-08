# pipeline/scripts/03_compute_cell_scores.py

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any

import numpy as np

PIPELINE_ROOT = Path(__file__).resolve().parents[1]
if str(PIPELINE_ROOT) not in sys.path:
    sys.path.insert(0, str(PIPELINE_ROOT))

from rare_species_map.config import DATA_PROCESSED, H3_VISUALIZATION_RESOLUTION
from rare_species_map.duckdb_utils import get_connection
from rare_species_map.gam_scorer import compute_cell_scores


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
        default=str(DATA_PROCESSED / "cell_scores.parquet"),
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
    observations_path: Path,
    species_occupancy_path: Path,
) -> str:
    """
    Build DuckDB query to aggregate cell statistics.

    Returns a query that selects all cells with their metrics.
    This is used to extract data for Python-based GAM fitting.
    """
    return f"""
    WITH observations AS (
        SELECT
            h3_resHigh,
            speciesKey,
            recordedBy
        FROM parquet_scan('{observations_path.as_posix()}')
        WHERE
            h3_resHigh IS NOT NULL
            AND speciesKey IS NOT NULL
    ),

    observations_by_cell AS (
        SELECT
            h3_resHigh,
            COUNT(*) AS count_observations
        FROM observations
        GROUP BY h3_resHigh
    ),

    species_by_cell AS (
        SELECT DISTINCT
            h3_resHigh,
            speciesKey
        FROM observations
    ),

    rarity_by_cell AS (
        SELECT
            species_by_cell.h3_resHigh,
            COUNT(*) AS count_species,
            SUM(species_occupancy.rarity) AS sum_rarity
        FROM species_by_cell
        INNER JOIN parquet_scan('{species_occupancy_path.as_posix()}') AS species_occupancy
            ON species_by_cell.speciesKey = species_occupancy.speciesKey
        GROUP BY species_by_cell.h3_resHigh
    ),

    observer_counts_by_cell AS (
        SELECT
            h3_resHigh,
            recordedBy,
            COUNT(*) AS obs_by_observer
        FROM observations
        GROUP BY h3_resHigh, recordedBy
    ),

    shannon_by_cell AS (
        SELECT
            o.h3_resHigh,
            -SUM( (o.obs_by_observer::DOUBLE / ob.count_observations) * LN(o.obs_by_observer::DOUBLE / ob.count_observations) ) AS shannon_H
        FROM observer_counts_by_cell o
        JOIN observations_by_cell ob
            ON o.h3_resHigh = ob.h3_resHigh
        GROUP BY o.h3_resHigh
    ),

    neff_by_cell AS (
        SELECT
            h3_resHigh,
            shannon_H,
            EXP(shannon_H) AS neff_observers
        FROM shannon_by_cell
    ),

    confidence_by_cell AS (
        SELECT
            h3_resHigh,
            neff_observers,
            1 - EXP( - neff_observers / 4 ) AS confidence_score
        FROM neff_by_cell
    )



    SELECT
        observations_by_cell.h3_resHigh,
        observations_by_cell.count_observations,
        rarity_by_cell.count_species,
        rarity_by_cell.sum_rarity,
        confidence_by_cell.confidence_score
    FROM observations_by_cell
    INNER JOIN rarity_by_cell
        ON observations_by_cell.h3_resHigh = rarity_by_cell.h3_resHigh
    LEFT JOIN confidence_by_cell
        ON observations_by_cell.h3_resHigh = confidence_by_cell.h3_resHigh
    WHERE
        observations_by_cell.count_observations > 0
        AND rarity_by_cell.sum_rarity > 0
    ORDER BY observations_by_cell.h3_resHigh
    """


def fetch_cell_data(
    observations_path: Path,
    species_occupancy_path: Path,
) -> tuple[list[int], np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """
    Fetch aggregated cell statistics from DuckDB.

    Returns:
        (h3_indices, count_observations, count_species, sum_rarity, confidence_scores)
    """
    con = get_connection()
    query = build_aggregation_query(observations_path, species_occupancy_path)

    result = con.execute(query).fetchall()
    con.close()

    if not result:
        raise ValueError("No cells found in aggregation query")

    h3_indices = []
    count_observations = []
    count_species = []
    sum_rarity = []
    confidence_scores = []

    for row in result:
        h3_indices.append(row[0])
        count_observations.append(row[1])
        count_species.append(row[2])
        sum_rarity.append(row[3])
        confidence_scores.append(row[4] if row[4] is not None else 0.0)

    return (
        h3_indices,
        np.array(count_observations, dtype=np.float64),
        np.array(count_species, dtype=np.float64),
        np.array(sum_rarity, dtype=np.float64),
        np.array(confidence_scores, dtype=np.float64)
    )


def export_scores_to_parquet(
    output_path: Path,
    h3_indices: list[int],
    count_observations: np.ndarray,
    count_species: np.ndarray,
    sum_rarity: np.ndarray,
    gam_result: Any,
    confidence_scores: np.ndarray,
) -> None:
    """
    Export GAM-computed scores to Parquet file.

    Saves all original columns plus GAM-computed columns.
    Multiplies rarity_zscore by confidence_score for each cell.
    """
    try:
        import pyarrow as pa
    except ImportError as exc:
        raise RuntimeError("Exporting to Parquet requires pyarrow") from exc

    # Build arrays for output
    n = len(h3_indices)

    # Log-transformed inputs (for reference/diagnostics)
    log_count_observations = np.log(count_observations + 1.0)
    log_sum_rarity = np.log(sum_rarity + 1e-8)

    # GAM outputs
    expected_log_sum_rarity = gam_result.fitted_values
    residuals = gam_result.residuals
    residual_std = gam_result.residual_std
    zscores = gam_result.zscores

    # Apply confidence weighting to z-scores
    weighted_zscores = zscores * confidence_scores

    # Create PyArrow table
    table = pa.table(
        {
            "h3_resHigh": pa.array(h3_indices, type=pa.uint64()),
            "count_observations": pa.array(count_observations, type=pa.uint64()),
            "count_species": pa.array(count_species, type=pa.uint64()),
            "sum_rarity": pa.array(sum_rarity, type=pa.float64()),
            "log_count_observations": pa.array(log_count_observations, type=pa.float64()),
            "log_sum_rarity": pa.array(log_sum_rarity, type=pa.float64()),
            "expected_log_sum_rarity": pa.array(expected_log_sum_rarity, type=pa.float64()),
            "residual": pa.array(residuals, type=pa.float64()),
            "residual_std": pa.array(residual_std, type=pa.float64()),
            "rarity_zscore": pa.array(weighted_zscores, type=pa.float64()),
        }
    )

    # Write to Parquet with compression
    import pyarrow.parquet as pq

    output_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, output_path, compression="zstd", row_group_size=100000)


def print_stats(
    h3_indices: list[int],
    count_observations: np.ndarray,
    sum_rarity: np.ndarray,
    gam_result: Any,
) -> None:
    """Print summary statistics about the scoring results."""
    n_cells = len(h3_indices)
    residuals = gam_result.residuals
    zscores = gam_result.zscores

    print()
    print("=== H3 cell score statistics (GAM-based) ===")
    print(f"H3 resolution         : {H3_VISUALIZATION_RESOLUTION}")
    print(f"Cells                 : {n_cells:,}")
    print(f"Min observations      : {int(np.min(count_observations)):,}")
    print(f"Avg observations      : {np.mean(count_observations):,.2f}")
    print(f"Max observations      : {int(np.max(count_observations)):,}")
    print(f"Min sum rarity        : {np.min(sum_rarity):.8f}")
    print(f"Avg sum rarity        : {np.mean(sum_rarity):.8f}")
    print(f"Max sum rarity        : {np.max(sum_rarity):.8f}")
    print()
    print("=== Residual scores ===")
    print(f"Min residual          : {np.min(residuals):.8f}")
    print(f"Avg residual          : {np.mean(residuals):.8f}")
    print(f"Max residual          : {np.max(residuals):.8f}")
    print(f"Std residual          : {np.std(residuals):.8f}")
    print()
    print("=== Z-scores (standardized) ===")
    print(f"Min z-score           : {np.min(zscores):.8f}")
    print(f"Avg z-score           : {np.mean(zscores):.8f}")
    print(f"Max z-score           : {np.max(zscores):.8f}")
    print(f"Std z-score           : {np.std(zscores):.8f}")
    print()
    print("=== GAM Model ===")
    print("Model                 : GAM smooth spline")
    print("Formula               : y ~ s(x)")
    print("y                     : log(sum_rarity + epsilon)")
    print("x                     : log(count_observations + 1)")
    print("Score                 : (y - fitted) / local_residual_std")


def fetch_diagnostics_data(
    output_path: Path,
    sample_size: int,
) -> tuple[dict[str, Any], dict[str, np.ndarray]]:
    """
    Fetch sampled data for diagnostic plots from output Parquet.
    """
    if sample_size <= 0:
        raise ValueError("--diagnostics-sample-size must be greater than 0")

    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("Diagnostics require pyarrow") from exc

    # Read entire table (or sample if large)
    table = pq.read_table(output_path)
    n_total = len(table)

    if n_total > sample_size:
        # Random sample
        indices = np.random.choice(n_total, size=sample_size, replace=False)
        table = table.take(indices)

    # Convert to numpy arrays
    data = {
        "count_observations": table["count_observations"].to_numpy(zero_copy_only=False),
        "sum_rarity": table["sum_rarity"].to_numpy(zero_copy_only=False),
        "log_count_observations": table["log_count_observations"].to_numpy(zero_copy_only=False),
        "log_sum_rarity": table["log_sum_rarity"].to_numpy(zero_copy_only=False),
        "expected_log_sum_rarity": table["expected_log_sum_rarity"].to_numpy(zero_copy_only=False),
        "residual": table["residual"].to_numpy(zero_copy_only=False),
        "residual_std": table["residual_std"].to_numpy(zero_copy_only=False),
        "rarity_zscore": table["rarity_zscore"].to_numpy(zero_copy_only=False),
    }

    summary = {
        "n_cells": n_total,
        "sample_cells": len(table),
        "model_type": "GAM smooth spline",
    }

    return summary, data


def generate_diagnostic_plots(
    output_path: Path,
    diagnostics_output_dir: Path,
    sample_size: int,
) -> None:
    """Generate diagnostic plots for GAM scoring."""
    try:
        matplotlib_config_dir = PIPELINE_ROOT / ".matplotlib"
        matplotlib_config_dir.mkdir(parents=True, exist_ok=True)
        os.environ["MPLCONFIGDIR"] = str(matplotlib_config_dir)

        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from scipy import interpolate, stats
    except ImportError as exc:
        raise RuntimeError(
            "Diagnostic plots require matplotlib, scipy. "
            "Run `uv sync` in the pipeline directory."
        ) from exc

    diagnostics_output_dir.mkdir(parents=True, exist_ok=True)

    summary, data = fetch_diagnostics_data(
        output_path=output_path,
        sample_size=sample_size,
    )

    count_observations = data["count_observations"]
    sum_rarity = data["sum_rarity"]
    log_count_observations = data["log_count_observations"]
    log_sum_rarity = data["log_sum_rarity"]
    expected_log_sum_rarity = data["expected_log_sum_rarity"]
    residuals = data["residual"]
    residual_std = data["residual_std"]
    zscores = data["rarity_zscore"]

    # Filter finite values
    finite_mask = (
        np.isfinite(count_observations)
        & np.isfinite(sum_rarity)
        & np.isfinite(log_count_observations)
        & np.isfinite(log_sum_rarity)
        & np.isfinite(expected_log_sum_rarity)
        & np.isfinite(residuals)
        & np.isfinite(zscores)
    )

    count_observations = count_observations[finite_mask]
    sum_rarity = sum_rarity[finite_mask]
    log_count_observations = log_count_observations[finite_mask]
    log_sum_rarity = log_sum_rarity[finite_mask]
    expected_log_sum_rarity = expected_log_sum_rarity[finite_mask]
    residuals = residuals[finite_mask]
    residual_std = residual_std[finite_mask]
    zscores = zscores[finite_mask]

    plt.style.use("default")

    # Plot 1: Observed vs fitted in original space
    fig, ax = plt.subplots(figsize=(9, 6))
    ax.scatter(count_observations, sum_rarity, s=5, alpha=0.18, linewidths=0)
    ax.set_title("sum_rarity vs count_observations (observed)")
    ax.set_xlabel("count_observations")
    ax.set_ylabel("sum_rarity")
    ax.grid(True, alpha=0.25)
    fig.tight_layout()
    fig.savefig(diagnostics_output_dir / "observed_vs_count.png", dpi=160)
    plt.close(fig)

    # Plot 2: Log-space scatter with GAM fit
    fig, ax = plt.subplots(figsize=(9, 6))
    ax.scatter(log_count_observations, log_sum_rarity, s=5, alpha=0.18, linewidths=0, label="Observed")

    # Plot fitted spline
    if len(log_count_observations) > 1:
        sort_idx = np.argsort(log_count_observations)
        x_sorted = log_count_observations[sort_idx]
        y_fitted_sorted = expected_log_sum_rarity[sort_idx]

        # Smooth the fitted line for visualization
        try:
            tck = interpolate.splrep(x_sorted, y_fitted_sorted, s=len(x_sorted) * 0.01)
            x_fine = np.linspace(x_sorted[0], x_sorted[-1], 300)
            y_fine = interpolate.splev(x_fine, tck)
            ax.plot(x_fine, y_fine, color="#d62728", linewidth=2, label="GAM fit")
        except Exception:
            ax.plot(x_sorted, y_fitted_sorted, color="#d62728", linewidth=2, label="GAM fit")

    ax.set_title("log(sum_rarity) vs log(count_observations) + GAM fit")
    ax.set_xlabel("log(count_observations + 1)")
    ax.set_ylabel("log(sum_rarity + epsilon)")
    ax.grid(True, alpha=0.25)
    ax.legend(loc="best")
    fig.tight_layout()
    fig.savefig(diagnostics_output_dir / "gam_fit_logspace.png", dpi=160)
    plt.close(fig)

    # Plot 3: Residuals vs fitted values
    fig, ax = plt.subplots(figsize=(9, 6))
    ax.scatter(expected_log_sum_rarity, residuals, s=5, alpha=0.18, linewidths=0)
    ax.axhline(y=0, color="#d62728", linewidth=2, linestyle="--", label="y=0")
    ax.set_title("Residuals vs Fitted values (GAM)")
    ax.set_xlabel("Fitted values (expected log(sum_rarity))")
    ax.set_ylabel("Residuals")
    ax.grid(True, alpha=0.25)
    ax.legend(loc="best")
    fig.tight_layout()
    fig.savefig(diagnostics_output_dir / "residuals_vs_fitted.png", dpi=160)
    plt.close(fig)

    # Plot 4: Residual variance vs x
    fig, ax = plt.subplots(figsize=(9, 6))
    ax.scatter(log_count_observations, np.abs(residuals), s=5, alpha=0.18, linewidths=0, label="Abs residuals")

    # Plot estimated local std
    if len(log_count_observations) > 1:
        sort_idx = np.argsort(log_count_observations)
        x_sorted = log_count_observations[sort_idx]
        std_sorted = residual_std[sort_idx]

        try:
            tck = interpolate.splrep(x_sorted, std_sorted, s=len(x_sorted) * 0.01)
            x_fine = np.linspace(x_sorted[0], x_sorted[-1], 300)
            std_fine = interpolate.splev(x_fine, tck)
            ax.plot(x_fine, std_fine, color="#d62728", linewidth=2, label="Local residual std")
        except Exception:
            ax.plot(x_sorted, std_sorted, color="#d62728", linewidth=2, label="Local residual std")

    ax.set_title("Residual magnitude vs fitted x")
    ax.set_xlabel("log(count_observations + 1)")
    ax.set_ylabel("|Residual| and local std")
    ax.grid(True, alpha=0.25)
    ax.legend(loc="best")
    fig.tight_layout()
    fig.savefig(diagnostics_output_dir / "residual_variance_vs_x.png", dpi=160)
    plt.close(fig)

    # Plot 5: Histogram of z-scores
    fig, ax = plt.subplots(figsize=(9, 6))
    ax.hist(zscores, bins=50, alpha=0.7, edgecolor="black")
    ax.axvline(x=0, color="#d62728", linewidth=2, linestyle="--", label="x=0")
    ax.set_title("Distribution of standardized z-scores")
    ax.set_xlabel("Z-score (residual / local_std)")
    ax.set_ylabel("Frequency")
    ax.grid(True, alpha=0.25, axis="y")
    ax.legend(loc="best")
    fig.tight_layout()
    fig.savefig(diagnostics_output_dir / "zscores_histogram.png", dpi=160)
    plt.close(fig)

    # Plot 6: Q-Q plot of z-scores
    fig, ax = plt.subplots(figsize=(7, 7))
    zscores_finite = zscores[np.isfinite(zscores)]
    if len(zscores_finite) > 1:
        zscores_sorted = np.sort(zscores_finite)
        probabilities = (np.arange(1, len(zscores_sorted) + 1) - 0.5) / len(zscores_sorted)
        theoretical = stats.norm.ppf(probabilities)

        ax.scatter(theoretical, zscores_sorted, s=5, alpha=0.18, linewidths=0)

        # Reference line
        min_val = float(min(np.min(theoretical), np.min(zscores_sorted)))
        max_val = float(max(np.max(theoretical), np.max(zscores_sorted)))
        ax.plot([min_val, max_val], [min_val, max_val], color="#d62728", linewidth=2)

    ax.set_title("Q-Q plot of standardized z-scores")
    ax.set_xlabel("Theoretical normal quantiles")
    ax.set_ylabel("Observed z-score quantiles")
    ax.grid(True, alpha=0.25)
    fig.tight_layout()
    fig.savefig(diagnostics_output_dir / "zscores_qqplot.png", dpi=160)
    plt.close(fig)

    # Write summary
    summary_path = diagnostics_output_dir / "diagnostics_summary.txt"
    summary_path.write_text(
        "\n".join(
            [
                "=== GAM Smooth Spline Scoring Model ===",
                f"Formula: y ~ s(x)",
                f"  y = log(sum_rarity + epsilon)",
                f"  x = log(count_observations + 1)",
                f"Score = (y - fitted) / local_residual_std",
                "",
                f"Cells in output: {summary['n_cells']:,}",
                f"Cells sampled: {summary['sample_cells']:,}",
                "",
                "Residual statistics:",
                f"  Min: {np.min(residuals):.8f}",
                f"  Mean: {np.mean(residuals):.8f}",
                f"  Max: {np.max(residuals):.8f}",
                f"  Std: {np.std(residuals):.8f}",
                "",
                "Z-score statistics:",
                f"  Min: {np.min(zscores):.8f}",
                f"  Mean: {np.mean(zscores):.8f}",
                f"  Max: {np.max(zscores):.8f}",
                f"  Std: {np.std(zscores):.8f}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    print()
    print("=== Diagnostic plots ===")
    print(f"Output directory : {diagnostics_output_dir}")
    print(f"Sampled cells    : {summary['sample_cells']:,}")
    print("Plots            :")
    print("  observed_vs_count.png")
    print("  gam_fit_logspace.png")
    print("  residuals_vs_fitted.png")
    print("  residual_variance_vs_x.png")
    print("  zscores_histogram.png")
    print("  zscores_qqplot.png")
    print("  diagnostics_summary.txt")


def main() -> None:
    args = parse_args()

    observations_path = Path(args.observations).resolve()
    species_occupancy_path = Path(args.species_occupancy).resolve()
    output_path = Path(args.output).resolve()
    diagnostics_output_dir = Path(args.diagnostics_output_dir).resolve()

    if not observations_path.exists():
        raise FileNotFoundError(f"Observations parquet not found: {observations_path}")

    if not species_occupancy_path.exists():
        raise FileNotFoundError(
            f"Species occupancy parquet not found: {species_occupancy_path}"
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)

    print()
    print("=== H3 Cell Score Pipeline (GAM) ===")
    print(f"Observations      : {observations_path}")
    print(f"Species occupancy : {species_occupancy_path}")
    print(f"Output            : {output_path}")
    print()

    # Fetch aggregated cell data from DuckDB
    print("Fetching cell aggregates from DuckDB...")
    h3_indices, count_observations, count_species, sum_rarity, confidence_scores = fetch_cell_data(
        observations_path=observations_path,
        species_occupancy_path=species_occupancy_path,
    )
    print(f"  Cells loaded: {len(h3_indices):,}")

    # Fit GAM model
    print("Fitting GAM smooth spline model...")
    gam_result = compute_cell_scores(count_observations, sum_rarity)
    print(f"  GAM model fitted successfully")

    # Export results to Parquet
    print("Exporting results to Parquet...")
    export_scores_to_parquet(
        output_path=output_path,
        h3_indices=h3_indices,
        count_observations=count_observations,
        count_species=count_species,
        sum_rarity=sum_rarity,
        gam_result=gam_result,
        confidence_scores=confidence_scores,
    )
    print(f"  Written to: {output_path}")

    print()
    print("H3 cell score generation completed.")

    # Print statistics
    print_stats(h3_indices, count_observations, sum_rarity, gam_result)

    # Generate diagnostics if requested
    if not args.no_diagnostics:
        generate_diagnostic_plots(
            output_path=output_path,
            diagnostics_output_dir=diagnostics_output_dir,
            sample_size=args.diagnostics_sample_size,
        )


if __name__ == "__main__":
    main()
