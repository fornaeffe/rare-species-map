# pipeline/scripts/03_compute_cell_scores.py

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any

PIPELINE_ROOT = Path(__file__).resolve().parents[1]
if str(PIPELINE_ROOT) not in sys.path:
    sys.path.insert(0, str(PIPELINE_ROOT))

from rare_species_map.config import DATA_PROCESSED, H3_VISUALIZATION_RESOLUTION
from rare_species_map.duckdb_utils import get_connection


DEFAULT_DIAGNOSTICS_DIR = DATA_PROCESSED / "diagnostics" / "cell_scores"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute H3 cell rarity residual scores from observations and species rarity"
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
        help="Output directory for regression diagnostic plots",
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


def build_query(
    observations_path: Path,
    species_occupancy_path: Path,
    output_path: Path,
) -> str:
    return f"""
    COPY (
        WITH observations AS (
            SELECT
                h3_resHigh,
                speciesKey
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

        cell_metrics AS (
            SELECT
                observations_by_cell.h3_resHigh,
                observations_by_cell.count_observations,
                rarity_by_cell.count_species,
                rarity_by_cell.sum_rarity,
                ln(observations_by_cell.count_observations::DOUBLE) AS log_count_observations,
                ln(rarity_by_cell.sum_rarity) AS log_sum_rarity
            FROM observations_by_cell
            INNER JOIN rarity_by_cell
                ON observations_by_cell.h3_resHigh = rarity_by_cell.h3_resHigh
            WHERE
                observations_by_cell.count_observations > 0
                AND rarity_by_cell.sum_rarity > 0
        ),

        regression AS (
            SELECT
                regr_intercept(log_sum_rarity, log_count_observations) AS intercept,
                regr_slope(log_sum_rarity, log_count_observations) AS slope,
                corr(log_sum_rarity, log_count_observations) AS correlation,
                COUNT(*) AS regression_n_cells
            FROM cell_metrics
        )

        SELECT
            cell_metrics.h3_resHigh,
            cell_metrics.count_observations,
            cell_metrics.count_species,
            cell_metrics.sum_rarity,
            cell_metrics.log_count_observations,
            cell_metrics.log_sum_rarity,
            regression.intercept AS regression_intercept,
            regression.slope AS regression_slope,
            regression.correlation AS regression_correlation,
            regression.regression_n_cells,
            (
                regression.intercept
                + regression.slope * cell_metrics.log_count_observations
            ) AS expected_log_sum_rarity,
            (
                cell_metrics.log_sum_rarity
                - (
                    regression.intercept
                    + regression.slope * cell_metrics.log_count_observations
                )
            ) AS rarity_score
        FROM cell_metrics
        CROSS JOIN regression
    )
    TO '{output_path.as_posix()}'
    (
        FORMAT PARQUET,
        COMPRESSION ZSTD,
        ROW_GROUP_SIZE 100000
    )
    """


def print_stats(output_path: Path) -> None:
    con = get_connection()

    stats_query = f"""
    SELECT
        COUNT(*) AS n_cells,
        MIN(count_observations) AS min_observations,
        AVG(count_observations) AS avg_observations,
        MAX(count_observations) AS max_observations,
        MIN(sum_rarity) AS min_sum_rarity,
        AVG(sum_rarity) AS avg_sum_rarity,
        MAX(sum_rarity) AS max_sum_rarity,
        MIN(rarity_score) AS min_score,
        AVG(rarity_score) AS avg_score,
        MAX(rarity_score) AS max_score,
        any_value(regression_intercept) AS regression_intercept,
        any_value(regression_slope) AS regression_slope,
        any_value(regression_correlation) AS regression_correlation,
        any_value(regression_n_cells) AS regression_n_cells
    FROM parquet_scan('{output_path.as_posix()}')
    """

    stats = con.execute(stats_query).fetchone()

    print()
    print("=== H3 cell score statistics ===")
    print(f"H3 resolution         : {H3_VISUALIZATION_RESOLUTION}")
    print(f"Cells                 : {stats[0]:,}")
    print(f"Min observations      : {stats[1]:,}")
    print(f"Avg observations      : {stats[2]:,.2f}")
    print(f"Max observations      : {stats[3]:,}")
    print(f"Min sum rarity        : {stats[4]:.8f}")
    print(f"Avg sum rarity        : {stats[5]:.8f}")
    print(f"Max sum rarity        : {stats[6]:.8f}")
    print(f"Min rarity score      : {stats[7]:.8f}")
    print(f"Avg rarity score      : {stats[8]:.8f}")
    print(f"Max rarity score      : {stats[9]:.8f}")
    print()
    print("=== Log-log regression ===")
    print("Model                 : log(sum_rarity) ~ log(count_observations)")
    print(f"Intercept             : {stats[10]:.8f}")
    print(f"Slope                 : {stats[11]:.8f}")
    print(f"Correlation           : {stats[12]:.8f}")
    print(f"Regression cells      : {stats[13]:,}")

    con.close()


def fetch_diagnostics_data(
    output_path: Path,
    sample_size: int,
) -> tuple[dict[str, Any], dict[str, list[float]]]:
    if sample_size <= 0:
        raise ValueError("--diagnostics-sample-size must be greater than 0")

    con = get_connection()

    summary_query = f"""
    SELECT
        COUNT(*) AS n_cells,
        any_value(regression_intercept) AS regression_intercept,
        any_value(regression_slope) AS regression_slope,
        any_value(regression_correlation) AS regression_correlation,
        any_value(regression_n_cells) AS regression_n_cells
    FROM parquet_scan('{output_path.as_posix()}')
    """
    summary_row = con.execute(summary_query).fetchone()

    n_cells = int(summary_row[0])
    summary = {
        "n_cells": n_cells,
        "regression_intercept": float(summary_row[1]),
        "regression_slope": float(summary_row[2]),
        "regression_correlation": float(summary_row[3]),
        "regression_n_cells": int(summary_row[4]),
    }

    if n_cells <= sample_size:
        sample_where = ""
    else:
        sample_where = f"WHERE hash(h3_resHigh) % {n_cells} < {sample_size}"

    sample_query = f"""
    SELECT
        count_observations::DOUBLE AS count_observations,
        sum_rarity::DOUBLE AS sum_rarity,
        log_count_observations::DOUBLE AS log_count_observations,
        log_sum_rarity::DOUBLE AS log_sum_rarity,
        rarity_score::DOUBLE AS rarity_score
    FROM parquet_scan('{output_path.as_posix()}')
    {sample_where}
    """

    table = con.execute(sample_query).to_arrow_table()
    con.close()

    data = {column: table[column].to_pylist() for column in table.column_names}
    summary["sample_cells"] = len(data["rarity_score"])

    return summary, data


def generate_diagnostic_plots(
    output_path: Path,
    diagnostics_output_dir: Path,
    sample_size: int,
) -> None:
    try:
        matplotlib_config_dir = PIPELINE_ROOT / ".matplotlib"
        matplotlib_config_dir.mkdir(parents=True, exist_ok=True)
        os.environ["MPLCONFIGDIR"] = str(matplotlib_config_dir)

        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np
        from scipy import stats
    except ImportError as exc:
        raise RuntimeError(
            "Diagnostic plots require matplotlib, numpy, and scipy. "
            "Run `uv sync` in the pipeline directory after adding matplotlib."
        ) from exc

    diagnostics_output_dir.mkdir(parents=True, exist_ok=True)

    summary, data = fetch_diagnostics_data(
        output_path=output_path,
        sample_size=sample_size,
    )

    count_observations = np.asarray(data["count_observations"], dtype=float)
    sum_rarity = np.asarray(data["sum_rarity"], dtype=float)
    log_count_observations = np.asarray(data["log_count_observations"], dtype=float)
    log_sum_rarity = np.asarray(data["log_sum_rarity"], dtype=float)
    residuals = np.asarray(data["rarity_score"], dtype=float)

    finite_mask = (
        np.isfinite(count_observations)
        & np.isfinite(sum_rarity)
        & np.isfinite(log_count_observations)
        & np.isfinite(log_sum_rarity)
        & np.isfinite(residuals)
    )

    count_observations = count_observations[finite_mask]
    sum_rarity = sum_rarity[finite_mask]
    log_count_observations = log_count_observations[finite_mask]
    log_sum_rarity = log_sum_rarity[finite_mask]
    residuals = residuals[finite_mask]

    intercept = summary["regression_intercept"]
    slope = summary["regression_slope"]

    plt.style.use("default")

    fig, ax = plt.subplots(figsize=(9, 6))
    ax.scatter(count_observations, sum_rarity, s=5, alpha=0.18, linewidths=0)
    if len(count_observations) > 0:
        x_min = max(1.0, float(np.nanmin(count_observations)))
        x_max = float(np.nanmax(count_observations))
        x_line = np.linspace(x_min, x_max, 200)
        y_line = np.exp(intercept) * np.power(x_line, slope)
        ax.plot(
            x_line,
            y_line,
            color="#d62728",
            linewidth=2,
            label="log-log fit back-transformed",
        )
    ax.set_title("sum_rarity vs count_observations")
    ax.set_xlabel("count_observations")
    ax.set_ylabel("sum_rarity")
    ax.grid(True, alpha=0.25)
    ax.legend(loc="best")
    fig.tight_layout()
    fig.savefig(diagnostics_output_dir / "sum_rarity_vs_count_observations.png", dpi=160)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(9, 6))
    ax.scatter(log_count_observations, log_sum_rarity, s=5, alpha=0.18, linewidths=0)
    if len(log_count_observations) > 0:
        x_min = float(np.nanmin(log_count_observations))
        x_max = float(np.nanmax(log_count_observations))
        x_line = np.linspace(x_min, x_max, 200)
        y_line = intercept + slope * x_line
        ax.plot(x_line, y_line, color="#d62728", linewidth=2, label="OLS fit")
    ax.set_title("log(sum_rarity) vs log(count_observations)")
    ax.set_xlabel("log(count_observations)")
    ax.set_ylabel("log(sum_rarity)")
    ax.grid(True, alpha=0.25)
    ax.legend(loc="best")
    fig.tight_layout()
    fig.savefig(
        diagnostics_output_dir / "log_sum_rarity_vs_log_count_observations.png",
        dpi=160,
    )
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(7, 7))
    residuals = residuals[np.isfinite(residuals)]
    if len(residuals) > 1:
        std = np.std(residuals, ddof=1)
        if std > 0:
            standardized = (residuals - np.mean(residuals)) / std
            standardized = np.sort(standardized)
            probabilities = (
                np.arange(1, len(standardized) + 1) - 0.5
            ) / len(standardized)
            theoretical = stats.norm.ppf(probabilities)
            ax.scatter(theoretical, standardized, s=5, alpha=0.18, linewidths=0)
            min_value = float(min(np.min(theoretical), np.min(standardized)))
            max_value = float(max(np.max(theoretical), np.max(standardized)))
            ax.plot(
                [min_value, max_value],
                [min_value, max_value],
                color="#d62728",
                linewidth=2,
            )
    ax.set_title("Q-Q plot of standardized residuals")
    ax.set_xlabel("Theoretical normal quantiles")
    ax.set_ylabel("Observed standardized residual quantiles")
    ax.grid(True, alpha=0.25)
    fig.tight_layout()
    fig.savefig(diagnostics_output_dir / "residuals_qqplot.png", dpi=160)
    plt.close(fig)

    summary_path = diagnostics_output_dir / "diagnostics_summary.txt"
    summary_path.write_text(
        "\n".join(
            [
                "Model: log(sum_rarity) ~ log(count_observations)",
                f"Cells in output: {summary['n_cells']:,}",
                f"Cells sampled: {summary['sample_cells']:,}",
                f"Regression cells: {summary['regression_n_cells']:,}",
                f"Intercept: {intercept:.10f}",
                f"Slope: {slope:.10f}",
                f"Correlation: {summary['regression_correlation']:.10f}",
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
    print("  sum_rarity_vs_count_observations.png")
    print("  log_sum_rarity_vs_log_count_observations.png")
    print("  residuals_qqplot.png")


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
    print("=== H3 Cell Score Pipeline ===")
    print(f"Observations      : {observations_path}")
    print(f"Species occupancy : {species_occupancy_path}")
    print(f"Output            : {output_path}")
    print()

    con = get_connection()
    query = build_query(
        observations_path=observations_path,
        species_occupancy_path=species_occupancy_path,
        output_path=output_path,
    )

    print("Running DuckDB query...")
    print()

    con.execute(query)
    con.close()

    print("H3 cell score generation completed.")

    print_stats(output_path)

    if not args.no_diagnostics:
        generate_diagnostic_plots(
            output_path=output_path,
            diagnostics_output_dir=diagnostics_output_dir,
            sample_size=args.diagnostics_sample_size,
        )


if __name__ == "__main__":
    main()
