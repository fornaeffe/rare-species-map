# pipeline/rare_species_map/gam_scorer.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import numpy as np
from scipy import interpolate

from rare_species_map.config import (
    GAM_LOG_EPSILON,
    GAM_N_SPLINES,
    GAM_ROLLING_WINDOW_SIZE,
    GAM_VARIANCE_METHOD,
)


@dataclass
class GAMFitResult:
    """Result of GAM fitting on cell score data."""

    n_cells: int
    x_data: np.ndarray
    y_data: np.ndarray
    fitted_values: np.ndarray
    residuals: np.ndarray
    residual_std: np.ndarray
    zscores: np.ndarray
    gam_model: Callable[[np.ndarray], np.ndarray]
    model_type: str = "GAM_smooth_spline"


def build_gam_log_data(
    count_observations: np.ndarray,
    sum_rarity: np.ndarray,
    min_observations: int = 1,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Build log-transformed data for GAM fitting.

    x = log(count_observations + 1)
    y = log(sum_rarity + epsilon)

    Args:
        count_observations: Array of observation counts per cell
        sum_rarity: Array of sum_rarity per cell
        min_observations: Minimum observations to include a cell

    Returns:
        (x, y) arrays ready for GAM fitting
    """
    # Filter valid cells
    mask = (count_observations >= min_observations) & (sum_rarity > 0)

    x = np.log(count_observations[mask] + 1.0)
    y = np.log(sum_rarity[mask] + GAM_LOG_EPSILON)

    return x, y


def fit_gam_spline(
    x: np.ndarray,
    y: np.ndarray,
    n_splines: int = GAM_N_SPLINES,
) -> Callable[[np.ndarray], np.ndarray]:
    """
    Fit a smooth spline using scipy.interpolate.

    Uses UnivariateSpline for robustness and simplicity.
    The smoothing parameter is automatically selected based on n_splines.

    Args:
        x: Input features (will be sorted internally)
        y: Target values
        n_splines: Approximate number of spline knots (affects smoothing)

    Returns:
        Callable that predicts y given x
    """
    # Sort by x for spline fitting
    sort_idx = np.argsort(x)
    x_sorted = x[sort_idx]
    y_sorted = y[sort_idx]

    # Remove duplicates at same x value (keep mean)
    unique_x, unique_indices = np.unique(x_sorted, return_inverse=True)
    if len(unique_x) < len(x_sorted):
        # Average y values for duplicate x
        y_unique = np.array([np.mean(y_sorted[unique_indices == i]) for i in range(len(unique_x))])
    else:
        unique_x = x_sorted
        y_unique = y_sorted

    # Fit smoothing spline
    # s parameter: larger s = more smoothing, smaller s = fit closer to data
    # We use: s = n * (1 - k/n)^3 where k is ratio of smoothing
    # Simple heuristic: s = len(unique_x) * 0.1 works well
    smoothing_factor = max(1.0, len(unique_x) * 0.1 / n_splines)

    try:
        spline = interpolate.UnivariateSpline(
            unique_x,
            y_unique,
            s=smoothing_factor,
            k=min(3, len(unique_x) - 1),  # cubic spline or less
        )
    except Exception as e:
        raise RuntimeError(f"Failed to fit spline: {e}") from e

    def predict_fn(x_new: np.ndarray) -> np.ndarray:
        """Predict using fitted spline."""
        return spline(x_new)

    return predict_fn


def estimate_residual_variance(
    x_sorted: np.ndarray,
    residuals_sorted: np.ndarray,
    method: str = GAM_VARIANCE_METHOD,
    window_size: int = GAM_ROLLING_WINDOW_SIZE,
) -> np.ndarray:
    """
    Estimate local residual standard deviation as a function of x.

    This handles heteroscedasticity: residual spread varies with x.

    Args:
        x_sorted: Input x values (must be sorted)
        residuals_sorted: Residuals corresponding to x_sorted
        method: 'rolling_window', 'binning', or 'spline'
        window_size: For rolling_window, the window size

    Returns:
        Array of local standard deviations for each point
    """
    abs_residuals = np.abs(residuals_sorted)

    if method == "rolling_window":
        return _estimate_variance_rolling(abs_residuals, window_size)
    elif method == "binning":
        return _estimate_variance_binning(x_sorted, abs_residuals, n_bins=20)
    elif method == "spline":
        return _estimate_variance_spline(x_sorted, abs_residuals)
    else:
        raise ValueError(f"Unknown variance estimation method: {method}")


def _estimate_variance_rolling(
    abs_residuals: np.ndarray,
    window_size: int,
) -> np.ndarray:
    """
    Estimate standard deviation using rolling window.

    For each point, compute std in a centered window.
    """
    n = len(abs_residuals)
    local_std = np.zeros(n)

    for i in range(n):
        start = max(0, i - window_size // 2)
        end = min(n, i + window_size // 2 + 1)
        local_std[i] = np.std(abs_residuals[start:end], ddof=1)

    # Avoid zero std
    local_std = np.maximum(local_std, 1e-8)

    return local_std


def _estimate_variance_binning(
    x: np.ndarray,
    abs_residuals: np.ndarray,
    n_bins: int = 20,
) -> np.ndarray:
    """
    Estimate standard deviation by binning x into quantiles.

    For each point, return the std of its bin.
    """
    quantiles = np.linspace(0, 1, n_bins + 1)
    bin_edges = np.quantile(x, quantiles)

    # Remove duplicates to avoid empty bins
    bin_edges = np.unique(bin_edges)

    bin_indices = np.digitize(x, bin_edges) - 1
    bin_indices = np.clip(bin_indices, 0, len(bin_edges) - 2)

    local_std = np.zeros(len(x))

    for bin_idx in range(len(bin_edges) - 1):
        mask = bin_indices == bin_idx
        if np.any(mask):
            bin_std = np.std(abs_residuals[mask], ddof=1)
            bin_std = max(bin_std, 1e-8)
            local_std[mask] = bin_std

    return local_std


def _estimate_variance_spline(
    x: np.ndarray,
    abs_residuals: np.ndarray,
) -> np.ndarray:
    """
    Estimate standard deviation using smoothing spline on squared residuals.

    Fit a spline to the squared residuals, then take sqrt.
    """
    # Sort by x
    sort_idx = np.argsort(x)
    x_sorted = x[sort_idx]
    residuals_sorted = abs_residuals[sort_idx]

    # Fit cubic spline on squared residuals
    squared_residuals = residuals_sorted**2

    # Use scipy interpolate for smoothing
    n_knots = max(5, min(50, len(x_sorted) // 10))
    knots = np.linspace(x_sorted[0], x_sorted[-1], n_knots)

    try:
        # Create a smoothing spline
        tck = interpolate.splrep(x_sorted, squared_residuals, s=len(x_sorted) * 0.1)
        fitted_squared = interpolate.splev(x_sorted, tck)

        # Ensure non-negative and take sqrt
        fitted_squared = np.maximum(fitted_squared, 1e-8)
        local_std_sorted = np.sqrt(fitted_squared)
    except Exception:
        # Fallback to simple rolling window if spline fails
        local_std_sorted = _estimate_variance_rolling(residuals_sorted, 30)

    # Inverse sort back to original order
    local_std = np.empty_like(local_std_sorted)
    local_std[sort_idx] = local_std_sorted

    return local_std


def compute_cell_scores(
    count_observations: np.ndarray,
    sum_rarity: np.ndarray,
) -> GAMFitResult:
    """
    Compute GAM-based rarity scores for H3 cells.

    Pipeline:
    1. Build log-transformed data
    2. Fit GAM smooth spline
    3. Compute residuals and local std
    4. Compute standardized z-scores

    Args:
        count_observations: Array of observation counts
        sum_rarity: Array of sum rarity per cell

    Returns:
        GAMFitResult with all computed scores
    """
    # Build data
    x, y = build_gam_log_data(count_observations, sum_rarity)

    n_cells = len(x)

    # Fit GAM
    gam_model = fit_gam_spline(x, y)

    # Compute fitted values
    fitted_y = gam_model(x)

    # Compute residuals
    residuals = y - fitted_y

    # Sort by x for variance estimation
    sort_idx = np.argsort(x)
    x_sorted = x[sort_idx]
    residuals_sorted = residuals[sort_idx]

    # Estimate local residual std
    local_std_sorted = estimate_residual_variance(
        x_sorted,
        residuals_sorted,
        method=GAM_VARIANCE_METHOD,
        window_size=GAM_ROLLING_WINDOW_SIZE,
    )

    # Unsort back to original order
    local_std = np.empty_like(local_std_sorted)
    local_std[sort_idx] = local_std_sorted

    # Compute z-scores
    zscores = np.where(
        local_std > 0,
        residuals / local_std,
        0.0,  # If std is 0, z-score is 0
    )

    return GAMFitResult(
        n_cells=n_cells,
        x_data=x,
        y_data=y,
        fitted_values=fitted_y,
        residuals=residuals,
        residual_std=local_std,
        zscores=zscores,
        gam_model=gam_model,
    )
