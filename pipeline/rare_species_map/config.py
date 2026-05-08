# pipeline/rare_species_map/config.py

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_ROOT = PROJECT_ROOT / "pipeline"

DATA_DIR = PROJECT_ROOT / "data"

DATA_RAW = DATA_DIR / "raw"
DATA_PROCESSED = DATA_DIR / "processed"
DATA_TILES = DATA_DIR / "tiles"

H3_OCCUPANCY_RESOLUTION = 6
H3_VISUALIZATION_RESOLUTION = 7

MAX_COORDINATE_UNCERTAINTY = 10000

DEFAULT_COUNTRY = "IT"

# GAM Scoring Parameters
# =====================

# Epsilon for log transform to avoid log(0)
GAM_LOG_EPSILON = 1e-8

# Number of spline basis functions for GAM
GAM_N_SPLINES = 20

# Method for estimating local residual standard deviation
# Options: 'rolling_window', 'binning', 'spline'
GAM_VARIANCE_METHOD = "rolling_window"

# Size of rolling window for rolling_window variance method
# Applies to residuals sorted by x
GAM_ROLLING_WINDOW_SIZE = 100

# Minimum observations per cell to include in GAM fit
GAM_MIN_OBSERVATIONS_PER_CELL = 1
