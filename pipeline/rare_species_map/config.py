# pipeline/rare_species_map/config.py

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_ROOT = PROJECT_ROOT / "pipeline"

DATA_DIR = PROJECT_ROOT / "data"

DATA_RAW = DATA_DIR / "raw" / "raw.csv"
DATA_PROCESSED = DATA_DIR / "processed"
DATA_TILES = PROJECT_ROOT / "web" / "static" / "tiles"

H3_OCCUPANCY_RESOLUTION = 5
H3_VISUALIZATION_RESOLUTIONS = [3, 4, 5, 6, 7]
H3_ZOOM_RANGES = [[0, 4], [4, 5], [5, 6], [6, 7], [7, 12]]
# H3_VISUALIZATION_RESOLUTIONS = [8]
# H3_ZOOM_RANGES = [[12, 15]]

MAX_COORDINATE_UNCERTAINTY = 10000

DEFAULT_COUNTRY = "IT"

