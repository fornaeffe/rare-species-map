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
