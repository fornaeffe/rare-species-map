from pathlib import Path
from typing import Final


PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
PIPELINE_ROOT: Final[Path] = PROJECT_ROOT / "pipeline"

DATA_DIR: Final[Path] = PROJECT_ROOT / "data"

DATA_RAW: Final[Path] = DATA_DIR / "raw" / "raw.csv"
DATA_PROCESSED: Final[Path] = DATA_DIR / "processed"
DATA_TILES: Final[Path] = PROJECT_ROOT / "web" / "static" / "tiles"
GENERATED_JSONS: Final[Path] = DATA_TILES

H3_OCCUPANCY_RESOLUTION: Final[int] = 5
H3_VISUALIZATION_RESOLUTIONS: Final[tuple[int, ...]] = (3, 4, 5, 6, 7)
H3_ZOOM_RANGES: Final[tuple[tuple[int, int], ...]] = (
    (0, 4),
    (4, 5),
    (5, 7),
    (7, 8),
    (8, 12),
)


