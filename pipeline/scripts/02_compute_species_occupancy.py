from __future__ import annotations

import sys
from pathlib import Path


PIPELINE_ROOT = Path(__file__).resolve().parents[1]
if str(PIPELINE_ROOT) not in sys.path:
    sys.path.insert(0, str(PIPELINE_ROOT))

from rare_species_map.cli import main_compute_species_occupancy


if __name__ == "__main__":
    main_compute_species_occupancy()
