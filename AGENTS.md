# Rare Species Map — AI Context

## Project overview

Read README.md to learn about the project overview

The project is intentionally designed as:
- static-first
- preprocessing-heavy
- frontend-light

The frontend must NEVER perform expensive geospatial computations live.

---

# Technology stack

## Python pipeline

- Python >= 3.14
- DuckDB
- H3
- Parquet
- freestiler for PMTiles generation
- UV for dependency management

Important:
- do NOT use pandas unless explicitly necessary
- prefer DuckDB SQL operations
- prefer streaming / columnar processing
- the input dataset can exceed 90 GB

The pipeline must be memory-efficient.

Modules in rare_species_map/:
- `config.py`: centralized configuration
- `duckdb_utils.py`: DuckDB connection and utilities

---

# Frontend stack

- SvelteKit
- TypeScript
- npm
- MapLibre GL JS

The frontend is a static SPA.

No backend is planned initially.

---

# Repository structure

```text
rarity-map/
│
├── data/
│   ├── raw/
│   ├── processed/
│
├── pipeline/
│   ├── pyproject.toml
│   ├── scripts/
│   └── rare_species_map/
│
├── web/
│
└── docs/
```

---

# Data pipeline

Backend preprocessing is currently implemented through:

- `pipeline/scripts/run_pipeline.py` for end-to-end execution
- `pipeline/scripts/01_filter_to_parquet.py`
- `pipeline/scripts/02_compute_species_occupancy.py`
- `pipeline/scripts/03_compute_cell_scores.py`
- `pipeline/scripts/04_generate_pmtiles.py`

The four numbered steps can still be run individually. Use `run_pipeline.py`
when all steps should run in sequence from raw TSV to PMTiles.

Default processed outputs:

- `data/processed/observations_filtered.parquet`
- `data/processed/species_occupancy.parquet`
- `data/processed/cell_scores{resolution}.parquet`
- `web/static/tiles/rare_species_cells{resolution}.pmtiles`
- `web/static/tiles/cell_scores_summary{resolution}.json`

## Step 1

Script:
scripts/01_filter_to_parquet.py

Responsibilities:

- read huge GBIF TSV
- filter records
- generate Parquet
- generate H3 indexes

Important:

- use explicit CSV schema
- do NOT use read_csv_auto on full dataset
- use DuckDB COPY TO parquet

---

## Step 2

Script:
scripts/02_compute_species_occupancy.py

Compute species occupancy and rarity.

occupancy =
number of distinct H3 resolution 5 cells occupied by each species.

rarity =
1 / (occupancy)^0.5

Output:
data/processed/species_occupancy.parquet

---

## Step 3

Script:
scripts/03_compute_cell_scores.py

Compute rarity score per H3 visualization cell.

Output:
- data/processed/cell_scores{resolution}.parquet
- web/static/tiles/cell_scores_summary{resolution}.json


---

## Step 4

Script:
scripts/04_generate_pmtiles.py

Generate PMTiles vector tiles for the frontend.

The script uses `freestiler` to generate PMTiles with configurable zoom levels (default 0-12).

GeoJSONSeq is an intermediate debug artifact; the frontend consumes PMTiles.

Output:
- data/tiles/rare_species_cells.pmtiles
- web/static/tiles/rare_species_cells{resolution}.pmtiles

---

# DuckDB guidelines

DuckDB is the core engine.

Prefer:

- SQL aggregation
- SQL joins
- parquet_scan
- COPY TO parquet

Avoid:

- loading large datasets into Python memory
- row-by-row Python processing

---

# Performance constraints

The pipeline must handle:

- 90+ GB TSV input
- tens of millions of observations

Avoid:

- pandas full-table loading
- Python loops over observations
- in-memory global operations

---

# Coding style

# Python

- typed code where reasonable
- small focused modules
- reusable utility functions
- configuration centralized in config.py

Avoid giant monolithic scripts.

---

# Frontend guidelines

The frontend should:

- render vector tiles
- use MapLibre layers
- support smooth zooming
- support dynamic styling

Avoid:

- client-side geospatial analysis
- client-side heavy computation
