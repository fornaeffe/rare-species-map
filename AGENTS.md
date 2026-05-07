# Rare Species Map — AI Context

## Project overview

This project generates an interactive map showing areas where rare species are more likely to be observed, using GBIF / iNaturalist Research Grade observations.

The application consists of:

1. A Python geospatial preprocessing pipeline
2. A static SPA frontend
3. Vector tiles containing rarity scores aggregated on H3 cells

The project is intentionally designed as:
- static-first
- preprocessing-heavy
- frontend-light

The frontend must NEVER perform expensive geospatial computations live.

---

# Main idea

For each species:

occupancy(species) =
number of distinct H3 resolution 7 cells occupied by the species.

Species rarity:

rarity(species) =
1 / sqrt(occupancy)

For each H3 visualization cell (resolution 8):

- count_observations
- sum_rarity

are computed.

The final rarity score is NOT:

sum_rarity / count

Instead, the project models the empirical relationship:

log(sum_rarity) ~ log(count_observations)

using regression.

The final score is based on the residual from the expected value:
cells with more rarity than expected for their observation count receive higher scores.

This is extremely important and should not be changed unless explicitly requested.

---

# Technology stack

## Python pipeline

- Python >= 3.14
- DuckDB
- H3
- Parquet
- UV for dependency management

Important:
- do NOT use pandas unless explicitly necessary
- prefer DuckDB SQL operations
- prefer streaming / columnar processing
- the input dataset can exceed 90 GB

The pipeline must be memory-efficient.

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
│   └── tiles/
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

Compute species occupancy.

occupancy =
number of distinct H3 resolution 7 cells occupied by each species.

---

## Step 3

Compute rarity score per H3 resolution 8 cell.

---

## Step 4

Generate PMTiles vector tiles.

---

# Important implementation constraints

## H3

- occupancy resolution = 7
- visualization resolution = 8

H3 indexes should be stored as UINT64 whenever possible.

Avoid string H3 indexes in intermediate datasets.

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

---

# Important scientific caveats

The map estimates:
"areas where rare species are observed more often than expected given observation effort"

The project does NOT estimate:

- absolute biodiversity
- conservation priority
- ecological integrity

Avoid claims that overstate scientific validity.

---

# Future possible improvements

Not implemented initially:

- KDE smoothing
- observer effort normalization
- human density correction
- road accessibility correction
- temporal filtering
- taxonomic filtering
- uncertainty weighting

Keep architecture flexible for these future additions.