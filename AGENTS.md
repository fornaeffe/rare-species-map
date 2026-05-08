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
number of distinct H3 cells occupied by the species (resolution defined in config.py).

Species rarity:

rarity(species) =
1 / (occupancy)^0.25

For each H3 visualization cell (resolution defined in config.py):

- count_observations
- sum_rarity

are computed.

The final rarity score is NOT:

sum_rarity / count

Instead, the project models the empirical relationship using a **GAM smooth spline**:

y ~ s(x)

where:
- x = log(count_observations + 1)
- y = log(sum_rarity + epsilon)
- s(x) = smooth spline fit (non-linear)

The model estimates:
- expected_log_sum_rarity (fitted values from the spline)
- residual = y - expected_y
- residual_std = local standard deviation of residuals (models heteroscedasticity)

The final score is **standardized** by local variability:

**rarity_zscore = residual / residual_std**

This gives:
- z-scores centered at 0 (unbiased across observation effort)
- adjusted for the increasing variance at low observation counts
- cells with more rarity than expected receive higher scores
- robust across the full range of observation effort

This non-linear model with local variance adjustment is crucial for detecting ecological hotspots fairly.


---

# Technology stack

## Python pipeline

- Python >= 3.14
- DuckDB
- H3
- Parquet
- freestiler for PMTiles generation
- scipy (spline fitting)
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
- `gam_scorer.py`: GAM spline fitting and residual standardization

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
- `data/processed/cell_scores.parquet`
- `data/tiles/rare_species_cells.pmtiles`

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
number of distinct H3 resolution 7 cells occupied by each species.

rarity =
1 / (occupancy)^0.25

Output:
data/processed/species_occupancy.parquet

---

## Step 3

Script:
scripts/03_compute_cell_scores.py

Compute rarity score per H3 visualization cell using GAM smooth spline.

### Data aggregation (DuckDB)

For each H3 visualization cell:

- count_observations counts all observations
- count_species counts distinct species in the cell
- sum_rarity sums species rarity once per distinct species present in the cell

### GAM Model fitting (Python)

1. Build log-transformed data:
   - x = log(count_observations + 1)
   - y = log(sum_rarity + epsilon), where epsilon = 1e-8

2. Fit smooth spline: y ~ s(x)
   - Uses scipy.interpolate.UnivariateSpline
   - Computes fitted_values for each cell

3. Compute residuals:
   - residual = y - fitted_values

4. Estimate local residual standard deviation:
   - Models heteroscedasticity (variance increases at low observation counts)
   - Available methods: 'rolling_window', 'binning', 'spline'
   - Default: 'rolling_window' with window size 50

5. Standardize residuals:
   - rarity_zscore = residual / residual_std

### Output columns

data/processed/cell_scores.parquet

- h3_resHigh (uint64): H3 cell index
- count_observations (uint64): number of observations
- count_species (uint64): number of distinct species
- sum_rarity (float64): sum of species rarities
- log_count_observations (float64): log(count_observations + 1)
- log_sum_rarity (float64): log(sum_rarity + epsilon)
- expected_log_sum_rarity (float64): fitted value from GAM
- residual (float64): y - fitted
- residual_std (float64): local standard deviation
- rarity_zscore (float64): standardized score (final output for frontend)

### Diagnostic plots

Step 3 produces diagnostic plots by default:

- `observed_vs_count.png`: Raw scatter of sum_rarity vs count_observations
- `gam_fit_logspace.png`: Log-space data with fitted GAM spline
- `residuals_vs_fitted.png`: Residuals vs fitted values (should show no systematic pattern)
- `residual_variance_vs_x.png`: Local residual std vs x (shows heteroscedasticity model)
- `zscores_histogram.png`: Distribution of final z-scores (should be ~N(0,1))
- `zscores_qqplot.png`: Q-Q plot of z-scores vs normal distribution
- `diagnostics_summary.txt`: Numerical summary

Default diagnostics directory:
data/processed/diagnostics/cell_scores

Use `--no-diagnostics` to skip plot generation.

### Configuration

Adjustable GAM parameters in rare_species_map/config.py:

- `GAM_LOG_EPSILON`: epsilon for log transform (default: 1e-8)
- `GAM_N_SPLINES`: approximate number of spline basis functions (default: 20)
- `GAM_VARIANCE_METHOD`: method for local std estimation (default: 'rolling_window')
- `GAM_ROLLING_WINDOW_SIZE`: window size for rolling variance (default: 50)
- `GAM_MIN_OBSERVATIONS_PER_CELL`: minimum obs to include in fit (default: 1)

---

## Step 4

Script:
scripts/04_generate_pmtiles.py

Generate PMTiles vector tiles for the frontend.

Exports H3 cells as GeoJSON features with properties:

- h3: H3 cell index (string)
- count_observations: number of observations
- count_species: number of distinct species
- sum_rarity: sum of rarity values
- rarity_zscore: **final scoring metric** (z-score standardized by local effort)

The script uses `freestiler` to generate PMTiles with configurable zoom levels (default 0-12).

GeoJSONSeq is an intermediate debug artifact; the frontend consumes PMTiles.

Output:
data/tiles/rare_species_cells.pmtiles

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
