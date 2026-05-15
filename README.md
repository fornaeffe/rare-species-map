# Rare Species Map

## Project overview

This project generates an interactive map showing areas where rare species are more likely to be observed, using GBIF / iNaturalist Research Grade observations.

The application consists of:

1. A Python geospatial preprocessing pipeline
2. A static SPA frontend
3. Vector tiles containing rarity scores aggregated on H3 cells
4. A reverse proxy to add CORS to the generated data package served from GitHub Releases

---

# Main idea


# Current ecological scoring model

The current model estimates whether a geographic cell contains species that are unusually rare relative to the typical observations of the people who observed there.

The goal is to identify places where observers, on average, observe species rarer than the species they usually observe elsewhere.

This helps reduce observer bias caused by highly skilled or highly prolific naturalists.

---

# Species rarity

Species rarity is computed globally from occupancy.

For each species:

occupancy(species) =
number of distinct H3 cells occupied by the species.

Species rarity is then defined as:

rarity(species) =
1 / sqrt(occupancy)

Therefore:
- widespread species receive low rarity
- geographically restricted species receive high rarity


---

# Cell-level observer normalization

For each H3 visualization cell:

1. Observations are grouped by:
   - cell
   - observer
   - species

2. For each observer within a cell:
   - duplicate observations of the same species are ignored
   - the mean rarity of the observed species is computed

This produces:

mean_rarity(cell, observer)

---

# Observer baseline correction

For each observer:

observer_mean_rarity(observer) =
average mean rarity across all cells visited by that observer.

Then, for every cell-observer pair:

residual_rarity =
mean_rarity(cell, observer) - observer_mean_rarity(observer)

Interpretation:
- positive values mean the observer found species rarer than their usual standard
- negative values mean the observer found species more common than their usual standard

---

# Final cell rarity score

For each cell:

rarity_zscore =
mean residual rarity across all observers in that cell.

Interpretation:
- positive scores indicate cells where observers tend to observe species rarer than they normally observe elsewhere
- negative scores indicate cells dominated by comparatively common species

This score is used for map coloration.

---

# Confidence score

A separate confidence score is computed for visualization opacity.

The confidence score increases with:
- number of observations
- diversity of independent observers

Observer diversity is measured using the effective number of observers derived from Shannon entropy.

This prevents:
- single-observer hotspots
- poorly sampled cells
- unstable rarity estimates

Confidence is intentionally separated from rarity:
- rarity determines color
- confidence determines opacity

This allows visually highlighting:
- strong and well-supported hotspots
while fading:
- uncertain or weakly sampled areas.

---

# Important interpretation caveat

The map estimates:

"places where observers detect species rarer than the species they typically observe elsewhere"

The map does NOT directly estimate:
- absolute biodiversity
- conservation priority
- ecological integrity
- true species richness

The model is specifically designed to reduce:
- sampling effort bias
- observer skill bias
- observer specialization bias

while preserving geographically meaningful ecological signals.

# Licenses

All the code inside this repo is released under GPL-3.0 license.

Generated PMTiles and summary JSON files (available in Releases) are under CC BY-NC 4.0 license.

# Data source

- GBIF.org (7 May 2026) GBIF Occurrence Download https://doi.org/10.15468/dl.v2j3ye
