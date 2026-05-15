# Rare Species Map Web

Static SvelteKit frontend for the rare species residual score PMTiles.

## Local setup

```powershell
npm.cmd install
npm.cmd run tiles:copy
npm.cmd run dev
```

Default dev environment `.pmtiles` and `cell_scores_summary.json` should be
placed in `static/tiles/`.


## Build

```powershell
npm.cmd run build
```

The app is generated as a static SPA in `build/`.
