# Rare Species Map Web

Static SvelteKit frontend for the rare species residual score PMTiles.

## Local setup

```powershell
npm.cmd install
npm.cmd run tiles:copy
npm.cmd run dev
```

The default PMTiles URL is `/tiles/rare_species_cells.pmtiles`.

To use a different tile location at build/dev time:

```powershell
$env:PUBLIC_PMTILES_URL="https://example.com/rare_species_cells.pmtiles"
npm.cmd run dev
```

## Build

```powershell
npm.cmd run build
```

The app is generated as a static SPA in `build/`.
