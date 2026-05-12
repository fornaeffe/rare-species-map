<script lang="ts">
  import { onMount } from "svelte";
  import { asset } from '$app/paths';
  import { dev } from '$app/environment';
  import maplibregl, {
    type ExpressionSpecification,
    type FillLayerSpecification,
    type LngLatLike,
    type MapGeoJSONFeature,
    type MapMouseEvent,
    type MapOptions,
    type Popup,
  } from "maplibre-gl";
  import { Protocol } from "pmtiles";
  import {
    Activity,
    AlignVerticalJustifyCenter,
    Bird,
    ChevronDown,
    ChevronUp,
    Eye,
    Github,
    Layers,
    Leaf,
    LocateFixed,
    Minus,
    Plus,
    RefreshCw,
    Star,
    User,
    X,
  } from "lucide-svelte";

  const TILE_LAYER = "rare_species_cells";
  const RESOLUTIONS = [3, 4, 5, 6, 7] as const;
  // Una source per risoluzione: vengono tutte registrate al load e mai rimosse,
  // così MapLibre mantiene la tile cache per ogni livello per tutta la sessione.
  const sourceId = (res: number) => `rare-species-source-${res}`;
  const FILL_LAYER_ID = "rare-species-fill";
  const LINE_LAYER_ID = "rare-species-line";
  const HOVER_LINE_LAYER_ID = "rare-species-hover-line";

  type Metric =
    | "rarity_zscore"
    | "count_observations"
    | "count_species"
    | "count_observers";

  type CellProperties = {
    h3?: string;
    count_observations?: number;
    count_species?: number;
    count_observers?: number;
    rarity_zscore?: number;
    confidence_scores?: number;
  };

  type LayerMouseEvent = MapMouseEvent & {
    features?: MapGeoJSONFeature[];
  };

  type CellScoresSummary = {
    rarity_quantiles: number[];
    count_observations_quantiles: number[];
    count_species_quantiles: number[];
    count_observers_quantiles: number[];
    confidence_scores_quantiles: number[];
    species_vs_observations_quantiles: number[];
  }

  const metricLabels: Record<Metric, string> = {
    rarity_zscore: "Rarity score",
    count_observations: "Observations",
    count_species: "Species",
    count_observers: "Observers",
  };

  const metricIcons = {
    rarity_zscore: Star,
    count_observations: Eye,
    count_species: Leaf,
    count_observers: User,
  };

  const initialCenter: LngLatLike = [12.45, 42.7];

  let mapContainer: HTMLDivElement;
  let map = $state<maplibregl.Map | undefined>();
  let popup = $state<Popup | undefined>();
  let protocol = $state<Protocol | undefined>();
  let isMapReady = $state(false);
  let tileError = $state("");
  let metric = $state<Metric>("rarity_zscore");
  let opacity = $state(100);
  let selectedCell = $state<CellProperties | undefined>();
  let hoveredH3 = $state("");
  let currentResolution = $state(4);
  let cellScoresSummary = $state<CellScoresSummary | undefined>();
  let tileSource = $state<'production' | 'local-wrangler' | 'local-assets'>(
    dev ? 'local-assets' : 'production'
  );
  let controlPanelOpen = $state(true);

  // Cache dei summary caricati
  const summariesByResolution = new Map<number, CellScoresSummary>();

  async function loadCellScoresSummaries(): Promise<void> {
    const resolutions = [3, 4, 5, 6, 7];

    await Promise.all(
      resolutions.map(async (resolution) => {
        const response = await fetch(
          asset(`/tiles/cell_scores_summary${resolution}.json`)
        );

        if (!response.ok) {
          throw new Error(
            `Failed to load summary for resolution ${resolution}`
          );
        }

        const summary: CellScoresSummary =
          (await response.json()) as CellScoresSummary;

        summariesByResolution.set(resolution, summary);
      })
    );

    // Inizializza lo stato con il livello 4
    const initialSummary = summariesByResolution.get(4);

    if (initialSummary === undefined) {
      throw new Error('Missing summary for resolution 4');
    }

    cellScoresSummary = initialSummary;
  }

  loadCellScoresSummaries().catch((error) => {
    console.error("Error loading cell scores summaries:", error);
    tileError = "Failed to load cell scores summaries.";
  });

  function getResolutionForZoom(zoom: number): number {
    if (zoom >= 8) return 7;
    if (zoom >= 7) return 6;
    if (zoom >= 5) return 5;
    if (zoom >= 4) return 4;
    return 3;
  }

  function getPmtilesUrl(resolution: number): string {
    switch (tileSource) {
      case 'production':
        return `https://pmtiles-proxy.fornaeffe.workers.dev/releases/latest/download/rare_species_cells${resolution}.pmtiles`;
      case 'local-wrangler':
        return `http://127.0.0.1:8787/releases/latest/download/rare_species_cells${resolution}.pmtiles`;
      case 'local-assets':
        return asset(`/tiles/rare_species_cells${resolution}.pmtiles`);
    }
  }

  function getCurrentResolution(): number {
    return getResolutionForZoom(map?.getZoom() ?? 0);
  }

  // Cambia solo i layer (non le source) quando la risoluzione cambia.
  // Le source rimangono vive per tutta la sessione: MapLibre conserva la tile
  // cache per ogni risoluzione e non riscarica tile già ottenute.
  function handleZoomChange() {
    const newResolution = getCurrentResolution();

    if (newResolution !== currentResolution) {
      currentResolution = newResolution;
      cellScoresSummary = summariesByResolution.get(newResolution);
      swapLayersToResolution(newResolution);
    }
  }

  function swapLayersToResolution(resolution: number) {
    if (!map) return;

    // Rimuovi solo i layer, MAI le source: la cache tile resta intatta.
    if (map.getLayer(HOVER_LINE_LAYER_ID)) map.removeLayer(HOVER_LINE_LAYER_ID);
    if (map.getLayer(LINE_LAYER_ID)) map.removeLayer(LINE_LAYER_ID);
    if (map.getLayer(FILL_LAYER_ID)) map.removeLayer(FILL_LAYER_ID);

    addCellLayers(resolution);
  }

  function recreatePmtilesSources() {
    if (!map || !isMapReady) return;

    // Rimuovi i layer
    if (map.getLayer(HOVER_LINE_LAYER_ID)) map.removeLayer(HOVER_LINE_LAYER_ID);
    if (map.getLayer(LINE_LAYER_ID)) map.removeLayer(LINE_LAYER_ID);
    if (map.getLayer(FILL_LAYER_ID)) map.removeLayer(FILL_LAYER_ID);

    // Rimuovi le vecchie sources
    for (const res of RESOLUTIONS) {
      if (map.getSource(sourceId(res))) {
        map.removeSource(sourceId(res));
      }
    }

    // Ricrea le sources con i nuovi URL
    for (const res of RESOLUTIONS) {
      map.addSource(sourceId(res), {
        type: "vector",
        url: `pmtiles://${getPmtilesUrl(res)}`,
        promoteId: "h3",
        attribution:
          'Luca Fornasari, from GBIF data (<a href="https://doi.org/10.15468/dl.v2j3ye">https://doi.org/10.15468/dl.v2j3ye</a>)',
      });
    }

    // Ricrea i layer con la risoluzione corrente
    addCellLayers(currentResolution);
  }

  $effect(() => {
    tileSource;
    if (!isMapReady) return;
    recreatePmtilesSources();
  });

  $effect(() => {
    metric;
    opacity;
    isMapReady;

    if (!isMapReady) return;
    updateCellLayers();
  });


  onMount(() => {
    protocol = new Protocol();
    maplibregl.addProtocol("pmtiles", protocol.tile);

    const options: MapOptions = {
      container: mapContainer,
      center: initialCenter,
      zoom: 4.8,
      minZoom: 2,
      maxZoom: 12,
      attributionControl: false,
      style: {
        version: 8,
        glyphs: "https://demotiles.maplibre.org/font/{fontstack}/{range}.pbf",
        sources: {
          basemap: {
            type: "raster",
            tiles: [
              "https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png",
            ],
            tileSize: 256,
            attribution:
              '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/attributions">CARTO</a>',
          },
        },
        layers: [
          {
            id: "basemap",
            type: "raster",
            source: "basemap",
            paint: {
              "raster-saturation": -0.2,
              "raster-contrast": 0.04,
            },
          },
        ],
      },
    };

    map = new maplibregl.Map(options);
    popup = new maplibregl.Popup({
      closeButton: false,
      closeOnClick: false,
      offset: 14,
      className: "cell-popup",
    });

    map.addControl(
      new maplibregl.AttributionControl({ compact: true }),
      "bottom-left",
    );

    map.on("load", () => {
      // Registra tutte le source una volta sola al load.
      // Non vengono mai rimosse: MapLibre mantiene la tile cache per ognuna
      // per l'intera sessione, evitando scaricamenti ridondanti.
      for (const res of RESOLUTIONS) {
        map!.addSource(sourceId(res), {
          type: "vector",
          url: `pmtiles://${getPmtilesUrl(res)}`,
          promoteId: "h3",
          attribution:
            'Luca Fornasari, from GBIF data (<a href="https://doi.org/10.15468/dl.v2j3ye">https://doi.org/10.15468/dl.v2j3ye</a>)',
        });
      }

      const initialResolution = getCurrentResolution();
      currentResolution = initialResolution;
      addCellLayers(initialResolution);
      isMapReady = true;
    });

    map.on("mousemove", FILL_LAYER_ID, handleCellHover);
    map.on("mouseleave", FILL_LAYER_ID, clearHover);
    map.on("click", FILL_LAYER_ID, handleCellClick);
    map.on("zoomend", handleZoomChange);
    map.on("error", (event) => {
      const message = event.error?.message ?? "";
      tileError = `Map error: ${message}`
    });
    map.on("sourcedata", (event) => {
      if (event.sourceId?.startsWith("rare-species-source") && event.isSourceLoaded) {
        tileError = "";
      }
    });

    return () => {
      maplibregl.removeProtocol("pmtiles");
      popup?.remove();
      map?.remove();
      protocol = undefined;
      map = undefined;
    };
  });

  function addCellLayers(resolution: number) {
    if (!map) return;

    map.addLayer({
      id: FILL_LAYER_ID,
      type: "fill",
      source: sourceId(resolution),
      "source-layer": TILE_LAYER,
      paint: {
        "fill-color": metricColorExpression(),
        "fill-opacity": fillOpacityExpression(),
      },
    } satisfies FillLayerSpecification);

    map.addLayer({
      id: LINE_LAYER_ID,
      type: "line",
      source: sourceId(resolution),
      "source-layer": TILE_LAYER,
      paint: {
        "line-color": "rgba(25, 45, 37, 0.5)",
        "line-opacity": [
          "interpolate",
          ["linear"],
          ["zoom"],
          4,
          0,
          8,
          0.32,
          12,
          0.48,
        ],
        "line-width": [
          "interpolate",
          ["linear"],
          ["zoom"],
          4,
          0.1,
          9,
          0.45,
          12,
          0.9,
        ],
      },
    });

    map.addLayer({
      id: HOVER_LINE_LAYER_ID,
      type: "line",
      source: sourceId(resolution),
      "source-layer": TILE_LAYER,
      filter: ["==", ["get", "h3"], ""],
      paint: {
        "line-color": "#101613",
        "line-opacity": 0.95,
        "line-width": ["interpolate", ["linear"], ["zoom"], 4, 1, 10, 2.2],
      },
    });
  }

  function updateCellLayers() {
    if (!map?.getLayer(FILL_LAYER_ID)) return;

    map.setPaintProperty(FILL_LAYER_ID, "fill-color", metricColorExpression());
    map.setPaintProperty(
      FILL_LAYER_ID,
      "fill-opacity",
      fillOpacityExpression(),
    );
  }

  function fillOpacityExpression(): ExpressionSpecification {
    return [
      "interpolate",
      ["linear"],
      ["coalesce", ["get", "confidence_scores"], 0],

      0,
      0,
      1,
      1 * (opacity / 100),
    ];
  }

  function metricColorExpression(): ExpressionSpecification {
    if (metric === "count_observations") {

      const maxCount = cellScoresSummary?.count_observations_quantiles[2] ?? 500;

      return [
        "interpolate",
        ["linear"],
        ["coalesce", ["get", "count_observations"], 0],
        0,
        "#e8f3ea",
        maxCount * 0.2,
        "#b8d9c3",
        maxCount * 0.4,
        "#6fb0a4",
        maxCount * 0.6,
        "#397996",
        maxCount * 0.8,
        "#24476f",
        maxCount,
        "#171d3f",
      ];
    }

    if (metric === "count_species") {

      const maxCount = cellScoresSummary?.count_species_quantiles[2] ?? 500;

      return [
        "interpolate",
        ["linear"],
        ["coalesce", ["get", "count_species"], 0],
        0,
        "#f1ffcf",
        maxCount * 0.25,
        "#c6f76a",
        maxCount * 0.5,
        "#7dde46",
        maxCount * 0.75,
        "#3caa52",
        maxCount,
        "#245726",
      ];
    }

    if (metric === "count_observers") {
      const maxCount = cellScoresSummary?.count_observers_quantiles[2] ?? 500;
      return [
        "interpolate",
        ["linear"],
        ["coalesce", ["get", "count_observers"], 0],
        0,
        "#fff1cf",
        maxCount * 0.25,
        "#f7c66a",
        maxCount * 0.5,
        "#de7d46",
        maxCount * 0.75,
        "#aa3c52",
        maxCount,
        "#572461",
      ];
    }

    const q3 = cellScoresSummary?.rarity_quantiles[3] ?? 0;
    return [
      "interpolate",
      ["linear"],
      ["coalesce", ["get", "rarity_zscore"], 0],
      -q3, '#053061',
      -q3 * 0.67, '#4575b4',
      -q3 * 0.33, '#abd9e9',
      0, '#f7f7f7',
      q3 * 0.33, '#fdae61',
      q3 * 0.67, '#d73027',
      q3, '#67001f'
    ];
  }

  function handleCellHover(event: LayerMouseEvent) {
    if (!map || !event.features?.length) return;

    map.getCanvas().style.cursor = "pointer";
    const feature = event.features[0];
    const properties = feature.properties as CellProperties;
    hoveredH3 = properties.h3 ?? "";

    map.setFilter(HOVER_LINE_LAYER_ID, ["==", ["get", "h3"], hoveredH3]);
    popup
      ?.setLngLat(event.lngLat)
      .setHTML(
        `<strong>${formatScore(properties.rarity_zscore)}</strong> <span>(${formatCount(
          properties.count_observations
        )} observations)</span>`,
      )
      .addTo(map);
  }

  function clearHover() {
    if (!map) return;
    map.getCanvas().style.cursor = "";
    hoveredH3 = "";
    map.setFilter(HOVER_LINE_LAYER_ID, ["==", ["get", "h3"], ""]);
    popup?.remove();
  }

  function handleCellClick(event: LayerMouseEvent) {
    if (!event.features?.length) return;
    selectedCell = event.features[0].properties as CellProperties;
  }

  function resetView() {
    map?.easeTo({ center: initialCenter, zoom: 4.8, duration: 650 });
  }

  function zoomBy(delta: number) {
    map?.easeTo({ zoom: (map.getZoom() ?? 5) + delta, duration: 250 });
  }

  function formatCount(value: number | undefined) {
    return value == null ? "0" : new Intl.NumberFormat("en-US").format(value);
  }

  function formatScore(value: number | undefined) {
    return value == null
      ? "n/a"
      : new Intl.NumberFormat("en-US", {
          signDisplay: "exceptZero",
          maximumFractionDigits: 1,
        }).format(value * 100) + " %";
  }

  function formatDecimal(value: number | undefined) {
    return value == null
      ? "n/a"
      : new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 }).format(
          value,
        );
  }
</script>

<svelte:head>
  <title>Rare Species Map</title>
</svelte:head>

<main class="app-shell">
  <div class="map-surface" bind:this={mapContainer}></div>

  <section class="topbar" aria-label="Map overview">
    <div>
      <div class="eyebrow">Where <a href="https://www.inaturalist.org" target="_blank">iNaturalist</a> observers have seen their rarest species</div>
      <h1>Rare Species Map</h1>
    </div>
    <div class="status-pill" class:error={tileError}>
      <span class="status-dot"></span>
      {tileError ? "Tile source issue" : "PMTiles vector source"}
      {#if dev}
        <div class="tile-source-select">
          <select
            id="tile-source-select"
            bind:value={tileSource}
            style="width: 100%; padding: 0.5rem; margin-top: 0.5rem; border-radius: 4px; border: 1px solid var(--color-border); font-size: 0.875rem;"
          >
            <option value="production">Production Proxy</option>
            <option value="local-wrangler">Local Wrangler (8787)</option>
            <option value="local-assets">Local Assets</option>
          </select>
        </div>
      {/if}
    </div>
  </section>

  <aside class="control-panel" class:collapsed={!controlPanelOpen} aria-label="Map controls">
    <div class="panel-section">
      <div class="section-title">
        <div style="display: flex; align-items: center; gap: 8px;">
          <Layers size={16} />
          <span>Metric</span>
        </div>
        <button
          type="button"
          class="panel-toggle"
          onclick={() => (controlPanelOpen = !controlPanelOpen)}
          aria-label={controlPanelOpen ? "Close controls" : "Open controls"}
          title={controlPanelOpen ? "Close controls" : "Open controls"}
        >
          {#if controlPanelOpen}
            <ChevronDown size={18} />
          {:else}
            <ChevronUp size={18} />
          {/if}
        </button>
      </div>

      <div class="segmented-control" role="group" aria-label="Visible metric">
        {#each Object.entries(metricLabels) as [key, label]}
          {@const MetricIcon = metricIcons[key as Metric]}
          <button
            type="button"
            class:active={metric === key}
            aria-pressed={metric === key}
            onclick={() => (metric = key as Metric)}
          >
            <MetricIcon size={15} />
            <span>{label}</span>
          </button>
        {/each}
      </div>
    </div>

    <div class="panel-section">
      <label class="slider-row">
        <span>Opacity</span>
        <strong>{opacity}%</strong>
        <input
          type="range"
          min="0"
          max="100"
          value={opacity}
          oninput={(event) => (opacity = event.currentTarget.valueAsNumber)}
        />
      </label>
    </div>

    

    <div class="legend" aria-label="Legend">
      <div class="legend-ramp metric-{metric}"></div>
      <div class="legend-labels">
        <span>Lower</span>
        <span>Higher</span>
      </div>
    </div>

    {#if tileError}
      <p class="tile-error">{tileError}</p>
    {/if}

    <a class="github-link" href="https://github.com/fornaeffe/rare-species-map" target="_blank" rel="noopener noreferrer" title="View on GitHub">
      <img src={asset("github.svg")} alt="GitHub" height="16"/>
      <span>More info on GitHub</span>
    </a>
  </aside>

  <aside class="cell-card" aria-label="Selected H3 cell">
    <div class="section-title">
      <div style="display: flex; align-items: center; gap: 0.5rem;">
        <Activity size={16} />
        <span>Selected Cell</span>
      </div>
      {#if selectedCell}
        <button
          type="button"
          class="close-button"
          onclick={() => (selectedCell = undefined)}
          aria-label="Deselect cell"
          title="Deselect cell"
        >
          <X size={16} />
        </button>
      {/if}
    </div>

    {#if selectedCell}
      <dl>
        <div>
          <dt>H3</dt>
          <dd>{selectedCell.h3}</dd>
        </div>
        <div>
          <dt>Rarity score</dt>
          <dd>{formatScore(selectedCell.rarity_zscore)}</dd>
        </div>
        <div>
          <dt>Observations</dt>
          <dd>{formatCount(selectedCell.count_observations)}</dd>
        </div>
        <div>
          <dt>Species</dt>
          <dd>{formatCount(selectedCell.count_species)}</dd>
        </div>
        <div>
          <dt>Observers</dt>
          <dd>{formatCount(selectedCell.count_observers)}</dd>
        </div>
        <div>
          <dt>Confidence</dt>
          <dd>{formatCount(selectedCell.confidence_scores)}</dd>
        </div>
      </dl>
    {:else}
      <p class="muted">No cell selected</p>
    {/if}

    
  </aside>

  <nav class="map-actions" aria-label="Map navigation">
    <button
      type="button"
      onclick={() => zoomBy(1)}
      aria-label="Zoom in"
      title="Zoom in"
    >
      <Plus size={18} />
    </button>
    <button
      type="button"
      onclick={() => zoomBy(-1)}
      aria-label="Zoom out"
      title="Zoom out"
    >
      <Minus size={18} />
    </button>
    <button
      type="button"
      onclick={resetView}
      aria-label="Reset view"
      title="Reset view"
    >
      <LocateFixed size={18} />
    </button>
    <button
      type="button"
      onclick={() => updateCellLayers()}
      aria-label="Refresh style"
      title="Refresh style"
    >
      <RefreshCw size={17} />
    </button>
  </nav>
</main>