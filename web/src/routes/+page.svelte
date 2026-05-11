<script lang="ts">
  import { onMount } from "svelte";
  import { asset } from '$app/paths';
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
    Eye,
    Layers,
    Leaf,
    LocateFixed,
    Minus,
    Plus,
    RefreshCw,
    Star,
    User,
  } from "lucide-svelte";

  const TILE_LAYER = "rare_species_cells";
  const SOURCE_ID = "rare-species-source";
  const FILL_LAYER_ID = "rare-species-fill";
  const LINE_LAYER_ID = "rare-species-line";
  const HOVER_LINE_LAYER_ID = "rare-species-hover-line";

  type Metric =
    | "rarity_zscore"
    | "count_observations"
    | "count_species"
    | "species_vs_observations"
    | "count_observers";

  type CellProperties = {
    h3?: string;
    count_observations?: number;
    count_species?: number;
    count_observers?: number;
    rarity_zscore?: number;
    confidence_scores?: number;
    species_vs_observations?: number;
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
    rarity_zscore: "Rarity Z-score",
    count_observations: "Observations",
    count_species: "Species",
    species_vs_observations: "Species vs Observations",
    count_observers: "Observers",
  };

  const metricIcons = {
    rarity_zscore: Star,
    count_observations: Eye,
    count_species: Leaf,
    count_observers: User,
    species_vs_observations: AlignVerticalJustifyCenter,
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
  let scoreFloor = $state(-1.5);
  let selectedCell = $state<CellProperties | undefined>();
  let hoveredH3 = $state("");
  let currentResolution = $state(4);
  let currentPmtilesUrl = $state<string>("");
  let cellScoresSummary = $state<CellScoresSummary | undefined>();

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
    // return asset(`/tiles/rare_species_cells${resolution}.pmtiles`);
    return `http://127.0.0.1:8787/releases/download/v0.1.0-alpha/rare_species_cells${resolution}.pmtiles`
  }

  function getCurrentResolution(): number {
    return getResolutionForZoom(map?.getZoom() ?? 0);
  }

  function getCurrentPmtilesUrl(): string {
    return getPmtilesUrl(getCurrentResolution());
  }


  function handleZoomChange() {
    const newResolution = getCurrentResolution();
    const newUrl = getPmtilesUrl(newResolution);

    if (newUrl !== currentPmtilesUrl) {
      currentPmtilesUrl = newUrl;
      currentResolution = newResolution;

      cellScoresSummary = summariesByResolution.get(newResolution);

      updateSource();
    }
  }

  function updateSource() {
    if (!map) return;

    // Remove old layers
    if (map.getLayer(FILL_LAYER_ID)) map.removeLayer(FILL_LAYER_ID);
    if (map.getLayer(LINE_LAYER_ID)) map.removeLayer(LINE_LAYER_ID);
    if (map.getLayer(HOVER_LINE_LAYER_ID)) map.removeLayer(HOVER_LINE_LAYER_ID);

    // Remove old source
    if (map.getSource(SOURCE_ID)) map.removeSource(SOURCE_ID);

    // Add new source and layers
    addCellLayers();
  }

  $effect(() => {
    metric;
    opacity;
    scoreFloor;
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
      new maplibregl.NavigationControl({ showCompass: false }),
      "bottom-right",
    );
    map.addControl(
      new maplibregl.AttributionControl({ compact: true }),
      "bottom-left",
    );

    map.on("load", () => {
      currentPmtilesUrl = getCurrentPmtilesUrl();
      addCellLayers();
      isMapReady = true;
    });

    map.on("mousemove", FILL_LAYER_ID, handleCellHover);
    map.on("mouseleave", FILL_LAYER_ID, clearHover);
    map.on("click", FILL_LAYER_ID, handleCellClick);
    map.on("zoomend", handleZoomChange);
    map.on("error", (event) => {
      const message = event.error?.message ?? "";
      if (
        message.includes("rare_species_cells") ||
        message.includes("pmtiles")
      ) {
        tileError = `PMTiles non trovati o non leggibili: ${currentPmtilesUrl}`;
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

  function pmtilesSourceUrl() {
    return `pmtiles://${currentPmtilesUrl}`;
  }

  function addCellLayers() {
    if (!map) return;

    map.addSource(SOURCE_ID, {
      type: "vector",
      url: pmtilesSourceUrl(),
      promoteId: "h3",
      attribution:
              'GBIF.org (07 May 2026) GBIF Occurrence Download <a href="https://doi.org/10.15468/dl.v2j3ye">https://doi.org/10.15468/dl.v2j3ye</a>',
    });

    map.addLayer({
      id: FILL_LAYER_ID,
      type: "fill",
      source: SOURCE_ID,
      "source-layer": TILE_LAYER,
      paint: {
        "fill-color": metricColorExpression(),
        "fill-opacity": fillOpacityExpression(),
      },
    } satisfies FillLayerSpecification);

    map.addLayer({
      id: LINE_LAYER_ID,
      type: "line",
      source: SOURCE_ID,
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
      source: SOURCE_ID,
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

    if (metric === "species_vs_observations") {


      return [
        "interpolate",
        ["linear"],
        ["coalesce", ["get", "species_vs_observations"], 0],
        cellScoresSummary?.species_vs_observations_quantiles[0] ?? 0,
        "#f1ffcf",
        cellScoresSummary?.species_vs_observations_quantiles[1] ?? 0.25,
        "#c6f76a",
        cellScoresSummary?.species_vs_observations_quantiles[2] ?? 0.5,
        "#7dde46",
        cellScoresSummary?.species_vs_observations_quantiles[3] ?? 0.75,
        "#3caa52",
        cellScoresSummary?.species_vs_observations_quantiles[4] ?? 1,
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

    return [
      "interpolate",
      ["linear"],
      ["coalesce", ["get", "rarity_zscore"], 0],
      -(cellScoresSummary?.rarity_quantiles[3] ?? 0), '#2166ac',
      0, '#f7f7f7',
      cellScoresSummary?.rarity_quantiles[3] ?? 0, '#b2182b'
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
      <h1>Rare Species Map</h1>
    </div>
    <div class="status-pill" class:error={tileError}>
      <span class="status-dot"></span>
      {tileError ? "Tile source issue" : "PMTiles vector source"}
    </div>
  </section>

  <aside class="control-panel" aria-label="Map controls">
    <div class="panel-section">
      <div class="section-title">
        <Layers size={16} />
        <span>Metric</span>
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
  </aside>

  <aside class="cell-card" aria-label="Selected H3 cell">
    <div class="section-title">
      <Activity size={16} />
      <span>Selected Cell</span>
    </div>

    {#if selectedCell}
      <dl>
        <div>
          <dt>H3</dt>
          <dd>{selectedCell.h3}</dd>
        </div>
        <div>
          <dt>Rarity Z-score</dt>
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
          <dt>Species vs Observations</dt>
          <dd>{formatDecimal(selectedCell.species_vs_observations)}</dd>
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

  <footer class="science-note">
    Areas where an observer should expect to see rarer species than his or her average.
  </footer>
</main>
