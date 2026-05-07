<script lang="ts">
  import { onMount } from 'svelte';
  import maplibregl, {
    type ExpressionSpecification,
    type FillLayerSpecification,
    type LngLatLike,
    type Map,
    type MapGeoJSONFeature,
    type MapMouseEvent,
    type MapOptions,
    type Popup
  } from 'maplibre-gl';
  import { Protocol } from 'pmtiles';
  import { Activity, Eye, Layers, LocateFixed, Minus, Plus, RefreshCw } from 'lucide-svelte';

  const TILE_LAYER = 'rare_species_cells';
  const SOURCE_ID = 'rare-species-source';
  const FILL_LAYER_ID = 'rare-species-fill';
  const LINE_LAYER_ID = 'rare-species-line';
  const HOVER_LINE_LAYER_ID = 'rare-species-hover-line';
  const pmtilesUrl =
    import.meta.env.PUBLIC_PMTILES_URL?.trim() || '/tiles/rare_species_cells.pmtiles';

  type Metric = 'rarity_score' | 'count_observations' | 'count_species';

  type CellProperties = {
    h3?: string;
    count_observations?: number;
    count_species?: number;
    sum_rarity?: number;
    rarity_score?: number;
  };

  type LayerMouseEvent = MapMouseEvent & {
    features?: MapGeoJSONFeature[];
  };

  const metricLabels: Record<Metric, string> = {
    rarity_score: 'Rarity residual',
    count_observations: 'Observations',
    count_species: 'Species'
  };

  const metricIcons = {
    rarity_score: Activity,
    count_observations: Eye,
    count_species: Layers
  };

  const initialCenter: LngLatLike = [12.45, 42.7];

  let mapContainer: HTMLDivElement;
  let map: Map | undefined;
  let popup: Popup | undefined;
  let protocol: Protocol | undefined;
  let isMapReady = false;
  let tileError = '';
  let metric: Metric = 'rarity_score';
  let opacity = 72;
  let scoreFloor = -1.5;
  let selectedCell: CellProperties | undefined;
  let hoveredH3 = '';

  $: if (isMapReady) {
    updateCellLayers();
  }

  onMount(() => {
    protocol = new Protocol();
    maplibregl.addProtocol('pmtiles', protocol.tile);

    const options: MapOptions = {
      container: mapContainer,
      center: initialCenter,
      zoom: 4.8,
      minZoom: 2,
      maxZoom: 12,
      attributionControl: false,
      style: {
        version: 8,
        glyphs: 'https://demotiles.maplibre.org/font/{fontstack}/{range}.pbf',
        sources: {
          basemap: {
            type: 'raster',
            tiles: ['https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png'],
            tileSize: 256,
            attribution:
              '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/attributions">CARTO</a>'
          }
        },
        layers: [
          {
            id: 'basemap',
            type: 'raster',
            source: 'basemap',
            paint: {
              'raster-saturation': -0.2,
              'raster-contrast': 0.04
            }
          }
        ]
      }
    };

    map = new maplibregl.Map(options);
    popup = new maplibregl.Popup({
      closeButton: false,
      closeOnClick: false,
      offset: 14,
      className: 'cell-popup'
    });

    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'bottom-right');
    map.addControl(new maplibregl.AttributionControl({ compact: true }), 'bottom-left');

    map.on('load', () => {
      addCellLayers();
      isMapReady = true;
    });

    map.on('mousemove', FILL_LAYER_ID, handleCellHover);
    map.on('mouseleave', FILL_LAYER_ID, clearHover);
    map.on('click', FILL_LAYER_ID, handleCellClick);
    map.on('error', (event) => {
      const message = event.error?.message ?? '';
      if (message.includes('rare_species_cells') || message.includes('pmtiles')) {
        tileError = `PMTiles non trovati o non leggibili: ${pmtilesUrl}`;
      }
    });

    return () => {
      maplibregl.removeProtocol('pmtiles');
      popup?.remove();
      map?.remove();
      protocol = undefined;
      map = undefined;
    };
  });

  function pmtilesSourceUrl() {
    return `pmtiles://${pmtilesUrl}`;
  }

  function addCellLayers() {
    if (!map) return;

    map.addSource(SOURCE_ID, {
      type: 'vector',
      url: pmtilesSourceUrl(),
      promoteId: 'h3'
    });

    map.addLayer({
      id: FILL_LAYER_ID,
      type: 'fill',
      source: SOURCE_ID,
      'source-layer': TILE_LAYER,
      filter: scoreFilter(),
      paint: {
        'fill-color': metricColorExpression(),
        'fill-opacity': fillOpacityExpression()
      }
    } satisfies FillLayerSpecification);

    map.addLayer({
      id: LINE_LAYER_ID,
      type: 'line',
      source: SOURCE_ID,
      'source-layer': TILE_LAYER,
      filter: scoreFilter(),
      paint: {
        'line-color': 'rgba(25, 45, 37, 0.5)',
        'line-opacity': ['interpolate', ['linear'], ['zoom'], 4, 0, 8, 0.32, 12, 0.48],
        'line-width': ['interpolate', ['linear'], ['zoom'], 4, 0.1, 9, 0.45, 12, 0.9]
      }
    });

    map.addLayer({
      id: HOVER_LINE_LAYER_ID,
      type: 'line',
      source: SOURCE_ID,
      'source-layer': TILE_LAYER,
      filter: ['==', ['get', 'h3'], ''],
      paint: {
        'line-color': '#101613',
        'line-opacity': 0.95,
        'line-width': ['interpolate', ['linear'], ['zoom'], 4, 1, 10, 2.2]
      }
    });
  }

  function updateCellLayers() {
    if (!map?.getLayer(FILL_LAYER_ID)) return;

    map.setPaintProperty(FILL_LAYER_ID, 'fill-color', metricColorExpression());
    map.setPaintProperty(FILL_LAYER_ID, 'fill-opacity', fillOpacityExpression());
    map.setFilter(FILL_LAYER_ID, scoreFilter());
    map.setFilter(LINE_LAYER_ID, scoreFilter());
  }

  function scoreFilter(): ExpressionSpecification {
    return ['>=', ['coalesce', ['get', 'rarity_score'], -999], scoreFloor];
  }

  function fillOpacityExpression(): ExpressionSpecification {
    return [
      'interpolate',
      ['linear'],
      ['zoom'],
      3,
      Math.max(0.08, opacity / 180),
      8,
      opacity / 100,
      12,
      Math.min(0.94, opacity / 86)
    ];
  }

  function metricColorExpression(): ExpressionSpecification {
    if (metric === 'count_observations') {
      return [
        'interpolate',
        ['linear'],
        ['coalesce', ['get', 'count_observations'], 0],
        1,
        '#e8f3ea',
        8,
        '#b8d9c3',
        30,
        '#6fb0a4',
        120,
        '#397996',
        500,
        '#24476f',
        2000,
        '#171d3f'
      ];
    }

    if (metric === 'count_species') {
      return [
        'interpolate',
        ['linear'],
        ['coalesce', ['get', 'count_species'], 0],
        1,
        '#fff1cf',
        4,
        '#f7c66a',
        12,
        '#de7d46',
        32,
        '#aa3c52',
        90,
        '#572461'
      ];
    }

    return [
      'interpolate',
      ['linear'],
      ['coalesce', ['get', 'rarity_score'], 0],
      -2,
      '#31517a',
      -0.75,
      '#72958a',
      0,
      '#f2e9c9',
      0.75,
      '#e6914f',
      1.8,
      '#b72f4a',
      3,
      '#5f1635'
    ];
  }

  function handleCellHover(event: LayerMouseEvent) {
    if (!map || !event.features?.length) return;

    map.getCanvas().style.cursor = 'pointer';
    const feature = event.features[0];
    const properties = feature.properties as CellProperties;
    hoveredH3 = properties.h3 ?? '';

    map.setFilter(HOVER_LINE_LAYER_ID, ['==', ['get', 'h3'], hoveredH3]);
    popup
      ?.setLngLat(event.lngLat)
      .setHTML(
        `<strong>${formatScore(properties.rarity_score)}</strong><span>${formatCount(
          properties.count_observations
        )} observations</span>`
      )
      .addTo(map);
  }

  function clearHover() {
    if (!map) return;
    map.getCanvas().style.cursor = '';
    hoveredH3 = '';
    map.setFilter(HOVER_LINE_LAYER_ID, ['==', ['get', 'h3'], '']);
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
    return value == null ? '0' : new Intl.NumberFormat('en-US').format(value);
  }

  function formatScore(value: number | undefined) {
    return value == null
      ? 'n/a'
      : new Intl.NumberFormat('en-US', {
          signDisplay: 'exceptZero',
          maximumFractionDigits: 3
        }).format(value);
  }

  function formatDecimal(value: number | undefined) {
    return value == null
      ? 'n/a'
      : new Intl.NumberFormat('en-US', { maximumFractionDigits: 5 }).format(value);
  }
</script>

<svelte:head>
  <title>Rare Species Map</title>
</svelte:head>

<main class="app-shell">
  <div class="map-surface" bind:this={mapContainer}></div>

  <section class="topbar" aria-label="Map overview">
    <div>
      <p class="eyebrow">H3 resolution 8 residual scores</p>
      <h1>Rare Species Map</h1>
    </div>
    <div class="status-pill" class:error={tileError}>
      <span class="status-dot"></span>
      {tileError ? 'Tile source issue' : 'PMTiles vector source'}
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
            on:click={() => (metric = key as Metric)}
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
        <input type="range" min="20" max="92" bind:value={opacity} />
      </label>

      <label class="slider-row">
        <span>Score floor</span>
        <strong>{scoreFloor.toFixed(1)}</strong>
        <input type="range" min="-3" max="2" step="0.1" bind:value={scoreFloor} />
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
          <dt>Rarity residual</dt>
          <dd>{formatScore(selectedCell.rarity_score)}</dd>
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
          <dt>Sum rarity</dt>
          <dd>{formatDecimal(selectedCell.sum_rarity)}</dd>
        </div>
      </dl>
    {:else}
      <p class="muted">No cell selected</p>
    {/if}
  </aside>

  <nav class="map-actions" aria-label="Map navigation">
    <button type="button" on:click={() => zoomBy(1)} aria-label="Zoom in" title="Zoom in">
      <Plus size={18} />
    </button>
    <button type="button" on:click={() => zoomBy(-1)} aria-label="Zoom out" title="Zoom out">
      <Minus size={18} />
    </button>
    <button type="button" on:click={resetView} aria-label="Reset view" title="Reset view">
      <LocateFixed size={18} />
    </button>
    <button
      type="button"
      on:click={() => updateCellLayers()}
      aria-label="Refresh style"
      title="Refresh style"
    >
      <RefreshCw size={17} />
    </button>
  </nav>

  <footer class="science-note">
    Areas where rare species are observed more often than expected given observation effort.
  </footer>
</main>
