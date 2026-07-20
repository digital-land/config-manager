// Load MapLibre GL JS
if (!document.querySelector('link[href*="maplibre"]')) {
  const css = document.createElement("link");
  css.rel = "stylesheet";
  css.href = "https://unpkg.com/maplibre-gl@3.6.2/dist/maplibre-gl.css";
  document.head.appendChild(css);
}

if (!window.maplibregl) {
  const script = document.createElement("script");
  script.src = "https://unpkg.com/maplibre-gl@3.6.2/dist/maplibre-gl.js";
  script.onload = initMap;
  document.head.appendChild(script);
} else {
  initMap();
}

const calculateBoundingBoxFromGeometries = (geometries) => {
  let minX = Infinity;
  let minY = Infinity;
  let maxX = -Infinity;
  let maxY = -Infinity;

  if (!geometries) return [];

  const pullOutCoordinates = (geometry) => {
    if (Array.isArray(geometry[0])) {
      geometry.forEach(pullOutCoordinates);
    } else {
      const [x, y] = geometry;

      if (isNaN(x) || isNaN(y)) {
        console.error("Invalid coordinates", x, y);
        return;
      }

      minX = Math.min(minX, x);
      minY = Math.min(minY, y);
      maxX = Math.max(maxX, x);
      maxY = Math.max(maxY, y);
    }
  };

  pullOutCoordinates(geometries);

  return [
    [minX, minY],
    [maxX, maxY],
  ];
};

async function addGeoJsonUrlsToMap(map, geoJsonUrls) {
  geoJsonUrls.forEach(async (url, index) => {
    const name = `geometry-${index}`;
    map.addSource(name, {
      type: "geojson",
      data: url,
    });

    map.addLayer({
      id: name,
      type: "fill",
      source: name,
      paint: {
        "fill-color": "#008",
        "fill-opacity": 0.4,
      },
    });

    map.addLayer({
      id: `${name}-border`,
      type: "line",
      source: name,
      paint: {
        "line-color": "#000000",
        "line-width": 1,
      },
    });
  });
}

// `text` is the cluster-count label colour, chosen for contrast on `colour`.
const LAYER_KEY = [
  { id: "new", label: "New", colour: "#00703c", text: "#ffffff" },
  { id: "changed", label: "Changed", colour: "#f47738", text: "#0b0c0c" },
  { id: "in_both", label: "Matching platform", colour: "#b59b00", text: "#0b0c0c" },
  { id: "existing", label: "Platform only", colour: "#1d70b8", text: "#ffffff" },
];

// Check-results page has no status — everything is one neutral group.
const NEUTRAL_CATEGORY = { id: "all", colour: "#1d70b8", text: "#ffffff" };

// Up to CLUSTER_MAX_ZOOM (~1km scale) everything reads as a count bubble —
// real clusters show their total, and a lone entity shows as a "1" bubble so it
// isn't missed. Above it, entities appear as their boundary + a highlight point.
const CLUSTER_MAX_ZOOM = 13;
const POLYGON_MIN_ZOOM = CLUSTER_MAX_ZOOM + 1; // boundaries appear once bubbles stop

// The highlight point stays solid through the bubble→entity handoff and only
// fades once you are zoomed right in (z16→18) and the boundary is the clear
// representation. Point-only entities (no polygon) keep their highlight always.
const POINT_HANDOFF_OPACITY = [
  "case",
  ["get", "has_polygon"],
  ["interpolate", ["linear"], ["zoom"], 16, 1, 18, 0],
  1,
];

// Checkbox key rendered as a map control so it stays visible in fullscreen.
class LayerToggleControl {
  // `activeFilter` comes from the category box the user clicked on the page
  // (new/changed/in_both/existing). When set, the map opens with only that
  // category ticked — the user can still tick the others back on afterwards.
  constructor(activeFilter) {
    const validFilter = LAYER_KEY.some((l) => l.id === activeFilter);
    this.enabled = validFilter
      ? new Set([activeFilter])
      : new Set(LAYER_KEY.map((l) => l.id));
  }

  onAdd(map) {
    this._map = map;
    this._container = document.createElement("div");
    this._container.className = "maplibregl-ctrl maplibregl-ctrl-group app-map-key";
    LAYER_KEY.forEach((layer) => {
      const label = document.createElement("label");
      const checkbox = document.createElement("input");
      checkbox.type = "checkbox";
      checkbox.checked = this.enabled.has(layer.id);
      checkbox.addEventListener("change", () => {
        if (checkbox.checked) {
          this.enabled.add(layer.id);
        } else {
          this.enabled.delete(layer.id);
        }
        this.apply();
      });
      const swatch = document.createElement("span");
      swatch.className = "app-map-key__swatch";
      swatch.style.borderColor = layer.colour;
      swatch.style.backgroundColor = layer.colour + "66";
      label.appendChild(checkbox);
      label.appendChild(swatch);
      label.appendChild(document.createTextNode(layer.label));
      this._container.appendChild(label);
    });
    // Reflect the initial selection on the map (a no-op when all are ticked).
    this.apply();
    return this._container;
  }

  onRemove() {
    this._container.remove();
    this._map = undefined;
  }

  apply() {
    const statuses = LAYER_KEY.map((l) => l.id).filter((s) => this.enabled.has(s));
    const statusFilter = ["in", ["get", "status"], ["literal", statuses]];
    const polygonFilter = ["all", ["==", ["geometry-type"], "Polygon"], statusFilter];
    if (this._map.getLayer("dataset-fill")) this._map.setFilter("dataset-fill", polygonFilter);
    if (this._map.getLayer("dataset-border")) this._map.setFilter("dataset-border", polygonFilter);
    // Each category has its own clustered source, so a category is toggled by
    // flipping the visibility of all of its layers — the other categories'
    // cluster counts are untouched because they never contained these points.
    LAYER_KEY.forEach((layer) => {
      const visibility = this.enabled.has(layer.id) ? "visible" : "none";
      [
        `clusters-${layer.id}`,
        `cluster-count-${layer.id}`,
        `point-bubbles-${layer.id}`,
        `point-count-${layer.id}`,
        `points-${layer.id}`,
      ].forEach((layerId) => {
        if (this._map.getLayer(layerId)) {
          this._map.setLayoutProperty(layerId, "visibility", visibility);
        }
      });
    });
  }
}

class EntitySearchControl {
  constructor(findEntity) {
    this.findEntity = findEntity;
  }

  onAdd(map) {
    this._map = map;
    this._container = document.createElement("div");
    this._container.className = "maplibregl-ctrl maplibregl-ctrl-group app-map-search";

    const label = document.createElement("label");
    label.style.display = "block";
    label.style.padding = "8px";

    const text = document.createElement("span");
    text.textContent = "Entity ID";
    text.style.display = "block";
    text.style.fontSize = "12px";
    text.style.fontWeight = "bold";
    text.style.marginBottom = "4px";

    const row = document.createElement("div");
    row.style.display = "flex";
    row.style.gap = "6px";

    this._input = document.createElement("input");
    this._input.type = "search";
    this._input.placeholder = "Search entity";
    this._input.autocomplete = "off";
    this._input.spellcheck = false;
    this._input.style.width = "140px";
    this._input.style.padding = "4px 6px";

    const button = document.createElement("button");
    button.type = "button";
    button.textContent = "Go";
    button.style.backgroundColor = "#00703c";
    button.style.border = "1px solid #00703c";
    button.style.color = "#ffffff";
    button.style.padding = "4px 8px";
    button.style.cursor = "pointer";

    this._message = document.createElement("div");
    this._message.style.marginTop = "4px";
    this._message.style.fontSize = "12px";
    this._message.style.maxWidth = "176px";

    const submit = () => {
      const entityId = this._input.value.trim();
      if (!entityId) {
        this.setMessage("Enter an entity ID.", false);
        return;
      }

      const feature = this.findEntity(entityId);
      if (!feature) {
        this.setMessage(`No entity found for ${entityId}.`, false);
        return;
      }

      this.focusFeature(feature);
      this.setMessage(`Showing ${entityId}.`, true);
    };

    button.addEventListener("click", submit);
    this._input.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        submit();
      }
    });

    row.appendChild(this._input);
    row.appendChild(button);
    label.appendChild(text);
    label.appendChild(row);
    label.appendChild(this._message);
    this._container.appendChild(label);
    return this._container;
  }

  setMessage(message, isSuccess) {
    this._message.textContent = message;
    this._message.style.color = isSuccess ? "#00703c" : "#d4351c";
  }

  focusFeature(feature) {
    const geometry = feature.geometry || {};
    if (geometry.type === "Point") {
      this._map.easeTo({ center: geometry.coordinates, zoom: 16 });
      return;
    }

    const bounds = new maplibregl.LngLatBounds();
    if (geometry.type === "Polygon") {
      geometry.coordinates.forEach((ring) => {
        ring.forEach((coord) => bounds.extend(coord));
      });
    } else if (geometry.type === "MultiPolygon") {
      geometry.coordinates.forEach((polygon) => {
        polygon.forEach((ring) => {
          ring.forEach((coord) => bounds.extend(coord));
        });
      });
    }

    if (!bounds.isEmpty()) {
      this._map.fitBounds(bounds, { padding: 40, maxZoom: 15, duration: 500 });
    }
  }

  onRemove() {
    this._container.remove();
    this._map = undefined;
  }
}

function addBoundaryGeoJsonToMap(map, geoJsonUrl) {
  console.log("Adding boundary from:", geoJsonUrl);

  map.addSource("boundary", {
    type: "geojson",
    data: geoJsonUrl,
  });

  map.addLayer({
    id: "boundary-line",
    type: "line",
    source: "boundary",
    layout: {},
    paint: {
      "line-color": "#ff0000",
      "line-width": 2,
      "line-opacity": 0.8,
    },
  });

  console.log("Boundary layer added successfully");
}

function addCategoryCluster(map, category, points, showPopup, hasPolygons) {
  const sourceId = `points-${category.id}`;
  map.addSource(sourceId, {
    type: "geojson",
    data: { type: "FeatureCollection", features: points },
    cluster: true,
    clusterMaxZoom: CLUSTER_MAX_ZOOM,
    clusterRadius: 50,
  });

  // Cluster bubble — one colour per category, size grows with the count.
  map.addLayer({
    id: `clusters-${category.id}`,
    type: "circle",
    source: sourceId,
    filter: ["has", "point_count"],
    paint: {
      "circle-color": category.colour,
      "circle-radius": ["step", ["get", "point_count"], 14, 10, 18, 50, 24],
      "circle-stroke-color": "#ffffff",
      "circle-stroke-width": 2,
    },
  });

  map.addLayer({
    id: `cluster-count-${category.id}`,
    type: "symbol",
    source: sourceId,
    filter: ["has", "point_count"],
    layout: {
      "text-field": ["get", "point_count_abbreviated"],
      "text-font": ["Open Sans Bold", "Arial Unicode MS Bold"],
      "text-size": 13,
    },
    paint: { "text-color": category.text },
  });

  // Individual (unclustered) points stay as a count bubble while zoomed out,
  // then become the actual point marker once the user zooms in.
  map.addLayer({
    id: `point-bubbles-${category.id}`,
    type: "circle",
    source: sourceId,
    filter: ["!", ["has", "point_count"]],
    ...(hasPolygons && { maxzoom: POLYGON_MIN_ZOOM }),
    paint: {
      "circle-color": category.colour,
      "circle-radius": ["interpolate", ["linear"], ["zoom"], 11, 8, 13, 12],
      "circle-stroke-color": "#ffffff",
      "circle-stroke-width": 2,
    },
  });

  map.addLayer({
    id: `point-count-${category.id}`,
    type: "symbol",
    source: sourceId,
    filter: ["!", ["has", "point_count"]],
    ...(hasPolygons && { maxzoom: POLYGON_MIN_ZOOM }),
    layout: {
      "text-field": "1",
      "text-font": ["Open Sans Bold", "Arial Unicode MS Bold"],
      "text-size": 13,
    },
    paint: { "text-color": category.text },
  });

  // Actual entity marker, shown once the bubble representation stops.
  map.addLayer({
    id: `points-${category.id}`,
    type: "circle",
    source: sourceId,
    filter: ["!", ["has", "point_count"]],
    minzoom: POLYGON_MIN_ZOOM,
    paint: {
      "circle-color": category.colour,
      "circle-radius": 6,
      "circle-stroke-color": "#000000",
      "circle-stroke-width": 1,
      "circle-opacity": POINT_HANDOFF_OPACITY,
      "circle-stroke-opacity": POINT_HANDOFF_OPACITY,
    },
  });

  // Click a cluster to zoom in until it breaks apart.
  map.on("click", `clusters-${category.id}`, (e) => {
    const feature = map.queryRenderedFeatures(e.point, {
      layers: [`clusters-${category.id}`],
    })[0];
    if (!feature) return;
    map
      .getSource(sourceId)
      .getClusterExpansionZoom(feature.properties.cluster_id, (err, zoom) => {
        if (err) return;
        map.easeTo({ center: feature.geometry.coordinates, zoom });
      });
  });
  map.on("click", `point-bubbles-${category.id}`, showPopup);
  map.on("click", `points-${category.id}`, showPopup);
  ["clusters", "point-bubbles", "points"].forEach((kind) => {
    map.on("mouseenter", `${kind}-${category.id}`, () => {
      map.getCanvas().style.cursor = "pointer";
    });
    map.on("mouseleave", `${kind}-${category.id}`, () => {
      map.getCanvas().style.cursor = "";
    });
  });
}

function initMap() {
  const { containerId, geometries, geometryPoints, boundaryGeoJsonUrl, entityFilter } =
    window.serverContext;

  if (!geometries || geometries.length === 0) {
    return null;
  }

  const points = geometryPoints || [];
    const searchableFeatures = [...geometries, ...points];
    const featuresByEntity = new Map();

    searchableFeatures.forEach((feature) => {
      const entityId = feature?.properties?.entity;
      if (entityId && !featuresByEntity.has(entityId)) {
        featuresByEntity.set(entityId, feature);
      }
    });

  const map = new maplibregl.Map({
    container: containerId,
    style:
      "https://api.maptiler.com/maps/basic-v2/style.json?key=ncAXR9XEn7JgHBLguAUw",
    zoom: 11,
    center: [-2.5, 54.5],
    maxBounds: [
      [-10.5, 49.5],
      [2.0, 61.0],
    ],
  });

  map.addControl(new maplibregl.ScaleControl(), "bottom-left");
  map.addControl(new maplibregl.NavigationControl());
  map.addControl(new maplibregl.FullscreenControl());

  map.on("load", async () => {
    console.log("Adding geometries to map:", geometries.length);

    // The transform page tags each feature with a status (new/in_both/changed/
    // existing) to drive per-category colours, clusters and the toggle. The
    // check-results page has no status — one neutral group, no toggle.
    const hasStatus = points.some((f) => f.properties && f.properties.status);
    const categories = hasStatus ? LAYER_KEY : [NEUTRAL_CATEGORY];

    const statusColour = hasStatus
      ? ["case",
          ["==", ["get", "status"], "new"],     "#00703c",
          ["==", ["get", "status"], "in_both"], "#b59b00",
          ["==", ["get", "status"], "changed"], "#f47738",
          "#1d70b8"
        ]
      : "#1d70b8";

    // Polygon boundaries — only shown once zoomed in, so clustered points can
    // stand in for them when zoomed out.
    map.addSource("dataset", {
      type: "geojson",
      data: { type: "FeatureCollection", features: geometries },
    });
    map.addLayer({
      id: "dataset-fill",
      type: "fill",
      source: "dataset",
      minzoom: points.length ? POLYGON_MIN_ZOOM : 0,
      filter: ["==", ["geometry-type"], "Polygon"],
      paint: { "fill-color": statusColour, "fill-opacity": 0.4 },
    });
    map.addLayer({
      id: "dataset-border",
      type: "line",
      source: "dataset",
      minzoom: points.length ? POLYGON_MIN_ZOOM : 0,
      filter: ["==", ["geometry-type"], "Polygon"],
      paint: { "line-color": "#000000", "line-width": 1 },
    });

    const showPopup = (e) => {
      const feature = e.features[0];
      const { reference, name, entity } = feature.properties;
      const lines = [
        `<strong>Ref:</strong> ${reference}`,
        entity ? `<strong>Entity:</strong> ${entity}` : null,
        name || null,
      ].filter(Boolean);
      new maplibregl.Popup()
        .setLngLat(e.lngLat)
        .setHTML(lines.join("<br/>"))
        .addTo(map);
    };

    const hasPolygonFeatures = geometries.some(
      (f) => f.geometry && (f.geometry.type === "Polygon" || f.geometry.type === "MultiPolygon")
    );

    // One clustered source per category so clusters never mix categories.
    categories.forEach((category) => {
      const categoryPoints = hasStatus
        ? points.filter((f) => f.properties.status === category.id)
        : points;
      if (categoryPoints.length > 0) {
        addCategoryCluster(map, category, categoryPoints, showPopup, hasPolygonFeatures);
      }
    });

    map.on("click", "dataset-fill", showPopup);
    map.on("mouseenter", "dataset-fill", () => {
      map.getCanvas().style.cursor = "pointer";
    });
    map.on("mouseleave", "dataset-fill", () => {
      map.getCanvas().style.cursor = "";
    });

    // Fit to the geometry extent.
    const bounds = new maplibregl.LngLatBounds();
    geometries.forEach((feature) => {
      if (feature.geometry.type === "Point") {
        bounds.extend(feature.geometry.coordinates);
      } else if (feature.geometry.coordinates) {
        feature.geometry.coordinates.forEach((polygon) => {
          polygon[0].forEach((coord) => {
            bounds.extend(coord);
          });
        });
      }
    });
    if (!bounds.isEmpty()) {
      map.fitBounds(bounds, { padding: 20, maxZoom: 9, duration: 0 });
    }

    if (hasStatus) {
      map.addControl(new LayerToggleControl(entityFilter), "top-left");
        map.addControl(
          new EntitySearchControl((entityId) => featuresByEntity.get(entityId)),
          "top-left"
        );
    }

    if (boundaryGeoJsonUrl) {
      addBoundaryGeoJsonToMap(map, boundaryGeoJsonUrl);
    }
  });
}