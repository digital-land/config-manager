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

function getFeatureCentroid(feature) {
  const geom = feature.geometry;
  if (geom.type === "Point") {
    return geom.coordinates;
  }
  const ring =
    geom.type === "Polygon"
      ? geom.coordinates[0]
      : geom.type === "MultiPolygon"
      ? geom.coordinates[0][0]
      : null;
  if (!ring || ring.length === 0) return null;
  const lng = ring.reduce((s, c) => s + c[0], 0) / ring.length;
  const lat = ring.reduce((s, c) => s + c[1], 0) / ring.length;
  return [lng, lat];
}

const LAYER_KEY = [
  { id: "new", label: "New", colour: "#00703c" },
  { id: "in_both", label: "In both", colour: "#b59b00" },
  { id: "changed", label: "Changed", colour: "#f47738" },
  { id: "existing", label: "Existing", colour: "#1d70b8" },
];

// Checkbox key rendered as a map control so it stays visible in fullscreen.
class LayerToggleControl {
  constructor() {
    this.enabled = new Set(LAYER_KEY.map((l) => l.id));
  }

  onAdd(map) {
    this._map = map;
    this._container = document.createElement("div");
    this._container.className = "maplibregl-ctrl maplibregl-ctrl-group app-map-key";
    LAYER_KEY.forEach((layer) => {
      const label = document.createElement("label");
      const checkbox = document.createElement("input");
      checkbox.type = "checkbox";
      checkbox.checked = true;
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
    return this._container;
  }

  onRemove() {
    this._container.remove();
    this._map = undefined;
  }

  apply() {
    const statuses = LAYER_KEY.map((l) => l.id).filter((s) => this.enabled.has(s));
    const statusFilter = ["in", ["get", "status"], ["literal", statuses]];
    this._map.setFilter("dataset-fill", ["all", ["==", ["geometry-type"], "Polygon"], statusFilter]);
    this._map.setFilter("dataset-border", ["all", ["==", ["geometry-type"], "Polygon"], statusFilter]);
    this._map.setFilter("dataset-points", ["all", ["==", ["geometry-type"], "Point"], statusFilter]);
    if (this._map.getLayer("entity-pointers")) {
      this._map.setFilter("entity-pointers", statusFilter);
    }
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

function initMap() {
  const { containerId, geometries, boundaryGeoJsonUrl } = window.serverContext;

  if (!geometries || geometries.length === 0) {
    return null;
  }

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

    // The transform page tags each feature with a status (new/both/platform)
    // to drive per-status colours and the layer toggle. The check-results page
    // has no status — render a single colour and skip the toggle there.
    const hasStatus = geometries.some((f) => f.properties && f.properties.status);

    map.addSource("dataset", {
      type: "geojson",
      data: {
        type: "FeatureCollection",
        features: geometries,
      },
    });

    const statusColour = hasStatus
      ? ["case",
          ["==", ["get", "status"], "new"],     "#00703c",
          ["==", ["get", "status"], "in_both"], "#b59b00",
          ["==", ["get", "status"], "changed"], "#f47738",
          "#1d70b8"
        ]
      : "#1d70b8";

    map.addLayer({
      id: "dataset-fill",
      type: "fill",
      source: "dataset",
      filter: ["==", ["geometry-type"], "Polygon"],
      paint: {
        "fill-color": statusColour,
        "fill-opacity": 0.4,
      },
    });

    map.addLayer({
      id: "dataset-border",
      type: "line",
      source: "dataset",
      filter: ["==", ["geometry-type"], "Polygon"],
      paint: {
        "line-color": "#000000",
        "line-width": 1,
      },
    });

    map.addLayer({
      id: "dataset-points",
      type: "circle",
      source: "dataset",
      filter: ["==", ["geometry-type"], "Point"],
      paint: {
        "circle-color": statusColour,
        "circle-radius": 6,
        "circle-stroke-color": "#000000",
        "circle-stroke-width": 1,
      },
    });

    // Zoom-dependent pointer markers for new/updated entities — visible when
    // zoomed out, fade out as you zoom in. Gone by ~zoom 13 (roughly the 500m
    // scale mark at UK latitudes) so they don't obscure the real geometry.
    const centroids = hasStatus
      ? {
          type: "FeatureCollection",
          features: geometries
            .filter((f) => f.properties.status === "new" || f.properties.status === "changed")
            .map((f) => {
              const coords = getFeatureCentroid(f);
              return coords
                ? { type: "Feature", geometry: { type: "Point", coordinates: coords }, properties: f.properties }
                : null;
            })
            .filter(Boolean),
        }
      : { type: "FeatureCollection", features: [] };

    if (centroids.features.length > 0) {
      map.addSource("entity-centroids", { type: "geojson", data: centroids });
      map.addLayer({
        id: "entity-pointers",
        type: "circle",
        source: "entity-centroids",
        paint: {
          "circle-color": statusColour,
          "circle-radius": ["interpolate", ["linear"], ["zoom"], 5, 14, 10, 10, 12, 6],
          "circle-stroke-color": "#ffffff",
          "circle-stroke-width": 2,
          // Fade both the fill AND the white stroke, otherwise the ring lingers
          // after the fill has gone. Gone by ~zoom 13 (roughly the 500m mark).
          "circle-opacity": ["interpolate", ["linear"], ["zoom"], 10, 1, 13, 0],
          "circle-stroke-opacity": ["interpolate", ["linear"], ["zoom"], 10, 1, 13, 0],
        },
      });
    }

    // Simple bounds calculation using MapLibre's built-in method
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

    // Add popup on click for both polygons and points
    const showPopup = (e) => {
      const feature = e.features[0];
      new maplibregl.Popup()
        .setLngLat(e.lngLat)
        .setHTML(
          `<strong>Ref:</strong> ${feature.properties.reference}<br/>${
            feature.properties.name || ""
          }`
        )
        .addTo(map);
    };

    map.on("click", "dataset-fill", showPopup);
    map.on("click", "dataset-points", showPopup);

    map.on("mouseenter", "dataset-fill", () => {
      map.getCanvas().style.cursor = "pointer";
    });
    map.on("mouseenter", "dataset-points", () => {
      map.getCanvas().style.cursor = "pointer";
    });

    map.on("mouseleave", "dataset-fill", () => {
      map.getCanvas().style.cursor = "";
    });
    map.on("mouseleave", "dataset-points", () => {
      map.getCanvas().style.cursor = "";
    });

    if (hasStatus) {
      map.addControl(new LayerToggleControl(), "top-left");
    }

    if (boundaryGeoJsonUrl) {
      addBoundaryGeoJsonToMap(map, boundaryGeoJsonUrl);
    }
  });
}