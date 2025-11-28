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

function addBoundaryGeoJsonToMap(map, geoJsonUrl) {
  console.log("Adding boundary from:", geoJsonUrl);

  try {
    const url = new URL(geoJsonUrl);
    const geometryCurie = url.searchParams.get("geometry_curie");
    console.log("geometry_curie found:", geometryCurie);

    if (geometryCurie) {
      // For datasets with geometry_curie, fetch the actual boundary
      const decodedCurie = decodeURIComponent(geometryCurie);
      const boundaryUrl = `https://www.planning.data.gov.uk/entity.geojson?curie=${decodedCurie}`;
      console.log("Fetching boundary from:", boundaryUrl);

      map.addSource("boundary", {
        type: "geojson",
        data: boundaryUrl,
      });
    } else {
      // For other datasets, use the URL directly
      console.log("No geometry_curie, using direct URL");
      map.addSource("boundary", {
        type: "geojson",
        data: geoJsonUrl,
      });
    }

    map.addLayer({
      id: "boundary",
      type: "line",
      source: "boundary",
      paint: {
        "line-color": "#ff0000",
        "line-width": 3,
        "line-opacity": 1,
      },
    });

    console.log("Boundary layer added successfully");
  } catch (error) {
    console.error("Error in addBoundaryGeoJsonToMap:", error);
  }
}

function initMap() {
  const { containerId, geometries, boundaryGeoJsonUrl } = window.serverContext;

  if (!geometries || geometries.length === 0) {
    document.getElementById(containerId).innerHTML =
      '<div style="padding: 20px;">No geometries available</div>';
    return;
  }

  const map = new maplibregl.Map({
    container: containerId,
    style:
      "https://api.maptiler.com/maps/basic-v2/style.json?key=ncAXR9XEn7JgHBLguAUw",
    zoom: 11,
    center: [-0.1298779, 51.4959698],
  });

  map.addControl(new maplibregl.ScaleControl(), "bottom-left");
  map.addControl(new maplibregl.NavigationControl());
  map.addControl(new maplibregl.FullscreenControl());

  map.on("load", async () => {
    console.log("Adding geometries to map:", geometries.length);

    map.addSource("dataset", {
      type: "geojson",
      data: {
        type: "FeatureCollection",
        features: geometries,
      },
    });

    map.addLayer({
      id: "dataset-fill",
      type: "fill",
      source: "dataset",
      paint: {
        "fill-color": "#008",
        "fill-opacity": 0.4,
      },
    });

    map.addLayer({
      id: "dataset-border",
      type: "line",
      source: "dataset",
      paint: {
        "line-color": "#000000",
        "line-width": 1,
      },
    });

    // Simple bounds calculation using MapLibre's built-in method
    const bounds = new maplibregl.LngLatBounds();
    geometries.forEach((feature) => {
      if (feature.geometry.coordinates) {
        feature.geometry.coordinates.forEach((polygon) => {
          polygon[0].forEach((coord) => {
            bounds.extend(coord);
          });
        });
      }
    });

    if (!bounds.isEmpty()) {
      map.fitBounds(bounds, { padding: 20 });
    }

    // Add popup on click
    map.on("click", "dataset-fill", (e) => {
      const feature = e.features[0];
      new maplibregl.Popup()
        .setLngLat(e.lngLat)
        .setHTML(
          `<strong>Ref:</strong> ${feature.properties.reference}<br/>${
            feature.properties.name || ""
          }`
        )
        .addTo(map);
    });

    map.on("mouseenter", "dataset-fill", () => {
      map.getCanvas().style.cursor = "pointer";
    });

    map.on("mouseleave", "dataset-fill", () => {
      map.getCanvas().style.cursor = "";
    });

    if (boundaryGeoJsonUrl) {
      try {
        addBoundaryGeoJsonToMap(map, boundaryGeoJsonUrl);
      } catch (error) {
        console.error("Error adding boundary:", error);
      }
    }
  });
}
