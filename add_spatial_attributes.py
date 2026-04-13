#!/usr/bin/env python3
"""
add_spatial_attributes.py

Enriches helsinki_benches_deduped_en.json with:
  - is_nature:     True for YLRE park benches or OSM benches within park/forest polygons
  - is_waterfront: True for benches within 200 m of the sea coastline

Generates district_stats.json with per-district bench statistics:
  - bench_count, area_km2, bench_density, city_count, comm_count,
    population (if PxWeb available), pop_density, benches_per_100pop

Requirements: shapely, pyproj, scipy, requests
"""

import json
import time
import requests
from pyproj import Transformer
from shapely.geometry import shape, Point
from shapely.ops import unary_union
from scipy.spatial import cKDTree

BENCH_FILE   = "helsinki_benches_deduped_en.json"
STATS_FILE   = "district_stats.json"
WFS_BASE     = "https://kartta.hel.fi/ws/geoserver/avoindata/wfs"
OVERPASS_URLS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
]
BBOX = "60.10,24.82,60.35,25.25"   # south,west,north,east
WATERFRONT_M = 200.0                # metres from coastline

_wgs84_to_3067  = Transformer.from_crs("EPSG:4326", "EPSG:3067", always_xy=True)
_3879_to_wgs84  = Transformer.from_crs("EPSG:3879", "EPSG:4326", always_xy=True)
_3879_to_3067   = Transformer.from_crs("EPSG:3879", "EPSG:3067", always_xy=True)


# ── Overpass helper ──────────────────────────────────────────────────────────

def overpass_query(query, label=""):
    for url in OVERPASS_URLS:
        try:
            print(f"  [{label}] {url} …")
            r = requests.post(url, data={"data": query}, timeout=210)
            if r.status_code == 200:
                return r.json()
            print(f"    HTTP {r.status_code}")
        except Exception as e:
            print(f"    Error: {e}")
        time.sleep(3)
    return None


# ── WFS helper ───────────────────────────────────────────────────────────────

def fetch_wfs_all(layer_name, page=500):
    features, start = [], 0
    while True:
        params = {
            "service": "WFS", "version": "2.0.0", "request": "GetFeature",
            "typeNames": layer_name, "outputFormat": "application/json",
            "count": page, "startIndex": start,
        }
        r = requests.get(WFS_BASE, params=params, timeout=120)
        r.raise_for_status()
        batch = r.json().get("features", [])
        features.extend(batch)
        if len(batch) < page:
            break
        start += len(batch)
    return features


# ── District boundaries + area ───────────────────────────────────────────────

def fetch_districts():
    """Return {district_name: {"area_km2": float, "polygon_wgs84": shapely}}."""
    print("Fetching Kaupunginosajako district polygons …")
    raw = fetch_wfs_all("avoindata:Kaupunginosajako")
    result = {}

    for f in raw:
        name = f["properties"].get("nimi_fi")
        if not name:
            continue
        g = f.get("geometry")
        if not g:
            continue

        def ring_to_wgs84(coords):
            return [list(_3879_to_wgs84.transform(x, y)) for x, y in coords]

        def ring_to_metric(coords):
            return [list(_3879_to_3067.transform(x, y)) for x, y in coords]

        try:
            if g["type"] == "Polygon":
                poly_wgs84  = shape({"type": "Polygon", "coordinates": [ring_to_wgs84(r)  for r in g["coordinates"]]})
                poly_metric = shape({"type": "Polygon", "coordinates": [ring_to_metric(r) for r in g["coordinates"]]})
                area_km2 = poly_metric.area / 1e6
            elif g["type"] == "MultiPolygon":
                wgs_parts, met_parts = [], []
                for part in g["coordinates"]:
                    wgs_parts.append(shape({"type": "Polygon", "coordinates": [ring_to_wgs84(r)  for r in part]}))
                    met_parts.append(shape({"type": "Polygon", "coordinates": [ring_to_metric(r) for r in part]}))
                poly_wgs84  = unary_union(wgs_parts)
                area_km2 = sum(p.area for p in met_parts) / 1e6
            else:
                continue

            result[name] = {
                "area_km2":    round(area_km2, 4),
                "polygon_wgs84": poly_wgs84,
            }
        except Exception as e:
            print(f"  Warning: failed to process district {name}: {e}")

    print(f"  → {len(result)} districts loaded")
    return result


# ── Population (PxWeb) ───────────────────────────────────────────────────────

def fetch_population():
    """Try to fetch kaupunginosa-level population from stat.hel.fi PxWeb API.
    Returns dict {district_name: population} or empty dict on failure."""
    print("Fetching population from stat.hel.fi PxWeb …")
    try:
        base = "https://stat.hel.fi/pxweb/api/v1/fi/Helsinki/"
        r = requests.get(base, timeout=10)
        if r.status_code != 200:
            print(f"  stat.hel.fi returned HTTP {r.status_code} – skipping population")
            return {}

        items = r.json()
        # Look for a population / väestö table
        pop_item = next(
            (i for i in items
             if any(kw in i.get("id","").lower() for kw in ["vaes","väes","pop","01_v"])),
            None
        )
        if not pop_item:
            print(f"  Population table not found in API root – skipping")
            return {}

        # Navigate one level deeper
        sub_url = base + pop_item["id"] + "/"
        r2 = requests.get(sub_url, timeout=10)
        if r2.status_code != 200:
            return {}

        sub_items = r2.json()
        table = next(
            (i for i in sub_items if i.get("type") == "t" and
             any(kw in i.get("id","").lower() for kw in ["kaupu","district","asukas"])),
            sub_items[0] if sub_items else None
        )
        if not table:
            return {}

        table_url = sub_url + table["id"]
        # Get table metadata
        meta = requests.get(table_url, timeout=10).json()
        variables = meta.get("variables", [])
        # Build a query: select all areas, latest year
        query = {"query": [], "response": {"format": "json-stat2"}}
        for var in variables:
            vals = [v["code"] for v in var.get("values", [])]
            if var.get("code") in ("Alue", "alue", "Kaupunginosa"):
                query["query"].append({"code": var["code"], "selection": {"filter": "all", "values": ["*"]}})
            elif var.get("code") in ("Vuosi", "vuosi", "Year"):
                query["query"].append({"code": var["code"], "selection": {"filter": "item", "values": [vals[-1]]}})
            else:
                query["query"].append({"code": var["code"], "selection": {"filter": "item", "values": [vals[-1]]}})

        data_resp = requests.post(table_url, json=query, timeout=15)
        if data_resp.status_code != 200:
            return {}

        # Parse json-stat2 format
        js = data_resp.json()
        dims = js.get("dimension", {})
        values = js.get("value", [])
        area_dim = next((d for d in dims if d.lower() in ("alue","kaupunginosa")), list(dims.keys())[0])
        area_cats = dims[area_dim]["category"]["label"]
        pop_dict = {}
        for i, (code, label) in enumerate(area_cats.items()):
            name = label.upper().strip()
            if i < len(values) and values[i] is not None:
                pop_dict[name] = int(values[i])
        print(f"  → {len(pop_dict)} district populations loaded")
        return pop_dict

    except Exception as e:
        print(f"  Population fetch failed: {e}")
        return {}


# ── Coastline (waterfront detection) ─────────────────────────────────────────

def fetch_coastline_kdtree():
    """Fetch OSM coastline ways, return cKDTree in EPSG:3067 (metric)."""
    print("Fetching coastline from OSM …")
    query = f"""
[out:json][timeout:180];
way["natural"="coastline"]({BBOX});
out geom;
"""
    data = overpass_query(query, "coastline")
    if not data:
        print("  Coastline fetch failed – waterfront will not be assigned")
        return None

    points = []
    for el in data.get("elements", []):
        if el["type"] == "way" and "geometry" in el:
            for nd in el["geometry"]:
                x, y = _wgs84_to_3067.transform(nd["lon"], nd["lat"])
                points.append([x, y])

    if not points:
        print("  No coastline points found")
        return None

    print(f"  → {len(points)} coastline points → building KDTree")
    return cKDTree(points)


# ── Park / nature polygons ────────────────────────────────────────────────────

def fetch_park_union():
    """Fetch OSM park/forest polygons, return shapely union (WGS84) or None."""
    print("Fetching park/forest/wood polygons from OSM …")
    query = f"""
[out:json][timeout:180];
(
  way["leisure"="park"]({BBOX});
  way["landuse"="forest"]({BBOX});
  way["natural"="wood"]({BBOX});
  way["leisure"="nature_reserve"]({BBOX});
  relation["leisure"="park"]({BBOX});
  relation["landuse"="forest"]({BBOX});
  relation["natural"="wood"]({BBOX});
);
out geom;
"""
    data = overpass_query(query, "parks")
    if not data:
        print("  Park fetch failed")
        return None

    polygons = []
    for el in data.get("elements", []):
        if el["type"] == "way" and "geometry" in el:
            coords = [(nd["lon"], nd["lat"]) for nd in el["geometry"]]
            if len(coords) >= 4 and coords[0] == coords[-1]:
                try:
                    poly = shape({"type": "Polygon", "coordinates": [coords]})
                    if poly.is_valid:
                        polygons.append(poly)
                    elif poly.buffer(0).is_valid:
                        polygons.append(poly.buffer(0))
                except Exception:
                    pass
        elif el["type"] == "relation":
            # Collect outer ring members
            for member in el.get("members", []):
                if member.get("role") == "outer" and "geometry" in member:
                    coords = [(nd["lon"], nd["lat"]) for nd in member["geometry"]]
                    if len(coords) >= 4 and coords[0] == coords[-1]:
                        try:
                            poly = shape({"type": "Polygon", "coordinates": [coords]})
                            if poly.is_valid:
                                polygons.append(poly)
                        except Exception:
                            pass

    if not polygons:
        return None

    print(f"  → {len(polygons)} park polygons → merging …")
    try:
        return unary_union(polygons)
    except Exception as e:
        print(f"  Union failed: {e}")
        return polygons[0] if polygons else None


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    # Load bench data
    print(f"Loading {BENCH_FILE} …")
    with open(BENCH_FILE, encoding="utf-8") as f:
        data = json.load(f)
    features = data["features"]
    print(f"  {len(features)} benches")

    # Fetch spatial resources
    coastline_tree = fetch_coastline_kdtree()
    park_union     = fetch_park_union()
    districts      = fetch_districts()
    population     = fetch_population()

    # Per-district counters
    district_city_count = {}
    district_comm_count = {}

    print(f"\nClassifying {len(features)} benches …")
    n_nature     = 0
    n_waterfront = 0

    for i, feat in enumerate(features):
        props        = feat["properties"]
        lon, lat     = feat["geometry"]["coordinates"]
        source       = props.get("source", "")
        ylre_source  = props.get("ylre_source", "")

        # ── is_nature ───────────────────────────────────────────────────────
        if source in ("Helsinki_YLRE_park",):
            is_nature = True
        elif source == "YLRE+OSM" and "park" in ylre_source.lower():
            is_nature = True
        elif source == "OpenStreetMap" and park_union is not None:
            is_nature = bool(park_union.contains(Point(lon, lat)))
        else:
            is_nature = False

        props["is_nature"] = is_nature
        if is_nature:
            n_nature += 1

        # ── is_waterfront ───────────────────────────────────────────────────
        if coastline_tree is not None:
            x, y = _wgs84_to_3067.transform(lon, lat)
            dist, _ = coastline_tree.query([x, y])
            is_waterfront = bool(dist < WATERFRONT_M)
        else:
            is_waterfront = False

        props["is_waterfront"] = is_waterfront
        if is_waterfront:
            n_waterfront += 1

        # ── District counters ───────────────────────────────────────────────
        district = props.get("district")
        if district:
            is_comm = (source == "OpenStreetMap")
            if is_comm:
                district_comm_count[district] = district_comm_count.get(district, 0) + 1
            else:
                district_city_count[district] = district_city_count.get(district, 0) + 1

        if (i + 1) % 2000 == 0:
            print(f"  … {i+1}/{len(features)}", flush=True)

    # Save enriched bench data
    with open(BENCH_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"\nSaved enriched bench data → {BENCH_FILE}")
    print(f"  is_nature:     {n_nature:,} ({n_nature/len(features)*100:.1f}%)")
    print(f"  is_waterfront: {n_waterfront:,} ({n_waterfront/len(features)*100:.1f}%)")

    # Build district_stats.json
    print("\nBuilding district statistics …")
    all_districts = sorted(set(list(districts.keys()) + list(district_city_count.keys()) + list(district_comm_count.keys())))
    stats = []
    for name in all_districts:
        city_n  = district_city_count.get(name, 0)
        comm_n  = district_comm_count.get(name, 0)
        total_n = city_n + comm_n
        info    = districts.get(name, {})
        area    = info.get("area_km2")
        pop     = population.get(name) or population.get(name.title()) or None

        # Normalize district name variants for population lookup
        if pop is None:
            for key, val in population.items():
                if key.upper() == name.upper():
                    pop = val
                    break

        stats.append({
            "district":           name,
            "bench_count":        total_n,
            "city_count":         city_n,
            "comm_count":         comm_n,
            "area_km2":           area,
            "bench_density":      round(total_n / area, 2)  if area and area > 0 else None,
            "population":         pop,
            "pop_density":        round(pop / area, 1)       if pop and area and area > 0 else None,
            "benches_per_100pop": round(total_n / pop * 100, 2) if pop and pop > 0 else None,
            "comm_pct":           round(comm_n / total_n * 100, 1) if total_n > 0 else 0,
        })

    # Sort by bench density (descending)
    stats.sort(key=lambda x: x["bench_density"] or 0, reverse=True)

    with open(STATS_FILE, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(stats)} districts → {STATS_FILE}")

    # Preview top 5
    print("\nTop 5 districts by bench density (benches/km²):")
    for s in stats[:5]:
        print(f"  {s['district']:<20} {s['bench_density']:>6.1f} benches/km²  (n={s['bench_count']})")


if __name__ == "__main__":
    main()
