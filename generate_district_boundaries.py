#!/usr/bin/env python3
"""
generate_district_boundaries.py

1. Fetches Helsinki district polygons from WFS → district_boundaries.json
   (simplified GeoJSON in WGS84, bench stats embedded)
2. Tries aluesarjat.fi PxWeb for population & average income
3. Updates district_stats.json with population / pop_density / avg_income

Requirements: shapely, pyproj, requests
"""

import json
import time
import requests
from pyproj import Transformer
from shapely.geometry import shape, mapping
from shapely.ops import unary_union

WFS_BASE = "https://kartta.hel.fi/ws/geoserver/avoindata/wfs"
BOUNDARIES_FILE = "district_boundaries.json"
STATS_FILE      = "district_stats.json"

_3879_to_wgs84  = Transformer.from_crs("EPSG:3879", "EPSG:4326", always_xy=True)
_3879_to_3067   = Transformer.from_crs("EPSG:3879", "EPSG:3067", always_xy=True)


# ── WFS helper ────────────────────────────────────────────────────────────────

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


# ── Generate district_boundaries.json ─────────────────────────────────────────

def generate_boundaries(stats_dict):
    print("Fetching Kaupunginosajako district polygons …")
    raw = fetch_wfs_all("avoindata:Kaupunginosajako")
    print(f"  → {len(raw)} features")

    geojson_features = []
    for f in raw:
        name = f["properties"].get("nimi_fi")
        if not name:
            continue
        g = f.get("geometry")
        if not g:
            continue

        def ring_wgs84(coords):
            return [list(_3879_to_wgs84.transform(x, y)) for x, y in coords]
        def ring_metric(coords):
            return [list(_3879_to_3067.transform(x, y)) for x, y in coords]

        try:
            if g["type"] == "Polygon":
                poly_wgs84  = shape({"type": "Polygon", "coordinates": [ring_wgs84(r)  for r in g["coordinates"]]})
                poly_metric = shape({"type": "Polygon", "coordinates": [ring_metric(r) for r in g["coordinates"]]})
            elif g["type"] == "MultiPolygon":
                wgs_parts, met_parts = [], []
                for part in g["coordinates"]:
                    wgs_parts.append(shape({"type": "Polygon", "coordinates": [ring_wgs84(r)  for r in part]}))
                    met_parts.append(shape({"type": "Polygon", "coordinates": [ring_metric(r) for r in part]}))
                poly_wgs84  = unary_union(wgs_parts)
                poly_metric = unary_union(met_parts)
            else:
                continue

            area_km2 = poly_metric.area / 1e6
            # Simplify for smaller file (≈50m tolerance in degrees)
            poly_simplified = poly_wgs84.simplify(0.0005, preserve_topology=True)

        except Exception as e:
            print(f"  Warning: {name}: {e}")
            continue

        stat = stats_dict.get(name, {})
        props = {
            "district":      name,
            "area_km2":      round(area_km2, 3),
            "bench_count":   stat.get("bench_count", 0),
            "bench_density": stat.get("bench_density"),
            "city_count":    stat.get("city_count", 0),
            "comm_count":    stat.get("comm_count", 0),
            "comm_pct":      stat.get("comm_pct", 0),
            "population":    stat.get("population"),
            "pop_density":   stat.get("pop_density"),
            "avg_income":    stat.get("avg_income"),
        }
        geojson_features.append({
            "type": "Feature",
            "properties": props,
            "geometry": mapping(poly_simplified),
        })

    geojson = {"type": "FeatureCollection", "features": geojson_features}
    with open(BOUNDARIES_FILE, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False)
    print(f"  Saved {len(geojson_features)} districts → {BOUNDARIES_FILE}")
    return geojson_features


# ── PxWeb helper ──────────────────────────────────────────────────────────────

def pxweb_get(url, timeout=10):
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


def pxweb_post(url, query, timeout=20):
    try:
        r = requests.post(url, json=query, timeout=timeout)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


def navigate_pxweb(base_url, keyword_sets, max_depth=3):
    """BFS-navigate a PxWeb API to find tables containing any of keyword_sets."""
    from collections import deque
    queue = deque([(base_url, 0)])
    visited = set()
    results = []

    while queue:
        url, depth = queue.popleft()
        if url in visited or depth > max_depth:
            continue
        visited.add(url)

        items = pxweb_get(url)
        if not items or not isinstance(items, list):
            continue

        for item in items:
            item_id   = item.get("id", "")
            item_text = (item.get("text", "") + item.get("id", "")).lower()
            item_type = item.get("type", "l")
            child_url = url.rstrip("/") + "/" + item_id

            if item_type == "t":
                # It's a table – check keywords
                for kw_set in keyword_sets:
                    if all(kw in item_text for kw in kw_set):
                        results.append((child_url, item_text))
                        break
            else:
                # It's a folder – recurse
                if depth + 1 <= max_depth:
                    queue.append((child_url + "/", depth + 1))

    return results


def extract_pxweb_table(table_url, area_code="Alue", year_code="Vuosi", value_col=None):
    """Fetch a PxWeb table and return {area_name: value}."""
    meta = pxweb_get(table_url)
    if not meta:
        return {}

    variables = meta.get("variables", [])
    query_parts = []
    area_var = None
    year_var = None

    for var in variables:
        code   = var.get("code", "")
        values = [v["code"] for v in var.get("values", [])]
        texts  = [v["text"] for v in var.get("values", [])]
        code_l = code.lower()

        if any(kw in code_l for kw in ("alue", "kaupunginosa", "area")):
            area_var = {"code": code, "texts": texts, "values": values}
            query_parts.append({"code": code, "selection": {"filter": "all", "values": ["*"]}})
        elif any(kw in code_l for kw in ("vuosi", "year", "tid")):
            year_var = values
            query_parts.append({"code": code, "selection": {"filter": "item", "values": [values[-1]]}})
        else:
            query_parts.append({"code": code, "selection": {"filter": "item", "values": [values[-1]]}})

    if not area_var:
        return {}

    query = {"query": query_parts, "response": {"format": "json-stat2"}}
    data = pxweb_post(table_url, query)
    if not data:
        return {}

    try:
        dims   = data.get("dimension", {})
        values = data.get("value", [])
        # Find area dimension
        area_dim = next(
            (k for k in dims if any(kw in k.lower() for kw in ("alue","kaupunginosa","area"))),
            list(dims.keys())[0]
        )
        cats = dims[area_dim]["category"]
        labels = cats.get("label", {})
        indices = cats.get("index", {})
        result = {}
        for code, label in labels.items():
            idx = indices.get(code, None)
            if idx is not None and idx < len(values) and values[idx] is not None:
                result[label.upper().strip()] = values[idx]
        return result
    except Exception as e:
        print(f"  Parse error: {e}")
        return {}


# ── Fetch population ──────────────────────────────────────────────────────────

def fetch_population():
    """Try multiple PxWeb endpoints for Helsinki kaupunginosa population."""
    print("Fetching population data …")

    endpoints = [
        "https://aluesarjat.fi/pxweb/api/v1/fi/Aluesarjat/",
        "https://aluesarjat.fi/pxweb/api/v1/sv/Aluesarjat/",
        "https://aluesarjat.fi/pxweb/api/v1/en/Aluesarjat/",
    ]
    pop_keywords = [["väestö"], ["befolkning"], ["vaesto"], ["population"], ["asukas"]]

    for base in endpoints:
        print(f"  Trying {base} …")
        tables = navigate_pxweb(base, pop_keywords, max_depth=3)
        if tables:
            print(f"  Found {len(tables)} candidate tables: {[t[1][:40] for t in tables[:3]]}")
            for url, label in tables:
                print(f"    Querying: {url}")
                result = extract_pxweb_table(url)
                if result:
                    print(f"    → {len(result)} districts found")
                    return result
        time.sleep(1)

    print("  Population data not found via PxWeb")
    return {}


# ── Fetch average income ──────────────────────────────────────────────────────

def fetch_income():
    """Try to fetch average household disposable income by kaupunginosa."""
    print("Fetching income data …")

    endpoints = [
        "https://aluesarjat.fi/pxweb/api/v1/fi/Aluesarjat/",
    ]
    income_keywords = [["tulo"], ["tulotaso"], ["ansio"]]

    for base in endpoints:
        print(f"  Trying {base} …")
        tables = navigate_pxweb(base, income_keywords, max_depth=3)
        if tables:
            print(f"  Found {len(tables)} candidate tables: {[t[1][:40] for t in tables[:3]]}")
            for url, label in tables:
                print(f"    Querying: {url}")
                result = extract_pxweb_table(url)
                if result:
                    # Filter to plausible income values (€1,000 – €100,000/year)
                    filtered = {k: v for k, v in result.items() if isinstance(v, (int, float)) and 1000 < v < 200000}
                    if filtered:
                        print(f"    → {len(filtered)} districts with income data")
                        return filtered
        time.sleep(1)

    print("  Income data not found via PxWeb")
    return {}


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    # Load existing district_stats
    with open(STATS_FILE, encoding="utf-8") as f:
        stats_list = json.load(f)
    stats_dict = {s["district"]: s for s in stats_list}

    # Generate district boundaries
    generate_boundaries(stats_dict)

    # Try to enrich with population + income
    population = fetch_population()
    income     = fetch_income()

    if not population and not income:
        print("\nNo external statistics fetched – district_stats.json unchanged")
        return

    # Fuzzy-match district names (stats may use different capitalisation)
    def match(name, lookup):
        if name in lookup:
            return lookup[name]
        n_up = name.upper().strip()
        for k, v in lookup.items():
            if k.upper().strip() == n_up:
                return v
        return None

    updated = 0
    for s in stats_list:
        d    = s["district"]
        area = s.get("area_km2")

        pop = match(d, population)
        if pop is not None:
            s["population"] = int(pop)
            if area and area > 0:
                s["pop_density"] = round(pop / area, 1)
                if s.get("bench_count"):
                    s["benches_per_100pop"] = round(s["bench_count"] / pop * 100, 2)
            updated += 1

        inc = match(d, income)
        if inc is not None:
            s["avg_income"] = int(inc)

    print(f"\nUpdated {updated} districts with population data")

    with open(STATS_FILE, "w", encoding="utf-8") as f:
        json.dump(stats_list, f, ensure_ascii=False, indent=2)
    print(f"Saved → {STATS_FILE}")

    # Regenerate boundaries with updated stats
    stats_dict = {s["district"]: s for s in stats_list}
    generate_boundaries(stats_dict)


if __name__ == "__main__":
    main()
