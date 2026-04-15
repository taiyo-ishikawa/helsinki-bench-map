#!/usr/bin/env python3
"""
add_cafe_proximity.py

Adds is_near_cafe=True/False to every bench in helsinki_benches_deduped_en.json.

Classification logic:
  - Fetch OSM amenity=cafe (+ amenity=coffee_shop) nodes/ways for Helsinki bbox
  - For each bench, check if any cafe is within CAFE_BUFFER_M metres (EPSG:3067)
  - Uses scipy cKDTree for fast nearest-neighbour lookup

Requirements: requests, pyproj, scipy
"""

import json
import requests
from pyproj import Transformer
from scipy.spatial import cKDTree

BENCH_FILE    = "helsinki_benches_deduped_en.json"
CAFE_BUFFER_M = 100.0   # metres

BBOX = "60.10,24.78,60.36,25.26"   # south,west,north,east
QUERY = f"""[out:json][timeout:120][bbox:{BBOX}];
(
  node["amenity"="cafe"];
  node["amenity"="coffee_shop"];
  way["amenity"="cafe"];
  way["amenity"="coffee_shop"];
);
out center tags;"""

ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
]

_wgs84_to_3067 = Transformer.from_crs("EPSG:4326", "EPSG:3067", always_xy=True)


def fetch_cafes():
    for ep in ENDPOINTS:
        try:
            print(f"  Trying {ep} …")
            r = requests.post(ep, data={"data": QUERY}, timeout=130)
            if r.status_code == 200:
                elements = r.json().get("elements", [])
                print(f"  ✓ {len(elements)} elements from {ep}")
                return elements
            print(f"  HTTP {r.status_code}")
        except Exception as e:
            print(f"  {ep}: {e}")
    return []


def main():
    print(f"Loading {BENCH_FILE} …")
    with open(BENCH_FILE, encoding="utf-8") as f:
        data = json.load(f)
    features = data["features"]
    print(f"  {len(features)} benches")

    print("\nFetching cafe locations from OSM …")
    elements = fetch_cafes()

    # Extract (x, y) in EPSG:3067 for each cafe
    cafe_pts = []
    for el in elements:
        if el["type"] == "node":
            lon, lat = el["lon"], el["lat"]
        elif el["type"] == "way" and "center" in el:
            lon, lat = el["center"]["lon"], el["center"]["lat"]
        else:
            continue
        x, y = _wgs84_to_3067.transform(lon, lat)
        cafe_pts.append([x, y])

    if not cafe_pts:
        print("No cafe locations found — aborting.")
        return

    print(f"  {len(cafe_pts)} cafes → building cKDTree")
    tree = cKDTree(cafe_pts)

    print(f"\nClassifying {len(features)} benches (threshold: {CAFE_BUFFER_M} m) …")
    n_near = 0
    for i, feat in enumerate(features):
        lon, lat = feat["geometry"]["coordinates"]
        x, y = _wgs84_to_3067.transform(lon, lat)
        dist, _ = tree.query([x, y])
        is_near = bool(dist < CAFE_BUFFER_M)
        feat["properties"]["is_near_cafe"] = is_near
        if is_near:
            n_near += 1
        if (i + 1) % 2000 == 0:
            print(f"  … {i+1}/{len(features)}", flush=True)

    pct = n_near / len(features) * 100
    print(f"\nis_near_cafe=True: {n_near:,} / {len(features):,} ({pct:.1f}%)")

    with open(BENCH_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Saved → {BENCH_FILE}")


if __name__ == "__main__":
    main()
