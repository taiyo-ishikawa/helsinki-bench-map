#!/usr/bin/env python3
"""
classify_sidewalks.py

Adds is_sidewalk=True/False to every bench in helsinki_benches_deduped_en.json.

Classification logic:
  - YLRE street benches (Helsinki_YLRE_street): always is_sidewalk=True
  - YLRE park benches  (Helsinki_YLRE_park):   always is_sidewalk=False
  - OSM-only benches: check proximity (≤ BUFFER_M metres) to OSM footway/path/pedestrian.
    Falls back to heuristic (!is_nature) if Overpass is unavailable.

Requirements: requests, shapely, pyproj
"""

import json
import requests
from pyproj import Transformer
from shapely.geometry import Point, LineString
from shapely.strtree import STRtree

BENCH_FILE = "helsinki_benches_deduped_en.json"
BUFFER_M   = 10        # metres — bench within this distance of a footway → is_sidewalk

# Helsinki bounding box (south, west, north, east)
BBOX  = "60.10,24.78,60.36,25.26"
QUERY = f"""[out:json][timeout:120][bbox:{BBOX}];
(
  way["highway"~"^(footway|path|pedestrian|steps)$"];
  way["highway"="cycleway"]["foot"!="no"];
);
out geom;"""

ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]

# WGS84 → EPSG:3067 (Finnish metric projection, metres)
_to_metric = Transformer.from_crs("EPSG:4326", "EPSG:3067", always_xy=True)


# ── Overpass fetcher ──────────────────────────────────────────────────────────

def fetch_footways():
    """Fetch OSM footway / path / pedestrian way geometries from Overpass."""
    for ep in ENDPOINTS:
        try:
            print(f"  Trying {ep} …")
            r = requests.post(ep, data={"data": QUERY}, timeout=130)
            if r.status_code == 200:
                ways = [el for el in r.json().get("elements", []) if el["type"] == "way"]
                print(f"  ✓ {len(ways)} ways from {ep}")
                return ways
            else:
                print(f"  HTTP {r.status_code}")
        except Exception as e:
            print(f"  {ep}: {e}")
    return []


def build_strtree(ways):
    """Convert Overpass way geometry to buffered polygons in EPSG:3067 and build STRtree."""
    buffers = []
    for w in ways:
        geom = w.get("geometry", [])
        if len(geom) < 2:
            continue
        try:
            coords = [_to_metric.transform(n["lon"], n["lat"]) for n in geom]
            buffers.append(LineString(coords).buffer(BUFFER_M))
        except Exception:
            pass
    print(f"  {len(buffers)} buffered lines built")
    return buffers, STRtree(buffers)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print(f"Loading {BENCH_FILE} …")
    with open(BENCH_FILE, encoding="utf-8") as f:
        data = json.load(f)
    features = data["features"]
    print(f"  {len(features)} benches")

    # Split by classification method
    ylre_street, ylre_park, osm_only = [], [], []
    for feat in features:
        p    = feat["properties"]
        src  = p.get("source", "")
        ysrc = p.get("ylre_source", "")
        if src == "Helsinki_YLRE_street" or ysrc == "Helsinki_YLRE_street":
            ylre_street.append(feat)
        elif src == "Helsinki_YLRE_park" or ysrc == "Helsinki_YLRE_park":
            ylre_park.append(feat)
        else:
            osm_only.append(feat)

    print(f"  Street (YLRE): {len(ylre_street)}  Park (YLRE): {len(ylre_park)}  OSM-only: {len(osm_only)}")

    # YLRE benches — classified by source directly
    for feat in ylre_street:
        feat["properties"]["is_sidewalk"] = True
    for feat in ylre_park:
        feat["properties"]["is_sidewalk"] = False

    # OSM-only benches — try Overpass, else fall back to heuristic
    if osm_only:
        print("\nFetching OSM footway data for OSM-only benches …")
        ways = fetch_footways()

        if ways:
            print("Building spatial index …")
            buffers, tree = build_strtree(ways)
            print(f"Classifying {len(osm_only)} OSM benches against footway buffer …")
            n_sw = 0
            for i, feat in enumerate(osm_only):
                lon, lat = feat["geometry"]["coordinates"]
                x, y = _to_metric.transform(lon, lat)
                pt   = Point(x, y)
                candidates = tree.query(pt)
                is_sw = any(buffers[j].contains(pt) for j in candidates)
                feat["properties"]["is_sidewalk"] = is_sw
                if is_sw:
                    n_sw += 1
                if (i + 1) % 500 == 0:
                    print(f"  … {i+1}/{len(osm_only)}", flush=True)
            print(f"  OSM sidewalk: {n_sw}/{len(osm_only)}")
        else:
            print("  Overpass unavailable → using heuristic: !is_nature & !is_waterfront")
            n_sw = 0
            for feat in osm_only:
                p = feat["properties"]
                is_sw = not p.get("is_nature", False) and not p.get("is_waterfront", False)
                feat["properties"]["is_sidewalk"] = is_sw
                if is_sw:
                    n_sw += 1
            print(f"  OSM sidewalk (heuristic): {n_sw}/{len(osm_only)}")

    # Summary
    total_sw = sum(1 for f in features if f["properties"].get("is_sidewalk"))
    pct = total_sw / len(features) * 100
    print(f"\nTotal is_sidewalk=True: {total_sw}/{len(features)} ({pct:.1f}%)")

    with open(BENCH_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Saved → {BENCH_FILE}")


if __name__ == "__main__":
    main()
