#!/usr/bin/env python3
"""
add_age_data.py

Fetches age-group statistics for Helsinki postal code areas from Statistics
Finland Paavo 2023 (PxWeb API) and aggregates them to kaupunginosa districts.

Adds pct_65plus (% aged 65+) and pct_youth (% aged 0–17) to district_stats.json.

Data sources:
  • Postal code boundaries: Helsinki WFS (avoindata:Postinumeroalue, EPSG:3879)
  • Age data: Statistics Finland PxWeb paavo_pxt_12ey.px (json-stat2)
  • District polygons: district_boundaries.json (WGS84)

Requirements: requests, shapely, pyproj
"""

import json
import requests
from pyproj import Transformer
from shapely.geometry import shape, Point

STATS_FILE      = "district_stats.json"
BOUNDARIES_FILE = "district_boundaries.json"

HKI_WFS    = "https://kartta.hel.fi/ws/geoserver/avoindata/wfs"
PAAVO_URL  = ("https://pxdata.stat.fi/PxWeb/api/v1/fi/"
              "Postinumeroalueittainen_avoin_tieto/uusin/paavo_pxt_12ey.px")

# EPSG:3879 → WGS84 (for Helsinki WFS postal code geometry)
_3879_to_wgs84 = Transformer.from_crs("EPSG:3879", "EPSG:4326", always_xy=True)

# Age group codes (Paavo 2023 field names)
YOUTH_FIELDS = ["he_0_2", "he_3_6", "he_7_12", "he_13_15", "he_16_17"]   # 0–17
ELDER_FIELDS = ["he_65_69", "he_70_74", "he_75_79", "he_80_84", "he_85_"] # 65+
POP_FIELD    = "he_vakiy"
ALL_FIELDS   = [POP_FIELD] + YOUTH_FIELDS + ELDER_FIELDS


# ── Helpers ───────────────────────────────────────────────────────────────────

def ring_wgs84(coords):
    return [list(_3879_to_wgs84.transform(x, y)) for x, y in coords]


def fetch_postal_polygons():
    """Fetch all Helsinki postal code polygons from Helsinki WFS (EPSG:3879 → WGS84)."""
    print("Fetching Helsinki postal code polygons …")
    params = {
        "service": "WFS", "version": "2.0.0", "request": "GetFeature",
        "typeNames": "avoindata:Postinumeroalue",
        "outputFormat": "application/json",
    }
    r = requests.get(HKI_WFS, params=params, timeout=60)
    r.raise_for_status()
    features = r.json().get("features", [])
    print(f"  → {len(features)} postal code areas")

    result = {}   # code → {centroid: Point, geom: shapely polygon}
    for feat in features:
        code = feat["properties"].get("tunnus")
        g    = feat.get("geometry")
        if not code or not g:
            continue
        try:
            if g["type"] == "Polygon":
                poly = shape({"type": "Polygon",
                              "coordinates": [ring_wgs84(r_) for r_ in g["coordinates"]]})
            elif g["type"] == "MultiPolygon":
                from shapely.ops import unary_union
                parts = [shape({"type": "Polygon",
                                "coordinates": [ring_wgs84(r_) for r_ in part]})
                         for part in g["coordinates"]]
                poly = unary_union(parts)
            else:
                continue
            if not poly.is_valid:
                poly = poly.buffer(0)
            result[code] = poly.centroid
        except Exception as e:
            print(f"  Warning {code}: {e}")

    print(f"  → {len(result)} valid polygons")
    return result


def load_district_polygons():
    """Load WGS84 district polygons from district_boundaries.json."""
    with open(BOUNDARIES_FILE, encoding="utf-8") as f:
        gj = json.load(f)
    polys = {}
    for feat in gj["features"]:
        d = feat["properties"]["district"]
        try:
            poly = shape(feat["geometry"])
            if not poly.is_valid:
                poly = poly.buffer(0)
            polys[d] = poly
        except Exception:
            pass
    print(f"  Loaded {len(polys)} district polygons")
    return polys


def fetch_age_data(postal_codes):
    """Fetch age-group data from Statistics Finland Paavo PxWeb API."""
    print(f"Fetching age data for {len(postal_codes)} Helsinki postal codes …")
    query = {
        "query": [
            {"code": "Postinumeroalue",
             "selection": {"filter": "item", "values": sorted(postal_codes)}},
            {"code": "Tiedot",
             "selection": {"filter": "item", "values": ALL_FIELDS}},
            {"code": "Vuosi",
             "selection": {"filter": "item", "values": ["2023"]}},
        ],
        "response": {"format": "json-stat2"},
    }
    r = requests.post(PAAVO_URL, json=query, timeout=60)
    r.raise_for_status()
    return r.json()


def parse_age_data(stat2, postal_codes):
    """Parse json-stat2 response → {postal_code: {field: value}}."""
    dims   = stat2["dimension"]
    values = stat2["value"]

    # Find dimension order and sizes
    dim_keys = list(dims.keys())               # e.g. [Postinumeroalue, Tiedot, Vuosi]
    dim_sizes = [len(dims[k]["category"]["label"]) for k in dim_keys]

    pno_dim   = next(k for k in dim_keys if "Posti" in k or "posti" in k)
    year_dim  = next((k for k in dim_keys if "Vuosi" in k or "Year" in k or "year" in k), None)
    # "variable" dimension = the one that's neither postal code nor year
    exclude   = {pno_dim, year_dim} if year_dim else {pno_dim}
    tieto_dim = next(k for k in dim_keys if k not in exclude)

    pno_cat   = dims[pno_dim]["category"]
    tieto_cat = dims[tieto_dim]["category"]

    pno_index   = pno_cat["index"]     # code → flat-index position
    tieto_index = tieto_cat["index"]   # field → flat-index position
    tieto_label = tieto_cat["label"]   # field → label

    n_pno   = dim_sizes[dim_keys.index(pno_dim)]
    n_tieto = dim_sizes[dim_keys.index(tieto_dim)]
    # Assuming Vuosi (year) dim has size 1 after query filter

    result = {}
    for code in postal_codes:
        if code not in pno_index:
            continue
        p_i = pno_index[code]
        row = {}
        for field in ALL_FIELDS:
            if field not in tieto_index:
                continue
            t_i = tieto_index[field]
            # flat index: pno * n_tieto + tieto  (year dim = 1, so ignored)
            flat = p_i * n_tieto + t_i
            row[field] = values[flat]
        result[code] = row

    print(f"  → Parsed {len(result)} postal code records")
    return result


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    # 1. Postal code centroids (WGS84)
    postal_centroids = fetch_postal_polygons()
    postal_codes = list(postal_centroids.keys())

    # 2. District polygons
    print("Loading district polygons …")
    district_polys = load_district_polygons()

    # 3. Map each postal code centroid → district
    print("Building postal code → district mapping …")
    pno_to_district = {}
    for code, centroid in postal_centroids.items():
        for district, poly in district_polys.items():
            if poly.contains(centroid):
                pno_to_district[code] = district
                break
    print(f"  Mapped {len(pno_to_district)}/{len(postal_codes)} postal codes to districts")

    # 4. Fetch age data from Paavo PxWeb
    stat2    = fetch_age_data(postal_codes)
    age_data = parse_age_data(stat2, postal_codes)

    # 5. Aggregate to district level
    district_agg = {}   # district → {pop, youth, elder}
    for code, row in age_data.items():
        d = pno_to_district.get(code)
        if not d:
            continue
        if d not in district_agg:
            district_agg[d] = {"pop": 0, "youth": 0, "elder": 0}
        pop   = row.get(POP_FIELD)   or 0
        youth = sum(row.get(f) or 0 for f in YOUTH_FIELDS)
        elder = sum(row.get(f) or 0 for f in ELDER_FIELDS)
        district_agg[d]["pop"]   += pop
        district_agg[d]["youth"] += youth
        district_agg[d]["elder"] += elder

    # 6. Update district_stats.json
    with open(STATS_FILE, encoding="utf-8") as f:
        stats_list = json.load(f)

    updated = 0
    for s in stats_list:
        d = s["district"]
        if d not in district_agg:
            continue
        agg = district_agg[d]
        if agg["pop"] > 0:
            s["pct_65plus"] = round(agg["elder"] / agg["pop"] * 100, 1)
            s["pct_youth"]  = round(agg["youth"] / agg["pop"] * 100, 1)
            updated += 1

    print(f"\nUpdated {updated} districts with age data")
    with open(STATS_FILE, "w", encoding="utf-8") as f:
        json.dump(stats_list, f, ensure_ascii=False, indent=2)
    print(f"Saved → {STATS_FILE}")

    # Quick sanity check
    sample = [(s["district"], s.get("pct_65plus"), s.get("pct_youth"))
              for s in stats_list if s.get("pct_65plus") is not None][:5]
    print("\nSample (district, % 65+, % 0–17):")
    for row in sample:
        print(f"  {row[0]}: {row[1]}%  /  {row[2]}%")


if __name__ == "__main__":
    main()
