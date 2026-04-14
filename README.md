# Helsinki Bench Map

Interactive web map exploring **12,752 public benches** in Helsinki, combining the city's official YLRE registry with OpenStreetMap community data. Includes district-level statistical analysis, demographic correlations, and a spatial heatmap.

**Live site:** https://taiyo-ishikawa.github.io/helsinki-bench-map/

---

## Features

### Map tab

- Marker cluster map of all 12,752 benches (City-registered in blue, Community-mapped in orange)
- **Filters** (combinable):
  - District (Kaupunginosa) — 60 neighbourhoods
  - Location character — 🌿 Nature/Park · 🌊 Waterfront · 🚶 Sidewalk (multi-select)
  - Bench type — Fixed / Removable / Single-seat / Table-bench
  - Bench model group — 5 YLRE-derived categories (multi-select)
  - Backrest — with / without (OSM data)
- Click any marker for full attribute details (YLRE + OSM attributes shown side-by-side for merged records)
- Mobile-friendly: filter panel auto-closes on selection

### Insight tab

- **Bench density ranking** — stacked bar chart by district (city-registered vs community-mapped), top / bottom 30 toggle
- **District map** — interactive choropleth; click to see per-district stats
- **Scatter plots** with Pearson r and two-tailed p-value:
  - Population density vs bench density
  - Median income vs bench density
  - % Population aged 65+ vs bench density
  - % Population aged 0–17 vs bench density
- **Bench density heatmap** — filter-responsive leaflet.heat layer
- All charts update in real time when filters change (Source · Location · Type · Model Group)

---

## Data sources

| Source | Content | License |
|--------|---------|---------|
| [Helsinki YLRE via HRI WFS](https://hri.fi/data/en_GB/dataset/helsingin-kaupungin-yleisten-alueiden-rekisteri) | City-registered benches with model, material, type, maintenance class | CC BY 4.0 |
| [OpenStreetMap / Overpass API](https://overpass-api.de/) | Community-mapped benches (`amenity=bench`) | ODbL 1.0 |
| [Helsinki WFS — YLRE_Viheralue_alue](https://kartta.hel.fi/) | Green area polygons for nature/park classification (4,682 polygons, EPSG:3879) | CC BY 4.0 |
| [Helsinki WFS — Kaupunginosajako](https://kartta.hel.fi/) | District boundary polygons | CC BY 4.0 |
| [Helsinki WFS — Postinumeroalue](https://kartta.hel.fi/) | Postal code boundaries for spatial join | CC BY 4.0 |
| [Statistics Finland Paavo 2023](https://www.stat.fi/org/avoindata/paikkatietoaineistot/paavo.html) | Population, median income, age groups by postal code | CC BY 4.0 |

---

## Data pipeline

```
fetch_helsinki_benches.py        YLRE WFS (Katuosat + Viherosat) + Overpass OSM
        ↓  15,628 raw benches
translate_to_english.py          Finnish field values → English
deduplicate_benches.py           Merge YLRE+OSM duplicates within 5 m (scipy cKDTree)
add_district.py                  Assign kaupunginosa via point-in-polygon (shapely)
add_spatial_attributes.py        is_waterfront — within 200 m of OSM coastline
fix_osm_nature.py                is_nature — YLRE Viheralue polygon test (STRtree)
classify_sidewalks.py            is_sidewalk — Overpass footway proximity (10 m buffer) for YLRE street + OSM-only benches; YLRE park always False
        ↓  helsinki_benches_deduped_en.json  (12,752 benches)
generate_district_boundaries.py  district_boundaries.json + district_stats.json (bench stats per district)
add_age_data.py                  pct_65plus, pct_youth — Paavo PxWeb API → postal code centroids → district join
```

---

## Dataset summary

| Category | Count |
|----------|------:|
| YLRE + OSM (merged at < 5 m) | 3,040 |
| YLRE only — street (`Katuosat`) | 1,072 |
| YLRE only — park (`Viherosat`) | 4,904 |
| OSM only | 3,736 |
| **Total** | **12,752** |

**Location flags:**

| Flag | Count | Share |
|------|------:|------:|
| `is_nature` — within YLRE green area polygon | 8,779 | 68.8 % |
| `is_sidewalk` — within 10 m of OSM footway (YLRE street + OSM-only benches; plazas excluded) | 4,458 | 35.0 % |
| `is_waterfront` — within 50 m of coastline | 1,746 | 13.7 % |

**District statistics** (60 districts total):
- 45 districts with population & age data (Statistics Finland Paavo 2023)
- 44 districts with median income data

---

## Bench model groups

Five groups derived from the `ylre_material` field (YLRE city-registered benches only):

| Group | Benches | Description |
|-------|--------:|-------------|
| Wood & Steel | 666 | Wooden seat + steel/stainless frame. Models: HKI-D1, D3, D4, D12, D13 |
| Metal | 39 | All-steel construction. Models: HKI-D9, D10, D11 |
| Stone & Slab | 122 | Granite/stone base or concrete slab. Models: HKI-D2, D16, slab, tree-surround |
| Park & Heritage | 263 | Place-specific park and historic-landscape models. Models: HKI-D5–D8, D14 |
| Named Brands | 118 | Commercially procured street furniture (Benkert, Victor Stanley, Sineu, Monena, etc.) |

Benches without model data (OSM-only records and unrecognised YLRE values) are excluded when any model group filter is active.

---

## Tech stack

| Component | Technology |
|-----------|-----------|
| Interactive map | [Leaflet.js 1.9.4](https://leafletjs.com/) + [MarkerCluster](https://github.com/Leaflet/Leaflet.markercluster) |
| Heatmap layer | [leaflet.heat 0.2.0](https://github.com/Leaflet/Leaflet.heat) |
| Charts | [Chart.js 4.4.3](https://www.chartjs.org/) (scatter, stacked bar) |
| Statistical tests | Pearson r + two-tailed p-value in pure JS (log-gamma → regularised incomplete beta → t-distribution) |
| Basemap (map) | CARTO Light |
| Basemap (heatmap) | OpenStreetMap standard tiles |
| Data pipeline | Python — `requests`, `shapely`, `pyproj`, `scipy` |
| Hosting | GitHub Pages (single `index.html` + JSON data files) |

---

## Repository structure

```
index.html                        Main app (map + insight tab, all JS/CSS inline)
helsinki_benches_deduped_en.json  Processed bench dataset (12,752 features)
district_stats.json               Per-district statistics (population, income, age, bench counts)
district_boundaries.json          Simplified district polygons in WGS84

fetch_helsinki_benches.py         Step 1: Data acquisition
translate_to_english.py           Step 2: Localisation
deduplicate_benches.py            Step 3: Deduplication
add_district.py                   Step 4: District assignment
add_spatial_attributes.py         Step 5: Spatial flags (waterfront)
fix_osm_nature.py                 Step 6: Nature/park classification
classify_sidewalks.py             Step 7: Sidewalk classification
generate_district_boundaries.py   Step 8: District stats + boundaries
add_age_data.py                   Step 9: Age demographic data
```

---

## License

Code: [MIT](LICENSE)  
Data: subject to individual source licenses listed above (CC BY 4.0 / ODbL 1.0)
