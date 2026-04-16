# Simplified Geometry Investigation

**Author:** Sian Teesdale
**Date:** April 2026

## Overview

This analysis investigates why geometries submitted by Local Planning Authorities (LPAs) appear differently on the Planning platform compared to the raw source data. The trigger was Barnet Council flagging a discrepancy between their submitted conservation area boundaries and what was appearing on the platform.

The investigation uses the Totteridge conservation area (entity `44002422`) as a case study, comparing Barnet's raw GeoPackage against the geometry stored in Datasette and served on the Planning platform.

## Files

- **[1_initial_analysis.ipynb](1_initial_analysis.ipynb)** — the main notebook, stepping through the investigation
- **[wkt.py](wkt.py)** — a copy of the WKT transformation script from [digital-land-python](https://github.com/digital-land/digital-land-python/blob/8974e00c083a5247e8dbd7b663af27e0f26c8a7e/digital_land/datatype/wkt.py) at the commit used in the pipeline. This is included here for reference so the transformation logic can be read alongside the analysis without needing to navigate the source repo.

## What the notebook does

1. **Loads the data** — reads Barnet's raw conservation area GeoPackage (via a User-Agent workaround for their server) and the Planning platform's conservation area GeoJSON.

2. **Confirms the mismatch** — shows that Barnet's raw Totteridge geometry does not match the platform geometry using both `.equals()` and `.equals_exact()`.

3. **Walks through the transformation pipeline step by step** — replicates the stages in `wkt.py` manually to identify which step changes the geometry:
   - CRS conversion (skipped — Barnet data is already WGS84)
   - 6 d.p. precision round-trip via `dump_wkt`
   - Simplification (`simplify(0.000005)`)
   - Coordinate grid snapping (`set_precision(0.000001)`)
   - `make_valid`, `make_multipolygon`, `buffer(0)`, and winding order (`orient`)

4. **Confirms the pipeline matches Datasette** — runs the raw Barnet WKT through the actual installed `WktDataType.normalise()` function and compares to the platform geometry. The result is a 0.7 mm² symmetric difference, confirming the pipeline is behaving correctly and consistently.

5. **Quantifies the damage from each step** — uses a `report()` function to measure vertex count and area difference (vs the raw geometry and vs the previous step) at each stage:

| Step                | Vertices | vs Raw  |
| ------------------- | -------- | ------- |
| Raw                 | 2971     | —       |
| 6 d.p. round-trip   | 2971     | 224 m²  |
| simplify(0.000005)  | 520      | 1010 m² |
| set_precision(1e-6) | 520      | 858 m²  |

6. **Tests whether changing precision helps** — finds that increasing to 7 or 8 d.p. makes things worse, not better, because the finer coordinates give the simplifier more vertices to remove.

7. **Tests removing simplification entirely** — with no simplification, the difference from raw drops to just 0.12 m² (negligible rounding). This is replicated across multiple Barnet conservation areas (Monken Hadley, Wood Street) with consistent findings.

## Key findings

- **Simplification is the primary cause** of geometry change. It removes ~80% of vertices and introduces ~1000 m² of cumulative boundary deviation.
- The simplification step was designed to fix invalid geometries from other sources (OSGB, Mercator). For already-valid WGS84 data like Barnet's, it runs unnecessarily and causes significant information loss.
- **Removing simplification** would reduce the difference from raw to sub-1 m², which is just floating-point rounding and has no practical impact.
- Increasing decimal places does not help and slightly worsens the outcome.
