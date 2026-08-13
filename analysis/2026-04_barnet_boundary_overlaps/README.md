# Barnet boundary overlaps

**Author**: Sian Teesdale

**Date created**: April 2026

**Dataset Scope**: `conservation-area`, `article-4-direction-area`, `listed-building-outline`

**Purpose**: Ad-hoc investigation into geometries crossing LPA boundaries around Barnet — recovered from a local git stash and brought into this repo for the record (see notebook header for full context).

## Files

- **[barnet_boundary_overlaps.ipynb](barnet_boundary_overlaps.ipynb)** — the investigation, in three parts:
  1. **Trent Park vs. Monken Hadley** — are these two conservation areas (Enfield's Trent Park, Barnet's Monken Hadley) genuinely separate, or the same area split across two LPAs' submissions?
  2. **Listed building outline `42115855`** — confirms this Camden-submitted entity does sit within Barnet's boundary.
  3. **Neighbouring LPA overlaps generally** — across Barnet's six neighbours (Brent, Camden, Enfield, Haringey, Harrow, Hertsmere) and three datasets (conservation-area, article-4-direction-area, listed-building-outline), finds 17 entities whose geometry overlaps into Barnet's boundary.

Reads live from `files.planning.data.gov.uk` throughout — no local data files needed to re-run it.

## Data sources

- `https://files.planning.data.gov.uk/dataset/local-planning-authority.geojson`
- `https://files.planning.data.gov.uk/dataset/conservation-area.geojson`
- `https://files.planning.data.gov.uk/dataset/article-4-direction-area.geojson`
- `https://files.planning.data.gov.uk/dataset/listed-building-outline.geojson`
