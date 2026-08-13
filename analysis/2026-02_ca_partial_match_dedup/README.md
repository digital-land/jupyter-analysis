# Conservation area partial-match duplicate resolution

**Author**: Sian Teesdale

**Date created**: February 2026

**Dataset Scope**: `conservation-area`

**Purpose**: Follows on from the `duplicate_geometry_check` expectation, which flags pairs of conservation area entities whose geometries overlap significantly (`complete_match` — both directions >95% overlap, or `single_match` — one direction only). This works out which partial-match pairs are genuine duplicates that should be redirected (via `old-entity.csv` in the `conservation-area` pipeline) versus legitimate separate entities that just happen to overlap — e.g. a smaller conservation area nested inside a larger one, or two LPAs whose boundaries genuinely overlap. See [digital-land/config#2021](https://github.com/digital-land/config/issues/2021).

## Outcome: what we're doing about it

Agreed with Swati that a pair is likely a "smaller CA within a larger CA" (i.e. not a duplicate, keep separate) when: same LPA, similar CA names, and one geometry is >95% contained within the other. Where entities belong to *different* organisations, no single rule covered every case — investigated instances individually (Hillingdon/Buckinghamshire, Historic-England-vs-LPA submission date conflicts).

This fed directly into an automated deduplication script in the `config` repo, refined over several PRs:
- [digital-land/config#2239](https://github.com/digital-land/config/pull/2239) — initial `deduplicate-ca-geogs.py`, redirecting complete matches (and name-similar single matches from a government-seeded source) via `old-entity.csv`
- [digital-land/config#2371](https://github.com/digital-land/config/pull/2371) — script improvements
- [digital-land/config#2384](https://github.com/digital-land/config/pull/2384) — added `retire-mhclg-ca-data.py`

## Files

- **[analysis_of_ca_partial_match_geoms.ipynb](analysis_of_ca_partial_match_geoms.ipynb)** — the exploratory analysis: same-LPA case study (Dorset), different-LPA case study (Hillingdon/Buckinghamshire), and the Historic England vs. LPA submission-date question. Reads live from `files.planning.data.gov.uk`, so results reflect current duplicate/entity state rather than a fixed snapshot.
- **[deduplicate-ca-geogs.ipynb](deduplicate-ca-geogs.ipynb)** — notebook version of the redirect logic that was later ported into `config`'s `bin/deduplicate-ca-geogs.py`: builds `old-entity.csv` redirect rows for complete matches, and for single matches from a government-seeded source (Historic England) where the entity names are sufficiently similar.
- **[archive/analysis_from_2026_02.ipynb](archive/analysis_from_2026_02.ipynb)** — the original February 2026 version of the exploratory analysis, reading from manually exported `Book2.xlsx`/`local-plan-boundary.geojson` snapshots. Superseded by `analysis_of_ca_partial_match_geoms.ipynb`; kept for the record only, do not re-run or use for current conclusions.

## Data sources

- `https://files.planning.data.gov.uk/reporting/duplicate_entity_expectation_geographies.csv` — `duplicate_geometry_check` expectation results enriched with entity metadata and WKT geometry, used by the main analysis notebook.
- `https://files.planning.data.gov.uk/reporting/duplicate_entity_expectation.csv` — same underlying check without geometry, used by `deduplicate-ca-geogs.ipynb`.
- `https://files.planning.data.gov.uk/dataset/local-plan-boundary.geojson` — LPA boundaries, used for mapping context.
- `config` repo's `pipeline/conservation-area/old-entity.csv` — existing redirects, appended to with new ones identified here.
