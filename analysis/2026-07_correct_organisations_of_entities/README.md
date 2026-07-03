# Quality "some" entities misattributed to the wrong non-government organisation

**Author**: Sian Teesdale

**Date created**: 2nd July 2026

**Dataset Scope**: all datasets with a `quality=some` entity owned by a `local-authority:`, `national-park-authority:`, or `development-corporation:`

**Purpose**: Follows on from [digital-land/config#2651](https://github.com/digital-land/config/issues/2651), which found `brownfield-land` entity ranges misattributed to the wrong council (Wokingham instead of Woking; Cheshire West and Chester instead of Cheshire East). This finds every other entity across the platform showing the same bug pattern: `quality=some`, attributed to one local body, but whose data actually traces back to a *different* local body's own submission.

## The analysis: `4_full_population_provenance_trace.ipynb`

**Why `organisation_entity` can't be trusted for this check.** The obvious first approach — compare an entity's `organisation_entity` against `entity-organisation.csv`'s range assignment — doesn't work. The pipeline (`digital_land/phase/priority.py`) always force-overwrites `organisation_entity` to match the range file, regardless of who actually submitted the data. So that field can never disagree with the range file, even when the range file itself is wrong. See `archive/README.md` for how this was proven and ruled out.

**What actually works**: trace each entity's *real* submitting organisation via its resource lineage — `fact` → `fact_resource` → `log` → `endpoint` → `source` — which is independent of `organisation_entity` entirely, then diff that against what the entity is currently attributed to. Validated against all 4 known-bad entities from the ticket before running platform-wide: every one traces unambiguously to the *correct* organisation.

**Three legitimate look-alike patterns had to be filtered out**, because each looks identical to a real misattribution bug at the single-entity level:

- **National seeding** — a central `government-organisation:` (MHCLG, Historic England) legitimately contributes data on behalf of many LPAs.
- **Regional aggregation** — a non-government body legitimately aggregates on behalf of many *other* local authorities (e.g. the Greater London Authority submits `brownfield-site` data for dozens of London boroughs). Detected structurally: a true-org serving more than 10 distinct assigned orgs in a dataset is treated as an aggregator, not a bug.
- **Local Government Reorganisation succession** — an active council correctly inheriting historical data from a since-dissolved predecessor (e.g. South Northamptonshire → West Northamptonshire, Eden → Westmorland and Furness). Detected by checking whether the true org has an `end_date`.

**Result**: 446 confirmed misattributions, headed by the two ticket examples (Wokingham→Woking: 77 entities, Cheshire W&C→Cheshire East: 7) plus new discoveries the ticket didn't already list — most notably Northumberland National Park Authority → Northumberland County Council (231 entities in `listed-building-outline`), which has no active endpoint registered at all for that dataset and so would be invisible to any check that starts by filtering to "active providers."

Output: `data/flagged_entities_full_population_with_provenance.csv`, sorted with `confirmed_misattribution` first.

## `archive/`

Three earlier notebooks kept as a record of the investigative path, not as tools to re-run — one is a proven dead end, the other two are fully superseded because notebook 4 produces a strict superset of what they found. See `archive/README.md` for what each was trying to do and why it's there.

## Data sources

- [Datasette](https://datasette.planning.data.gov.uk) `digital-land` database — `organisation` table (org names, entity IDs, `end_date`), `source`/`source_pipeline` tables (who is registered to provide data), `endpoint` and `log` tables (endpoint URLs and fetch history)
- Datasette per-dataset databases — `entity` table (`organisation_entity`, `quality`), `fact` and `fact_resource` tables (resource lineage)
- [digital-land/config](https://github.com/digital-land/config/tree/main/pipeline) on GitHub — `entity-organisation.csv` (entity ID ranges per organisation), referenced for background but not used directly by notebook 4

Outputs are written to the repo's top-level `data/` directory (gitignored), not committed here — see repo README on data handling.
