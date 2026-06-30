# Count Organisations & Providers on Platform

This report counts how many organisations and providers are contributing data to each dataset on [planning.data.gov.uk](https://www.planning.data.gov.uk), and classifies each as either **authoritative** or **some** quality.

**Author**: Sian Teesdale
**Date created**: 30th June 2026

## What the script does

`run_analysis.py` runs against live data and produces four CSVs in `outputs/`:

| File                              | Description                                                                 |
| --------------------------------- | --------------------------------------------------------------------------- |
| `providers_quality.csv`           | One row per (dataset, organisation) — provider quality and entity count     |
| `providers_count_summary.csv`     | One row per dataset — authoritative / some / total provider counts          |
| `organisations_quality.csv`       | One row per (dataset, organisation) — organisation quality and entity count |
| `organisations_count_summary.csv` | One row per dataset — authoritative / some / total organisation counts      |

Quality classifications:

- **authoritative** — the organisation submits its own data and owns the resulting entities
- **some** — the organisation contributes data but does not own the entities (e.g. a national body seeding data on behalf of local authorities)

## Providers vs Organisations

These are two different lenses on the same question.

**Providers** are organisations with an active endpoint in the data pipeline — they are actively submitting data. An organisation is counted as a provider if it appears in `source`/`source_pipeline` with a non-empty endpoint.

**Organisations** are organisations that own entities on the platform, determined by the `organisation_entity` field on each entity in the dataset's entity table. An organisation can own entities without being an active provider (e.g. if their data was loaded historically or seeded by a third party).

In practice, most organisations appear in both counts. Differences arise when:

- An organisation has stopped submitting data but still has entities on the platform
- A national body (e.g. MHCLG) seeds data for local authorities — it appears as a provider but the entities are owned by the local authorities themselves

## Data sources

| Source                                                                                     | What it provides                                                                                                                                        |
| ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [Datasette](https://datasette.planning.data.gov.uk) — `digital-land` database              | `source` and `source_pipeline` tables (active providers), `organisation` table (org names, entity IDs, active/inactive status)                          |
| [Datasette](https://datasette.planning.data.gov.uk) — per-dataset databases                | `entity` table with `organisation_entity` and `quality` fields                                                                                          |
| [digital-land/config on GitHub](https://github.com/digital-land/config/tree/main/pipeline) | `entity-organisation.csv` (entity ID ranges per org, used to detect designated providers) and `lookup.csv` (used to detect alternative/seeding sources) |

Inactive organisations (those with an `end_date` in the organisation table) are excluded from alternative-source detection — only active organisations can be classified as `some` quality providers.

## Example: Conservation Area

Conservation area is a good illustration of the two quality types.

**Providers (132 total)**

- **130 authoritative** — the 130 local planning authorities and national park authorities that submit their own conservation area data. Their entities are owned by them.
- **2 some** — national bodies that seed conservation area data on behalf of local authorities:
  - **MHCLG** (`government-organisation:D1342`) — seeds 431 entities across many LPAs
  - **Historic England** (`government-organisation:PB1164`) — seeds 3 entities

**Organisations (306 total)**

- **130 authoritative** — the same LPAs and NPAs that own their entities directly
- **176 some** — organisations that have entities attributed to them on the platform but where the quality is `some` (typically LPAs whose data pipeline has stalled or whose data has been superseded by a national seeder)

The difference between the provider count (132) and the organisation count (306) reflects organisations that have entities on the platform but are no longer actively submitting data through the pipeline.

## Running the script

```bash
cd reports/count_organisations_providers_platform
python run_analysis.py
```

Outputs are written to `outputs/`. The script fetches all data live and takes approximately 8–10 minutes to complete.
