# Data quality ticket checks

**Author**: Sian Wilson

This folder groups a batch of small, ticket-driven data-quality investigations together — each is a standalone notebook answering one specific GitHub ticket's question, sharing the same `helpers.py`. See each section below for the relevant ticket and headline result.

## P-05 — Entities should have an `organisation_entity`

**Date created**: 24th September 2026

**Notebook**: [P-05_missing_organisation_entity.ipynb](P-05_missing_organisation_entity.ipynb)

**Test definition**: entities in the `entity` table for a dataset which don't have a value for the `organisation-entity` field.

Related prior work, `[analysis/2026-07_correct_organisations_of_entities](../2026-07_correct_organisations_of_entities/)`, looks at a _different_ population — entities that **do** have an `organisation_entity`, but possibly the wrong one — and explicitly excludes/drops any entity with a blank `organisation_entity` before its trace starts, without ever counting how many that was. This notebook measures that excluded population directly.

**Method**: one aggregate `COUNT(*)` query per dataset via the Datasette SQL API (not a row-level pull — several datasets have 100k+ entities and only a count is needed), split into a plain total and a `WHERE organisation_entity IS NULL OR organisation_entity = ''` count, run in parallel across every dataset with a published Datasette database.

**Result**: **108 entities across 4 datasets** are missing `organisation_entity`, out of ~122 datasets successfully checked:

| Dataset                   | Total entities | Missing `organisation_entity` | %    |
| ------------------------- | -------------- | ----------------------------- | ---- |
| `central-activities-zone` | 10             | 10                            | 100% |
| `local-plan-timetable`    | 2,700          | 82                            | 3.0% |
| `local-plan-document`     | 509            | 8                             | 1.6% |
| `brownfield-land`         | 37,736         | 8                             | 0.0% |

`central-activities-zone` is a full outage — every entity in the dataset is missing `organisation_entity`. The other three are minor. Of the 108, 107 are `quality='some'` and 1 is `quality='authoritative'`.

**Two datasets couldn't be checked** (distinct from the ~157 registered-but-unpublished datasets that are correctly excluded because they have no data collected yet):

- `title-boundary` — has a published database but no `entity` table at all.
- `planning-application` — its `entity` table (100k+ rows) has no index on `organisation_entity`, and a full-table-scan `WHERE` query consistently hits Datasette's SQL time limit. Would need a different query strategy (e.g. paginating over an indexed column) to check.

**`local-plan-document` and `local-plan-timetable` are stale, superseded datasets.** Checking a sample entity (3800418) showed it now lives under `development-plan-document` with `organisation-entity` correctly populated — the `local-plan-document` copy is a leftover from before that migration. Dropped from the diagnosis below; their gaps aren't real, current data-quality problems.

### Diagnosis: is this missing data, or a broken config mapping?

A blank `organisation_entity` doesn't tell you _why_. Checked `digital-land/config`'s `pipeline/{dataset}/entity-organisation.csv` and `lookup.csv` for an existing assignment before assuming an entity is genuinely unassigned.

**This is a full sweep, not just the two datasets with a known live gap** — every pipeline in `digital-land/config` that has an `entity-organisation.csv` (55) or `lookup.csv` (55) was checked directly for a broken organisation curie, independent of whether it currently shows up as blank `organisation_entity` live (a broken/duplicate range assignment could in principle resolve to a valid-but-wrong row instead of blanking out, so checking only the already-known-blank entities wouldn't necessarily catch everything). Result: **12 broken curies found, all in `entity-organisation.csv`, and all in the same two datasets already known — `central-activities-zone` and `brownfield-land`. Nothing else on the platform has this problem.** `lookup.csv` had zero invalid organisation values across all ~2.7M rows checked, platform-wide.

For the 18 live-blank entities in those two datasets, **17 are a broken config mapping, not missing data** — a real assignment exists but the org curie doesn't resolve on the platform:

- **`central-activities-zone` (10/10 entities)**: `entity-organisation.csv` assigns e.g. `local-organisation:LBH`, but the platform only recognises the `local-authority:` prefix (`local-authority:LBH` = London Borough of Lambeth exists; `local-organisation:LBH` doesn't). One-line prefix fix, same correction for all 10 (different borough per entity — Camden, Hackney, Islington, Kensington & Chelsea, Lambeth, Southwark, Tower Hamlets, Wandsworth, Westminster, and the City of London).
- **`brownfield-land` (7/8 entities)**: `entity-organisation.csv` has _two conflicting rows_ for the same entity range — e.g. entities 1741626–1741630 are assigned both the valid `local-authority:SAW` (Sandwell MBC) and the invalid `local-authority:SAN`; entities 1741642–1741643 both `local-authority:WKF` (valid, Wakefield) and `local-authority:WFK` (invalid — a transposed typo). Needs the invalid duplicate row removed from `entity-organisation.csv`.
- **1 entity (1742531, `brownfield-land`)** is `likely_duplicate_entity`, not a config bug at all: `entity-organisation.csv` already assigns a single, valid `local-authority:BAN` (Basingstoke and Deane), but the entity itself is a duplicate. A brand-new resource (12 days old at time of writing) from Basingstoke's own endpoint failed to match back to the long-standing canonical entity for the same site (1700711 — same name/reference `BDB/63103`, coordinates ~15m apart, 8 resources back to 2018) and got a new entity ID minted instead, with a blank-organisation `lookup.csv` row.

**How widespread is that mechanism?** (Section 10 — a `lookup.csv` row with blank `organisation`, whose `(prefix, reference)` is shared with a _different_, org-assigned entity.) Checked all 55 `lookup.csv` files: **1,987 historical occurrences, confined to just 2 pipelines** (`listed-building`: 1,929, `brownfield-land`: 58). Cross-checked every one of those against live data — **only 1 (our own entity 1742531) is both still a live entity and still blank today**; everything else has self-healed (retired/redirected on a later pipeline run, or — for `listed-building`'s 1,891 still-live survivors — already has `organisation_entity` populated via a different route entirely, independent of the blank `lookup.csv` cell). So this is a real, recurring pipeline mechanism worth understanding on its own, but normally transient — not currently a live contributor to the missing-`organisation_entity` problem beyond this single case.

No entity in scope was genuinely unassigned in config, so the URL-based fallback (tracing the endpoint's registered organisation, or matching the endpoint URL's domain against `organisation.website`) wasn't needed this run — it's built into the notebook for any future re-run where one is.

### Validating the suggested fixes against actual endpoint provenance

The suggestions above come from pattern-matching the config CSV text (prefix swaps, string similarity) — plausible, but still inference. Independently traced `fact → fact_resource → log → endpoint → source` (same lineage as `2026-07_correct_organisations_of_entities`) for all 18 entities to check whether the organisation actually registered against the entity's submitting endpoint agrees:

- **`brownfield-land` (8/8) and 1 `central-activities-zone` entity: direct agreement.** The traced submitting organisation matches the suggested fix exactly — strong, independent confirmation these corrections are right.
- **9 `central-activities-zone` entities trace to `local-authority:GLA` (Greater London Authority) instead of the individual borough.** Checked further: GLA is itself a registered active source for `central-activities-zone` (alongside Lambeth and Southwark individually), and it's the only source with data covering these entities. Central Activities Zone is a pan-London designation, so GLA submitting London-wide on behalf of multiple boroughs is exactly the regional-aggregation pattern `entity-organisation.csv`'s per-entity override exists to handle (confirmed by reading `digital_land/phase/priority.py`: the per-entity config assignment is designed to take precedence over the submitting endpoint's own organisation). This isn't a contradiction of the suggested fix — it's the expected signature of correct aggregator handling, and is called out as such in `trace_agreement` rather than being logged as a plain disagreement. **No entity showed a genuine, unexplained disagreement.**

**Next step**: this reframes the ticket's second aim from "is this worth fixing" to "here are the exact fixes needed" — 17 of 18 are precise, low-risk, and now independently corroborated corrections to `entity-organisation.csv` (2 PRs: one prefix fix for `central-activities-zone`, one duplicate-row removal for `brownfield-land`); the 1 remaining case needs its `lookup.csv` row reconciled against canonical entity 1700711 rather than a config fix. Take `data/missing_organisation_entity.csv` to Swati as the concrete list, plus the broader duplicate-entity mechanism (1,929 historical occurrences in `listed-building` alone, Section 10) as a separate, worth-understanding observation.

**Output** (gitignored, not committed — see repo root README on data handling): `data/missing_organisation_entity.csv` — one row per affected entity in the two live, current datasets (18 rows: `central-activities-zone`, `brownfield-land`). `local-plan-document`/`local-plan-timetable` excluded entirely — confirmed stale, not real current issues (see above). Columns: `dataset`, `entity`, `name`, `reference`, `quality`, `entity_url`, `classification` (`broken_mapping` / `likely_duplicate_entity`), `config_assigned_curies`, `suggested_fix`, `suggested_curie`, `confidence`, `endpoint_trace_organisation`, `endpoint_url_traced`, `trace_agreement`. The platform-wide prevalence table above and the config-repo sweep (55 `entity-organisation.csv` + 55 `lookup.csv` files, 12 issues found, confirming no other dataset has this problem) are both visible as printed output in the notebook itself, rather than needing their own export.
