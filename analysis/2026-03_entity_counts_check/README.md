# Entity Counts Check

**Author:** Sian Teesdale
**Date:** March 2026

## Overview

This analysis investigates whether entity counts are consistent across the data pipeline, focusing on four datasets: `article-4-direction-area`, `listed-building-outline`, `tree`, and `tree-preservation-zone`.

## Files

- **[1_initial_check_with_all_data.ipynb](1_initial_check_with_all_data.ipynb)** — initial (incorrect) approach; do not use for conclusions
- **[2_comparing_reporting_historic_endpoints_with_dataset_resource.ipynb](2_comparing_reporting_historic_endpoints_with_dataset_resource.ipynb)** — corrected analysis with valid findings

---

## Notebook 1 — Initial check (incorrect approach)

This notebook compared entity counts across three sources: the Planning platform (published data), `reporting_historic_endpoints`, and transformed resources. The approach was flawed because the platform data has been pruned and filtered before publication, so comparing it against backend pipeline data will naturally produce mismatches that are not indicative of a real problem.

**Results (not meaningful):**
- Platform vs `dataset_resource`: 230 mismatches, 53 matches
- Platform vs transformed resources: 104 mismatches, 180 matches

These figures should be ignored — they reflect the expected difference between published and unpublished data, not a data integrity issue.

---

## Notebook 2 — Correct analysis

This notebook corrects the approach by comparing only within the backend pipeline: `reporting_historic_endpoints` entity counts against counts derived directly from transformed resource CSV files. This is a like-for-like comparison at the same stage of the pipeline.

Additional fixes over Notebook 1:
- Deduplicated resources in `reporting_historic_endpoints` before merging (Notebook 1 was inflating counts by keeping duplicates)
- Used `_stream=on` when fetching `dataset_resource` CSVs rather than `_size=max`

**Data sources:**
- `reporting_historic_endpoints` — fetched from Datasette with pagination (`rowid__gt` loop)
- `dataset_resource` — CSV exports from Datasette per dataset
- Transformed resources — individual CSV files from `files.planning.data.gov.uk/[collection]-collection/transformed/[dataset]/[resource_hash].csv`

### Results

**Resource level** (291 matches, 8 mismatches):

The 8 mismatches are all cases where `dataset_resource` has a `NaN` count and the transformed resource has 0 entities — i.e. resources with no data, not a genuine discrepancy.

**LPA level** (287 matches, 0 mismatches):

When aggregated to LPA level, there are no mismatches at all.

### Conclusion

Entity counts between `reporting_historic_endpoints` and transformed resources are fully consistent at the LPA level. The backend pipeline data is internally coherent for the four datasets tested.
