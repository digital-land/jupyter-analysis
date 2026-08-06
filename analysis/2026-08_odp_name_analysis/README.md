# ODP `name` field quality

**Author**: Sian Teesdale

**Date created**: 5th August 2026

**Dataset Scope**: `article-4-direction-area`, `conservation-area`, `listed-building-outline`, `tree-preservation-zone`, `tree`

**Purpose**: First-pass exploration of how "weird" the `name` field is across the ODP geography datasets — exact duplicate names, blank/missing names, names that are bare reference codes rather than descriptions, and repeated boilerplate/placeholder text (e.g. `"Town and Country Planning Direction No. X, YYYY"`, `"No name for this Entry"`). For review, and then findings to be handed to pipelines/data engineering to raise specific issues with LPAs.

## The analysis: `initial_odp_name_analysis.ipynb`

Defines four reusable checks (`duplicate_names`, `code_like_names`, `missing_names`, `top_ngrams`) and runs them across all five datasets, then drills into each dataset individually to inspect specific flagged phrases.

**Headline numbers:**

| Dataset                  | Rows    | Duplicate names | Blank/missing names | Bare-code names |
| ------------------------ | ------- | --------------- | ------------------- | --------------- |
| article-4-direction-area | 7,276   | 2,335 (32.1%)   | 6 (0.1%)            | 104 (1.4%)      |
| conservation-area        | 11,008  | 1,807 (16.4%)   | 110 (1.0%)          | 10 (0.1%)       |
| listed-building-outline  | 126,199 | 25,082 (19.9%)  | 2,245 (1.8%)        | 144 (0.1%)      |
| tree-preservation-zone   | 102,170 | 62,733 (61.4%)  | 24,290 (23.8%)      | 12,050 (11.8%)  |
| tree                     | 256,643 | 179,860 (70.1%) | 50,180 (19.6%)      | 43,535 (17.0%)  |

**Notable patterns:**

- **Placeholder text instead of a real name** — e.g. `listed-building-outline` has 845 rows reading `"No name for this Entry"`, 220 reading `"No given name"`, and 149 reading `"No Address Supplied"`. These aren't caught by a blank check since the field isn't actually empty.
- **Legal boilerplate standing in for a name** — e.g. `"Town and Country Planning Direction No. X, YYYY"` repeated with only the number/year changing; `"tree preservation order"` appears in 15–23% of `tree`/`tree-preservation-zone` names as boilerplate rather than anything distinguishing.
- **Bare reference codes** — names that are just a number or short code with no description, e.g. `T1`, `3B`, `0164B2`, `11/802`, `5001/2025/TPO`. Worst in `tree` (17.0%) and `tree-preservation-zone` (11.8%).
- `tree` and `tree-preservation-zone` are the most affected overall — 61–70% of names are duplicated across rows, and roughly 1 in 5 rows has a blank name outright.

## Data sources

CSV exports from `https://files.planning.data.gov.uk/dataset/[dataset].csv`, fetched directly in the notebook.
