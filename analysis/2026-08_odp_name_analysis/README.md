# ODP `name` field quality

**Author**: Sian Teesdale

**Date created**: 5th August 2026

**Dataset Scope**: `article-4-direction-area`, `conservation-area`, `listed-building-outline`, `tree-preservation-zone`, `tree`

**Purpose**: Exploration of how "weird" the `name` field is across the ODP geography datasets — exact duplicate names, blank/missing names, names that are bare reference codes rather than descriptions, and repeated boilerplate/placeholder text. Findings reviewed and turned into concrete expectations/issues below, to be handed to pipelines/data engineering to raise with LPAs.

## Outcome: what we're doing about it

- **New expectation: duplicate names**, across all five datasets (`article-4-direction-area`, `conservation-area`, `listed-building-outline`, `tree-preservation-zone`, `tree`).
- **New issue: LBO name doesn't match its linked `listed-building` record, and is a specific placeholder string** — `"No name for this Entry"`, `"No given name"` (`listed-building-outline` only).
- **New issue: names that look like bare codes** — `article-4-direction-area`, `conservation-area`, `listed-building-outline`.

## Files

- **[initial_odp_name_analysis.ipynb](initial_odp_name_analysis.ipynb)** — first-pass exploration across all five datasets: duplicate names, blank names, bare-code names, repeated boilerplate phrases.
- **[tree_and_lbo_name_checks.ipynb](tree_and_lbo_name_checks.ipynb)** — follow-up narrowing to `tree`, `tree-preservation-zone`, and `listed-building-outline`. Checks `tree`/`tree-preservation-zone` names against the ODP guidance (name can be a description, the `reference`, an address, or blank), and cross-references `listed-building-outline` against its linked `listed-building` record.

---

## Notebook 1 — `initial_odp_name_analysis.ipynb`

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

## Notebook 2 — `tree_and_lbo_name_checks.ipynb`

Narrows scope to `tree`, `tree-preservation-zone`, and `listed-building-outline`, and moves from "does this look weird" to "is this actually against guidance/expectation."

**`tree` / `tree-preservation-zone`**: ODP guidance allows the `name` field to be a description, the `reference` value, an address (`address`/`address-text`), or blank. `classify_name()` buckets every row into one of these (plus an informally-observed fifth pattern — name matching the `tree-preservation-order` reference) versus genuinely unexplained. Result: most "bare-code" names from notebook 1 turn out to be legitimate reference/TPO reuse — e.g. in `tree-preservation-zone`, of 12,050 code-like names only 5,936 are unexplained by any allowed pattern.

**`listed-building-outline` (LBO)**: cross-checked against its geospatial-free counterpart `listed-building` (LB), linked via LBO's `listed-building` column against LB's `reference`. Two key findings:

- Of LBO rows that link to an LB record, only ~51% have an exact name match — though many "mismatches" are just LBO appending extra locating detail onto the LB name (e.g. LB `"NORTH LODGE"` → LBO `"NORTH LODGE - B1318 (EAST SIDE) GOSFORTH PARK"`), not a real error.
- Placeholder-name rows (`"No given name"`, `"No name for this Entry"`, `"No address supplied"`) are each a single-organisation habit. 844 of 845 `"No name for this Entry"` rows link straight to an LB record that already has a proper name — the placeholder is masking a real name one join away. None of the three phrases appear anywhere in LB's own `name` field.

## Data sources

- CSV exports from `https://files.planning.data.gov.uk/dataset/[dataset].csv`, fetched directly in both notebooks.
- Notebook 2 additionally fetches `https://files.planning.data.gov.uk/dataset/listed-building.csv` to cross-reference `listed-building-outline`.
