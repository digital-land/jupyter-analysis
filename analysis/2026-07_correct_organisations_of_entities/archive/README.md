# Archive — exploration that led to the final approach

These three notebooks are kept as a record of the investigative path, not as tools to re-run. Each is either a dead end (documented as such, useful to know not to repeat) or fully superseded by `../4_full_population_provenance_trace.ipynb`, which produces a strict superset of everything they found. See `../README.md` for the current, maintained analysis.

`helpers.py` is duplicated here so these notebooks remain runnable standalone if anyone wants to — it won't be kept in sync with the parent copy going forward.

## `1_find_flagged_entities.ipynb`

**Goal**: find `quality=some` entities provided by a non-government organisation (`local-authority:`, `national-park-authority:`, `development-corporation:`) that currently has an active, registered endpoint for that dataset — matching the original ticket's literal framing. Scope: 13 datasets, ~7,492 entities.

**Why archived**: notebook 4 starts from every `quality=some` entity from a non-gov org, active provider or not, and keeps an `is_active_provider` column throughout. Everything this notebook produced is reproducible by filtering notebook 4's output to `is_active_provider == True` — nothing here is unreachable from notebook 4. The one thing this notebook has going for it is speed (~1 minute vs. notebook 4's ~5, far fewer Datasette calls), which might matter if someone only ever wants the lightweight "which active providers have quality=some data" list without the full provenance trace.

## `2_range_lookup_investigation.ipynb`

**Goal**: the first attempted root-cause check for entities flagged in notebook 1 — does the entity's ID fall within an `entity-organisation.csv` range assigned to a *different* organisation than the one it's currently attributed to? This was meant to directly detect the bug pattern in [digital-land/config#2651](https://github.com/digital-land/config/issues/2651) (WOK→WOI, CHW→CHE).

**Why archived**: this is a genuine dead end, not just a superseded approach. `organisation_entity` on the entity table is always force-overwritten by the pipeline to match `entity-organisation.csv`'s range assignment, regardless of who actually submitted the data (see `digital_land/phase/priority.py`'s `PriorityPhase.process`, which sets `row["organisation"] = authoritative_organisation` whenever the actual submitter doesn't match the range file). That means this range-comparison check can structurally never disagree with itself. We validated this directly: running it against the known CHW misattribution (490 authoritative / 7 `some` entities, all 7 confirmed elsewhere to be Cheshire East's data) still classified every one of them as `own_range`. Kept so nobody re-tries this specific approach expecting it to work.

## `3_trace_true_provenance.ipynb`

**Goal**: the approach that actually works — trace each flagged entity's real submitting organisation via its resource lineage (`fact` → `fact_resource` → `log` → `endpoint` → `source`), independent of what `organisation_entity`/`entity-organisation.csv` claims, then diff the two. This is the method notebook 4 uses.

**Why archived**: this notebook only ever traced notebook 1's narrower population (active providers only, 13 datasets), finding 161 confirmed misattributions. Notebook 4 runs the identical trace and classification logic over the full population and finds 446 — a strict superset, including cases (like Northumberland National Park Authority, which has no registered endpoint at all for `listed-building-outline`) that notebook 1's pre-filter would never present to this notebook in the first place. Nothing this notebook can show isn't already in notebook 4's output.
