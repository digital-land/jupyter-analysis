"""
Provider & Organisation Quality Breakdown — all datasets.

Outputs:
  providers_quality.csv             — one row per (dataset, organisation), quality: authoritative | some
  providers_count_summary.csv       — one row per dataset with authoritative / some / total counts
  organisations_quality.csv         — one row per (dataset, organisation), quality: authoritative | some
  organisations_count_summary.csv   — one row per dataset with authoritative / some / total counts
"""

import os
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "outputs")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch_table_csv(table_name):
    """Stream a full digital-land Datasette table as CSV — one request, no pagination."""
    return pd.read_csv(
        f"https://datasette.planning.data.gov.uk/digital-land/{table_name}.csv?_stream=on"
    )


def datasette_sql(db, sql, page_size=900):
    """Run SQL via Datasette JSON API with LIMIT/OFFSET pagination."""
    clean_sql = " ".join(sql.split())
    all_rows, columns, offset = [], None, 0
    while True:
        paginated = f"{clean_sql} LIMIT {page_size} OFFSET {offset}"
        url = (
            f"https://datasette.planning.data.gov.uk/{db}.json?"
            + urllib.parse.urlencode({"sql": paginated})
        )
        data = requests.get(url).json()
        if "rows" not in data:
            raise ValueError(f"Datasette SQL error ({db}): {data.get('error', 'unknown')}")
        if columns is None:
            columns = data["columns"]
        batch = data["rows"]
        all_rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return pd.DataFrame(all_rows, columns=columns)


def _fetch_csv(url, **kwargs):
    """Fetch a CSV from any URL, returning None on failure (used in parallel loaders)."""
    try:
        return pd.read_csv(url, **kwargs)
    except Exception:
        return None


# ── 1. Source data (CSV streaming — one request each) ────────────────────────

print("Fetching source and source_pipeline...")
source = fetch_table_csv("source")
source_pipeline = fetch_table_csv("source_pipeline")
merged = source.merge(source_pipeline, on="source")
active = merged[merged["endpoint"] != ""]
print(f"  {active['pipeline'].nunique()} datasets with active providers")


# ── 2. Organisation lookup ────────────────────────────────────────────────────

print("Fetching organisation table...")
org_df = pd.read_csv(
    "https://datasette.planning.data.gov.uk/digital-land/organisation.csv?_stream=on"
)
org_to_entity = org_df.set_index("organisation")["entity"].to_dict()
entity_to_org = {int(v): k for k, v in org_to_entity.items()}
org_names = org_df[["organisation", "name"]].rename(columns={"name": "organisation_name"})
active_orgs = set(org_df[org_df["end_date"].isna()]["organisation"])


# ── 3. Entity-organisation.csv + lookup.csv from GitHub (parallel) ────────────

GITHUB_BASE = "https://raw.githubusercontent.com/digital-land/config/main/pipeline"
COLLECTIONS = [
    "agricultural-land-classification", "air-quality-management-area", "ancient-woodland",
    "archaeological-priority-area", "area-of-outstanding-natural-beauty", "article-4-direction",
    "asset-of-community-value", "border", "brownfield-land", "brownfield-site", "built-up-area",
    "central-activities-zone", "community-infrastructure-levy-schedule", "conservation-area",
    "design-code", "developer-contributions", "development-corporation-boundary", "document",
    "educational-establishment", "flood-risk-zone", "flood-storage-area", "green-belt",
    "heritage-coast", "historic-england", "infrastructure-funding-statement", "infrastructure-project",
    "legislation", "listed-building", "local-area-requirements", "local-authority-district",
    "local-nature-reserve", "local-plan", "local-planning-authority", "local-resilience-forum-boundary",
    "national-nature-reserve", "national-park", "nature-improvement-area", "nutrient-neutrality",
    "organisation", "ownership-status", "parish", "planning-application", "planning-condition",
    "planning-permission-status", "planning-permission-type", "ramsar", "region", "site-category",
    "site-of-special-scientific-interest", "special-area-of-conservation", "special-protection-area",
    "title-boundary", "transport-access-node", "tree-preservation-order", "ward",
]

print("Loading entity-organisation.csv and lookup.csv files (parallel)...")
with ThreadPoolExecutor(max_workers=10) as pool:
    eo_futures = {
        pool.submit(_fetch_csv, f"{GITHUB_BASE}/{c}/entity-organisation.csv"): c
        for c in COLLECTIONS
    }
    lkp_futures = {
        pool.submit(_fetch_csv, f"{GITHUB_BASE}/{c}/lookup.csv", low_memory=False): c
        for c in COLLECTIONS
    }
    eo_parts = [f.result() for f in as_completed(eo_futures) if f.result() is not None]
    lkp_parts = [f.result() for f in as_completed(lkp_futures) if f.result() is not None]

all_eo = pd.concat(eo_parts, ignore_index=True)
all_eo.columns = [c.strip() for c in all_eo.columns]
print(f"  entity-organisation: {len(all_eo)} rows across {all_eo['dataset'].nunique()} datasets")

all_lookup = pd.concat(lkp_parts, ignore_index=True)
all_lookup = all_lookup[all_lookup["entity"].notna()].copy()
all_lookup["entity"] = all_lookup["entity"].astype(int)
print(f"  lookup: {len(all_lookup)} rows")

# Build orgs_with_ranges here so both the orgs loop and classify_final can use it.
orgs_with_ranges = all_eo.groupby("dataset")["organisation"].apply(set).to_dict()


# ── 4. Providers quality loop (parallel per dataset) ─────────────────────────

def get_provider_quality(dataset, org_entity_ids, batch_size=50):
    parts = []
    for i in range(0, len(org_entity_ids), batch_size):
        batch = org_entity_ids[i:i + batch_size]
        placeholders = ",".join(str(e) for e in batch)
        sql = f"SELECT organisation_entity, quality FROM entity WHERE organisation_entity IN ({placeholders})"
        parts.append(datasette_sql(dataset, sql))
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(
        columns=["organisation_entity", "quality"]
    )


def _provider_quality_for_dataset(dataset, org_entity_ids):
    try:
        eq = get_provider_quality(dataset, org_entity_ids)
        if eq.empty:
            return None
        eq["organisation"] = eq["organisation_entity"].dropna().astype(int).map(entity_to_org)
        summary = eq.groupby(["organisation", "quality"]).size().reset_index(name="entity_count")
        summary["dataset"] = dataset
        print(f"  providers  {dataset}: {len(eq)} entity rows")
        return summary
    except Exception:
        return None  # no Datasette entity table for this dataset


print("Running providers quality loop (parallel)...")
work = []
for dataset in sorted(active["pipeline"].unique()):
    orgs = active[active["pipeline"] == dataset]["organisation"].unique()
    org_entity_ids = [int(org_to_entity[o]) for o in orgs if o in org_to_entity]
    if org_entity_ids:
        work.append((dataset, org_entity_ids))

with ThreadPoolExecutor(max_workers=5) as pool:
    futures = {pool.submit(_provider_quality_for_dataset, ds, ids): ds for ds, ids in work}
    provider_rows = [f.result() for f in as_completed(futures) if f.result() is not None]

providers_df = pd.concat(provider_rows, ignore_index=True) if provider_rows else pd.DataFrame()


# ── 5. Organisations quality loop + alt-source detection (parallel) ───────────

# Org types that need coverage > 1 to count as an alt-source.
# Single-owner coverage suggests a dissolved/reorganised body (post-LGR dev-corp,
# defunct LA) with stale lookup entries, not a genuine national-scale seeder.
LA_PREFIXES = ("local-authority:", "national-park-authority:", "development-corporation:")


def _org_quality_for_dataset(dataset, all_lookup, orgs_with_ranges, entity_to_org):
    try:
        entity_q = datasette_sql(
            dataset, "SELECT entity, organisation_entity, quality FROM entity"
        )
        if entity_q.empty:
            return None
        entity_q["entity"] = entity_q["entity"].astype(int)
    except Exception:
        return None

    # --- organisations quality (direct organisation_entity lookup) ---
    direct = entity_q.copy()
    direct["organisation"] = direct["organisation_entity"].dropna().astype(int).map(entity_to_org)
    direct = direct.dropna(subset=["organisation"])
    org_summary = None
    if not direct.empty:
        org_summary = direct.groupby(["organisation", "quality"]).size().reset_index(name="entity_count")
        org_summary["dataset"] = dataset
        print(f"  orgs       {dataset}: {direct['organisation'].nunique()} orgs")

    # --- alt-source detection ---
    alt_entry = None
    some_ent = entity_q[entity_q["quality"] == "some"].copy()
    if not some_ent.empty:
        owner_orgs = set(
            some_ent["organisation_entity"].dropna().astype(int).map(entity_to_org).dropna()
        )
        designated_orgs = orgs_with_ranges.get(dataset, set())
        lkp_some = all_lookup[all_lookup["entity"].isin(some_ent["entity"])]

        entity_owner_map = (
            some_ent.set_index("entity")["organisation_entity"]
            .dropna().astype(int).map(entity_to_org)
        )
        candidates = lkp_some[
            ~lkp_some["organisation"].isin(owner_orgs) &
            ~lkp_some["organisation"].isin(designated_orgs) &
            lkp_some["organisation"].isin(active_orgs)
        ].copy()
        candidates["owner"] = candidates["entity"].map(entity_owner_map)
        coverage = candidates.groupby("organisation")["owner"].nunique()

        alt_orgs = [
            org for org, count in coverage.items()
            if (org.startswith(LA_PREFIXES) and count > 1)
            or (not org.startswith(LA_PREFIXES) and count >= 1)
        ]
        if alt_orgs:
            seeded_counts = {
                org: int(candidates[candidates["organisation"] == org]["entity"].nunique())
                for org in alt_orgs
            }
            alt_entry = {"dataset": dataset, "alternative_source_orgs": alt_orgs, "seeded_counts": seeded_counts}

    return org_summary, alt_entry


print("Running organisations quality loop (parallel)...")
org_rows = []
alt_source_rows = []

with ThreadPoolExecutor(max_workers=5) as pool:
    futures = {
        pool.submit(_org_quality_for_dataset, ds, all_lookup, orgs_with_ranges, entity_to_org): ds
        for ds in sorted(all_eo["dataset"].unique())
    }
    for f in as_completed(futures):
        result = f.result()
        if result is None:
            continue
        org_summary, alt_entry = result
        if org_summary is not None:
            org_rows.append(org_summary)
        if alt_entry is not None:
            alt_source_rows.append(alt_entry)

orgs_df = pd.concat(org_rows, ignore_index=True) if org_rows else pd.DataFrame()
alt_sources_df = pd.DataFrame(alt_source_rows) if alt_source_rows else pd.DataFrame(
    columns=["dataset", "alternative_source_orgs"]
)
alt_by_dataset = (
    alt_sources_df.set_index("dataset")["alternative_source_orgs"]
    .apply(lambda x: set(x) if isinstance(x, list) else set())
    .to_dict()
    if len(alt_sources_df) else {}
)
alt_seeded_by_dataset = {
    row["dataset"]: row.get("seeded_counts", {})
    for row in alt_source_rows
}


# ── 6. Provider classification ────────────────────────────────────────────────

all_active_providers = (
    active.groupby(["pipeline", "organisation"])
    .size().reset_index()[["pipeline", "organisation"]]
    .rename(columns={"pipeline": "dataset"})
)

provider_with_quality = all_active_providers.merge(
    providers_df[["dataset", "organisation", "quality", "entity_count"]],
    on=["dataset", "organisation"], how="left",
)
provider_with_quality["quality"] = provider_with_quality["quality"].fillna("no entities")


def _classify(group):
    qualities = set(group["quality"].unique()) - {"no entities"}
    if not qualities:
        return "no entities"
    if qualities == {"authoritative"}:
        return "authoritative only"
    if qualities == {"some"}:
        return "some only"
    return "both"


provider_classification = (
    provider_with_quality
    .groupby(["dataset", "organisation"])
    .apply(_classify, include_groups=False)
    .reset_index(name="classification")
)


# ── 7. Final provider classification ─────────────────────────────────────────

def classify_final(row):
    cls = row["classification"]
    org = row["organisation"]
    dataset = row["dataset"]

    if cls in ("authoritative only", "both"):
        return "authoritative"

    if cls == "some only":
        if org in orgs_with_ranges.get(dataset, set()):
            return None  # designated provider with stalled data — exclude
        return "some"   # external seeder (no entity range)

    if cls == "no entities":
        if org in alt_by_dataset.get(dataset, set()):
            return "some"  # seeded data via lookup but not owner (e.g. MHCLG/local-plan)

    return None


providers_final = provider_classification.copy()
providers_final["quality"] = providers_final.apply(classify_final, axis=1)
providers_final = providers_final[providers_final["quality"].notna()]


# ── 8. Save provider CSVs ─────────────────────────────────────────────────────

entity_totals = (
    providers_df.groupby(["dataset", "organisation"])["entity_count"]
    .sum().reset_index(name="total_entity_count")
)

# Build seeded-count lookup for alt-source orgs (those with no directly-owned entities)
seeded_rows = [
    {"dataset": ds, "organisation": org, "seeded_count": cnt}
    for ds, counts in alt_seeded_by_dataset.items()
    for org, cnt in counts.items()
]
seeded_df = pd.DataFrame(seeded_rows) if seeded_rows else pd.DataFrame(
    columns=["dataset", "organisation", "seeded_count"]
)

providers_export = (
    providers_final
    .merge(entity_totals, on=["dataset", "organisation"], how="left")
    .merge(seeded_df, on=["dataset", "organisation"], how="left")
    .merge(org_names, on="organisation", how="left")
)
# For alt-source orgs with 0 directly-owned entities, use seeded entity count from lookup
providers_export["total_entity_count"] = providers_export.apply(
    lambda r: int(r["seeded_count"]) if (
        pd.notna(r["seeded_count"]) and
        (pd.isna(r["total_entity_count"]) or r["total_entity_count"] == 0)
    ) else (int(r["total_entity_count"]) if pd.notna(r["total_entity_count"]) else 0),
    axis=1,
)
# Exclude 'some' providers with 0 entities — no seeded data found in lookup
providers_export = providers_export[
    ~((providers_export["quality"] == "some") & (providers_export["total_entity_count"] == 0))
][["dataset", "organisation", "organisation_name", "quality", "total_entity_count"]]
providers_export.to_csv(os.path.join(OUTPUT_DIR, "providers_quality.csv"), index=False)
print(f"Saved providers_quality.csv — {len(providers_export)} rows")

# Build count summary from filtered export so totals stay consistent
providers_count = (
    providers_export.groupby(["dataset", "quality"])["organisation"]
    .nunique().unstack(fill_value=0).reset_index()
)
for col in ["authoritative", "some"]:
    if col not in providers_count.columns:
        providers_count[col] = 0
providers_count["total"] = providers_count[["authoritative", "some"]].sum(axis=1)
providers_count.to_csv(os.path.join(OUTPUT_DIR, "providers_count_summary.csv"), index=False)
print(f"Saved providers_count_summary.csv — {len(providers_count)} rows")


# ── 9. Save organisation CSVs ─────────────────────────────────────────────────

orgs_classified = (
    orgs_df
    .merge(org_names, on="organisation", how="left")
    .groupby(["dataset", "organisation", "organisation_name"])
    .apply(lambda g: "authoritative" if "authoritative" in set(g["quality"]) else "some",
           include_groups=False)
    .reset_index(name="quality")
)
org_entity_totals = (
    orgs_df.groupby(["dataset", "organisation"])["entity_count"]
    .sum().reset_index(name="total_entity_count")
)
orgs_export = orgs_classified.merge(org_entity_totals, on=["dataset", "organisation"], how="left")
orgs_export.to_csv(os.path.join(OUTPUT_DIR, "organisations_quality.csv"), index=False)
print(f"Saved organisations_quality.csv — {len(orgs_export)} rows")

orgs_count = (
    orgs_export.groupby(["dataset", "quality"])["organisation"]
    .nunique().unstack(fill_value=0).reset_index()
)
for col in ["authoritative", "some"]:
    if col not in orgs_count.columns:
        orgs_count[col] = 0
orgs_count["total"] = orgs_count[["authoritative", "some"]].sum(axis=1)
orgs_count.to_csv(os.path.join(OUTPUT_DIR, "organisations_count_summary.csv"), index=False)
print(f"Saved organisations_count_summary.csv — {len(orgs_count)} rows")

print("\nDone.")
