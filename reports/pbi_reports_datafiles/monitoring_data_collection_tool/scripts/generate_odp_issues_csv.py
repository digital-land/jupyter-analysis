import os
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import argparse

SPATIAL_DATASETS = [
    "article-4-direction-area",
    "conservation-area",
    "listed-building-outline",
    "tree-preservation-zone",
    "tree",
]
DOCUMENT_DATASETS = [
    "article-4-direction",
    "conservation-area-document",
    "tree-preservation-order",
]
ALL_DATASETS = SPATIAL_DATASETS + DOCUMENT_DATASETS

def get_datasette_http():
    retry_strategy = Retry(total=3, status_forcelist=[400], backoff_factor=0.2)
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    return http

def get_datasette_query(db: str, sql: str, url="https://datasette.planning.data.gov.uk") -> pd.DataFrame:
    full_url = f"{url}/{db}.json"
    params = {"sql": sql, "_shape": "array", "_size": "max"}
    try:
        http = get_datasette_http()
        response = http.get(full_url, params=params)
        response.raise_for_status()
        return pd.DataFrame(response.json())
    except Exception as e:
        print(f"[ERROR] Datasette query failed: {e}")
        return pd.DataFrame()

def get_provisions():
    sql = """
        SELECT
            p.cohort,
            p.organisation,
            c.start_date AS cohort_start_date,
            o.name AS organisation_name
        FROM provision p
        INNER JOIN cohort c ON c.cohort = p.cohort
        INNER JOIN organisation o ON o.organisation = p.organisation
        WHERE p.provision_reason = 'expected'
          AND p.project = 'open-digital-planning'
        GROUP BY p.organisation, p.cohort
    """
    return get_datasette_query("digital-land", sql)

def get_issue_type_chunk(dataset_clause, offset):
    sql = f"""
        SELECT
            edits.*,
            eds.endpoint_end_date,
            eds.endpoint_entry_date,
            eds.latest_status,
            eds.latest_exception
        FROM endpoint_dataset_issue_type_summary edits
        LEFT JOIN (
            SELECT endpoint, end_date as endpoint_end_date,
                   entry_date as endpoint_entry_date,
                   latest_status, latest_exception
            FROM endpoint_dataset_summary
        ) eds ON edits.endpoint = eds.endpoint
        {dataset_clause}
        LIMIT 1000 OFFSET {offset}
    """
    return get_datasette_query("performance", sql)

def get_full_issue_type_summary(datasets):
    dataset_clause = "WHERE " + " OR ".join(f"edits.dataset = '{ds}'" for ds in datasets)
    df_list = []
    offset = 0
    while True:
        chunk = get_issue_type_chunk(dataset_clause, offset)
        if chunk.empty:
            break
        df_list.append(chunk)
        if len(chunk) < 1000:
            break
        offset += 1000
    return pd.concat(df_list, ignore_index=True)

def generate_detailed_issue_csv(output_dir: str, dataset_type="all") -> str:
    datasets = {
        "spatial": SPATIAL_DATASETS,
        "document": DOCUMENT_DATASETS,
        "all": ALL_DATASETS
    }.get(dataset_type, ALL_DATASETS)

    print("[INFO] Fetching provisions...")
    provisions = get_provisions()

    print("[INFO] Fetching detailed issue-level data...")
    issues = get_full_issue_type_summary(datasets)

    print("[INFO] Merging data...")
    merged = provisions.merge(issues.drop(columns=["organisation_name"], errors="ignore"), on=["organisation", "cohort"], how="inner")

    print("[INFO] Saving CSV...")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "odp-issue.csv")
    merged[
        [
            "organisation",
            "cohort",
            "organisation_name",
            "pipeline",
            "issue_type",
            "severity",
            "responsibility",
            "count_issues",
            "collection",
            "endpoint",
            "endpoint_url",
            "latest_status",
            "latest_exception",
            "resource",
            "latest_log_entry_date",
            "endpoint_entry_date",
            "endpoint_end_date",
            "resource_start_date",
            "resource_end_date",
        ]
    ].to_csv(output_path, index=False)

    print(f"[SUCCESS] CSV saved: {output_path} ({len(merged)} rows)")
    return output_path

def parse_args():
    parser = argparse.ArgumentParser(description="Generate detailed ODP issue-level CSV")
    parser.add_argument(
        "--output-dir",
        type=str,
        required=True,
        help="Directory to save the output CSV"
    )
    return parser.parse_args()

if __name__ == "__main__":

    args = parse_args()
    generate_detailed_issue_csv(args.output_dir, dataset_type="all")
