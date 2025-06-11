import os
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import argparse

ALL_PIPELINES = {
    "article-4-direction": ["article-4-direction", "article-4-direction-area"],
    "conservation-area": ["conservation-area", "conservation-area-document"],
    "listed-building": ["listed-building-outline"],
    "tree-preservation-order": [
        "tree-preservation-order",
        "tree-preservation-zone",
        "tree",
    ],
}

def get_datasette_http():
    """
    Returns a requests session with retry logic to handle larger datasette queries.
    """
    retry_strategy = Retry(total=3, status_forcelist=[400], backoff_factor=0.2)
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    return http


def get_datasette_query(db: str, sql: str, url="https://datasette.planning.data.gov.uk") -> pd.DataFrame:
    """
    Executes SQL against the given datasette database and returns a DataFrame.
    """
    full_url = f"{url}/{db}.json"
    params = {"sql": sql, "_shape": "array", "_size": "max"}

    try:
        http = get_datasette_http()
        response = http.get(full_url, params=params)
        response.raise_for_status()
        return pd.DataFrame.from_dict(response.json())
    except Exception as e:
        print(f"Datasette query failed: {e}")
        return pd.DataFrame()

def get_provisions():
    """
    Retrieves cohort provisions (expected organisations and their cohorts).
    """
    sql = """
        SELECT
            p.cohort,
            p.organisation,
            c.start_date as cohort_start_date,
            org.name as name
        FROM provision p
        INNER JOIN cohort c ON c.cohort = p.cohort
        INNER JOIN organisation org ON org.organisation = p.organisation
        WHERE p.provision_reason = "expected"
          AND p.project = "open-digital-planning"
        GROUP BY p.organisation, p.cohort
    """
    return get_datasette_query("digital-land", sql)


def get_endpoints():
    """
    Retrieves latest reporting endpoint data.
    """
    sql = """
        SELECT
            rle.organisation,
            rle.collection,
            rle.pipeline,
            rle.endpoint,
            rle.endpoint_url,
            rle.licence,
            rle.latest_status as status,
            rle.days_since_200,
            rle.latest_exception as exception,
            rle.resource,
            rle.latest_log_entry_date,
            rle.endpoint_entry_date,
            rle.endpoint_end_date,
            rle.resource_start_date,
            rle.resource_end_date
        FROM reporting_latest_endpoints rle
    """
    df = get_datasette_query("performance", sql)
    df["organisation"] = df["organisation"].str.replace("-eng", "", regex=False)
    return df

def generate_odp_summary_csv(output_dir: str) -> str:
    """
    Generates a CSV file showing provision status by dataset and saves it to output_dir.
    """
    provisions = get_provisions()
    endpoints = get_endpoints()

    output_rows = []

    for _, row in provisions.iterrows():
        organisation = row["organisation"]
        cohort = row["cohort"]
        name = row["name"]
        cohort_start_date = row["cohort_start_date"]

        for collection, pipelines in ALL_PIPELINES.items():
            for pipeline in pipelines:
                match = endpoints[
                    (endpoints["organisation"] == organisation) &
                    (endpoints["pipeline"] == pipeline)
                ]

                if not match.empty:
                    for _, ep in match.iterrows():
                        output_rows.append({
                            "organisation": organisation,
                            "cohort": cohort,
                            "name": name,
                            "collection": collection,
                            "pipeline": pipeline,
                            "endpoint": ep["endpoint"],
                            "endpoint_url": ep["endpoint_url"],
                            "licence": ep["licence"],
                            "status": ep["status"],
                            "days_since_200": ep["days_since_200"],
                            "exception": ep["exception"],
                            "resource": ep["resource"],
                            "latest_log_entry_date": ep["latest_log_entry_date"],
                            "endpoint_entry_date": ep["endpoint_entry_date"],
                            "endpoint_end_date": ep["endpoint_end_date"],
                            "resource_start_date": ep["resource_start_date"],
                            "resource_end_date": ep["resource_end_date"],
                            "cohort_start_date": cohort_start_date,
                        })
                else:
                    output_rows.append({
                        "organisation": organisation,
                        "cohort": cohort,
                        "name": name,
                        "collection": collection,
                        "pipeline": pipeline,
                        "endpoint": "No endpoint added",
                        "endpoint_url": "",
                        "licence": "",
                        "status": "",
                        "days_since_200": "",
                        "exception": "",
                        "resource": "",
                        "latest_log_entry_date": "",
                        "endpoint_entry_date": "",
                        "endpoint_end_date": "",
                        "resource_start_date": "",
                        "resource_end_date": "",
                        "cohort_start_date": cohort_start_date,
                    })

    # Convert to DataFrame
    df_final = pd.DataFrame(output_rows)

    # Save
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "odp-status.csv")
    df_final.to_csv(output_path, index=False)
    print(f"CSV generated at {output_path} with {len(df_final)} rows")
    return output_path

def parse_args():
    """
    Parses command-line arguments for the output directory.

    Returns:
        argparse.Namespace: Parsed arguments containing the output directory path.
    """
    parser = argparse.ArgumentParser(description="Datasette batch exporter")
    parser.add_argument(
        "--output-dir",
        type=str,
        required=True,
        help="Directory to save exported CSVs"
    )
    return parser.parse_args()



# Run Script

if __name__ == "__main__":
    # Parse arguments from CLI
    args = parse_args()

    # Set your desired output path here
    output_directory = args.output_dir
    
    generate_odp_summary_csv(output_directory)
