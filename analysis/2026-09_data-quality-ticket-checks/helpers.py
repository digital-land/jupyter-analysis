"""
Shared Datasette fetch helpers, lifted from
analysis/2026-07_correct_organisations_of_entities/helpers.py (plus a
published-database lookup added here) so ticket check notebooks in this
folder don't need to pull in the row-level fetch utilities they don't use.
"""

import time
import urllib.parse

import pandas as pd
import requests


def list_published_datasets():
    """Names of databases actually published on Datasette (excludes digital-land/performance).

    Not every row in the `dataset` table has a database here -- most that don't
    simply have no data collected on the platform yet, which is a normal,
    expected state rather than a fetch failure.
    """
    dbs = requests.get("https://datasette.planning.data.gov.uk/-/databases.json").json()
    return {d["name"] for d in dbs} - {"digital-land", "performance"}


def fetch_table_csv(table_name, db="digital-land"):
    """Stream a full digital-land Datasette table as CSV -- one request, no pagination.

    Lifted from analysis/2026-07_correct_organisations_of_entities/helpers.py.
    """
    return pd.read_csv(
        f"https://datasette.planning.data.gov.uk/{db}/{table_name}.csv?_stream=on"
    )


def fetch_config_csv(dataset, filename):
    """Fetch one of a pipeline's config CSVs (e.g. `lookup`, `entity-organisation`)
    from the digital-land/config GitHub repo. Not every dataset has every file --
    returns an empty DataFrame with no columns if the file doesn't exist (404).
    """
    url = f"https://raw.githubusercontent.com/digital-land/config/main/pipeline/{dataset}/{filename}.csv"
    r = requests.get(url, timeout=30)
    if r.status_code == 404:
        return pd.DataFrame()
    r.raise_for_status()
    from io import StringIO

    return pd.read_csv(StringIO(r.text))


def list_config_pipelines_with_file(filename):
    """Names of pipelines (datasets) in digital-land/config that have a given
    pipeline/{dataset}/{filename}.csv file -- one GitHub API call (recursive
    tree listing), not one 404-probing request per candidate dataset.
    """
    r = requests.get(
        "https://api.github.com/repos/digital-land/config/git/trees/main",
        params={"recursive": "1"},
        timeout=30,
    )
    r.raise_for_status()
    tree = r.json()["tree"]
    suffix = f"/{filename}.csv"
    return sorted(
        t["path"].split("/")[1]
        for t in tree
        if t["path"].startswith("pipeline/") and t["path"].endswith(suffix)
    )


def chunk(items, size):
    """Split a list into batches -- used to keep entity `__in` filters under
    Datasette's URL length limit (HTTP 414 above roughly 100-150 hashes/ids)."""
    items = list(items)
    for i in range(0, len(items), size):
        yield items[i:i + size]


def datasette_sql(db, sql, page_size=900, retries=2):
    """Run SQL via Datasette JSON API with LIMIT/OFFSET pagination.

    Retries on transient failures (5xx, connection errors, or a non-JSON/
    truncated response under concurrent load) -- without this, running this
    against ~120+ datasets concurrently produces a handful of spurious
    failures that have nothing to do with the dataset's actual data.
    """
    clean_sql = " ".join(sql.split())
    all_rows, columns, offset = [], None, 0
    while True:
        paginated = f"{clean_sql} LIMIT {page_size} OFFSET {offset}"
        url = (
            f"https://datasette.planning.data.gov.uk/{db}.json?"
            + urllib.parse.urlencode({"sql": paginated})
        )
        for attempt in range(retries + 1):
            try:
                data = requests.get(url, timeout=30).json()
                break
            except (requests.RequestException, ValueError):
                if attempt < retries:
                    time.sleep(0.5 * (attempt + 1))
                    continue
                raise
        # Datasette returns "rows": [] alongside "ok": false on a genuine SQL error
        # (e.g. "no such table: entity") -- check ok/error explicitly, not just
        # whether "rows" is present, or a real error silently looks like an empty result.
        if not data.get("ok", True) or data.get("error"):
            raise ValueError(f"Datasette SQL error ({db}): {data.get('error', 'unknown')}")
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
