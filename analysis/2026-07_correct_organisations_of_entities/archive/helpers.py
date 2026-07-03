"""
Shared Datasette fetch helpers, lifted from
reports/count_organisations_providers_platform/run_analysis.py so they can be
imported without triggering that script's top-level analysis run.
"""

import time
import urllib.error
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests


def fetch_table_csv(table_name, db="digital-land"):
    """Stream a full digital-land Datasette table as CSV — one request, no pagination."""
    return pd.read_csv(
        f"https://datasette.planning.data.gov.uk/{db}/{table_name}.csv?_stream=on"
    )


def fetch_filtered_table_csv(db, table, columns=None, retries=3, **filters):
    """Stream a filtered table as CSV via Datasette's table view (e.g. quality='some').

    Much faster than paginating a SQL query with LIMIT/OFFSET (datasette_sql below),
    since that re-scans the table from the top on every page — O(n^2) for large tables.
    _stream=on only lifts the row-count cap on the table view, not on arbitrary SQL.
    Retries on transient 5xx errors and connection resets, which show up under high
    concurrency (e.g. thousands of batched requests for provenance tracing).
    """
    params = {"_stream": "on", **filters}
    if columns:
        params["_col"] = list(columns)
    url = (
        f"https://datasette.planning.data.gov.uk/{db}/{table}.csv?"
        + urllib.parse.urlencode(params, doseq=True)
    )
    for attempt in range(retries + 1):
        try:
            return pd.read_csv(url)
        except urllib.error.HTTPError as e:
            if e.code >= 500 and attempt < retries:
                time.sleep(0.5 * (attempt + 1))
                continue
            raise
        except (urllib.error.URLError, ConnectionResetError, ConnectionError, TimeoutError):
            if attempt < retries:
                time.sleep(0.5 * (attempt + 1))
                continue
            raise


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


def fetch_csv(url, **kwargs):
    """Fetch a CSV from any URL, returning None on failure (used in parallel loaders)."""
    try:
        return pd.read_csv(url, **kwargs)
    except Exception:
        return None


def chunk(items, size):
    """Split a list into batches — used to keep entity/fact/resource __in filters under
    Datasette's URL length limit (HTTP 414 above roughly 100-150 64-char hashes)."""
    items = list(items)
    for i in range(0, len(items), size):
        yield items[i:i + size]


def _run_jobs(jobs, max_workers):
    """One pass over (fn, args, kwargs) jobs. Returns (results, failed_jobs)."""
    results = []
    failed_jobs = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_to_job = {pool.submit(fn, *args, **kwargs): (fn, args, kwargs) for fn, args, kwargs in jobs}
        for f in as_completed(future_to_job):
            try:
                r = f.result()
            except Exception:
                failed_jobs.append(future_to_job[f])
                continue
            if r is not None and not r.empty:
                results.append(r)
    return results, failed_jobs


def parallel_fetch(jobs, max_workers=15):
    """Run (fn, args, kwargs) jobs concurrently, returning non-empty results.

    A batch that fails even after fetch_filtered_table_csv's own internal retries
    (e.g. a 500 under heavy concurrent load) is retried once more here at a third of
    the concurrency, since that's usually enough for a transient server-side blip to
    clear. A batch that fails both passes is dropped and counted -- silently losing a
    batch would misclassify every entity in it as "no data found" downstream, so any
    remaining failures are reported rather than swallowed. max_workers=15 rather than
    the 30 used for simpler single-pass fetches, since provenance tracing issues far
    more batched requests and higher concurrency triggered connection resets against
    the shared Datasette instance during testing.
    """
    results, failed = _run_jobs(jobs, max_workers)
    if failed:
        print(f"  ({len(failed)} batch requests failed -- retrying once at lower concurrency)")
        more_results, failed = _run_jobs(failed, max(1, max_workers // 3))
        results.extend(more_results)
    if failed:
        print(f"  ({len(failed)} batch requests failed after retries and were skipped)")
    return results
