import urllib
import os
import sqlite3
import pandas as pd
import geopandas as gpd
import shapely.wkt


global FILES_URL

FILES_URL = 'https://datasette.planning.data.gov.uk/'

def download_dataset(dataset, output_dir_path, overwrite=False):
    dataset_file_name = f'{dataset}.db'
    
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)
    
    output_file_path = os.path.join(output_dir_path, dataset_file_name)

    if overwrite is False and os.path.exists(output_file_path):
        return
    
    final_url = os.path.join(FILES_URL, dataset_file_name)
    print(f'downloading data from {final_url}')
    print(f'to: {output_file_path}')
    urllib.request.urlretrieve(final_url, os.path.join(output_dir_path, dataset_file_name))
    print('download complete')


def get_pdp_dataset(dataset, geometry_field = "geometry", crs_out=4326, underscore_cols=True):

    df = pd.read_csv(f"https://files.planning.data.gov.uk/dataset/{dataset}.csv", dtype = "str")
    df.columns = [x.replace("-", "_") for x in df.columns]

    df_valid_geom = df[df[geometry_field].notnull()].copy()

    # load geometry and create GDF
    df_valid_geom[geometry_field] = df_valid_geom[geometry_field].apply(shapely.wkt.loads)
    gdf = gpd.GeoDataFrame(df_valid_geom, geometry = geometry_field)

    # Transform to ESPG:27700 for more interpretable area units
    gdf.set_crs(epsg=4326, inplace=True)
    gdf.to_crs(epsg=crs_out, inplace=True)

    return gdf


def query_sqlite(db_path, query_string):

    with sqlite3.connect(db_path) as con:
            
        cursor = con.execute(query_string)
        cols = [column[0] for column in cursor.description]
        results_df = pd.DataFrame.from_records(data=cursor.fetchall(), columns=cols)

    return results_df


def datasette_query(db, sql_string):
    params = urllib.parse.urlencode({
        "sql": sql_string,
        "_size": "max"
        })
    url = f"https://datasette.planning.data.gov.uk/{db}.csv?{params}"
    df = pd.read_csv(url)
    return df


def datasette_query_paginated(db, sql_string, page_size=1000):
    # datasette's `_size=max` still caps a single response, so large tables
    # need LIMIT/OFFSET paging to avoid being silently truncated
    frames = []
    offset = 0

    while True:
        page_sql = f"{sql_string}\nLIMIT {page_size} OFFSET {offset}"
        page_df = datasette_query(db, page_sql)
        if page_df.empty:
            break

        frames.append(page_df)

        if len(page_df) < page_size:
            break
        offset += page_size

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)