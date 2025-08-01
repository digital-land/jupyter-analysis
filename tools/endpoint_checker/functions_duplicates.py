import pandas as pd
import sqlite3

def find_mod_spatialite():
    # Look for it in common paths
    candidates = [
        "mod_spatialite",  # default — system loader will try system paths
        "/usr/lib/mod_spatialite.so",
        "/usr/local/lib/mod_spatialite.so",
        "/opt/homebrew/lib/mod_spatialite.dylib",
        # other OS-specific paths...
    ]
    for path in candidates:
        try:
            # Try loading it temporarily to test if it's valid
            conn = sqlite3.connect(":memory:")
            conn.enable_load_extension(True)
            conn.load_extension(path)
            return path
        except Exception:
            continue

    raise OSError("Could not find mod_spatialite extension. Please install it.")

def connect(database=None, load_extension=True, mod_spatialite_path=None, *args, **kwargs):

    conn = sqlite3.connect(database or ":memory:", *args, **kwargs)

    if load_extension:
        conn.enable_load_extension(True)

        # Default path if not provided
        if mod_spatialite_path is None:
            mod_spatialite_path = find_mod_spatialite()

        conn.load_extension(mod_spatialite_path)

    return conn

def query_sqlite(conn, query_string):        
    cursor = conn.execute(query_string)
    cols = [column[0] for column in cursor.description]
    results_df = pd.DataFrame.from_records(data=cursor.fetchall(), columns=cols)

    return results_df


def count_valid_values(conn, field_name):

    df = query_sqlite(
        conn,
        f"select {field_name} from entity where {field_name} != '' "
    )
    return len(df)


def get_duplicates_between_orgs(dataset, live_path, new_path):

    mod_spatialite_path = find_mod_spatialite()
    live_conn = connect(live_path, mod_spatialite_path=mod_spatialite_path)  
    new_conn = connect(new_path, mod_spatialite_path=mod_spatialite_path)

    #get new endpoint entities and write to temp table in live dataset sqlite db
    results_temp = query_sqlite(
        new_conn, 
        """
        SELECT entity, name, organisation_entity, reference, geometry, point
        FROM entity
        """)
    
    results_temp.to_sql("entity_new", live_conn, index=False, if_exists='replace')

    # if dataset is tree with points instead of geometry (multipolygons), use points and st_equals join
    if (dataset == "tree") & (count_valid_values(new_conn, "point") > count_valid_values(new_conn, "geometry")):

        print("Dataset is tree with points instead of polygons, checking for geometry duplicates using points")

        #checks current entities with existing dataset
        sql_point = """
            SELECT a.entity AS live_entity,
                a.name AS live_name,
                a.reference AS live_reference,
                a.organisation_entity AS live_organisation_entity,
                a.point live_geometry,
                b.entity AS new_entity,
                b.name AS new_name,
                b.reference AS new_reference,
                b.organisation_entity AS new_organisation_entity,
                b.point AS new_geometry
            FROM
                (SELECT entity, name, organisation_entity, reference, point
                FROM entity
                WHERE ST_IsValid(GeomFromText(point))
                ) a
            JOIN
                entity_new b 
            ON a.organisation_entity <> b.organisation_entity
                AND ST_EQUALS(GeomFromText(a.point), GeomFromText(b.point)) 
            WHERE ST_IsValid(GeomFromText(b.point))
            """
        
        results = query_sqlite(live_conn, sql_point)

    else:
         
        print("checking for geometry duplicates using geometry field (multipolygon)")
        sql_geom = """
        WITH calc AS (
        SELECT a.entity AS live_entity,
            a.name AS live_name,
            a.reference AS live_reference,
            a.organisation_entity AS live_organisation_entity,
            a.geometry live_geometry,
            b.entity AS new_entity,
            b.name AS new_name,
            b.reference AS new_reference,
            b.organisation_entity AS new_organisation_entity,
            b.geometry AS new_geometry,
            ST_Area(ST_Intersection(GeomFromText(a.geometry), GeomFromText(b.geometry))) as area_geom_intersection,
            ST_Area(GeomFromText(a.geometry)) as area_geom_live,
            ST_Area(GeomFromText(b.geometry)) as area_geom_new,
            ST_Area(ST_Intersection(GeomFromText(a.geometry), GeomFromText(b.geometry))) / ST_Area(ST_Union(GeomFromText(a.geometry), GeomFromText(b.geometry))) as pct_overlap

        FROM entity a
        JOIN entity_new b 
        ON a.organisation_entity <> b.organisation_entity
            AND ST_Intersects(GeomFromText(a.geometry), GeomFromText(b.geometry))
        WHERE ST_IsValid(GeomFromText(a.geometry))
            AND ST_IsValid(GeomFromText(b.geometry))
        )

        SELECT *
        FROM CALC
        WHERE pct_overlap > 0.95
        """
        
        results = query_sqlite(live_conn, sql_geom)

    print(f"{len(results)} geographical matches found between new and existing entities")

    return results
    
