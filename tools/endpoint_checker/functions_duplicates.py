import pandas as pd
import spatialite

def query_sqlite(db_path, query_string):

    with spatialite.connect(db_path) as con:
            
        cursor = con.execute(query_string)
        cols = [column[0] for column in cursor.description]
        results_df = pd.DataFrame.from_records(data=cursor.fetchall(), columns=cols)

    return results_df


def count_valid_values(db_path, field_name):

    df = query_sqlite(
        db_path,
        f"select {field_name} from entity where {field_name} != '' "
    )
    return len(df)


def get_duplicates_between_orgs(dataset, live_path, new_path):  

    #get new endpoint entities and write to temp table in live dataset sqlite db
    results_temp = query_sqlite(
        new_path, 
        """
        SELECT entity, name, organisation_entity, reference, geometry, point
        FROM entity
        """)
    
    with spatialite.connect(live_path) as con:
            results_temp.to_sql("entity_new", con, index=False, if_exists='replace')

    # if dataset is tree with points instead of geometry (multipolygons), use points and st_equals join
    if (dataset == "tree") & (count_valid_values(new_path, "point") > count_valid_values(new_path, "geometry")):

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
        
        results = query_sqlite(live_path, sql_point)

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
            ST_Area(ST_Intersection(GeomFromText(a.geometry), GeomFromText(b.geometry))) / ST_Area(ST_Union(GeomFromText(a.geometry), GeomFromText(a.geometry))) as pct_overlap

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
        
        results = query_sqlite(live_path, sql_geom)

    print(f"{len(results)} geographical matches found between new and existing entities")

    return results
    
