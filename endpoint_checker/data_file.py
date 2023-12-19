import spatialite
import pandas as pd

def get_duplicates_between_orgs(dataset_path,current_path):  

    #get current endpoint entities
    sql_db1 = f"""
        SELECT entity,name,organisation_entity,reference,geometry
        FROM entity
        WHERE ST_IsValid(GeomFromText(geometry))
        """
    
    with spatialite.connect(current_path) as con1:
        cursor1 = con1.execute(sql_db1)
        cols1 = [column[0] for column in cursor1.description]
        results_temp = pd.DataFrame.from_records(data=cursor1.fetchall(), columns=cols1)
        
    temporary_table = "temp_table"
    results_temp.to_sql(temporary_table, con1, index=False, if_exists='replace')

    #checks current entities with existing dataset
    sql_db2 = f"""
            SELECT a.entity AS primary_entity,
                a.name AS primary_name,
                a.reference AS primary_reference,
                a.organisation_entity AS primary_organisation_entity,
                a.geometry primary_geometry,
                b.entity AS secondary_entity,
                b.name AS secondary_name,
                b.reference AS secondary_reference,
                b.organisation_entity AS secondary_organisation_entity,
                b.geometry AS secondary_geometry,
                100 *(ST_Area(ST_Intersection(GeomFromText(a.geometry), GeomFromText(b.geometry)))/ MIN(ST_Area(GeomFromText(a.geometry)), ST_Area(GeomFromText(b.geometry)))) AS pct_overlap
            FROM
                (SELECT entity,name,organisation_entity,reference,geometry
                 FROM entity
                 WHERE ST_IsValid(GeomFromText(geometry))
                ) a
            JOIN
                {temporary_table} b 
            ON a.organisation_entity <> b.organisation_entity
            AND ST_Intersects(GeomFromText(a.geometry), GeomFromText(b.geometry))
            WHERE 100 *(ST_Area(ST_Intersection(GeomFromText(a.geometry), GeomFromText(b.geometry)))/ MIN(ST_Area(GeomFromText(a.geometry)), ST_Area(GeomFromText(b.geometry)))) > 95;    
            """
    
    with spatialite.connect(dataset_path) as con:
        results_temp.to_sql(temporary_table, con, index=False, if_exists='replace')
        cursor = con.execute(sql_db2)
        cols = [column[0] for column in cursor.description]
        results = pd.DataFrame.from_records(data=cursor.fetchall(), columns=cols)

    return results
    