import spatialite
import pandas as pd

def get_organisation_summary(dataset_path):

    sql = """
        SELECT organisation_entity,COUNT(entity) as entity_count
        FROM entity
        GROUP BY organisation_entity
        ORDER BY organisation_entity ASC;
    
    """ 
    with spatialite.connect(dataset_path) as con:
        cursor = con.execute(sql)
        cols = [column[0] for column in cursor.description]
        results = pd.DataFrame.from_records(data=cursor.fetchall(), columns=cols)
    
    return results

def get_duplicates_between_orgs(dataset_path,org1,org2):    
    sql = f"""
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
                (SELECT entity,
                        name,
                        organisation_entity,
                        reference,
                        geometry
                FROM entity
                WHERE ST_IsValid(GeomFromText(geometry))
                AND organisation_entity = '{org1}') a
            JOIN
                (SELECT entity,
                        name,
                        organisation_entity,
                        reference,
                        geometry
                FROM entity
                WHERE ST_IsValid(GeomFromText(geometry))
                AND organisation_entity = '{org2}') b 
            ON a.organisation_entity <> b.organisation_entity
            AND ST_Intersects(GeomFromText(a.geometry), GeomFromText(b.geometry))
            WHERE 100 *(ST_Area(ST_Intersection(GeomFromText(a.geometry), GeomFromText(b.geometry)))/ MIN(ST_Area(GeomFromText(a.geometry)), ST_Area(GeomFromText(b.geometry)))) > 95;
        """
    with spatialite.connect(dataset_path) as con:
        cursor = con.execute(sql)
        cols = [column[0] for column in cursor.description]
        results = pd.DataFrame.from_records(data=cursor.fetchall(), columns=cols)
    
    return results

