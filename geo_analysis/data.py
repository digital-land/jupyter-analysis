import spatialite
import pandas as pd

def get_entity_dataset(dataset_path):

    sql = """
    SELECT 
        entity,
        name,
        organisation_entity,
        reference,
        entry_date,
        start_date,
        geometry
        -- CASE WHEN organisation_entity = 16 THEN 'Historic England' ELSE 'LPA' END AS organisation_type
        FROM entity;

""" 
    with spatialite.connect(dataset_path) as con:
        cursor = con.execute(sql)
        cols = [column[0] for column in cursor.description]
        results = pd.DataFrame.from_records(data=cursor.fetchall(), columns=cols)
    
    return results



def nrow(df):
    return print(f"No. of records in entity_df: {len(df):,}")