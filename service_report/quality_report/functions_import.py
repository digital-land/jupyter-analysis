from functions_core import *

def get_provision_lookup(db_path):
    
    q = f"""
    SELECT 
        distinct organisation, pipeline, cohort
    FROM endpoint_dataset_resource_summary
    """

    # get organisation, pipeline and cohort flag from performance table
    df = query_sqlite(db_path, q)

    return df


def get_endpoint_res_issues(db_path):

    # get table of active endpoints and resources, with issue summaries per resource joined on
    q = f"""
        SELECT 
            rhe.organisation, rhe.name as organisation_name, 
            rhe.collection, rhe.pipeline, rhe.endpoint, rhe.resource, rhe.latest_status, rhe.endpoint_entry_date, rhe.resource_start_date, 
            CAST(JULIANDAY('now') - JULIANDAY(rhe.resource_start_date) AS int) as resource_age_days,
            its.issue_type, its.count_issues, its.severity, its.responsibility
        FROM reporting_historic_endpoints rhe
        LEFT JOIN endpoint_dataset_issue_type_summary its on rhe.resource = its.resource
        WHERE 1=1
            AND rhe.endpoint_end_date = ""
            AND rhe.resource_end_date = ""
            AND rhe.latest_status = 200
    """

    df = query_sqlite(db_path, q)

    return df