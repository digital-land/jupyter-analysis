from functions_core import *

def get_issue_quality_lookup():

    q = """
    SELECT 
        description,
        issue_type,
        name,
        severity,
        responsibility,
        quality_criteria_level || " - " || quality_criteria as quality_criteria,
        quality_criteria_level as quality_level
    FROM issue_type
    """

    df = datasette_query("digital-land", q)

    return df


def get_odp_provision_lookup():
    
    q = f"""
    SELECT *
    FROM provision
    WHERE project = "open-digital-planning"
    """

    # get organisation, pipeline and cohort flag from performance table
    df = datasette_query("digital-land", q)

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

def get_organisation_lookup():

    q = """
        select entity as organisation_entity, name as organisation_name, organisation, dataset as org_type, end_date, 
        local_planning_authority as LPACD, local_authority_district,
        case when local_planning_authority != "" or organisation in ("local-authority:NDO", "local-authority:PUR") then 1 else 0 end as lpa_flag
        from organisation
        where name != "Waveney District Council"
        """

    df = datasette_query("digital-land", q)

    return df

