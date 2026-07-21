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
    df = datasette_query_paginated("digital-land", q)

    return df


def get_endpoint_res_issues():

    # get table of active endpoints and resources, with issue summaries per resource joined on
    # queried over HTTP (not the downloaded .db) since direct .db downloads are now blocked (403)
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

    df = datasette_query_paginated("performance", q)

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


def get_provision_rule_lookup():
    # defines, per dataset, whether it's ODP-scoped (project) and/or "mandated"
    # (provision_reason) - used to work out which datasets are mandated
    q = """
    SELECT dataset, project, provision_reason, role
    FROM provision_rule
    """

    df = datasette_query("digital-land", q)

    return df


def get_quality_priority_lookup():
    # the platform's own quality ladder (none < some < indicative < authoritative < usable < trustworthy)
    q = """
    SELECT quality, priority
    FROM quality
    """

    df = datasette_query("digital-land", q)

    return dict(zip(df["quality"], df["priority"]))


def get_entity_quality_lookup(pipeline):
    # per-entity authoritative-source signal, computed by the platform itself against each
    # dataset's own database. Not every dataset has an entity/quality/organisation_entity
    # column (e.g. pure reference/enum datasets), so failures are swallowed and return empty.
    q = """
    SELECT organisation_entity, quality, COUNT(*) as n
    FROM entity
    WHERE organisation_entity IS NOT NULL AND organisation_entity != ""
    GROUP BY organisation_entity, quality
    """

    try:
        df = datasette_query(pipeline, q)
    except Exception:
        return pd.DataFrame(columns=["organisation_entity", "quality", "n", "pipeline"])

    df["pipeline"] = pipeline
    return df

