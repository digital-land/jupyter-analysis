
import pandas as pd
import geopandas as gpd
import numpy as np
from functions_core import *

def make_freshness_input_table(base_table, age_days = 365):

    # get old resources from base table
    df = base_table[base_table["resource_age_days"] > age_days][["collection", "pipeline", "organisation", "organisation_name"]]

    # add in extra fields
    df["issue_type"] = "not_fresh"
    df["quality_criteria"] = "1 - endpoint updated in last year"
    df["quality_level"] = 1

    return df


def make_issues_input_table(base_table, issues_lookup):

    # join on quality key and restrict fields
    df = base_table.merge(
        issues_lookup[["issue_type", "quality_criteria", "quality_level"]],
        how = "left",
        on = "issue_type"
    )[["LPACD", "collection", "pipeline", "organisation", "organisation_name", "issue_type", "quality_criteria", "quality_level"]]

    return df


def make_ca_provenance_issues_table(lpa_gdf, ca_gdf):

    # spatial join
    lpa_ca_join = gpd.sjoin(
        lpa_gdf[["LPACD", "organisation", "organisation_name", "geometry"]],
        ca_gdf[["entity", "organisation_entity", "lpa_flag", "point"]],
        how = "inner",
        predicate = "intersects"
    )

    # take max of ca LPA flag for each LPA
    lpa_prov = lpa_ca_join.groupby(["LPACD", "organisation", "organisation_name"], as_index=False).agg(
        prov_rank_max = ("lpa_flag", "max")
    )

    # only LPAs with CA data not from an LPA
    lpa_non_auth = lpa_prov[lpa_prov["prov_rank_max"] == 0].copy()

    # add in extra fields for output
    lpa_non_auth[["collection", "pipeline"]] = "conservation-area"
    lpa_non_auth["issue_type"] = "non_auth"
    lpa_non_auth["quality_criteria"] = "1 - authoritative data from the LPA"
    lpa_non_auth["quality_level"] = 1

    return lpa_non_auth[["LPACD", "collection", "pipeline", "organisation", "organisation_name", "issue_type", "quality_criteria", "quality_level"]]


def make_ca_count_match_issues_table(base_table):

    q = """
    SELECT distinct organisation
    FROM expectation
    WHERE 1=1
        AND name = 'Check number of conservation-area entities inside the local planning authority boundary matches the manual count' 
        AND passed = 'False'
    """

    expectation_results = datasette_query("digital-land", q)

    df = base_table.merge(
        expectation_results,
        how = "inner",
        on = "organisation"
    )[["LPACD", "organisation", "organisation_name"]]

    df["collection"] = "conservation-area"
    df["pipeline"] = "conservation-area"
    df["quality_criteria"] = "3 - entity count matches LPA"
    df["quality_level"] = 3

    return df


def make_lpa_boundary_issues_table(base_table):

    q = """
    SELECT distinct organisation, dataset as pipeline
    FROM expectation
    WHERE 1=1
        AND name like '%outside%' 
        AND message not like '%error%'
        AND passed = 'False'
    """

    bounds_results = datasette_query("digital-land", q)

    df = base_table.merge(
        bounds_results,
        how = "inner",
        on = "organisation"
    )[["LPACD", "organisation", "organisation_name", "pipeline"]]

    df["quality_criteria"] = "3 - entities within LPA boundary"
    df["quality_level"] = 3

    return df


def make_score_summary_table(quality_input_df, level_map):

    df = quality_input_df.groupby([
        "LPACD", "pipeline", "organisation", "organisation_name"
    ],
        as_index=False,
        dropna=False
    ).agg(
        quality_level = ("quality_level", "min")
    )

    df["quality_level"] = df["quality_level"].replace(np.nan, 4)

    if (all(level in level_map for level in df["quality_level"].unique())):

        df["quality_level_label"] = df["quality_level"].map(level_map)

    else:
        print("values in input df `quality_level` field do not match keys in level_map dict")
        raise(Warning)

    return df
