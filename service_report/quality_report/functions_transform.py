
import pandas as pd
import numpy as np

def make_freshness_input_table(base_table, age_days = 365):

    # get old resources from base table
    df = base_table[base_table["resource_age_days"] > age_days][["collection", "pipeline", "organisation", "organisation_name"]]

    # add in extra fields
    df["issue_type"] = "not_fresh"
    df["quality_category"] = "1 - endpoint updated in last year"
    df["quality_level"] = 1

    return df


def make_issues_input_table(base_table, issues_lookup):

    # join on quality key and restrict fields
    df = base_table.merge(
        issues_lookup[["issue_type", "quality_category", "quality_level"]],
        how = "left",
        on = "issue_type"
    )[["collection", "pipeline", "organisation", "organisation_name", "issue_type", "quality_category", "quality_level"]]

    return df


def make_score_summary_table(quality_input_df, level_map):

    df = quality_input_df.groupby([
        "collection", "pipeline", "organisation", "organisation_name"
    ],
        as_index=False,
        dropna=False
    ).agg(
        quality_level = ("quality_level", "min")
    )

    df.replace(np.nan, 4, inplace=True)

    if (all(level in level_map for level in df["quality_level"].unique())):

        df["quality_level_label"] = df["quality_level"].map(level_map)

    else:
        print("values in input df `quality_level` field do not match keys in level_map dict")
        raise(Warning)

    return df