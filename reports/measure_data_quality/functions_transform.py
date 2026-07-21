
import pandas as pd
import numpy as np

def get_single_source_pipelines(pipelines, provision_rule_df):
    # single-source datasets are everything NOT in ODP scope and NOT "mandated" scope
    # (statutory, or encouraged-for-LPAs) - staleness only applies to these, since ODP and
    # mandated datasets already have their own quality signals to lean on
    odp_datasets = set(provision_rule_df.loc[provision_rule_df["project"] == "open-digital-planning", "dataset"])
    mandated_datasets = set(provision_rule_df.loc[
        (provision_rule_df["provision_reason"] == "statutory")
        | ((provision_rule_df["provision_reason"] == "encouraged") & (provision_rule_df["role"] == "local-planning-authority")),
        "dataset"
    ])

    return [p for p in pipelines if p not in odp_datasets and p not in mandated_datasets]


def make_issues_input_table(base_table, issues_lookup):

    # join on quality key and restrict fields
    df = base_table.merge(
        issues_lookup[["issue_type", "quality_criteria", "quality_level"]],
        how = "left",
        on = "issue_type"
    )[["collection", "pipeline", "organisation", "organisation_name", "issue_type", "quality_criteria", "quality_level"]]

    return df


def make_authoritative_lookup(entity_quality_raw, quality_priority_map, org_lookup):
    # flags whether a provision's data is confirmed to come from the authoritative source,
    # using the platform's own per-entity `quality` field (see each dataset's own entity
    # table) rather than an approximation - an organisation can be the "expected" provider
    # for a dataset yet still have its area covered by an alternative source's entities.
    # Aggregated leniently: if ANY entity attributed to an organisation is quality-tier
    # "authoritative" or better, the whole organisation+pipeline provision counts as authoritative.
    # Pipelines/organisations with no entity_quality_raw rows (e.g. reference/enum datasets
    # with no organisation_entity/quality columns) get no row here - treated as "not checked".

    df = entity_quality_raw.copy()
    df["priority"] = df["quality"].map(quality_priority_map)
    df = df.dropna(subset=["priority", "organisation_entity"])

    authoritative_priority = quality_priority_map["authoritative"]
    df["is_authoritative_entity"] = df["priority"] >= authoritative_priority

    summary = df.groupby(["pipeline", "organisation_entity"], as_index=False).agg(
        is_authoritative = ("is_authoritative_entity", "max")
    )
    summary["organisation_entity"] = summary["organisation_entity"].astype(int)

    summary = summary.merge(
        org_lookup[["organisation_entity", "organisation", "organisation_name"]],
        how = "left",
        on = "organisation_entity"
    )
    summary["authoritative_check_available"] = True

    return summary[["pipeline", "organisation", "organisation_name", "is_authoritative", "authoritative_check_available"]]


def make_score_summary_table(quality_input_df, auth_lookup, level_map):
    # quality_input_df holds severity-only issues - authoritative status is a separate axis, see auth_lookup

    # rung: 1 = some data (severe issues present), 2 = usable, 3 = trustworthy (no issues)
    df = quality_input_df.groupby([
        "collection", "pipeline", "organisation", "organisation_name"
    ],
        as_index=False,
        dropna=False
    ).agg(
        severity_level = ("quality_level", "min")
    )

    df["severity_level"] = df["severity_level"].replace(np.nan, 4)
    df["quality_rung"] = df["severity_level"] - 1

    # bring in authoritative status (organisation-level, from make_authoritative_lookup).
    # missing a match means "not checked", which is treated the same as non-authoritative
    # (not proven authoritative -> not elevated), but flagged separately so it's distinguishable
    # from a provision that was actually checked and found non-authoritative.
    df = df.merge(
        auth_lookup[["organisation", "pipeline", "is_authoritative", "authoritative_check_available"]],
        how = "left",
        on = ["organisation", "pipeline"]
    )
    df["is_authoritative"] = df["is_authoritative"].fillna(False).astype(bool)
    df["authoritative_check_available"] = df["authoritative_check_available"].fillna(False).astype(bool)

    df["quality_level"] = np.where(df["is_authoritative"], df["quality_rung"] + 3, df["quality_rung"]).astype(int)

    if (all(level in level_map for level in df["quality_level"].unique())):

        df["quality_level_label"] = df["quality_level"].map(level_map)

    else:
        print("values in input df `quality_level` field do not match keys in level_map dict")
        raise(Warning)

    return df.drop(columns=["severity_level", "quality_rung"])


def apply_zero_entity_override(qual_summary, entity_quality_raw, org_lookup, level_map):
    # An active endpoint that produced zero actual entities (e.g. every submitted row failed
    # processing) has nothing for the severity or authoritative axes to meaningfully score -
    # it should read as "no data", not as some rung/authoritative combination computed from
    # issue metadata alone. Only overrides pipelines whose entity table was actually queried
    # successfully (present in entity_quality_raw) - if the fetch failed for a pipeline
    # entirely, we don't know its entity counts, so we leave those scores as computed rather
    # than wrongly zeroing out every organisation in that pipeline.

    queryable_pipelines = entity_quality_raw["pipeline"].unique()

    has_entities = entity_quality_raw[["pipeline", "organisation_entity"]].drop_duplicates().copy()
    has_entities["organisation_entity"] = has_entities["organisation_entity"].astype(int)
    has_entities = has_entities.merge(
        org_lookup[["organisation_entity", "organisation"]],
        how = "left",
        on = "organisation_entity"
    )[["pipeline", "organisation"]].drop_duplicates()
    has_entities["has_entities"] = True

    df = qual_summary.merge(has_entities, how = "left", on = ["pipeline", "organisation"])
    df["has_entities"] = df["has_entities"].fillna(False)

    zero_entity_mask = df["pipeline"].isin(queryable_pipelines) & ~df["has_entities"]
    df.loc[zero_entity_mask, "quality_level"] = 0
    df.loc[zero_entity_mask, "quality_level_label"] = level_map[0]

    return df.drop(columns=["has_entities"])


def apply_staleness_cap(qual_summary, ep_res_issues, single_source_pipelines, level_map, age_days = 365):
    # staleness only applies to single-source datasets (not ODP-scoped, not mandated) - those
    # have no other provider to cross-check freshness against. It acts as another criterion
    # gating the top rung specifically: a provision can't be "trustworthy" (rung 3) if its
    # endpoint hasn't been refreshed in over a year, so a stale trustworthy provision is capped
    # down to "usable" (rung 2). It is NOT a general one-step downgrade - a provision already at
    # "usable" or "some data" (rung 1-2) is left alone, same as a provision already at 0 ("no data").

    stale = ep_res_issues[
        ep_res_issues["pipeline"].isin(single_source_pipelines)
        & (ep_res_issues["resource_age_days"] > age_days)
    ][["pipeline", "organisation"]].drop_duplicates()
    stale["is_stale"] = True

    df = qual_summary.merge(stale, how = "left", on = ["pipeline", "organisation"])
    df["is_stale"] = df["is_stale"].fillna(False)

    cap_mask = df["is_stale"] & (df["quality_level"] > 0)

    rung = np.where(df["is_authoritative"], df["quality_level"] - 3, df["quality_level"])
    capped_rung = np.minimum(rung, 2)
    capped_quality_level = np.where(df["is_authoritative"], capped_rung + 3, capped_rung)

    df.loc[cap_mask, "quality_level"] = capped_quality_level[cap_mask]
    df.loc[cap_mask, "quality_level_label"] = df.loc[cap_mask, "quality_level"].map(level_map)

    return df.drop(columns=["is_stale"])


def add_zero_entity_detail(detail_df, entity_quality_raw, org_lookup):
    # flags provisions with an active endpoint but zero entities in the dataset's entity
    # table - same check apply_zero_entity_override uses to force quality_level to 0, but
    # surfaced as its own column here so it's visible on the detail table directly, rather
    # than only being inferable from the score.

    queryable_pipelines = entity_quality_raw["pipeline"].unique()

    has_entities = entity_quality_raw[["pipeline", "organisation_entity"]].drop_duplicates().copy()
    has_entities["organisation_entity"] = has_entities["organisation_entity"].astype(int)
    has_entities = has_entities.merge(
        org_lookup[["organisation_entity", "organisation"]],
        how = "left",
        on = "organisation_entity"
    )[["pipeline", "organisation"]].drop_duplicates()
    has_entities["has_entities"] = True

    df = detail_df.merge(has_entities, how = "left", on = ["pipeline", "organisation"])
    df["has_zero_entities"] = df["pipeline"].isin(queryable_pipelines) & df["has_entities"].isna()

    return df.drop(columns = ["has_entities"])


def add_staleness_detail(detail_df, ep_res_issues, single_source_pipelines, age_days = 365):
    # surfaces the actual staleness signal (age in days, and whether it crossed the threshold)
    # on a detail table, so stale single-source provisions can be identified and followed up
    # on directly from the output rather than only being visible as a capped score.

    age = ep_res_issues.groupby(["pipeline", "organisation"], as_index=False).agg(
        resource_age_days = ("resource_age_days", "max")
    )

    df = detail_df.merge(age, how = "left", on = ["pipeline", "organisation"])
    df["is_stale"] = df["pipeline"].isin(single_source_pipelines) & (df["resource_age_days"] > age_days)

    return df
