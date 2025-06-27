import pandas as pd
import ast
import argparse
import os

def parse_args():
    """
    Parses command-line arguments for specifying the output directory.

    Returns:
        argparse.Namespace: Contains the output directory path.
    """
    parser = argparse.ArgumentParser(description="Duplicate geometry checker")
    parser.add_argument(
        "--output-dir",
        type=str,
        required=True,
        help="Directory to save exported CSVs"
    )
    return parser.parse_args()

def parse_details(val):
    """
    Safely parses a stringified dictionary from the 'details' column using `ast.literal_eval`.

    Parameters:
        val (str): A string containing a dictionary-like structure.

    Returns:
        dict: Parsed dictionary, or empty dict if parsing fails.
    """
    try:
        return ast.literal_eval(val)
    except Exception:
        return {}

def extract_stats(details_dict):
    """
    Extracts summary statistics from a parsed 'details' dictionary.

    Parameters:
        details_dict (dict): Parsed dictionary from the 'details' column.

    Returns:
        pd.Series: Contains actual/expected values and match counts.
    """
    return pd.Series({
        "actual": details_dict.get("actual"),
        "expected": details_dict.get("expected"),
        "complete_match_count": len(details_dict.get("complete_matches", [])) if isinstance(details_dict.get("complete_matches"), list) else 0,
        "single_match_count": len(details_dict.get("single_matches", [])) if isinstance(details_dict.get("single_matches"), list) else 0,
        "complete_matches": details_dict.get("complete_matches", []),
        "single_matches": details_dict.get("single_matches", [])
    })

def main(output_dir):
    """
    Main function for processing duplicate geometry checks.

    - Downloads expectations with 'duplicate_geometry_check' operation.
    - Parses match results and enriches them with entity and organisation metadata.
    - Outputs both detailed and summary CSVs to the specified output directory.
    """

    # Load expectation records where operation is 'duplicate_geometry_check'
    url = "https://datasette.planning.data.gov.uk/digital-land/expectation.csv?_stream=on"
    df = pd.read_csv(url)
    df = df[df["operation"] == "duplicate_geometry_check"].copy()

    # Parse the 'details' field into dictionaries
    df["details_parsed"] = df["details"].apply(parse_details)

    # Extract match records (complete and single) from details
    records = []
    for _, row in df.iterrows():
        dataset = row["dataset"]
        operation = row["operation"]
        details = row["details_parsed"]

        for match in details.get("complete_matches", []):
            records.append({
                "dataset": dataset,
                "operation": operation,
                "message": "complete_match",
                "entity_a": match.get("entity_a"),
                "organisation_entity_a": match.get("organisation_entity_a"),
                "entity_b": match.get("entity_b"),
                "organisation_entity_b": match.get("organisation_entity_b"),
            })

        for match in details.get("single_matches", []):
            records.append({
                "dataset": dataset,
                "operation": operation,
                "message": "single_match",
                "entity_a": match.get("entity_a"),
                "organisation_entity_a": match.get("organisation_entity_a"),
                "entity_b": match.get("entity_b"),
                "organisation_entity_b": match.get("organisation_entity_b"),
            })

    df_matches = pd.DataFrame(records)

    # URLs for entity tables by dataset
    url_map = {
        "conservation-area": "https://datasette.planning.data.gov.uk/conservation-area/entity.csv?_stream=on",
        "article-4-direction-area": "https://datasette.planning.data.gov.uk/article-4-direction-area/entity.csv?_stream=on",
        "listed-building-outline": "https://datasette.planning.data.gov.uk/listed-building-outline/entity.csv?_stream=on",
        "tree-preservation-zone": "https://datasette.planning.data.gov.uk/tree-preservation-zone/entity.csv?_stream=on",
        "tree": "https://datasette.planning.data.gov.uk/tree/entity.csv?_stream=on",
    }

    # Columns to retain from entity tables
    columns_to_keep = ["entity", "dataset", "end_date", "entry_date", "geometry", "name", "organisation_entity"]
    entity_tables = {}

    # Download and store each dataset's entity table
    for dataset_name, entity_url in url_map.items():
        df_entity = pd.read_csv(entity_url)
        df_entity["dataset"] = dataset_name
        entity_tables[dataset_name] = df_entity[columns_to_keep].copy()

    # Combine all entity tables into one DataFrame
    df_entities = pd.concat(entity_tables.values(), ignore_index=True)

    # Merge metadata for entity_a
    df_matches = df_matches.merge(
        df_entities,
        how="left",
        left_on=["dataset", "entity_a"],
        right_on=["dataset", "entity"]
    ).rename(columns={
        "end_date": "entity_a_end_date",
        "entry_date": "entity_a_entry_date",
        "geometry": "entity_a_geometry",
        "name": "entity_a_name",
        "organisation_entity": "entity_a_organisation"
    }).drop(columns=["entity"])

    # Merge metadata for entity_b
    df_matches = df_matches.merge(
        df_entities,
        how="left",
        left_on=["dataset", "entity_b"],
        right_on=["dataset", "entity"]
    ).rename(columns={
        "end_date": "entity_b_end_date",
        "entry_date": "entity_b_entry_date",
        "geometry": "entity_b_geometry",
        "name": "entity_b_name",
        "organisation_entity": "entity_b_organisation"
    }).drop(columns=["entity"])

    # Reorder final column layout
    ordered_cols = [
        "dataset", "operation", "message",
        "entity_a", "entity_a_name", "entity_a_organisation", "entity_a_entry_date", "entity_a_end_date", "entity_a_geometry",
        "entity_b", "entity_b_name", "entity_b_organisation", "entity_b_entry_date", "entity_b_end_date", "entity_b_geometry"
    ]
    df_matches = df_matches[ordered_cols]

    # Load organisation lookup table
    org_url = "https://datasette.planning.data.gov.uk/digital-land/organisation.csv?_stream=on"
    df_org = pd.read_csv(org_url)[["entity", "name"]].rename(columns={
        "entity": "organisation_entity",
        "name": "organisation_name"
    })

    # Add readable name for entity_a_organisation
    df_matches = df_matches.merge(
        df_org,
        how="left",
        left_on="entity_a_organisation",
        right_on="organisation_entity"
    ).rename(columns={"organisation_name": "entity_a_organisation_name"}).drop(columns=["organisation_entity"])

    # Insert entity_a_organisation_name after entity_a_organisation
    a_cols = df_matches.columns.tolist()
    a_index = a_cols.index("entity_a_organisation") + 1
    a_cols.insert(a_index, a_cols.pop(a_cols.index("entity_a_organisation_name")))
    df_matches = df_matches[a_cols]

    # Add readable name for entity_b_organisation
    df_matches = df_matches.merge(
        df_org,
        how="left",
        left_on="entity_b_organisation",
        right_on="organisation_entity"
    ).rename(columns={"organisation_name": "entity_b_organisation_name"}).drop(columns=["organisation_entity"])

    # Insert entity_b_organisation_name after entity_b_organisation
    b_cols = df_matches.columns.tolist()
    b_index = b_cols.index("entity_b_organisation") + 1
    b_cols.insert(b_index, b_cols.pop(b_cols.index("entity_b_organisation_name")))
    df_matches = df_matches[b_cols]

    # Save detailed match output
    os.makedirs(output_dir, exist_ok=True)
    matches_csv = os.path.join(output_dir, "duplicate_entity_expectation.csv")
    df_matches.to_csv(matches_csv, index=False)

    # Re-parse stats and generate summary view
    df["details_parsed"] = df["details"].apply(parse_details)
    stats_df = pd.concat([df[["dataset", "severity"]], df["details_parsed"].apply(extract_stats)], axis=1)
    stats_df = stats_df.sort_values(by="complete_match_count", ascending=False).reset_index(drop=True)

    # Save summary CSV
    summary_csv = os.path.join(output_dir, "duplicate_entity_expectation_summary.csv")
    stats_df.drop(columns=["complete_matches", "single_matches"]).to_csv(summary_csv, index=False)

# Entry point
if __name__ == "__main__":
    args = parse_args()
    main(args.output_dir)
