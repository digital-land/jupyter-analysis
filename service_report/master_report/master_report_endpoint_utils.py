import urllib
import pandas as pd

datasette_url = "https://datasette.planning.data.gov.uk/"


def get_endpoints(organisation):
    if organisation:
        query = f" s.organisation = '{organisation}'"
    else:
        query = f" s.organisation LIKE '%'"
    params = urllib.parse.urlencode({
        "sql": f"""
        select
          e.endpoint_url,
          l.status,
          l.exception,
          s.collection,
          group_concat(DISTINCT sp.pipeline) as pipelines,
          s.organisation,
          o.name,
          ro.resource,
          max(l.entry_date) maxentrydate,
          max(e.entry_date) entrydate,
          e.end_date
        from
          log l
          inner join source s on l.endpoint = s.endpoint
          inner join resource_endpoint ro on l.endpoint = ro.endpoint
          inner join organisation o on s.organisation=o.organisation
          inner join endpoint e on l.endpoint = e.endpoint
          inner join source_pipeline sp on s.source = sp.source
        where
           {query} and not collection="brownfield-land"
        group by
          l.endpoint,
          l.status
        order by
          l.endpoint,
          s.collection,
          maxentrydate desc
        """,
        "_size": "max"
    })
    
    url = f"{datasette_url}digital-land.csv?{params}"
    endpoints_df = pd.read_csv(url)
    return endpoints_df

def get_latest_endpoints(organisation):
    all_endpoints=get_endpoints(organisation)
    new_df=all_endpoints.copy()
    new_df['maxentrydate'] = pd.to_datetime(new_df['maxentrydate'])
    new_df['entrydate'] = pd.to_datetime(new_df['entrydate'])
    new_df['last_status'] = None
    new_df['last_updated_date'] = None
    new_df['date_last_status_200'] = None
    
    for index, row in new_df.iterrows():
        if index < len(new_df) - 1 and (row['status']!=200 or pd.isna(row['status'])):
            if row['endpoint_url'] == new_df.at[index + 1, 'endpoint_url']:
                new_df.at[index, 'last_status'] = new_df.at[index + 1, 'status']
                new_df.at[index, 'last_updated_date'] = new_df.at[index + 1, 'maxentrydate']   
    
    new_df.drop_duplicates(subset='endpoint_url', keep='first', inplace=True)
    new_df.reset_index(drop=True, inplace=True)
    for index, row in new_df.iterrows():
        if row['last_status'] is not None:
            if row['last_status'] != 200  or row['last_status'] is None:
                filtered_df = all_endpoints[(all_endpoints['endpoint_url'] == row['endpoint_url'] ) & (all_endpoints['status'] == 200)]
                if not filtered_df.empty:
                    new_df.at[index, 'date_last_status_200'] = filtered_df['maxentrydate'].values[0][:19]

    latest_endpoints = new_df.sort_values('entrydate').drop_duplicates(subset='pipelines', keep='last')
    return latest_endpoints

def get_issue_types_with_severity_info():
    params = urllib.parse.urlencode({
    "sql": f"""
    select issue_type, severity
    from issue_type
    """,
    "_size": "max"
    })

    url = f"{datasette_url}digital-land.csv?{params}"
    df = pd.read_csv(url)
    info_issues_df = df[df['severity'] == 'info']
    info_issues = info_issues_df['issue_type'].tolist()
    return info_issues


def get_issues_for_resource(resource, dataset):
    params = urllib.parse.urlencode({
        "sql": f"""
        select issue_type from issue
        where resource = '{resource}'
        """,
        "_size": "max"
    })
    url = f"{datasette_url}{dataset}.csv?{params}"
    issues_df = pd.read_csv(url)
    return issues_df

def produce_output_csv(all_orgs_recent_endpoints, organisation_dataset_property_dict, property_name, ignore_property_value, output_columns):
    rows_list = []
    for organisation, dataset_property in organisation_dataset_property_dict.items():
        for dataset, property in dataset_property.items():
            if property != ignore_property_value:
                row = all_orgs_recent_endpoints[organisation][all_orgs_recent_endpoints[organisation]['pipelines'].str.contains(dataset)]
                row = row[output_columns]
                row['pipelines']=dataset
                row.insert(2, property_name, property)
                row=row.drop_duplicates(subset='pipelines')
                rows_list.append(row)
    output_df = pd.concat(rows_list)
    output_df.reset_index(drop=True, inplace=True)
    return output_df

def handle_skip_dataset(same_datasets_endpoints_df, dataset, row):
    skip_dataset = False
    for index, same_dataset_row in same_datasets_endpoints_df.iterrows():
        if row["entrydate"] < same_dataset_row["entrydate"]:
            skip_dataset = True
    return skip_dataset