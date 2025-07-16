import os
import urllib
import hashlib
from urllib.error import HTTPError
import csv
from pathlib import Path
import shutil
import sys
import logging

from functions_lookup import (
    save_resource_unidentified_lookups,
    standardise_lookups,
    add_unnassigned_to_lookups,
)
from functions_column import add_extra_column_mappings, add_extra_concats

from digital_land.register import hash_value, Item
from digital_land.update import add_endpoint, add_source
from digital_land.pipeline import Pipeline
from digital_land.specification import Specification
from digital_land.collection import Collection
from digital_land.collect import Collector
from digital_land.commands import pipeline_run
from digital_land.commands import dataset_create
from digital_land.package.dataset import DatasetPackage
from digital_land.organisation import Organisation


def create_source_csv(path):
    fieldnames = [
        "endpoint",
        "endpoint-url",
        "plugin",
        "parameters",
        "entry-date",
        "start-date",
        "end-date",
    ]

    with open(path, "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames)
        dictwriter.writeheader()


def create_endpoint_csv(path):
    fieldnames = [
        "source",
        "collection",
        "pipelines",
        "organisation",
        "endpoint",
        "documentation-url",
        "licence",
        "attribution",
        "entry-date",
        "start-date",
        "end-date",
    ]
    with open(path, "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames)
        dictwriter.writeheader()


def del_dir_contents(dir):
    for filename in os.listdir(dir):
        file_path = os.path.join(dir, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print("Failed to delete %s. Reason: %s" % (file_path, e))


def get_workflow_data(data_dir, collection, dataset):
    # ensure data_dir exists
    os.makedirs(data_dir, exist_ok=True)
    # download specification
    specification_dir = os.path.join(data_dir, "specification")
    os.makedirs(specification_dir, exist_ok=True)
    source_url = "https://raw.githubusercontent.com/digital-land/"
    specification_csvs = [
        "attribution.csv",
        "licence.csv",
        "typology.csv",
        "theme.csv",
        "collection.csv",
        "dataset.csv",
        "dataset-field.csv",
        "field.csv",
        "datatype.csv",
        "prefix.csv",
        # deprecated ..
        "pipeline.csv",
        "provision-rule.csv",
        "dataset-schema.csv",
        "schema.csv",
        "schema-field.csv",
    ]
    for specification_csv in specification_csvs:
        urllib.request.urlretrieve(
            f"{source_url}/specification/main/specification/{specification_csv}",
            os.path.join(specification_dir, specification_csv),
        )

    # collection directory structure
    collection_dir = os.path.join(data_dir, "collection")
    collection_log_dir = os.path.join(collection_dir, "log")
    collection_resource_dir = os.path.join(collection_dir, "resource")
    os.makedirs(collection_dir, exist_ok=True)
    os.makedirs(collection_log_dir, exist_ok=True)
    del_dir_contents(collection_log_dir)
    os.makedirs(collection_resource_dir, exist_ok=True)
    del_dir_contents(collection_resource_dir)
    # create source and endpoint csvs
    create_source_csv(os.path.join(collection_dir, "source.csv"))
    create_endpoint_csv(os.path.join(collection_dir, "endpoint.csv"))

    # pipeline directory structure & download
    pipeline_dir = os.path.join(data_dir, "pipeline")
    os.makedirs(pipeline_dir, exist_ok=True)
    pipeline_csvs = [
        "column.csv",
        "concat.csv",
        "convert.csv",
        "default.csv",
        "filter.csv",
        "lookup.csv",
        "patch.csv",
        "skip.csv",
        "transform.csv",
        "combine.csv",
    ]
    for pipeline_csv in pipeline_csvs:
        try:
            urllib.request.urlretrieve(
                f"{source_url}/{collection}/main/pipeline/{pipeline_csv}",
                os.path.join(pipeline_dir, pipeline_csv),
            )
        except HTTPError as e:
            collection_affix = collection.replace("-collection", "")
            try:
                urllib.request.urlretrieve(
                    f"{source_url}/config/main/pipeline/{collection_affix}/{pipeline_csv}",
                    os.path.join(pipeline_dir, pipeline_csv),
                )
            except HTTPError as err:
                print(f"Decentralised Collection Error: +{e}")
                print(f"Centralised (Config) Repository Error: +{err}")

    # add transformedd, dataset and issue directories
    dataset_dir = os.path.join(data_dir, "dataset")
    os.makedirs(dataset_dir, exist_ok=True)
    del_dir_contents(dataset_dir)
    transformed_dir = os.path.join(data_dir, "transformed")
    os.makedirs(transformed_dir, exist_ok=True)
    del_dir_contents(transformed_dir)
    os.makedirs(os.path.join(transformed_dir, dataset))
    issue_dir = os.path.join(data_dir, "issue")
    os.makedirs(issue_dir, exist_ok=True)

    # addd var directory wiith dataset_resource
    var_dir = os.path.join(data_dir, "var")
    os.makedirs(var_dir, exist_ok=True)
    dataset_resource_dir = os.path.join(var_dir, "dataset-resource")
    column_field_dir = os.path.join(var_dir, "column-field")

    # add cache and download organisation.csv
    cache_dir = os.path.join(var_dir, "cache")
    os.makedirs(cache_dir, exist_ok=True)
    urllib.request.urlretrieve(
        "https://files.planning.data.gov.uk/organisation-collection/dataset/organisation.csv",
        os.path.join(cache_dir, "organisation.csv"),
    )


def add_source_and_endpoint(
    collection_name,
    dataset,
    organisation,
    endpoint_url,
    plugin,
    collection_dir,
    documentation_url,
):
    """
    A function to add a source and an endpoint to a collection. Had to recreate function because
    of limitations with the current functions and the location of the collection directory
    """
    # create hashes
    endpoint_hash = hash_value(endpoint_url)
    source_hash_key = "%s|%s|%s" % (
        collection_name,
        organisation,
        endpoint_hash,
    )
    source_hash = hashlib.md5(source_hash_key.encode()).hexdigest()

    # take limited information (more if provided) and form the endpoint dictionary and the source dictionary
    source = {
        "source": source_hash,
        "collection": collection_name,
        "pipelines": dataset,
        "endpoint": endpoint_hash,
        "organisation": organisation,
    }

    if documentation_url:
        source["documentation-url"] = documentation_url

    endpoint = {
        "endpoint": endpoint_hash,
        "endpoint-url": endpoint_url,
    }
    if plugin:
        endpoint["plugin"] = plugin

    # create Collection object
    print(collection_dir)
    collection = Collection(directory=collection_dir)
    collection.load(collection_dir)

    add_endpoint(endpoint, collection.endpoint)
    add_source(source, collection.source)

    # finally save the new collection
    collection.save_csv(collection_dir)


def assign_entries(
    resource_path, dataset, organisation, pipeline_dir, specification_dir
):
    """
    assuming that the endpoint is new (strictly it doesn't have to be) then we neeed to assign new eentity numbers
    """
    lookup_path = os.path.join(pipeline_dir, "lookup.csv")
    save_resource_unidentified_lookups(
        resource_path,
        dataset,
        [organisation],
        pipeline_dir=pipeline_dir,
        specification_dir=specification_dir,
    )
    unassigned_entries = []
    with open("../../data/endpoint_checker/var/cache/unassigned-entries.csv") as f:
        dictreader = csv.DictReader(f)
        for row in dictreader:
            unassigned_entries.append(row)
    standardise_lookups(lookup_path)
    # if unassigned_entries is not None
    if len(unassigned_entries) > 0:
        add_unnassigned_to_lookups(unassigned_entries, lookup_path)


def new_dataset_create(
    input_paths,
    output_path,
    organisation_path,
    pipeline,
    dataset,
    specification_dir,
    issue_dir,
    column_field_dir,
    dataset_resource_dir,
):
    if not output_path:
        print("missing output path", file=sys.stderr)
        sys.exit(2)

    organisation = Organisation(organisation_path, Path(pipeline.path))
    package = DatasetPackage(
        dataset,
        organisation=organisation,
        path=output_path,
        specification_dir=specification_dir,  # TBD: package should use this specification object
    )
    package.create()
    for path in input_paths:
        package.load_transformed(path)
        path_obj = Path(path)
        package.load_column_fields(Path(column_field_dir) / path_obj.name)
        package.load_dataset_resource(Path(dataset_resource_dir) / path_obj.name)

    package.load_entities()

    old_entity_path = os.path.join(pipeline.path, "old-entity.csv")
    if os.path.exists(old_entity_path):
        package.load_old_entities(old_entity_path)

    issue_paths = os.path.join(issue_dir, dataset)
    if os.path.exists(issue_paths):
        for issue_path in os.listdir(issue_paths):
            package.load_issues(os.path.join(issue_paths, issue_path))
    else:
        logging.warning("No directory for this dataset in the provided issue_directory")

    package.add_counts()


def run_endpoint_workflow(
    collection_name,
    dataset,
    organisation,
    endpoint_url,
    plugin,
    data_dir,
    additional_col_mappings,
    additional_concats,
):
    # create the relevant structure and download the files
    get_workflow_data(data_dir, collection_name, dataset)
    collection_dir = os.path.join(data_dir, "collection")
    # create source.csv & endpoint.csv for provided endpoints using the standard command, not sure why this has one entry?
    add_source_and_endpoint(
        collection_name,
        dataset,
        organisation,
        endpoint_url,
        plugin,
        collection_dir,
        "testing123",
    )
    # run collector
    collector = Collector(dataset, collection_dir=Path(collection_dir))
    collector.collect(os.path.join(collection_dir, "endpoint.csv"))

    # collection step remove log and resource csvs so new ones are created after adding the source and endpoint
    try:
        os.remove(Path(collection_dir) / "log.csv")
        os.remove(Path(collection_dir) / "resource.csv")
    except OSError:
        pass
    collection = Collection(name=None, directory=collection_dir)
    collection.load(directory=collection_dir)
    collection.save_csv(directory=collection_dir)
    resources = collection.resource.entries
    print(resources)

    # stop if no resources collected
    if len(resources) == 0:
        print("No resources collected view collection logs for more info")
        return

    # retrieve unnassigned entities and assign
    for resource in resources:
        resource_path = os.path.join(collection_dir, "resource", resource["resource"])
        assign_entries(
            resource_path=resource_path,
            dataset=dataset,
            organisation=organisation,
            pipeline_dir=os.path.join(data_dir, "pipeline"),
            specification_dir=os.path.join(data_dir, "specification"),
        )

    # also add in any additional column mappings, it's useful for testing
    if additional_col_mappings is not None:
        column_path = os.path.join(data_dir, "pipeline", "column.csv")
        add_extra_column_mappings(column_path, additional_col_mappings)

    if additional_concats is not None:
        concat_path = os.path.join(data_dir, "pipeline", "concat.csv")
        add_extra_concats(concat_path, additional_concats)

    # define variables for Pipeline
    pipeline_dir = os.path.join(data_dir, "pipeline")
    pipeline = Pipeline(pipeline_dir, dataset)
    specification_dir = os.path.join(data_dir, "specification")
    specification = Specification(specification_dir)

    for resource in resources:
        # create issue directory
        os.makedirs(os.path.join(data_dir, "issue", dataset), exist_ok=True)
        os.makedirs(
            os.path.join(data_dir, "var", "column-field", dataset), exist_ok=True
        )
        os.makedirs(
            os.path.join(data_dir, "var", "dataset-resource", dataset), exist_ok=True
        )

        pipeline_run(
            dataset=dataset,  # I think dataset and pipeline here are identical maybe remove one of them
            pipeline=pipeline,
            specification=specification,  # isthis a directory
            input_path=os.path.join(
                data_dir, "collection", "resource", resource["resource"]
            ),
            output_path=os.path.join(
                data_dir, "transformed", dataset, f'{resource["resource"]}.csv'
            ),
            collection_dir=os.path.join(data_dir, "collection"),
            issue_dir=os.path.join(data_dir, "issue", dataset),
            column_field_dir=os.path.join(data_dir, "var", "column-field", dataset),
            dataset_resource_dir=os.path.join(
                data_dir, "var", "dataset-resource", dataset
            ),
            converted_resource_dir=os.path.join(data_dir, "collection", "resource"),
            organisation_path=os.path.join(
                data_dir, "var", "cache", "organisation.csv"
            ),
            save_harmonised=False,
            endpoints=[resource["endpoints"]],
            organisations=[organisation],
            entry_date=resource["start-date"],
            output_log_dir=os.path.join(data_dir, "log/")
        )

    # build dataset
    dataset_input_paths = [
        os.path.join(data_dir, "transformed", dataset, f'{resource["resource"]}.csv')
        for resource in resources
    ]
    new_dataset_create(
        input_paths=dataset_input_paths,
        output_path=os.path.join(data_dir, "dataset", f"{dataset}.sqlite3"),
        organisation_path=os.path.join(data_dir, "var", "cache", "organisation.csv"),
        pipeline=pipeline,
        dataset=dataset,
        specification_dir=specification_dir,
        issue_dir=os.path.join(data_dir, "issue"),
        column_field_dir=os.path.join(data_dir, "var", "column-field", dataset),
        dataset_resource_dir=os.path.join(data_dir, "var", "dataset-resource", dataset),
    )


def missing_columns(results, dataset, expected_columns):
    mapped_columns = set(results["field"])
    expected_columns_for_dataset = expected_columns.get(dataset, [])

    # Find the difference between the expected columns and the actual columns
    missing_columns = set(expected_columns_for_dataset) - set(mapped_columns)

    # Print the results
    if missing_columns:
        return print(
            f"Missing columns for dataset '{dataset}': {', '.join(missing_columns)}.\n"
        )
    else:
        return print(f"All expected columns for dataset '{dataset}' are present.")

    def get_endpoint_health(endpoint, dataset, data_dir):
        """
        This function establishes the health of an endpoint. it assumes the pipeline has been ran
        hence uses tables form the database and the log.csv. This is rudimentary for now but considers the following
        - checks log.csv to ensure a status code 200 is attained
        - checks the column mappings against the specification to ensure that the required columns were mapped
        - checks issue logs for issues that I unofficialy deem relevant
        - checks that some data made it into the database
        """
