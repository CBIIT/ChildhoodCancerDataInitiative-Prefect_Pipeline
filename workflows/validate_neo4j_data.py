from prefect import flow, task, get_run_logger
import os
import sys

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.utils import get_date, get_time, folder_dl
from src.neo4j_data_tools import (
    counts_DB_all_nodes_all_studies,
    validate_DB_with_input_tsvs,
    neo4j_validation_md,
)


@flow(
    name="Validate Neo4j DB",
    log_prints=True,
    flow_run_name="pull-neo4j-{runner}-" + f"{get_time()}",
)
def pull_neo4j_data(
    bucket: str,
    runner: str,
    tsv_folder: str,
    uri_parameter: str = "uri_parameter",
    username_parameter: str = "username_parameter",
    password_parameter: str = "password_parameter",
):
    logger = get_run_logger()

    # download folder from bucket
    logger.info(f"Downloading folder {tsv_folder}")
    folder_dl(bucket=bucket, remote_folder=tsv_folder)

    # query counts per node per study
    # it returns a pandas dataframe
    logger.info("Fetching entry counts per node per study")
    db_node_count_all_studies = counts_DB_all_nodes_all_studies(
        uri_parameter=uri_parameter,
        username_parameter=username_parameter,
        password_parameter=password_parameter,
    )

    # validate db info with files in tsv folder
    logger.info("Reading tsv files and validating records between tsv files and DB")
    validate_df = validate_DB_with_input_tsvs(
        uri_parameter=uri_parameter,
        username_parameter=username_parameter,
        password_parameter=password_parameter,
        tsv_folder=tsv_folder,
        studies_dataframe=db_node_count_all_studies,
    )

    # crete markdown report for this workflow
    logger.info("Creating markdown report for Neo4j validation")
    neo4j_validation_md(validate_df=validate_df, runner=runner)
