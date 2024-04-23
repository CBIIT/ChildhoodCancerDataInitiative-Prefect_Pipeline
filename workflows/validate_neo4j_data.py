from prefect import flow, get_run_logger
import os
import sys

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.utils import get_time, folder_dl, get_date, file_ul
from src.neo4j_data_tools import (
    counts_DB_all_nodes_all_studies,
    validate_DB_with_input_tsvs,
    neo4j_validation_md,
    validate_df_to_count_summary,
    validate_df_to_id_summary
)


@flow(
    name="Validate Neo4j DB",
    log_prints=True,
    flow_run_name="validate-neo4j-{runner}-" + f"{get_time()}",
)
def validate_neo4j_data(
    bucket: str,
    runner: str,
    tsv_folder: str = "path/to/ingestion_folder/in/s3/bucket",
    uri_parameter: str = "uri",
    username_parameter: str = "username",
    password_parameter: str = "password",
):
    logger = get_run_logger()

    # query counts per node per study
    # it returns a pandas dataframe
    logger.info("Fetching entry counts per node per study")
    db_node_count_all_studies = counts_DB_all_nodes_all_studies(
        uri_parameter=uri_parameter,
        username_parameter=username_parameter,
        password_parameter=password_parameter,
    )

    # download folder from bucket
    if tsv_folder != "path/to/ingestion_folder/in/s3/bucket":
        logger.info(f"Downloading folder {tsv_folder}")
        folder_dl(bucket=bucket, remote_folder=tsv_folder)
    else:
        logger.info("No ingestion files folder path provided")

    if tsv_folder != "path/to/ingestion_folder/in/s3/bucket":
        # validate db info with files in tsv folder
        logger.info("Reading tsv files and validating records between tsv files and DB")
        validate_df = validate_DB_with_input_tsvs(
            uri_parameter=uri_parameter,
            username_parameter=username_parameter,
            password_parameter=password_parameter,
            tsv_folder=tsv_folder,
            studies_dataframe=db_node_count_all_studies,
        )

        # create markdown report for validation purpose
        logger.info("Creating markdown report for Neo4j validation")
        count_summary_df = validate_df_to_count_summary(validate_df=validate_df)
        id_summary_df = validate_df_to_id_summary(validate_df=validate_df)
        neo4j_validation_md(count_summary_df=count_summary_df, id_summary_df=id_summary_df, runner=runner)

        df_for_bucket_upload = validate_df
    else:
        df_for_bucket_upload = db_node_count_all_studies

    # folder name in the bucket for file ul
    summary_file_name =  f"neo4j_validation_summary_{get_date()}.tsv"
    df_for_bucket_upload.to_csv(summary_file_name, sep='\t', index=False)
    bucket_folder = os.path.join(runner, "neo4j_validation_" + get_time())
    file_ul(
        bucket=bucket,
        output_folder=bucket_folder,
        sub_folder="",
        newfile=summary_file_name,
    )
    logger.info(f"Neo4j validation summary file {summary_file_name} has been uploaded to bucket {bucket} at folder {bucket_folder}")
