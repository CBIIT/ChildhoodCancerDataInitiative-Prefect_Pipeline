from prefect import flow, get_run_logger
import os
import sys

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.utils import get_time, folder_dl, get_date, file_ul, get_secret_centralized_worker
from src.db_data_tools import (
    counts_DB_all_nodes_all_studies_w_secrets,
    validate_DB_with_input_tsvs_w_secrets,
    db_validation_md,
    validate_df_to_count_summary,
    validate_df_to_id_summary
)


@flow(
    name="Validate DB",
    log_prints=True,
    flow_run_name="validate-db-{runner}-" + f"{get_time()}",
)
def validate_db_data(
    bucket: str,
    runner: str,
    tsv_folder: str,
    database_account_id: str,
    database_secret_path: str,
    database_secret_key_ip: str,
    database_secret_key_username: str,
    database_secret_key_password: str,
):
    """Pipeline that pulls specific stats from ingested studies from a db database

    Args:
        bucket (str): Bucket name of where output goes to
        runner (str): Unique runner name
        tsv_folder (str, optional): Folder path of load files in the provided bucket. Defaults to "".
        database_account_id (str): Account ID for the database
        database_secret_path (str): Secret path for the database
        database_secret_key_ip (str): Secret key for the IP of the database
        database_secret_key_username (str): Secret key for the username of the database
        database_secret_key_password (str): Secret key for the password of the database
    """

    logger = get_run_logger()

    logger.info("Getting uri, username and password parameter from AWS")
    # get uri, username, and password value
    uri = get_secret_centralized_worker(
        secret_path_name=database_secret_path,
        secret_key_name=database_secret_key_ip,
        account=database_account_id,
    )
    username = get_secret_centralized_worker(
        secret_path_name=database_secret_path,
        secret_key_name=database_secret_key_username,
        account=database_account_id,
    )
    password = get_secret_centralized_worker(
        secret_path_name=database_secret_path,
        secret_key_name=database_secret_key_password,
        account=database_account_id,
    )

    # it returns a pandas dataframe
    logger.info("Fetching entry counts per node per study")
    db_node_count_all_studies = counts_DB_all_nodes_all_studies_w_secrets(
        uri=uri,
        username=username,
        password=password,
    )

    # Run the queries
    logger.info("Pulling data from database")

    # download folder from bucket
    if tsv_folder != "":
        logger.info(f"Downloading folder {tsv_folder}")
        folder_dl(bucket=bucket, remote_folder=tsv_folder)
    else:
        logger.info("No ingestion files folder path provided")

    if tsv_folder != "":
        # validate db info with files in tsv folder
        logger.info("Reading tsv files and validating records between tsv files and DB")
        validate_df = validate_DB_with_input_tsvs_w_secrets(
            uri=uri,
            username=username,
            password=password,
            tsv_folder=tsv_folder,
            studies_dataframe=db_node_count_all_studies,
        )

        # create markdown report for validation purpose
        logger.info("Creating markdown report for db validation")
        count_summary_df = validate_df_to_count_summary(validate_df=validate_df)
        id_summary_df = validate_df_to_id_summary(validate_df=validate_df)
        db_validation_md(count_summary_df=count_summary_df, id_summary_df=id_summary_df, runner=runner)

        df_for_bucket_upload = validate_df
    else:
        df_for_bucket_upload = db_node_count_all_studies

    # folder name in the bucket for file ul
    summary_file_name =  f"db_validation_summary_{get_date()}.tsv"
    df_for_bucket_upload.to_csv(summary_file_name, sep='\t', index=False)
    bucket_folder = os.path.join(runner, "db_validation_" + get_time())
    file_ul(
        bucket=bucket,
        output_folder=bucket_folder,
        sub_folder="",
        newfile=summary_file_name,
    )
    logger.info(f"db validation summary file {summary_file_name} has been uploaded to bucket {bucket} at folder {bucket_folder}")
