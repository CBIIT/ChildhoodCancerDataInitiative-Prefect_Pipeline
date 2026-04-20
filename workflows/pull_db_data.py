from prefect import flow, task, get_run_logger
import os
import sys
from typing import Union

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.neo4j_data_tools import convert_csv_to_tsv_dcc, query_db_to_csv_w_secrets
from src.utils import get_secret_centralized_worker, get_time, folder_ul


@flow(
    name="Pull DB data",
    log_prints=True,
    flow_run_name="pull-db-{runner}-" + f"{get_time()}",
)
def pull_db_data(
    bucket: str,
    runner: str,
    database_account_id: str,
    database_secret_path: str,
    database_secret_key_ip: str,
    database_secret_key_username: str,
    database_secret_key_password: str,
    study_id_list: Union[list[str], None] = None,
):
    """Pipeline that pulls ingested studies from a DB database. Default pulls all studies unless a single study phs ID provided.

    Args:
        bucket (str): Bucket name of where output goes to
        runner (str): Unique runner name
        database_account_id (str): AWS account ID for accessing secrets
        database_secret_path (str): Path to the secret in AWS Secrets Manager
        database_secret_key_ip (str): Key name for the URI in the secret
        database_secret_key_username (str): Key name for the username in the secret
        database_secret_key_password (str): Key name for the password in the secret
        study_id_list (list[str], optional): List of Study IDs to pull data for multiple study pulls. If None, the pipeline pulls all the studies. Defaults to None.
    """
    logger = get_run_logger()

    # create a unqiue folder name for final outputs
    # destination in the bucket
    bucket_folder = runner + "/db_data_pull_outputs_" + get_time()

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

    # pulling data from DB
    logger.info("Starting pulling data from DB")
    db_data_folder = query_db_to_csv_w_secrets(
        output_dir="./pulled_db_csv",
        uri_secret=uri,
        username_secret=username,
        password_secret=password,
        study_id_list=study_id_list,
    )

    # upload converted tsv files to the bucket
    logger.info(
        f"Uploading folder of {db_data_folder} to the bucket {bucket} at {bucket_folder}"
    )
    folder_ul(
        local_folder=db_data_folder,
        bucket=bucket,
        destination=bucket_folder,
        sub_folder="",
    )

    # converting data pulled from DB (csv files) to tsv files
    logger.info("Starting to convert DB pulled csv to tsv files")
    export_folder = convert_csv_to_tsv_dcc(
        db_pulled_outdir=db_data_folder, output_dir="./"
    )

    # upload converted tsv files to the bucket
    logger.info(
        f"Uploading folder of {export_folder} to the bucket {bucket} at {bucket_folder}"
    )
    folder_ul(
        local_folder=export_folder,
        bucket=bucket,
        destination=bucket_folder,
        sub_folder="",
    )

    logger.info("Workflow of pulling data from DB is Finished")

    full_output_path = f"s3://{bucket}/{bucket_folder}/{export_folder}"

    return full_output_path
