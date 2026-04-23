from prefect import flow, task, get_run_logger
import os
import sys
from typing import Union

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.neo4j_data_tools import query_db_to_csv, convert_csv_to_tsv
from src.utils import get_time, folder_ul, get_secret_centralized_worker


@flow(
    name="Pull Neo4j data",
    log_prints=True,
    flow_run_name="pull-neo4j-{runner}-" + f"{get_time()}",
)
def pull_neo4j_data(
    bucket: str,
    runner: str,
    database_account_id: str,
    database_secret_path: str,
    database_secret_key_ip: str,
    database_secret_key_username: str,
    database_secret_key_password: str,
    study_id_list: Union[list[str], None] = None,
):
    """Pipeline that pulls ingested studies from a Neo4j database. Default pulls all studies unless a single study phs ID provided. 

    Args:
        bucket (str): Bucket name of where output goes to
        runner (str): Unique runner name
        database_account_id (str): database account id
        database_secret_path (str): database secret path
        database_secret_key_ip (str): database secret key for ip
        database_secret_key_username (str): database secret key for username
        database_secret_key_password (str): database secret key for password
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
    logger.info("Starting pulling data from neo4j DB")
    db_data_folder = query_db_to_csv(
        output_dir="./pulled_db_csv",
        db_uri=uri,
        db_username=username,
        db_password=password,
        study_id_list=study_id_list
    )

    # converting data pulled from DB (csv files) to tsv files
    logger.info("Starting to convert DB pulled csv to tsv files")
    export_folder = convert_csv_to_tsv(db_pulled_outdir=db_data_folder, output_dir="./")

    # upload converted tsv files to the bucket
    logger.info(f"Uploading folder of {export_folder} to the bucket {bucket} at {bucket_folder}")
    folder_ul(
        local_folder=export_folder,
        bucket=bucket,
        destination=bucket_folder,
        sub_folder="",
    )

    logger.info("Workflow of pulling data from Neo4j db is Finished")

    full_output_path = f"s3://{bucket}/{bucket_folder}/{export_folder}"

    return full_output_path
