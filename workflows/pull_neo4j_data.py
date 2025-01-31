from prefect import flow, task, get_run_logger
import os
import sys

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.neo4j_data_tools import query_db_to_csv, convert_csv_to_tsv
from src.utils import get_time, folder_ul


@flow(
    name="Pull Neo4j data",
    log_prints=True,
    flow_run_name="pull-neo4j-{runner}-" + f"{get_time()}",
)
def pull_neo4j_data(
    bucket: str,
    runner: str,
    secret_name: str,
    ip_key: str,
    username_key: str,
    password_key: str
) -> None: 
    """Pipeline that pulls all ingested studies from a Neo4j database

    Args:
        bucket (str): Bucket name of where output goes to
        runner (str): Unique runner name
        secret_name (str): sceret name in the secret manager
        ip_key (str): key name for DB ip
        username_key (str): key name for DB username
        password_key (str): key name for DB password
    """
    logger = get_run_logger()

    # create a unqiue folder name for final outputs
    # destination in the bucket
    bucket_folder = runner + "/db_data_pull_outputs_" + get_time()

    # pulling data from DB
    logger.info("Starting pulling data from neo4j DB")
    db_data_folder = query_db_to_csv(
        output_dir="./pulled_db_csv",
        secret_name=secret_name,
        ip_key=ip_key,
        username_key=username_key,
        password_key=password_key,
    )

    # upload db pulled data csv files to the bucket
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
    export_folder = convert_csv_to_tsv(
        db_pulled_outdir="./pulled_db_csv", output_dir="./"
    )

    # upload converted tsv files to the bucket
    logger.info(f"Uploading folder of {export_folder} to the bucket {bucket} at {bucket_folder}")
    folder_ul(
        local_folder=export_folder,
        bucket=bucket,
        destination=bucket_folder,
        sub_folder="",
    )

    logger.info("Workflow of pulling data from Neo4j db is Finished")

    return None
