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
    uri_parameter: str = "uri",
    username_parameter: str = "username",
    password_parameter: str = "password",
):
    """Pipeline that pulls all ingested studies from a Neo4j database

    Args:
        bucket (str): Bucket name of where output goes to
        runner (str): Unique runner name
        uri_parameter (str, optional): uri parameter. Defaults to "uri".
        username_parameter (str, optional): username parameter. Defaults to "username".
        password_parameter (str, optional): password parameter. Defaults to "password".
    """    
    logger = get_run_logger()

    # create a unqiue folder name for final outputs
    # destination in the bucket
    bucket_folder = runner + "/db_data_pull_outputs_" + get_time()

    # pulling data from DB
    logger.info("Starting pulling data from neo4j DB")
    db_data_folder = query_db_to_csv(
        output_dir="./pulled_db_csv",
        uri_parameter=uri_parameter,
        username_parameter=username_parameter,
        password_parameter=password_parameter,
    )

    # converting data pulled from DB (csv files) to tsv files
    logger.info("Starting to convert DB pulled csv to tsv files")
    export_folder = convert_csv_to_tsv(
        db_pulled_outdir="./pulled_db_csv", output_dir="./"
    )

    # upload db pulled data csv files and converted tsv files to the bucket
    logger.info(f"Uploading folder of {db_data_folder} to the bucket {bucket} at {bucket_folder}")
    folder_ul(
        local_folder=db_data_folder,
        bucket=bucket,
        destination=bucket_folder,
        sub_folder="",
    )
    logger.info(f"Uploading folder of {export_folder} to the bucket {bucket} at {bucket_folder}")
    folder_ul(
        local_folder=export_folder,
        bucket=bucket,
        destination=bucket_folder,
        sub_folder="",
    )

    logger.info("Workflow of pulling data from Neo4j db is Finished")

    return None
