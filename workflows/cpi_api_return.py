from prefect import flow, task, get_run_logger
import os
import sys


parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.neo4j_data_tools import pull_uniq_studies, export_to_csv_per_node_per_study, pull_data_per_node_per_study, cypher_query_parameters
from neo4j import GraphDatabase
from src.utils import get_time, folder_ul

cypher_query_particiapnt_per_study = """
MATCH (startNode:{node_label})-[:of_{node_label}]-(linkedNode)-[*0..5]-(study:study {{study_id:"{study_accession}"}})
RETURN startNode.{node_label}_id as {node_label}_id
"""

@flow(name="Participant ID pull per study", log_prints=True)
def pull_participants_in_db(bucket: str, runner: str, uri_parameter: str, username_parameter: str, password_parameter: str) -> None:
    """Pulls all participant ID from neo4j sandbox DB

    Args:
        bucket (str): bucket of where outputs upload to
        runner (str): unique runner name
        uri_parameter (str): db uri parameter
        username_parameter (str): db username parameter
        password_parameter (str): db password parameter
    """    
    logger = get_run_logger()
    logger.info("Getting uri, username and password parameter from AWS")
    # get uri, username, and password value
    uri, username, password = cypher_query_parameters(
        uri_parameter=uri_parameter,
        username_parameter=username_parameter,
        password_parameter=password_parameter,
        logger=logger,
    )

    # driver instance
    logger.info("Creating GraphDatabase driver using uri, username, and password")
    driver = GraphDatabase.driver(uri, auth=(username, password))

    # pulll study list
    study_list = pull_uniq_studies(driver=driver)
    logger.info(f"Study list: {study_list}")

    output_dir = "participant_id_per_study"

    for study in study_list:
        logger.info(f"Pulling participant_id from Node participant for study {study}")
        pull_data_per_node_per_study.submit(
            driver=driver,
            data_to_csv=export_to_csv_per_node_per_study,
            study_name=study,
            node_label="participant",
            query_str=cypher_query_particiapnt_per_study,
            output_dir=output_dir,
        )

    logger.info("All participant_id per study pulled")
    bucket_folder = runner + "/db_participant_id_pull_per_study" + get_time()
    logger.info(f"Uploading folder of {output_dir} to the bucket {bucket} at {bucket_folder}")
    folder_ul(
        bucket=bucket,
        local_folder=output_dir,
        destination=bucket_folder,
        sub_folder=""
    )
    logger.info("All participant_id per study uploaded to the bucket")
