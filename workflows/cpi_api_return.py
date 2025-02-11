from prefect import flow, task, get_run_logger
import os
import sys


parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.neo4j_data_tools import pull_uniq_studies, export_to_csv_per_node_per_study, pull_data_per_node_per_study, cypher_query_parameters
from neo4j import GraphDatabase
from src.utils import get_time, file_ul, get_secret
import pandas as pd
import requests


cypher_query_particiapnt_per_study = """
MATCH (startNode:{node_label})-[:of_{node_label}]-(linkedNode)-[*0..5]-(study:study {{study_id:"{study_accession}"}})
RETURN startNode.{node_label}_id as {node_label}_id
"""

API_DOMAIN = "https://participantindex.ccdi.cancer.gov"
API_GET_DOMAINS = "/v1/domains"
API_GET_RELEVANT_DOMAINS = "/v1/participant_ids/domains"
API_GET_ASSOCIATED_PARTICIPANT_IDS = "/v1/associated_participant_ids"

@task(name="Get access token for CPI API", log_prints=True, cache_key_fn=None, persist_result=False, retries=0)
def get_access_token(client_id: str, client_secret: str, token_url: str) -> str:
    """Retrieve an access token using the client credentials.

    Args:
        client_id (str): client id string
        client_secret (str): clinet secret string
        token_url (str): token url string

    Raises:
        Exception: Exception if failed to get access token

    Returns:
        str: token string
    """
    payload = {"grant_type": "client_credentials", "scope": "custom"}
    auth = (client_id, client_secret)

    response = requests.post(token_url, data=payload, auth=auth, verify=False)

    if response.status_code == 200:
        access_token = response.json().get("access_token")
        print(f"Debug: Access Token - {access_token}")
        return access_token
    else:
        raise Exception(
            f"Failed to get access token: {response.status_code} - {response.text}"
        )

@task(name="Get request return for CPI", log_prints=True, cache_key_fn=None, persist_result=False, retries=0)
def get_cpi_request(api_extension: str, access_token: str, request_body: str) -> dict:
    """Send a GET request to the API with the request body.

    Args:
        api_extension (str): api extension string
        access_token (str): access token string
        request_body (str): request body dict

    Raises:
        Exception: Exception if API request failed

    Returns:
        dict: response json
    """
    headers = {
        "Authorization": None,  # Ensure correct prefix
        "Content-Type": "application/json",
        "Accept": "application/json",  # Matching Postman behavior
    }
    print(f"Debug: Headers - {headers}")
    
    headers = {
        "Authorization": f"Bearer {access_token}",  # Ensure correct prefix
        "Content-Type": "application/json",
        "Accept": "application/json",  # Matching Postman behavior
    }
    print(f"Debug: Headers with new token - {headers}")
    api_url = API_DOMAIN + api_extension
    print(f"Debug: API URL - {api_url}")

    print(f"Debug: Headers - {headers}")
    print(f"Debug: Request Body - {request_body}")

    response = requests.get(api_url, json=request_body, headers=headers, verify=False)

    print(f"Debug: Response Code - {response.status_code}")
    print(f"Debug: Response Text - {response.text}")

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API request failed: {response.status_code} - {response.text}")


@flow(name="loop through all studies for participant ID pull", log_prints=True, persist_result=False, retries=0)
def pull_participant_id_loop(study_list: list, driver, out_dir: str, logger) -> None:
    """Loop through all studies for participant ID pull

    Args:
        study_list (list): A list of study accessions
        driver (_type_): graph database driver
        out_dir (str): output csv directory
        logger (_type_): logger instance
    """    
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    else:
        pass
    for study in study_list:
        logger.info(f"Pulling participant_id from Node participant for study {study}")
        pull_data_per_node_per_study.submit(
            driver=driver,
            data_to_csv=export_to_csv_per_node_per_study,
            study_name=study,
            node_label="participant",
            query_str=cypher_query_particiapnt_per_study,
            output_dir=out_dir,
        )
    return None

@flow(name="Combines participant ids from all studies into a single tsv", log_prints=True, persist_result=False, retries=0)
def consolidate_all_participant_id(folderpath: str, logger, output_name: str) -> None:
    """Read through csv files in the a folder and combines participant ids into a single file

    Args:
        folderpath (str): folder path that contains csv file of participant ids per study per file
        logger (_type_): logger instance
        output_name: a tsv file that contains all the 
    """    
    csv_list = [os.path.join(folderpath, filename) for filename in os.listdir(folderpath) if filename.endswith("csv")]
    combined_df = pd.DataFrame(columns=["study_id","participant_id"])
    for csv in csv_list:
        study_accession = csv.split("/")[1].split("_")[0]
        csv_df =  pd.read_csv(csv, header=0)
        csv_df["study_id"]=study_accession
        combined_df = pd.concat([combined_df, csv_df], ignore_index=True)
        del csv_df
    combined_df.to_csv(output_name, sep="\t", index=False)
    return None


@flow(name="Participant ID pull per study", log_prints=True, persist_result=False, retries=0)
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
    pull_participant_id_loop(
        study_list=study_list,
        driver=driver,
        out_dir=output_dir,
        logger=logger
    )
    logger.info("All participant_id per study pulled")

    # combines all participant id into a single tsv file
    consolidate_all_participant_id(folderpath=output_dir, output_name="sandbox_participant_id.tsv", logger=logger)

    bucket_folder = runner + "/db_participant_id_pull_" + get_time()
    logger.info(
        f"Uploading participant_id file sandbox_participant_id.tsv to the bucket {bucket} at {bucket_folder}"
    )
    file_ul(
        bucket=bucket,
        output_folder=bucket_folder,
        sub_folder="",
        newfile="sandbox_participant_id.tsv",
    )
    logger.info("All participant_id uploaded to the bucket")

@flow(name="Get Associated Domains of Participant IDs", log_prints=True, persist_result=False, retries=0)
def get_associated_domains_particpants(bucket: str, runner: str, uri_parameter: str, username_parameter: str, password_parameter: str) -> None:
    """Get Associated Domains of Participant IDs

    Args:
        bucket (str): bucket of where outputs upload to
        runner (str): unique runner name
        uri_parameter (str): db uri parameter
        username_parameter (str): db username parameter
        password_parameter (str): db password parameter
    """    
    logger = get_run_logger()
    logger.info("Getting uri, username and password parameter from AWS")

    # get secrets from AWS secret manager
    client_id = get_secret(secret_name_path="ccdi/nonprod/inventory/cpi_api_creds", secret_key_name="client_id")
    print(client_id)
    secret = get_secret(secret_name_path="ccdi/nonprod/inventory/cpi_api_creds", secret_key_name="secret")
    print(secret)
    access_token_url = get_secret(secret_name_path="ccdi/nonprod/inventory/cpi_api_creds", secret_key_name="access_token_url")
    print(access_token_url)
    access_token = get_access_token(client_id=client_id, client_secret=secret, token_url=access_token_url)
    logger.info(f"get access token: {access_token}")

    # get domains
    request_body = {
        "participant_ids": [
            {"domain_name": "USI", "participant_id": "PAKZVD"},
            {"domain_name": "USI", "participant_id": "PADJKU"},
        ]
    }

    domains_dict = get_cpi_request(
        api_extension=API_GET_ASSOCIATED_PARTICIPANT_IDS,
        access_token=access_token,
        request_body=request_body,
    )
    print(domains_dict)
    return None

if __name__ == "__main__":
    get_associated_domains_particpants(bucket="ccdi-validation", runner="QL", uri_parameter="uri", username_parameter="username", password_parameter="password")
