from prefect import flow, task, get_run_logger
import os
import sys


parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.neo4j_data_tools import pull_uniq_studies, export_to_csv_per_node_per_study, pull_data_per_node_per_study, cypher_query_parameters
from neo4j import GraphDatabase
from src.utils import get_time, file_ul, get_secret, list_to_chunks, folder_ul
import pandas as pd
import requests
import json


cypher_query_particiapnt_per_study = """
MATCH (startNode:{node_label})-[:of_{node_label}]-(linkedNode)-[*0..5]-(study:study {{study_id:"{study_accession}"}})
RETURN startNode.{node_label}_id as {node_label}_id
"""

API_DOMAIN = "https://participantindex.ccdi.cancer.gov"
API_GET_DOMAINS = "/v1/domains"
API_GET_RELEVANT_DOMAINS = "/v1/participant_ids/domains"
API_GET_ASSOCIATED_PARTICIPANT_IDS = "/v1/associated_participant_ids"

@task(name="Get access token for CPI API", log_prints=True)
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
        #print(f"Debug: Access Token - {access_token}")
        return access_token
    else:
        raise Exception(
            f"Failed to get access token: {response.status_code} - {response.text}"
        )

@task(name="Get request return for CPI", log_prints=True, cache_key_fn=None)
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


@flow(name="loop through all studies for participant ID pull", log_prints=True)
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

@flow(name="Combines participant ids from all studies into a single tsv", log_prints=True)
def consolidate_all_participant_id(folderpath: str, output_name: str) -> None:
    """Read through csv files in the a folder and combines participant ids into a single file

    Args:
        folderpath (str): folder path that contains csv file of participant ids per study per file
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


@flow(name="Participant ID pull per study", log_prints=True)
def pull_participants_in_db(bucket: str, upload_folder: str, uri_parameter: str, username_parameter: str, password_parameter: str) -> None:
    """Pulls all participant ID from neo4j sandbox DB

    Args:
        bucket (str): bucket of where outputs upload to
        upload_folder (str): unique runner name
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
    participant_filename = "sandbox_participant_id_" + get_time() + ".tsv"
    consolidate_all_participant_id(
        folderpath=output_dir,
        output_name=participant_filename,
    )


    logger.info(
        f"Uploading participant_id file sandbox_participant_id.tsv to the bucket {bucket} at {upload_folder}"
    )
    file_ul(
        bucket=bucket,
        output_folder=upload_folder,
        sub_folder="",
        newfile=participant_filename,
    )
    logger.info(f"All participant_id uploaded to the bucket {bucket} at {upload_folder}")
    # remove the local file
    return participant_filename

@flow(name="Get all domain information", log_prints=True)
def get_all_doamin_info(client_id: str, client_secret: str, token_url: str) -> str:
    """Get all domain information from the API

    Args:
        client_id (str): _description_
        client_secret (str): _description_
        token_url (str): _description_

    Returns:
        str: domain information file name
    """    
    access_token = get_access_token(
        client_id=client_id, client_secret=client_secret, token_url=token_url
    )
    all_domains = get_cpi_request(
        api_extension=API_GET_DOMAINS, access_token=access_token, request_body={}
    )
    domain_info_file = "all_domains.json"
    with open("all_domains.json", "w") as file:
        json.dump(all_domains, file)
    return domain_info_file


def reformat_domain_dict(filepath: str) -> dict:
    """Read a json file of domain API return and reformat into a dictionary

    Args:
        filepath (str): filepath of API regarding all domains

    Returns:
        dict: a dictionary of domain names
    """
    with open(filepath, "r") as file:
        domain_list = json.load(file)
    return_dict = {}
    for domain in domain_list:
        domain_name = domain["domain_name"]
        return_dict[domain_name] = domain
    return return_dict

@flow(name="Fetch associated ids for participants", log_prints=True)
def get_associated_ids(filepath: str, out_dir: str, domain_file: str, client_id: str, client_secret: str, token_url: str) -> str:
    """Read a tsv and get associatd id from different domains and returns a file per study

    Args:
        filepath (str): filepath of a tsv which contains participant_id and study accession
        out_dir (str): output directory
        domain_file (str): a json file contains all the domain metadata
        client_id (str): cpi api client id
        client_secret (str): cpi api client secret
        token_url (str): cpi api token url
    
    Returns:
        str: filepath of the output file
    """
    domain_dict = reformat_domain_dict(filepath=domain_file)

    id_df = pd.read_csv(filepath, sep="\t")
    uniq_studies = list(id_df["study_id"].unique())

    for study in uniq_studies:
        # for each study, get a new token
        token = get_access_token(
            client_id=client_id, client_secret=client_secret, token_url=token_url
        )
        # create a file for each study
        study_filename = f"{study}_participant_associated_domains.tsv"
        study_filepath = f"{out_dir}/{study_filename}"
        study_associated_df = pd.DataFrame(
            columns=[
                "study_id",
                "participant_id",
                "associated_id",
                "domain_name",
                "domain_description",
                "domain_category",
                "data_location",
            ]
        )
        study_associated_df.to_csv(study_filepath, sep="\t", index=False)

        # get subset of participant_ids for each study
        study_df = id_df[id_df["study_id"] == study]
        participant_ids = list(study_df["participant_id"])
        print(f"study {study} has {len(participant_ids)} participant_ids")
        # break the participant_id list into chunks for API call
        participant_ids_list = list_to_chunks(participant_ids, 50)
        print(f"chunks for study {study}: {len(participant_ids_list)}")

        for item in participant_ids_list:

            # get associated domains for 50 participant_ids
            item_list = []
            for participant_id in item:
                item_list.append(
                    {"domain_name": study, "participant_id": participant_id}
                )
            relevant_domains_item_return = get_cpi_request(
                api_extension=API_GET_ASSOCIATED_PARTICIPANT_IDS,
                access_token=token,
                request_body={"participant_ids": item_list},
            )
            # reformt return for easier access
            relevant_domains_item_return_reformat = {}
            # relevant_domains_item_return["participant_ids"] is a list
            for return_item in relevant_domains_item_return["participant_ids"]:
                # return_item["associated_ids"] is a list
                relevant_domains_item_return_reformat[return_item["participant_id"]] = (
                    return_item["associated_ids"]
                )
            records_to_write = []
            for id in item:
                if id in relevant_domains_item_return_reformat.keys():
                    id_associated_domains = relevant_domains_item_return_reformat[id]
                    if len(id_associated_domains) > 0:
                        for single_associated_domain in id_associated_domains:
                            records_to_write.append(
                                {
                                    "study_id": study,
                                    "participant_id": id,
                                    "associated_id": single_associated_domain[
                                        "participant_id"
                                    ],
                                    "domain_name": single_associated_domain[
                                        "domain_name"
                                    ],
                                    "domain_category": single_associated_domain[
                                        "domain_category"
                                    ],
                                    "domain_description": domain_dict[
                                        single_associated_domain["domain_name"]
                                    ]["domain_description"],
                                    "data_location": domain_dict[
                                        single_associated_domain["domain_name"]
                                    ]["data_location"],
                                }
                            )
                    else:
                        pass
                        # records_to_write.append(
                        #    {
                        #        "study_id": study,
                        #        "participant_id": id,
                        #        "associated_id": None,
                        #        "domain_name": None,
                        #        "domain_description": None,
                        #        "domain_category": None,
                        #        "data_location": None,
                        #    }
                        # )
                else:
                    pass
                    # records_to_write.append(
                    #    {
                    #        "study_id": study,
                    #        "participant_id": id,
                    #        "associated_id": None,
                    #        "domain_name": None,
                    #        "domain_description": None,
                    #        "domain_category": None,
                    #        "data_location": None,
                    #    }
                    # )
            # append the dataframe to exisitng file
            if len(records_to_write) > 0:
                study_df = pd.read_csv(study_filepath, sep="\t")
                study_df = pd.concat(
                    [study_df, pd.DataFrame(records_to_write)], ignore_index=True
                )
                study_df.to_csv(study_filepath, sep="\t", index=False)
            else:
                pass

    return None


@flow(name="Get Associated Domains of CCDI Participants", log_prints=True)
def get_associated_domains_ids(bucket: str, runner: str, uri_parameter: str, username_parameter: str, password_parameter: str) -> None:
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

    upload_folder = runner.strip("/") + "/cpi_api_return_" + get_time()

    # pull participant id from neo4j sandbox DB
    participant_filename = pull_participants_in_db(
        bucket=bucket,
        upload_folder=upload_folder,
        uri_parameter=uri_parameter,
        username_parameter=username_parameter,
        password_parameter=password_parameter,
    )
    logger.info("All participant_id pulled from neo4j sandbox DB")
   
    
    # get secrets from AWS secret manager
    client_id = get_secret(secret_name_path="ccdi/nonprod/inventory/cpi_api_creds", secret_key_name="client_id")
    #print(client_id)
    secret = get_secret(secret_name_path="ccdi/nonprod/inventory/cpi_api_creds", secret_key_name="secret")
    #print(secret)
    access_token_url = get_secret(secret_name_path="ccdi/nonprod/inventory/cpi_api_creds", secret_key_name="access_token_url")
    #print(access_token_url)

    # get all domain information
    domain_file = get_all_doamin_info(client_id=client_id, client_secret=secret, token_url=access_token_url)
    logger.info("Fetched all domain metadata from CPI API")

    # get associated ids for all the participants
    out_dir = "associated_participant_ids_" + get_time()
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    else:
        pass
    get_associated_ids(
        filepath=participant_filename,
        out_dir=out_dir,
        domain_file=domain_file,
        client_id=client_id,
        client_secret=secret,
        token_url=access_token_url,
    )
    logger.info(f"Fetched asscociated ids for all participants in the tsv {participant_filename}")
    folder_ul(local_folder=out_dir, bucket=bucket, destination=upload_folder, sub_folder="")
    logger.info(f"Uploaded associated ids for all participants to the bucket {bucket} folder path {upload_folder}")

    return None