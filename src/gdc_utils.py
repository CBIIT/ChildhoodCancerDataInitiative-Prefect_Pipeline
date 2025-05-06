# gdc utils
from prefect import flow, task, get_run_logger
import sys
import pandas as pd
import json
import requests
import time
from src.utils import get_time, file_dl, folder_ul, get_secret

def make_request(req_type: str, url: str, secret_name_path: str, secret_key_name: str, req_data="", max_retries=5, delay=2):
    """Wrapper for request function to handle request retries for timeouts and connection errors

    Args:
        req_type (str): Type of request (GET, PUT or POST)
        url (str): API URL to make request to
        secret_name_path (str): Path to AWS secrets manager where token hash stored
        secret_key_name (str): Authentication token string secret key name for GDC submission
        data (dict, optional): JSON formatted data. Defaults to {} (no data).
        max_retries (int, optional): _description_. Defaults to 3.
        delay (int, optional): _description_. Defaults to 10.

    Returns:
        str: Request response
    """
    runner_logger = get_run_logger()

    token = get_secret(secret_name_path, secret_key_name).strip()
    
    retries = 0

    # for GET requests
    if req_type.upper() == 'GET':
        while retries < max_retries:
            try:
                if req_data == "":
                    response = requests.get(url, headers={"X-Auth-Token": token})
                    return response
                else:
                    response = requests.get(url, json=req_data, headers={"X-Auth-Token": token, "Content-Type": "application/json"})
                    return response
            except Exception as e:
                runner_logger.warning(f"Error with request: {e}. Retrying...")
                retries += 1
                time.sleep(delay)
    # for POST requests
    elif req_type.upper() == 'POST':
        while retries < max_retries:
            try:
                response = requests.post(url, json=req_data, headers={"X-Auth-Token": token, "Content-Type": "application/json"})
                return response
            except Exception as e:
                runner_logger.warning(f"Error with request: {e}. Retrying...")
                retries += 1
                time.sleep(delay)
    # for PUT requests
    elif req_type.upper() == 'PUT':
        while retries < max_retries:
            try:
                response = requests.put(url, json=req_data, headers={"X-Auth-Token": token, "Content-Type": "application/json"})
                return response
            except Exception as e:
                runner_logger.warning(f"Error with request: {e}. Retrying...")
                retries += 1
                time.sleep(delay)
    else:
        runner_logger.error(f"{req_type} not one of ['GET', 'POST', 'PUT']")
        sys.exit(1)


    runner_logger.error(f"Max retries reached. {req_type.upper()} request {url} failed.")
    return str(e)

@flow(name="gdc_retrieve_current_nodes", log_prints=True)
def retrieve_current_nodes(project_id: str, node_type: str, secret_name_path: str, secret_key_name: str):
    """Query and return all nodes already submitted to GDC for project and node type

    Args:
        project_id (str): Project ID to submit node metadata for
        node_type (str): Node type for nodes to submit
        secret_name_path (str): Path to AWS secrets manager where token hash stored
        secret_key_name (str): Authentication token string secret key name for GDC submission

    Returns:
        list: List of dict node metadata that has already been submitted successfully to GDC
    """


    runner_logger = get_run_logger()

    offset_returns = []
    endpt = "https://api.gdc.cancer.gov/submission/graphql"
    null = ""  # None

    # need to do run queries 500 at a time to avoid time outs
    # may need to increase max number to avoid missing data if more data added in future

    # number nodes to query
    n_query = 500

    for offset in range(0, 500000, n_query):
        time.sleep(2)
        # print to runner_logger that running query
        runner_logger.info(
            f" Checking for {node_type} nodes already present in {project_id}, offset is {offset}"
        )

        # format query to be read into GraphiQL endpoint
        query1 = (
            "{\n\t"
            + node_type
            + '(project_id: "'
            + project_id
            + '", first: '
            + str(n_query)
            + ", offset:"
            + str(offset)
            + "){\n\t\tsubmitter_id\n\t\tid\n\t\tfile_size\n\t\tfile_name\n\t\tmd5sum\n\t\tfile_state\n\t\tstate\n\t}\n}"
        )
        query2 = {"query": query1, "variables": null}

        # retrieve response
        response = make_request("post", endpt, secret_name_path, secret_key_name, req_data=query2)

        # check if malformed
        try:
            # json.loads(response.text)["data"][node_type]
            json.loads(response.text)["data"][node_type]
        except:
            runner_logger.error(
                f" Response is malformed: {str(response.text)} for query {str(query2)}, trying again..."  # loads > dumps
            )
            response = make_request("post", endpt, secret_name_path, secret_key_name, req_data=query2)

        # check if anymore hits, if not break to speed up process

        if len(json.loads(response.text)["data"][node_type]) == n_query:
            offset_returns += json.loads(response.text)["data"][node_type]
        elif len(json.loads(response.text)["data"][node_type]) < n_query:
            offset_returns += json.loads(response.text)["data"][node_type]
            runner_logger.info(
                f" Completed retrieval of previously submitted {node_type} submitter_ids"  # loads > dumps
            )
            break
        else:  # i.e. len(json.loads(response.text)['data'][node_type]) == 0
            break


    return offset_returns