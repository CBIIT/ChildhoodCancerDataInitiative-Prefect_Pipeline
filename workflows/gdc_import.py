""" Script to submit node metadata to the GDC """

##############
#
# Env. Setup
#
##############

import requests
import json
import sys
import os
import argparse
import logging
from deepdiff import DeepDiff
import pandas as pd
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
from prefect import flow, get_run_logger
from src.utils import get_time, get_date, file_dl, folder_ul, file_ul


##############
#
# Functions
#
##############


def read_json(dir_path: str):
    """Reads in submission JSON file and returns a list of dicts, checks node types."""
    try:
        nodes = json.load(open(dir_path))
    except:
        logging.error(f" Cannot read in JSON file {dir_path}")
        sys.exit(1)

    return nodes


def loader(dir_path: str, node_type: str):
    """Checks that JSON file is a list of dicts and all nodes are of expected type and node type."""

    runner_logger = get_run_logger()

    nodes = read_json(dir_path)

    if type(nodes) != list:
        logging.error(f" JSON file {dir_path} not a list of dicts.")
        sys.exit(1)

    parsed_nodes = []

    for node in nodes:
        if node["type"] != node_type:
            runner_logger.warning(
                f" Node with submitter_id {node['submitter_id']} has type {node['type']}, {node_type} is expected. Removing from node import list."
            )
        elif type(node) != dict:
            runner_logger.warning(
                f" Node with submitter_id {node['submitter_id']} type != dict. Removing from node import list."
            )
        else:
            parsed_nodes.append(node)

    diff = len(nodes) - len(parsed_nodes)

    runner_logger.info(f" {str(diff)} nodes were parsed from file for submission.")

    return parsed_nodes


def read_token(dir_path: str):
    """Read in token file string"""
    try:
        token = open(dir_path).read().strip()
    except ValueError as e:
        logging.error(f" Error reading token file {dir_path}: {e}")
        sys.exit(1)

    return token


def retrieve_current_nodes(project_id: str, node_type: str, token: str):
    """Query and return all nodes already submitted to GDC for project and node type"""
    runner_logger = get_run_logger()

    offset_returns = []
    endpt = "https://api.gdc.cancer.gov/submission/graphql"
    null = ""  # None

    # need to do run queries 1000 at a time to avoid time outs
    # may need to increase max number to avoid missing data if more data added in future
    
    #number nodes to query 
    n_query = 500

    for offset in range(0, 20000, n_query):

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
            + "){\n\t\tsubmitter_id\n\t\tid\n\t}\n}"
        )
        query2 = {"query": query1, "variables": null}

        # retrieve response
        response = requests.post(endpt, json=query2, headers={"X-Auth-Token": token})

        # check if malformed
        try:
            json.loads(response.text)["data"][node_type]
        except:
            runner_logger.error(
                f" Response is malformed: {str(json.loads(response.text))} for query {str(query2)}"
            )

        # check if anymore hits, if not break to speed up process

        if len(json.loads(response.text)["data"][node_type]) == n_query:
            offset_returns += json.loads(response.text)["data"][node_type]
        elif len(json.loads(response.text)["data"][node_type]) < n_query:
            offset_returns += json.loads(response.text)["data"][node_type]
            break
        else:  # i.e. len(json.loads(response.text)['data'][node_type]) == 0
            break

    return offset_returns


def query_entities(node_uuids: list, project_id: list, token: list):
    """Query entity metadata from GDC to perform comparisons for nodes to update"""

    runner_logger = get_run_logger()

    gdc_node_metadata = {}

    program = project_id.split("-")[0]
    project = "-".join(project_id.split("-")[1:])

    api = f"https://api.gdc.cancer.gov/submission/{program}/{project}/entities/"

    uuids = [node["id"] for node in node_uuids]

    for offset in range(0, len(uuids), 30):  # query 30 at a time
        uuids_fmt = ",".join(uuids[offset : offset + 30])
        temp = requests.get(api + uuids_fmt, headers={"X-Auth-Token": token})
        try:
            entities = json.loads(temp.text)["entities"]
        except:
            runner_logger.error(
                f" Entities request output malformed: {json.loads(temp.text)}, for request {api+uuids_fmt}"
            )
            sys.exit(1)

        for entity in entities:

            # remove GDC internal fields and handle null values of optional fields
            entity_parse = entity_parser(entity["properties"])

            gdc_node_metadata[entity_parse["submitter_id"]] = entity_parse

    return gdc_node_metadata


def entity_parser(node: dict):
    """Parse out unnecessary GDC internal fields and handle null values"""

    del node["batch_id"]
    del node["state"]
    del node["projects"]
    del node["created_datetime"]
    del node["updated_datetime"]
    del node["id"]

    addn_rem = (
        []
    )  # optional props that have no value submitted to GDC currently (set as None)

    for k, v in node.items():
        if v == None:
            addn_rem.append(k)

    for prop in addn_rem:
        del node[prop]

    # add in projects.code to mimic submission file for case nodes
    if node["type"] == "case":
        node["projects"] = {"code": "-".join(node["project_id"].split("-")[1:])}

    return node


def json_compare(submit_file_metadata: dict, gdc_node_metadata: dict):
    """Compare node entity metadata; if node in submission file is different than in GDC
    then slate for import, otherwise ignore; use DeepDiff"""

    if DeepDiff(submit_file_metadata, gdc_node_metadata, ignore_order=True):
        return True
    else:
        return False


def compare_diff(nodes: list, project_id: str, node_type: str, token: str):
    """Determine if nodes in submission file are new entities or already exist in GDC"""

    runner_logger = get_run_logger()

    # retrieve node entities already in GDC
    gdc_nodes = retrieve_current_nodes(project_id, node_type, token)

    # submitter_ids of node entities already in GDC
    gdc_nodes_sub_ids = [node["submitter_id"] for node in gdc_nodes]

    # submitter_ids of node entities in submission file
    submission_nodes_sub_ids = [node["submitter_id"] for node in nodes]

    # parse new node entities that do not exist in GDC yet by submitter_id
    new_nodes = [
        node
        for node in nodes
        if node["submitter_id"]
        in list(set(submission_nodes_sub_ids) - set(gdc_nodes_sub_ids))
    ]

    # parse node entities in submission file that already exist in GDC
    check_nodes = [
        node
        for node in nodes
        if node["submitter_id"]
        in list(set(submission_nodes_sub_ids) & set(gdc_nodes_sub_ids))
    ]

    # assess if node entities in submission file that already exist in GDC
    # need to be updated in GDC (i.e. entities in file and GDC are different)

    if check_nodes:

        update_nodes = []

        # grab UUIDS
        check_nodes_ids = [
            node
            for node in gdc_nodes
            if node["submitter_id"] in [i["submitter_id"] for i in check_nodes]
        ]

        # get GDC version of node entities, returns a dict with keys as submitter_id
        gdc_entities = query_entities(check_nodes_ids, project_id, token)

        # json comparison here
        for node in check_nodes:
            if json_compare(node, gdc_entities[node["submitter_id"]]):
                update_nodes.append(node)
            else:
                pass  # if nodes are the same, ignore

        runner_logger.info(
            f" Out of {len(nodes)} nodes, {len(new_nodes)} are new entities and {len(check_nodes)} are previously submitted entities; of the previously submitted entities, {len(update_nodes)} need to be updated."
        )

    else:
        update_nodes = []

    # new nodes submit POST, update nodes submit PUT
    return new_nodes, update_nodes


def response_recorder(responses: list):
    """Parse and record responses"""

    runner_logger = get_run_logger()

    errors = []
    success_uuid = []

    for node in responses:
        if "40" in str(node[1]):
            errors.append([str(i) for i in node])
        elif "20" in str(node[1]):
            success_uuid.append(
                [node[0], json.loads(node[2])["entities"][0]["id"], str(node[2])]
            )
        else:
            runner_logger.warning(
                f" Unknown submission response code and status, {node[1]} for submitter_id {node[0]}"
            )

    if errors:
        error_df = pd.DataFrame(errors)
        error_df.columns = ["submitter_id", "status_code", "message"]
    else:
        error_df = pd.DataFrame(columns=["submitter_id", "status_code", "message"])

    if success_uuid:
        success_uuid_df = pd.DataFrame(success_uuid)
        success_uuid_df.columns = ["submitter_id", "uuid", "message"]
    else:
        success_uuid_df = pd.DataFrame(columns=["submitter_id", "uuid", "message"])

    return error_df, success_uuid_df


def submit(nodes: list, project_id: str, token: str, submission_type: str):
    """Submission of new node entities with POST request"""

    runner_logger = get_run_logger()

    assert submission_type in [
        "new",
        "update",
    ], "Invalid value. Allowed values: new, update"

    responses = []

    program = project_id.split("-")[0]
    project = "-".join(project_id.split("-")[1:])

    api = f"https://api.gdc.cancer.gov/submission/{program}/{project}/"

    if submission_type == "new":
        for node in nodes:
            res = requests.post(
                url=api,
                json=node,
                headers={"X-Auth-Token": token, "Content-Type": "application/json"},
            )
            runner_logger.info(
                f" POST request for node submitter_id {node['submitter_id']}: {str(res.text)}"
            )
            responses.append([node["submitter_id"], res.status_code, res.text])
    elif submission_type == "update":
        for node in nodes:
            res = requests.put(
                url=api,
                json=node,
                headers={"X-Auth-Token": token, "Content-Type": "application/json"},
            )
            runner_logger.info(
                f" PUT request for node submitter_id {node['submitter_id']}: {str(res.text)}"
            )
            responses.append([node["submitter_id"], res.status_code, res.text])

    return response_recorder(responses)


def get_secret():
    secret_name = "ccdi/nonprod/inventory/gdc-token"
    region_name = "us-east-1"
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e
    secret = get_secret_value_response["gdc-token"]

    return secret


@flow(
    name="GDC Import",
    log_prints=True,
    flow_run_name="{runner}_" + f"{get_time()}",
)
def runner(
    bucket: str,
    file_path: str,
    project_id: str,
    node_type: str,
    runner: str,
    token: str,
):
    """CCDI data curation pipeline

    Args:
        bucket (str): Bucket name of where the manifest is located in and the output goes to
        file_path (str): File path of the CCDI manifest
        runner (str): Unique runner name
        token (str): Authentication token for GDC submission
        project_id (str): GDC Project ID to submit to (e.g. CCDI-MCI, TARGET-AML)
        node_type (str): The GDC node type is being submitted

    Raises:
        ValueError: Value Error occurs when the pipeline fails to proceed.
    """
    # create a logging object
    runner_logger = get_run_logger()

    runner_logger.info(">>> Running GDC_IMPORT.py ....")

    # download the file
    file_dl(bucket, file_path)

    # check the manifest version before the workflow starts
    file_name = os.path.basename(file_path)

    """# get token
                pretoken = get_secret().strip()
                try:
                    token = json.loads(pretoken)['gdc-token']
                except:
                    runner_logger.warning("incorrect token parsing")"""
    # get token
    token = get_secret().strip()

    # load in nodes file
    nodes = loader(file_name, node_type)

    # parse nodes into new and update nodes
    new_nodes, update_nodes = compare_diff(nodes, project_id, node_type, token)

    # get time for file outputs
    dt = get_time()

    # submit nodes
    if new_nodes:
        error_df, success_uuid_df = submit(new_nodes, project_id, token, "new")

        error_df.to_csv(
            f"{project_id}_{node_type}_{dt}/NEW_NODES_SUBMISSION_ERRORS.tsv",
            sep="\t",
            index=False,
        )
        success_uuid_df.to_csv(
            f"{project_id}_{node_type}_{dt}/NEW_NODES_SUBMISSION_SUCCESS.tsv",
            sep="\t",
            index=False,
        )

    if update_nodes:
        error_df, success_uuid_df = submit(update_nodes, project_id, token, "update")

        error_df.to_csv(
            f"{project_id}_{node_type}_{dt}/UPDATED_NODES_SUBMISSION_ERRORS.tsv",
            sep="\t",
            index=False,
        )
        success_uuid_df.to_csv(
            f"{project_id}_{node_type}_{dt}/UPDATED_NODES_SUBMISSION_SUCCESS.tsv",
            sep="\t",
            index=False,
        )

    # folder upload
    folder_ul(
        local_folder=f"{project_id}_{node_type}_{dt}",
        bucket=bucket,
        destination=runner + f"/{project_id}_{node_type}_{dt}",
        sub_folder="",
    )
