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
import logging
from deepdiff import DeepDiff
import pandas as pd
import time
from typing import Literal

import boto3
from botocore.exceptions import ClientError
from prefect import flow, get_run_logger
from src.utils import get_time, file_dl, folder_ul, sanitize_return


##############
#
# Functions
#
##############


def read_json(dir_path: str):
    """Reads in submission JSON file and returns a list of dicts, checks node types.

    Args:
        dir_path (str): Local path on VM to JSON file with node metadata 

    Returns:
        list: Ingested list of node metadata in dict/JSON format
    """

    runner_logger = get_run_logger()

    try:
        nodes = json.load(open(dir_path))
    except:
        runner_logger.error(f" Cannot read in JSON file {dir_path}")
        sys.exit(1)

    return nodes


def loader(dir_path: str, node_type: str):
    """Checks that JSON file is a list of dicts and all nodes are of expected type and node type.

    Args:
        dir_path (str): Local path on VM to JSON file with node metadata  
        node_type (str): GDC node type of nodes in metadata JSON file

    Returns:
        list: Parsed list of nodes that are of same node type as specified in workflow run
    """

    runner_logger = get_run_logger()

    nodes = read_json(dir_path)

    if not isinstance(nodes, list):
        logging.error(f" JSON file {dir_path} not a list of dicts.")
        sys.exit(1)

    parsed_nodes = []

    # check if all nodes are of expected node type and are in correct format
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

    # perform count on number of nodes parsed from import JSON
    diff = len(nodes) - len(parsed_nodes)

    runner_logger.info(f" {str(diff)} nodes were parsed from file for submission.")

    return parsed_nodes


@flow(
    name="gdc_import_dbgap_retrieve",
    log_prints=True,
    flow_run_name="gdc_import_dbgap_retrieve_" + f"{get_time()}",
)
def dbgap_retrieve(phs_id_version: str):
    """For case submissions only, with formatted phs ID and version, e.g. phs002790.v7, query dbGaP for released subjects

    Args:
        phs_id_version (str): phs ID and version, e.g. phs002790.v7 to check participants/cases against

    Returns:
        list: List of case IDs/barcodes that are present for the study in dbGaP
    """

    runner_logger = get_run_logger()

    # number of entries to return on a page
    page_size = 500

    url = f"https://www.ncbi.nlm.nih.gov/gap/sstr/api/v1/study/{phs_id_version}/subjects?page=1&page_size={page_size}"

    # intial request
    response = requests.get(url)

    # check if phs ID and URL are correct
    if not str(response.status_code).startswith("20"):
        runner_logger.error("ERROR with dbGaP request, check phs ID and url")

    # initialize list of subject IDs
    subjects_dbgap = [
        subject["submitted_subject_id"]
        for subject in json.loads(response.text)["subjects"]
    ]

    # paginate thru API calls to retrieve cases released in dbGaP
    while json.loads(response.text)["pagination"]["link"]["next"] != None:
        response = requests.get(json.loads(response.text)["pagination"]["link"]["next"])
        if not str(response.status_code).startswith("20"):
            runner_logger.error("ERROR with dbGaP request, check phs ID and url")
        else:
            runner_logger.info(
                f"Response page #: {str(json.loads(response.text)['pagination']['page'])}"
            )
        subjects_dbgap += [
            subject["submitted_subject_id"]
            for subject in json.loads(response.text)["subjects"]
        ]

    return subjects_dbgap

def make_request(req_type: str, url: str, token:str, req_data="", max_retries=5, delay=2):
    """Wrapper for request function to handle request retries for timeouts and connection errors

    Args:
        req_type (str): Type of request (GET, PUT or POST)
        url (str): API URL to make request to
        token (str): GDC auth token string
        data (dict, optional): JSON formatted data. Defaults to {} (no data).
        max_retries (int, optional): _description_. Defaults to 3.
        delay (int, optional): _description_. Defaults to 10.

    Returns:
        str: Request response
    """
    runner_logger = get_run_logger()
    
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

def dbgap_compare(phs_id_version: str, nodes: list):
    """Perform comparison of dbGaP released cases for project to case nodes in submission file

    Args:
        phs_id_version (str): phs ID and version, e.g. phs002790.v7 to check participants/cases against
        nodes (list): List of case nodes to check if released in dbGaP

    Returns:
        list: Parsed list of case nodes that are released in dbGaP
    """

    runner_logger = get_run_logger()

    # retrieve from dbGaP already released cases
    subjects_dbgap = dbgap_retrieve(phs_id_version)

    # init list of parsed cases
    parsed_subjects = []

    # compare lists
    for node in nodes:
        if node["submitter_id"] in subjects_dbgap:
            parsed_subjects.append(node)

    runner_logger.info(
        f"Of {len(nodes)} case nodes in submission file, {len(parsed_subjects)} are released subjects in dbGaP and will move onto submission checking"
    )

    return parsed_subjects


def retrieve_current_nodes(project_id: str, node_type: str, token: str):
    """Query and return all nodes already submitted to GDC for project and node type

    Args:
        project_id (str): Project ID to submit node metadata for
        node_type (str): Node type for nodes to submit
        token (str): GDC token hash to make API calls with 

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
            + "){\n\t\tsubmitter_id\n\t\tid\n\t}\n}"
        )
        query2 = {"query": query1, "variables": null}

        # retrieve response
        response = make_request("post", endpt, token, req_data=query2)

        # check if malformed
        try:
            # json.loads(response.text)["data"][node_type]
            json.loads(response.text)["data"][node_type]
        except:
            runner_logger.error(
                f" Response is malformed: {str(response.text)} for query {str(query2)}, trying again..."  # loads > dumps
            )
            response = make_request("post", endpt, token, req_data=query2)

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


@flow(
    name="gdc_import_query_entities",
    log_prints=True,
    flow_run_name="gdc_import_query_entities_" + f"{get_time()}",
)
def query_entities(node_uuids: list, project_id: str, token: str):
    """Query entity metadata from GDC to perform comparisons for nodes to update.
    This function is different from retrieve_current_nodes() because one needs does not need to know
    a priori the GDC uuids/submitter_id to retrieve metadata for as well as what properties 
    to return for retrieve_current_nodes(); this function requires specifying a UUID 
    and returns all properties. 

    Args:
        node_uuids (list): UUIDs of nodes already in GDC
        project_id (str): Project ID to query node metadata for
        token (str): GDC token hash to make API calls with 

    Returns:
        list: List of dict node metadata that have already been submitted successfully to GDC
    """

    try:
        runner_logger = get_run_logger()

        gdc_node_metadata = {}

        program = project_id.split("-")[0]
        project = "-".join(project_id.split("-")[1:])

        api = f"https://api.gdc.cancer.gov/submission/{program}/{project}/entities/"

        uuids = [node["id"] for node in node_uuids]

        runner_logger.info(
            "Grabbing comparison JSONs to check if already submitted nodes need updating"
        )

        size = 20
        max_retries = 5

        for offset in range(0, len(uuids), size):  # query 20 at a time
            runner_logger.info(f"Querying entities chunk {round(offset/size)+1} of {len(range(0, len(uuids), size))}")
            uuids_fmt = ",".join(uuids[offset : offset + size]) #format UUIDs into comma sep list for API call
            retries = 0
            while retries < max_retries:
                try:
                    temp = make_request('get', api + uuids_fmt, token) #make request
                    entities = json.loads(temp.text)["entities"] #extract entities from request
                    retries = max_retries #update retries counter to exit loop
                except:
                    runner_logger.error(
                        f" Entities request output malformed: {str(temp.text)}, for request {api+uuids_fmt} , trying again..."  # loads > dumps
                    ) # throw error if entities not found in API call return
                    retries += 1
                    time.sleep(2)

            for entity in entities:

                # remove GDC internal fields and handle null values of optional fields
                entity_parse = entity_parser(entity["properties"])

                gdc_node_metadata[entity_parse["submitter_id"]] = entity_parse

            time.sleep(2)

        return gdc_node_metadata
    
    except Exception as e:
        # sanitize exception of any token information
        updated_error_message = sanitize_return(str(e), [token])
        runner_logger.error(updated_error_message)
        sys.exit(1)

def entity_parser(node: dict):
    """Parse out unnecessary GDC internal fields and handle null values

    Args:
        node (dict): Node metadata returned from /entities API call

    Returns:
        dict: Node metadata with extraneous/GDC internal key-value pairs removed
    """

    runner_logger = get_run_logger()

    for prop in [
        "batch_id",
        "state",
        "projects",
        "created_datetime",
        "updated_datetime",
        "id",
        "file_state"
    ]:
        if prop in node.keys():
            del node[prop]

    addn_rem = (
        []
    )  # optional props that have no value submitted to GDC currently (set as None)

    for k, v in node.items():
        if v == None:
            addn_rem.append(k)

    for prop in addn_rem:
        del node[prop]

    # remove extra link info from entity returned
    to_replace = ""
    replacement = []

    for k, v in node.items():
        if type(v) == dict:  # checks/breadcrumbs to inform future parsing if needded
            if len(node[k].keys()) > 1:
                runner_logger.warning(
                    f"For already submitted to GDC entitity comparison, entity {node['submitter_id']} of type {node['type']} is of dict class that has more than one key. Inspect for future comparisons."
                )
            elif "submitter_id" not in node[k].keys():
                runner_logger.warning(
                    f"For already submitted to GDC entitity comparison, entity {node['submitter_id']} of type {node['type']} is of dict class and does not contain 'submitter_id'. Inspect for future comparisons."
                )
        elif type(v) == list:  # parsing list of dicts
            for field in v:
                if type(field) == dict:
                    for field_k, field_v in field.items():
                        if field_k == "submitter_id":
                            replacement.append({field_k: field_v})
                            to_replace = k  # node key to replace with properly formatted list of dicts
    # assuming that in the submission nodes file,
    # that parent entity links are not formatted as a list of dicts when
    # there is only one entity; instead, just a dict
    if len(replacement) == 1:
        node[to_replace] = replacement[0]
    elif len(replacement) > 1:
        node[to_replace] = replacement
    else:
        pass

    # add in projects.code to mimic submission file for case nodes
    if node["type"] == "case":
        node["projects"] = {"code": "-".join(node["project_id"].split("-")[1:])}

    return node


def json_compare(submit_file_metadata: dict, gdc_node_metadata: dict):
    """Compare node entity metadata; if node in submission file is different than in GDC
    then slate for import, otherwise ignore; use DeepDiff for comparison

    Args:
        submit_file_metadata (dict): Node metadata for a given node entity present in submission file
        gdc_node_metadata (dict): Node metadata for a given node entity present in the GDC graph

    Returns:
        bool: Indication if metadata in file is different from what metadata has been submitted to GDC
    """

    if DeepDiff(submit_file_metadata, gdc_node_metadata, ignore_order=True):
        return True
    else:
        return False


def compare_diff(nodes: list, project_id: str, node_type: str, token: str, check_for_updates: str):
    """Determine if nodes in submission file are new entities or already exist in GDC

    Args:
        nodes (list): List of node entities in submission file to check  
        project_id (str): GDC Project ID
        node_type (str): Node entity type in GDC Data Model 
        token (str): Token hash
        check_for_updates (str): yes/no indication for checking previously submitted entities in GDC against nodes to be submitted for differences

    Returns:
        list: List of dict node metadata that is new (i.e. not previously submitted to GDC) or already submitted nodes
    """

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

    if check_for_updates.lower() == 'yes':

        update_nodes = []

        # grab UUIDS
        check_nodes_ids = [
            node
            for node in gdc_nodes
            if node["submitter_id"] in [i["submitter_id"] for i in check_nodes]
        ]

        chunk_size = 200

        gdc_entities = {}

        for chunk in range(0, len(check_nodes_ids), chunk_size):
            # get GDC version of node entities, returns a dict with keys as submitter_id
            # gdc_entities = query_entities(check_nodes_ids, project_id, token)
            runner_logger.info(
                f"Querying chunk {round(chunk/chunk_size)+1} of {len(range(0, len(check_nodes_ids), chunk_size))} for node entity comparison"
            )
            gdc_entities.update(
                query_entities(
                    check_nodes_ids[chunk : chunk + chunk_size], project_id, token
                )
            )

        # json comparison here
        for node in check_nodes:
            if json_compare(node, gdc_entities[node["submitter_id"]]):
                update_nodes.append(node)
            else:
                pass  # if nodes are the same, ignore

    else:
        update_nodes = []

    if check_for_updates.lower() == 'yes':
        runner_logger.info(
            f" Out of {len(nodes)} nodes, {len(new_nodes)} are new entities and {len(check_nodes)} are previously submitted entities; of the previously submitted entities, {len(update_nodes)} need to be updated."
        )
    else:
        runner_logger.info(
            f" Out of {len(nodes)} nodes, {len(new_nodes)} are new entities and {len(check_nodes)} are previously submitted entities."
        )

    # new nodes submit POST, update nodes submit PUT
    return new_nodes, update_nodes


def error_parser(response: str):
    """Read in a response and parse the returned message for output TSV files

    Args:
        response (str): API response from submission API call

    Returns:
        str: Parsed and formatted error message or original response if error cannot be determined
    """

    # find error message from submission API call response

    try:
        enum_dict = json.loads(response)
        new_dict = {}
        if "entities" in enum_dict.keys():
            new_dict["affected_field"] = enum_dict["entities"][0]["errors"][0][
                "keys"
            ][0]
            # parse error message to first 300 chars for simplcity
            if len(enum_dict["entities"][0]["errors"][0]["message"]) > 300:
                new_dict["error_msg"] = (
                    enum_dict["entities"][0]["errors"][0]["message"][:300]
                    + "..."
                )
            else:
                new_dict["error_msg"] = enum_dict["entities"][0]["errors"][0][
                    "message"
                ]
            return json.dumps(new_dict)
        else:
            return response
    except:
        return response


@flow(
    name="gdc_import_submission_response_recorder",
    log_prints=True,
    flow_run_name="gdc_import_submission_response_recorder_" + f"{get_time()}",
)
def response_recorder(responses: list):
    """Parse and record responses

    Args:
        responses (list): List of responses from submission API calls

    Returns:
        pd.DataFrame: dataframe of parsed and formatted API call responses for readability 
    """

    runner_logger = get_run_logger()

    errors = []
    success_uuid = []

    for node in responses:
        if "40" in str(node[1]):
            # errors.append([str(i) for i in node])
            errors.append([str(node[0]), str(node[1]), error_parser(str(node[2]))])
        elif "20" in str(node[1]):
            success_uuid.append(
                [node[0], json.loads(node[2])["entities"][0]["id"], str(node[2])]
            )
        else:
            runner_logger.warning(
                f" Unknown submission response code and status, {node[1]} for submitter_id {node[0]}"
            )
    # convert recorded responses to dataframes or return empty dataframe if no data
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


@flow(
    name="gdc_import_submission",
    log_prints=True,
    flow_run_name="gdc_import_submission_" + f"{get_time()}",
)
def submit(nodes: list, project_id: str, token: str, submission_type: str):
    """Submission of node entities with POST or PUT request

    Args:
        nodes (list): Node entity metadata to submitt to GDC
        project_id (str): GDC Project ID
        token (str): Token hash to submit node entity metadata
        submission_type (str): Either 'new' or 'update' to submit node entity metadata with POST (new) or PUT (update) request

    Returns:
        pd.DataFrame: dataframe of parsed and formatted API call responses for readability 
    """

    runner_logger = get_run_logger()

    try:
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
                res = make_request("post",
                    api,
                    token,
                    req_data=node
                )

                runner_logger.info(
                    f" POST request for node submitter_id {node['submitter_id']}: {str(res.text)}"
                )
                responses.append([node["submitter_id"], res.status_code, str(res.text)])
                time.sleep(2)
        elif submission_type == "update":
            for node in nodes:
                res = make_request("put",
                    api,
                    token,
                    req_data=node
                )

                runner_logger.info(
                    f" PUT request for node submitter_id {node['submitter_id']}: {str(res.text)}"
                )
                responses.append([node["submitter_id"], res.status_code, str(res.text)])
                time.sleep(2)

        # need to chunk responses into response recorder to not overwhelm stuff
        # start with 50 at a time?
        errors = []
        successes = []
        chunk_size = 50

        for chunk in range(0, len(responses), chunk_size):
            error_temp, success_temp = response_recorder(
                responses[chunk : chunk + chunk_size]
            )
            errors.append(error_temp)
            successes.append(success_temp)

        # return response_recorder(responses)
        return pd.concat(errors), pd.concat(successes)
    
    except Exception as e:
        # sanitize exception of any token information
        updated_error_message = sanitize_return(str(e), [token])
        runner_logger.error(updated_error_message)
        sys.exit(1)


def get_secret(secret_key_name):
    """Retrieve token hash for GDC API calls from AWS Secrets Manager

    Args:
        secret_key_name (str): Name of secret stored in AWS Secrets Manager

    Raises:
        e: Error when attempting to retrieve secret

    Returns:
        str: Secret hash returned from AWS Secrets Manager
    """

    # bucket and bucket location where secret is stored
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

    return json.loads(get_secret_value_response["SecretString"])[secret_key_name]

DropDownChoices = Literal["yes", "no"]

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
    secret_key_name: str,
    sstr: str,
    check_for_updates: DropDownChoices
):
    """CCDI data curation pipeline

    Args:
        bucket (str): Bucket name of where the manifest is located in and the output goes to
        file_path (str): File path of the CCDI manifest
        runner (str): Unique runner name
        secret_key_name (str): Authentication token string secret key name for GDC submission
        project_id (str): GDC Project ID to submit to (e.g. CCDI-MCI, TARGET-AML)
        node_type (str): The GDC node type is being submitted
        sstr (str): For case node submission, the phs ID and version of study in dbGaP (e.g. phs002790.v7)
        check_for_updates (str): 'yes' or 'no' to perform checks for nodes to UPDATE; if 'no', only submit NEW nodes

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

    # get token
    token = get_secret(secret_key_name).strip()

    # load in nodes file
    nodes = loader(file_name, node_type)

    # if phs ID provided and node type is case
    # check that cases released already in dbGaP
    if sstr != "" and node_type == "case":
        runner_logger.info("Checking case nodes against released subjects in dbGaP...")
        nodes = dbgap_compare(sstr, nodes)

    # parse nodes into new and update nodes
    new_nodes, update_nodes = compare_diff(nodes, project_id, node_type, token, check_for_updates)

    # get time for file outputs
    dt = get_time()

    os.mkdir(f"{project_id}_{node_type}_{dt}")

    # submit nodes
    if new_nodes:

        # init error and success df

        error_df_list = []
        success_uuid_df_list = []

        # chunk nodes to not overwhelm prefect

        if node_type in [
            "diagnosis",
            "treatment",
            "other_clinical_attribute",
            "follow_up",
        ]:
            chunk_size = 20
        else:
            chunk_size = 200

        for node_set in range(0, len(new_nodes), chunk_size):

            runner_logger.info(
                f"Submitting chunk {round(node_set/chunk_size)+1} of {len(range(0, len(new_nodes), chunk_size))} of new nodes"
            )

            error_df_temp, success_uuid_df_temp = submit(
                new_nodes[node_set : node_set + chunk_size], project_id, token, "new"
            )

            error_df_list.append(error_df_temp)
            success_uuid_df_list.append(success_uuid_df_temp)

        # concat all temp dfs

        error_df = pd.concat(error_df_list)
        success_uuid_df = pd.concat(success_uuid_df_list)

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

        error_df_list = []
        success_uuid_df_list = []

        if node_type in [
            "diagnosis",
            "treatment",
            "other_clinical_attribute",
            "follow_up",
        ]:
            chunk_size = 20
        else:
            chunk_size = 200

        for node_set in range(0, len(update_nodes), chunk_size):

            runner_logger.info(
                f"Submitting chunk {round(node_set/chunk_size)+1} of {len(range(0, len(update_nodes), chunk_size))} of updated nodes"
            )

            error_df_temp, success_uuid_df_temp = submit(
                update_nodes[node_set : node_set + chunk_size],
                project_id,
                token,
                "update",
            )

            error_df_list.append(error_df_temp)
            success_uuid_df_list.append(success_uuid_df_temp)

        # concat all temp dfs

        error_df = pd.concat(error_df_list)
        success_uuid_df = pd.concat(success_uuid_df_list)


        # save error and success messages to files
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

    # folder upload to s3 location
    folder_ul(
        local_folder=f"{project_id}_{node_type}_{dt}",
        bucket=bucket,
        destination=runner + "/",
        sub_folder="",
    )
