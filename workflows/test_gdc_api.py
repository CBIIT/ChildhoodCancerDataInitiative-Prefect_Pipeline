# prefect dependencies
import json
import requests

import boto3
from botocore.exceptions import ClientError
from prefect import flow, get_run_logger
from src.utils import get_time, file_dl, folder_ul

def get_secret(secret_key_name):
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

def send_gdc_submission():
    url = "https://api.gdc.cancer.gov/submission"
    
    # Sample headers - adjust as needed
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    # Sample data payload - replace with your actual data
    payload = {
        "key": "value"
    }
    
    try:
        response = requests.get(
            url,
            headers=headers,
            json=payload
        )
        
        # Print status code and response
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.json()}")
        
        return response
        
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None
    
def send_gdc_submission_entity_get(token: str):

    
    # Sample headers - adjust as needed
    headers={"X-Auth-Token": token, "Content-Type": "application/json"}
    
    try:
        response = requests.get(
            "https://api.gdc.cancer.gov/submission/CCDI/MCI/entities/005ebef7-c384-433a-bea9-91afdb332ecb",
            headers=headers,

        )
        
        # Print status code and response
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.json()}")
        
        return response
        
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None
    
def send_gdc_submission_post(token: str):
    url = "https://api.gdc.cancer.gov/submission"
    
    # Sample headers - adjust as needed
    headers={"X-Auth-Token": token, "Content-Type": "application/json"}

    runner_logger = get_run_logger()

    # Sample data payload - replace with your actual data
    payload = {'query': '{\n\tsample(project_id: "CCDI-MCI", first: 1, offset:0){\n\t\tsubmitter_id\n\t\tid\n\t}\n}', 'variables': ''}

    runner_logger.info(f"payload is: {payload}")
    
    try:
        response = requests.post(
            url,
            headers=headers,
            json=payload
        )
        
        # Print status code and response
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.json()}")
        
        return response
        
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None

@flow(
    name="GDC Test API",
    log_prints=True,
    flow_run_name="{runner}_" + f"{get_time()}",
)
def runner(
    runner: str,
    secret_key_name: str,
    bucket: str
):
    runner_logger = get_run_logger()

    runner_logger.info("TESTING API QUERIES")

    token = get_secret(secret_key_name).strip()

    runner_logger.info("Testing empty payload GET")

    runner_logger.info(send_gdc_submission())

    runner_logger.info("Testing sample payload POST to /submission/graphql")

    runner_logger.info(send_gdc_submission_post(token))

    runner_logger.info("Testing sample GET to https://api.gdc.cancer.gov/submission/CCDI/MCI/entities/005ebef7-c384-433a-bea9-91afdb332ecb")

    runner_logger.info(send_gdc_submission_entity_get(token))

