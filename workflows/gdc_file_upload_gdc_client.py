""" Script to upload files to the GDC """

##############
#
# Env. Setup
#
##############

import json
import requests
import os
import sys
import time
import subprocess

import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import contextvars


# prefect dependencies
import boto3
from botocore.exceptions import ClientError
from prefect import flow, get_run_logger
from src.utils import get_time, file_dl, folder_ul, sanitize_return


def read_input(file_path: str):
    """Read in TSV file and extract file_name, id (GDC uuid), md5sum and file_size columns

    Args:
        file_path (str): path to input file that contains required cols

    Returns:
        pd.DataFrame: DataFrame with extracted necessary metadata
    """

    runner_logger = get_run_logger()

    f_name = os.path.basename(file_path)

    try:
        file_metadata = pd.read_csv(f_name, sep="\t")[
            ["id", "md5sum", "file_size", "file_name"]
        ]
    except:
        runner_logger.error(f"Error reading and parsing file {f_name}.")
        sys.exit(1)

    if len(file_metadata) == 0:
        runner_logger.error(f"Error reading and parsing file {f_name}; empty file")
        sys.exit(1)

    return file_metadata


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


@flow(
    name="gdc_upload_retrieve_s3_url",
    log_prints=True,
    flow_run_name="gdc_upload_retrieve_s3_url_" + f"{get_time()}",
)
def retrieve_s3_url(rows: pd.DataFrame):
    """Query indexd with md5sum and file_size to retrieve file s3 url

    Args:
        rows (pd.DataFrame): Chunked input DataFrame with required metadata (md5sum and file_size)

    Returns:
        pd.DataFrame: DataFrame with s3 url appended to row for each file instance
    """

    runner_logger = get_run_logger()

    for index, row in rows.iterrows():
        query_url = f"https://nci-crdc.datacommons.io/index/index?hash=md5:{row['md5sum']}&size={row['file_size']}"

        response = requests.get(query_url)

        # parse response here
        try:
            s3_url = ""
            for record in json.loads(response.text)["records"]:
                for url in record["urls"]:
                    if url != "":
                        s3_url = url

            if s3_url == "":
                runner_logger.error(
                    f" No URL found: {str(response.text)} for query {query_url}"
                )
        except:
            runner_logger.error(
                f" Response is malformed: {str(response.text)} for query {query_url}"
            )
            s3_url = ""

        rows.loc[index, "s3_url"] = s3_url

    return rows


def retrieve_s3_url_handler(file_metadata: pd.DataFrame):
    """Handle flow input for s3 URL retrieval

    Args:
        file_metadata (pd.DataFrame): DataFrame containing id, md5sum and file_size

    Returns:
        pd.DataFrame: DataFrame with s3 url appended to row for each file instance
    """
    runner_logger = get_run_logger()

    chunk_size = 300  # how many rows to send into retrieve_s3_url

    subframes = []  # list to store dfs containing s3 urls + other metadata

    for chunk in range(0, len(file_metadata), chunk_size):
        runner_logger.info(
            f"Querying s3 urls for chunk {round(chunk/chunk_size)+1} of {len(range(0, len(file_metadata), chunk_size))} of files"
        )
        subframe = retrieve_s3_url(file_metadata[chunk : chunk + chunk_size])
        subframes.append(subframe)

    df_s3 = pd.concat(subframes)

    return df_s3



####### TODO 

# download gdc-client
# flow to handle uploads
# do test uploads from command line for recording any outputs/logs
# x = subprocess.run(["/Users/bullenca/Work/gdc-client", "upload", "4cf36925-9bfb-4089-b304-4ce80ecb36c6", "-t", "/Users/bullenca/Documents/gdc-user-token.2024-12-17.txt"], shell=False, capture_output=True)
# x.stdout, x.stderr


@flow(
    name="GDC File Upload",
    log_prints=True,
    flow_run_name="{runner}_" + f"{get_time()}",
)
def runner(
    bucket: str,
    project_id: str,
    manifest_path: str,
    runner: str,
    secret_key_name: str,
):
    """CCDI Pipeline to Upload files to GDC

    Args:
        bucket (str): Bucket name of where the manifest is located in and the response output goes to
        project_id (str): GDC Project ID to submit to (e.g. CCDI-MCI, TARGET-AML)
        manifest_path (str): File path of the CCDI file manifest in bucket
        runner (str): Unique runner name
        secret_key_name (str): Authentication token string secret key name for file upload to GDC
    """

    # runner_logger setup

    runner_logger = get_run_logger()

    runner_logger.info(">>> Running GDC_FILE_UPLOAD.py ....")

    dt = get_time()

    os.mkdir(f"GDC_file_upload_{project_id}_{dt}")

    # download the input manifest file
    file_dl(bucket, manifest_path)

    # extract file name before the workflow starts
    file_name = os.path.basename(manifest_path)

    token = get_secret(secret_key_name).strip()

    runner_logger.info(f">>> Reading input file {file_name} ....")

    file_metadata = read_input(file_name)

    # then query against indexd for the bucket URL of the file

    file_metadata_s3 = retrieve_s3_url_handler(file_metadata)

    responses = []

    chunk_size = 200

    for chunk in range(0, len(file_metadata_s3), chunk_size):
        runner_logger.info(
            f"Uploading chunk {round(chunk/chunk_size)+1} of {len(range(0, len(file_metadata_s3), chunk_size))} for files"
        )
        subresponses = uploader_api(
            file_metadata_s3[chunk : chunk + chunk_size], project_id, token
        )
        responses += subresponses

    responses_df = pd.DataFrame(responses, columns=["id", "status_code", "response"])

    # save response file

    responses_df.to_csv(
        f"GDC_file_upload_{project_id}_{dt}/{file_name}_upload_results.tsv",
        sep="\t",
        index=False,
    )

    # folder upload
    folder_ul(
        local_folder=f"GDC_file_upload_{project_id}_{dt}",
        bucket=bucket,
        destination=runner + "/",
        sub_folder="",
    )