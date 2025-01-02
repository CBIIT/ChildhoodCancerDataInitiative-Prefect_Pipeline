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



def upload_chunk(url, chunk_data, chunk_number, token):
    """Helper function to upload a single chunk."""

    runner_logger = get_run_logger()

    retries = 0
    max_retries = 10

    #token = get_secret(secret).strip()

    while retries < max_retries:
        try:
            files = {'file': (f'chunk_{chunk_number}', chunk_data)}
            headers = {"X-Auth-Token": token}
            response = requests.put(url, files=files, stream=True, headers=headers)
            if response.status_code == 200:
                runner_logger.info(f"Chunk {chunk_number} for file uploaded successfully!")
                return None
            else:
                runner_logger.error(f"Error uploading chunk {chunk_number}: {response.status_code}, retrying...")
                retries += 1 
        except Exception as e:
            runner_logger.error(f"Error uploading chunk {chunk_number}: {e}")
            retries += 1 

    runner_logger.error(f"Max retries reached. Failed to upload {os.path.basename(url)}")
    return [os.path.basename(url), "NOT UPLOADED", str(e)]

@flow(
    name="gdc_upload_make_upload_request",
    log_prints=True,
    flow_run_name="gdc_upload_make_upload_request_" + f"{get_time()}",
)
def upload_request_chunks(
    f_name: str, project_id: str, uuid: str, token: str):
    """Function to upload file

    Args:
        f_name (str): Name of file to upload
        project_id (str): Project ID in GDC to upload to
        uuid (str): UUID of file in GDC
        token (str): GDC auth token

    Returns:
        list: UUID, response code and response text from upload attempt
    """
    runner_logger = get_run_logger()

    program = project_id.split("-")[0]
    project = "-".join(project_id.split("-")[1:])

    chunk_size = 2 * 1024 * 1024 #5MB
    max_threads = 2

    file_size = os.path.getsize(f_name)
    chunk_count = (file_size // chunk_size) + (1 if file_size % chunk_size > 0 else 0)
    runner_logger.info(f"{chunk_count} chunks for file {f_name} upload")
    url = f"https://api.gdc.cancer.gov/v0/submission/{program}/{project}/files/{uuid}"

    try:
        with open(f_name, "rb") as f:
            with ThreadPoolExecutor(max_threads) as executor:
                futures = []

                for chunk_number in range(chunk_count):
                    chunk_data = f.read(chunk_size)
                    context = contextvars.copy_context()
                    futures.append(executor.submit(context.run, upload_chunk, url, chunk_data, chunk_number, token))
                
                for future in futures:
                    runner_logger.info(future.result()) #TODO: geta all status codes and do a check 

        f.close()
        return [uuid, "200", "success"]
        
    except Exception as e:
        runner_logger.error(f"Exception for file upload {f_name} raised: {e}")

        return [f_name, "NOT UPLOADED", str(e)]


@flow(
    name="gdc_upload_uploader_api",
    log_prints=True,
    flow_run_name="gdc_upload_uploader_api_" + f"{get_time()}",
)
def uploader_api(df: pd.DataFrame, project_id: str, token: str):
    """Handler function for uploading files using submission API /files endpoint

    Args:
        df (pd.DataFrame): dataframe with GDC UUID and s3_urls
        project_id (str): project ID of GDC project to upload to
        token (str): GDC Auth token string
    """
    try:

        runner_logger = get_run_logger()

        runner_logger.info(f"size of df of upload files is {len(df)}")

        subresponses = []

        for index, row in df.iterrows():
            try:
                f_bucket = row["s3_url"].split("/")[2]
                f_path = "/".join(row["s3_url"].split("/")[3:])
                f_name = os.path.basename(f_path)

                if f_name != row["file_name"]:
                    runner_logger.warning(
                        f"Expected file name {row['file_name']} does not match observed file name in s3 url, {f_name}"
                    )
                # trying to re-use file_dl() function
                file_dl(f_bucket, f_path)

                runner_logger.info(f"Downloaded file {f_name}")
            except:
                runner_logger.error(f"Cannot download file {row['file_name']}")

            # check that file exists
            if not os.path.isfile(row["file_name"]):
                runner_logger.error(
                    f"File {row['file_name']} not copied over or found from URL {row['s3_url']}"
                )
            else:  # proceed to uploaded with API
                subresponses.append(
                    upload_request_chunks(f_name, project_id, row["id"], token)
                )
                time.sleep(10)

                # delete file
                if os.path.exists(f_name):
                    os.remove(f_name)
                else:
                    runner_logger.warning(
                        f"The file {f_name} does not exist, cannot remove."
                    )

                # check delete
                if os.path.exists(f_name):
                    runner_logger.warning(
                        f"The file {f_name} still exists, error removing."
                    )
                else:
                    continue

        return subresponses

    except Exception as e:
        # sanitize exception of any token information
        updated_error_message = sanitize_return(str(e), [token])
        runner_logger.error(updated_error_message)
        sys.exit(1)


@flow(
    name="GDC File Upload",
    log_prints=True,
    flow_run_name="{runner}_" + f"{get_time()}",
)
def runner(
    bucket: str,
    project_id: str,
    file_path: str,
    runner: str,
    secret_key_name: str,
):
    """CCDI Pipeline to Upload files to GDC

    Args:
        bucket (str): Bucket name of where the manifest is located in and the response output goes to
        project_id (str): GDC Project ID to submit to (e.g. CCDI-MCI, TARGET-AML)
        file_path (str): File path of the CCDI file manifest in bucket
        runner (str): Unique runner name
        secret_key_name (str): Authentication token string secret key name for file upload to GDC
    """

    # runner_logger setup

    runner_logger = get_run_logger()

    runner_logger.info(">>> Running GDC_FILE_UPLOAD.py ....")

    dt = get_time()

    os.mkdir(f"GDC_file_upload_{project_id}_{dt}")

    # download the input manifest file
    file_dl(bucket, file_path)

    # extract file name before the workflow starts
    file_name = os.path.basename(file_path)

    token = get_secret(secret_key_name).strip()

    runner_logger.info(f">>> Reading input file {file_name} ....")

    file_metadata = read_input(file_path)

    # then query against indexd for the bucket URL of the file

    file_metadata_s3 = retrieve_s3_url_handler(file_metadata)

    # then copy file to prod s3 bucket based on the url
    # and upload the file to the GDC via the submission API
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
