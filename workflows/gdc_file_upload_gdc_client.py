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


@flow(
    name="gdc_upload_file_upload",
    log_prints=True,
    flow_run_name="gdc_upload_file_upload_" + f"{get_time()}",
)
def uploader_handler(df: pd.DataFrame, token_file: str, part_size: int, n_process: int):

    runner_logger = get_run_logger()

    subresponses = []

    chunk_size = int(part_size * 1024 * 1024)

    for index, row in df.iterrows():
        #TODO: code in retries?
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
            subresponses.append([row['id'], row['file_name'], "NOT uploaded"])
            continue #skip rest of attempt since no file

        # check that file exists
        if not os.path.isfile(row["file_name"]):
            runner_logger.error(
                f"File {row['file_name']} not copied over or found from URL {row['s3_url']}"
            )
            subresponses.append([row['id'], row['file_name'], "NOT uploaded"])
            continue 
        else:  # proceed to uploaded with API
            runner_logger.info(f"Attempting upload of file {row['file_name']} (UUID: {row['id']}), file_size {row['file_size']}....")
            #try:
            process = subprocess.Popen(["./gdc-client", "upload", row['id'], "-t", token_file, "-c", str(chunk_size), "-n", str(n_process)], shell=False, text=True, stdout=subprocess.PIPE)
            std_out, std_err = process.communicate()
            runner_logger.info(std_out)
            runner_logger.info(std_err)
            """if f"Upload finished for file {row['id']}" in std_out:
                    runner_logger.info(f"File {row['id']} successfully uploaded!")
                    subresponses.append([row['id'], row['file_name'], "uploaded"])
                else:
                    runner_logger.error(f"Upload of file {row['file_name']} (UUID: {row['id']}) failed: {std_out}")
                    subresponses.append([row['id'], row['file_name'], "NOT uploaded"])
            except Exception as e:
                runner_logger.error(f"Upload of file {row['file_name']} (UUID: {row['id']}) failed due to exception: {e}")
                subresponses.append([row['id'], row['file_name'], "NOT uploaded"])"""
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
                pass
    
    return subresponses



@flow(
    name="GDC File Upload",
    log_prints=True,
    flow_run_name="{runner}_" + f"{get_time()}",
)
def runner(
    bucket: str,
    project_id: str,
    manifest_path: str,
    gdc_client_path: str, 
    runner: str,
    secret_key_name: str,
    upload_part_size_mb: int,
    n_processes: int
):
    """CCDI Pipeline to Upload files to GDC

    Args:
        bucket (str): Bucket name of where the manifest is located in and the response output goes to
        project_id (str): GDC Project ID to submit to (e.g. CCDI-MCI, TARGET-AML)
        manifest_path (str): File path of the CCDI file manifest in bucket
        gdc_client_path (str): Path to GDC client to download to VM
        runner (str): Unique runner name
        secret_key_name (str): Authentication token string secret key name for file upload to GDC
        upload_part_size_mb (int): The upload part size in MB
        n_processes (int): The number of client connections to upload the files
    """

    # runner_logger setup

    runner_logger = get_run_logger()

    runner_logger.info(">>> Running GDC_FILE_UPLOAD.py ....")

    dt = get_time()

    os.mkdir(f"GDC_file_upload_{project_id}_{dt}")

    # download the input manifest file
    file_dl(bucket, manifest_path)

    # save a token file to give gdc-client
    token = get_secret(secret_key_name).strip()

    with open("token.txt", "w+") as w:
        w.write(token)
    w.close()

    # secure token file
    subprocess.run(["chmod", "600", "token.txt"], shell=False)

    # download the gdc-client
    file_dl(bucket, gdc_client_path)

    # change gdc-client to executable
    subprocess.run(["chmod", "755", "gdc-client"], shell=False)

    # extract file name before the workflow starts
    file_name = os.path.basename(manifest_path)

    runner_logger.info(f">>> Reading input file {file_name} ....")

    file_metadata = read_input(file_name).head(2).tail(1) ##TESTING, remove head(10)

    # then query against indexd for the bucket URL of the file

    file_metadata_s3 = retrieve_s3_url_handler(file_metadata)

    responses = []

    chunk_size = 200

    for chunk in range(0, len(file_metadata_s3), chunk_size):
        runner_logger.info(
            f"Uploading chunk {round(chunk/chunk_size)+1} of {len(range(0, len(file_metadata_s3), chunk_size))} for files"
        )
        subresponses = uploader_handler(
            file_metadata_s3[chunk : chunk + chunk_size], "token.txt", upload_part_size_mb, n_processes
        )
        responses += subresponses

    responses_df = pd.DataFrame(responses, columns=["id", "file_name", "status"])

    # save response file

    responses_df.to_csv(
        f"GDC_file_upload_{project_id}_{dt}/{file_name}_upload_results.tsv",
        sep="\t",
        index=False,
    )

    # delete token file
    if os.path.exists("token.txt"):
        try:
            os.remove("token.txt")
        except:
            runner_logger.error(
            f"Cannot remove file token.txt."
        )
    else:
        runner_logger.warning(
            f"The file token.txt does not exist, cannot remove."
        )

    # folder upload
    folder_ul(
        local_folder=f"GDC_file_upload_{project_id}_{dt}",
        bucket=bucket,
        destination=runner + "/",
        sub_folder="",
    )