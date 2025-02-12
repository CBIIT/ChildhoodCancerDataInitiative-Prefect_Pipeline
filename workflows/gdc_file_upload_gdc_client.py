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
import subprocess
import pandas as pd

# prefect dependencies
from prefect_shell import ShellOperation
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
                        runner_logger.info(
                            f"URL is: {s3_url}"
                        )

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
def uploader_handler(df: pd.DataFrame, gdc_client_exe_path: str, token_file: str, part_size: int, n_process: int):
    """Handles upload of chunk of files to GDC


    
    """

    runner_logger = get_run_logger()

    subresponses = []

    chunk_size = int(part_size * 1024 * 1024)

    big_chunk = int(20 * 1024 * 1024)

    for index, row in df.iterrows():
        # TODO: code in retries?
        try:
            f_bucket = row["s3_url"].split("/")[2]
            f_path = "/".join(row["s3_url"].split("/")[3:])
            f_name = os.path.basename(f_path)

            if f_name != row["file_name"]:
                runner_logger.warning(
                    f"Expected file name {row['file_name']} does not match observed file name in s3 url, {f_name}, not downloading file"
                )
            else:
            # trying to re-use file_dl() function
                file_dl(f_bucket, f_path)
                runner_logger.info(f"Downloaded file {f_name}")
        except:
            runner_logger.error(f"Cannot download file {row['file_name']}")
            subresponses.append([row["id"], row["file_name"], "NOT uploaded"])
            continue  # skip rest of attempt since no file

        # check that file exists
        if not os.path.isfile(row["file_name"]):
            runner_logger.error(
                f"File {row['file_name']} not copied over or found from URL {row['s3_url']}"
            )
            subresponses.append([row["id"], row["file_name"], "NOT uploaded", "File not copied from s3"])
            continue
        else:  # proceed to uploaded with API
            runner_logger.info(
                f"Attempting upload of file {row['file_name']} (UUID: {row['id']}), file_size {round(row['file_size']/(1024**3), 2)} GB ...."
            )
            try:
                """if row['file_size'] < 5368709120: #5GB file size cutoff:
                    process = subprocess.Popen(
                        [
                            "./gdc-client",
                            "upload",
                            row["id"],
                            "-t",
                            token_file,
                            "-c",
                            str(chunk_size),
                            "-n",
                            str(n_process),
                        ],
                        shell=False,
                        text=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                    )
                    response = ShellOperation(commands=[f"{gdc_client_exe_path} upload {row["id"]} -t {token_file} -c {chunk_size} -n {n_process}"]).run()
                else: # >= 5GB file size
                    process = subprocess.Popen(
                        [
                            "./gdc-client",
                            "upload",
                            row["id"],
                            "-t",
                            token_file,
                            "-c",
                            #str(chunk_size),
                            str(big_chunk),
                            "-n",
                            str(n_process), #8 connections
                        ],
                        shell=False,
                        text=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                    )
                
                std_out, std_err = process.communicate()"""
                response = ShellOperation(commands=[f"{gdc_client_exe_path} upload {row['id']} -t {token_file} -c {chunk_size} -n {n_process}"]).run()
                runner_logger.info(response)
                if f"upload finished for file {row['id']}" in response:
                    runner_logger.info(f"Upload finished for file {row['id']}")
                    subresponses.append([row["id"], row["file_name"], "uploaded", "success"])
                else:
                    #runner_logger.info(std_out)
                    #runner_logger.info(std_err)
                    runner_logger.info(response)
                    #subresponses.append([row["id"], row["file_name"], std_out, std_err])
                    subresponses.append([row["id"], row["file_name"], response, ""])
            except Exception as e:
                runner_logger.error(
                    f"Upload of file {row['file_name']} (UUID: {row['id']}) failed due to exception: {e}"
                )
                subresponses.append([row["id"], row["file_name"], "NOT uploaded", e])

            # delete file
            if os.path.exists(f_name):
                os.remove(f_name)
                runner_logger.info(f"The file {f_name} has been removed.")
            else:
                runner_logger.warning(
                    f"The file {f_name} does not exist, cannot remove."
                )

            # check delete
            if os.path.exists(f_name):
                runner_logger.error(
                    f"The file {f_name} still exists, error removing."
                )

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
    n_processes: int,
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
        n_processes (int): The number of client connections for multipart files uploads
    """

    # runner_logger setup

    runner_logger = get_run_logger()

    runner_logger.info(">>> Running GDC_FILE_UPLOAD.py ....")

    dt = get_time()

    token_dir = os.getcwd() #directory where token and gdc-client will be stored

    working_dir = f"/usr/local/data/GDC_file_upload_{project_id}_{dt}"

    os.mkdir(working_dir) #make download dir for files to upload

    runner_logger.info(requests.get("https://api.gdc.cancer.gov/status").text)

    # download the input manifest file
    file_dl(bucket, manifest_path)

    # save a token file to give gdc-client
    token = get_secret(secret_key_name).strip()

    with open("token.txt", "w+") as w:
        w.write(token)
    w.close()

    token_path = os.path.join(token_dir, "token.txt")

    # secure token file
    ShellOperation(commands=["chmod 600 token.txt"]).run()

    # download the gdc-client
    file_dl(bucket, gdc_client_path)

    # change gdc-client to executable
    ShellOperation(commands=["chmod 755 gdc-client"]).run()

    gdc_client_exe_path = os.path.join(token_dir, "gdc-client")

    # extract manifest file name
    file_name = os.path.basename(manifest_path)

    runner_logger.info(f">>> Reading input file {file_name} ....")

    file_metadata = read_input(file_name)

    # chdir to working path
    os.chdir(working_dir)

    responses = []

    chunk_size = 20

    for chunk in range(0, len(file_metadata), chunk_size):
        # query against indexd for the bucket URL of the file
        runner_logger.info(
            f"Grabbing s3 URL metadata for chunk {round(chunk/chunk_size)+1} of {len(range(0, len(file_metadata), chunk_size))}"
        )

        file_metadata_s3 = retrieve_s3_url(file_metadata[chunk:chunk+chunk_size])

        runner_logger.info(
            f"Uploading files in chunk {round(chunk/chunk_size)+1} of {len(range(0, len(file_metadata), chunk_size))}"
        )
        subresponses = uploader_handler(
            #file_metadata_s3[chunk : chunk + chunk_size],
            file_metadata_s3,
            gdc_client_exe_path,
            token_path,
            upload_part_size_mb,
            n_processes,
        )
        responses += subresponses

    responses_df = pd.DataFrame(
        responses, columns=["id", "file_name", "std_out", "std_err"]
    )

    # save response file

    responses_df.to_csv(
        f"{working_dir}/{file_name}_upload_results.tsv",
        sep="\t",
        index=False,
    )

    # delete token file
    if os.path.exists(token_path):
        try:
            os.remove(token_path)
        except:
            runner_logger.error(f"Cannot remove file token.txt.")
    else:
        runner_logger.warning(f"The file token.txt does not exist, cannot remove.")

    # folder upload
    folder_ul(
        local_folder=f"{working_dir}",
        bucket=bucket,
        destination=runner + "/",
        sub_folder="",
    )

    #r
