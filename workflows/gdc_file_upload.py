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
from prefect_shell import ShellOperation
import pandas as pd
from time import sleep
from typing import Literal

# prefect dependencies
import boto3
from botocore.exceptions import ClientError
from prefect import flow, task, get_run_logger
from src.utils import get_time, file_dl, folder_ul, get_secret
from src.gdc_utils import retrieve_current_nodes

@task(name="env_setup")
def env_setup(bucket, gdc_client_path, project_id, secret_key_name, secret_name_path):
    """Setup gdc-client and other env objects

    Args:
        bucket (str): S3 bucket name
        gdc_client_path (str): path to gdc-client in S3 bucket
        project_id (str): Project ID to query and upload nodes for
        secret_key_name (str): Secret Key Name
        secret_name_path (str): Secret Key Path

    Returns:
        str: path to token file to pass to gdc-client
        str: directory where token and gdc-client will be stored
        str: path to gdc-client
        str: path to working directory where files downloaded 
    """

    runner_logger = get_run_logger()

    runner_logger.info(f">>> Setting up env ....")

    dt = get_time()

    token_dir = os.getcwd()  # directory where token and gdc-client will be stored

    working_dir = f"/usr/local/data/GDC_file_upload_{project_id}_{dt}"

    # make dir to download files from DCF to VM, to then upload to GDC
    if not os.path.exists(working_dir):
        os.mkdir(working_dir)

    # save a token file to give gdc-client
    token = get_secret(secret_name_path, secret_key_name).strip()

    # save token locally since gdc-client takes a token file as input
    # not the hash directly
    with open("token.txt", "w+") as w:
        w.write(token)
    w.close()

    # secure token file
    ShellOperation(commands=["chmod 600 token.txt"]).run()

    # path to token file to provide to gdc-client for uploads
    token_path = os.path.join(token_dir, "token.txt")

    # download the gdc-client
    file_dl(bucket, gdc_client_path)

    # change gdc-client to executable
    ShellOperation(commands=["chmod 755 gdc-client"]).run()

    # path to gdc-client for uploads
    gdc_client_exe_path = os.path.join(token_dir, "gdc-client")

    return token_path, token_dir, gdc_client_exe_path, working_dir

@task(name="read_input_manifest_{file_path}")
def read_input(file_path: str):
    """Read in TSV file and extract file_name, md5sum and file_size columns

    Args:
        file_path (str): path to input file that contains required cols

    Returns:
        pd.DataFrame: DataFrame with extracted necessary metadata
    """

    runner_logger = get_run_logger()

    file_metadata = pd.read_csv(file_path, sep="\t")

    required_cols = ["file_url", "md5sum", "file_size", "file_name"]

    # check if required columns exist in the dataframe
    for col in required_cols:
        if col not in file_metadata.columns:
            raise ValueError(f"Missing required column: {col}")
    
    for col in required_cols:
        if file_metadata[col].isnull().any():
            raise ValueError(f"Missing values in required column: {col}")


    return file_metadata

@task(name="matching_uuid_task", log_prints=True)
def matching_uuid(manifest_df: pd.DataFrame, already_submitted: pd.DataFrame):
    """Retrieve UUIDs from GDC and match to file rows by md5sum and file_name"""

    print(manifest_df)

    print(already_submitted)

    #merge 2 dataframes on md5sum and file_name
    manifest_df = manifest_df.merge(already_submitted, on=["md5sum", "file_name"], how="left", suffixes=("", "_already_submitted"))
    print(manifest_df)
    print(manifest_df.columns)

    #filter out files in already_submitted with file_state == "validated", status column = "already uploaded, skip"

    #filter out files in manifest_df that do not have a matching md5sum and file_name, status column = "metadata not found, skip"

    #match id in already_submitted to manifest_df by file_name and md5sum, status column left blank



    return None


@flow(
    name="gdc_upload_file_upload",
    log_prints=True,
    flow_run_name="gdc_upload_file_upload_" + f"{get_time()}",
)
def uploader_handler(df: pd.DataFrame, gdc_client_exe_path: str, token_file: str, part_size: int, n_process: int):
    """Handles upload of chunk of files to GDC

    Args:
        df (pd.DataFrame): DataFrame of metadata for files to upload
        gdc_client_exe_path (str): Path to S3 location where Linux gdc-client package is located
        token_file (str): Name of VM stored instance of token
        part_size (int): Size (in megabytes) that file chunks should be uploaded in
        n_process (int): Number of concurrent connections to upload file

    Returns:
        list: A list of lists with file upload results
    """

    runner_logger = get_run_logger()

    # record upload results here
    subresponses = []

    for index, row in df.iterrows():
        # attempt to download file from s3 location to VM
        # to then upload with gdc-client
        try:
            runner_logger.info(f"The S3 URL is {row['file_url']}")

            f_bucket = row["file_url"].split("/")[2]
            f_path = "/".join(row["file_url"].split("/")[3:])
            f_name = os.path.basename(f_path)

            runner_logger.info(f"The bucket is {f_bucket}")
            runner_logger.info(f"The path is {f_path}")
            runner_logger.info(f"The file name is {f_name}")
            
            if f_name != row["file_name"]:
                runner_logger.warning(
                    f"Expected file name {row['file_name']} does not match observed file name in s3 url, {f_name}, not downloading file"
                )
            else:

                # download file to VM
                file_dl(f_bucket, f_path)
                runner_logger.info(f"Downloaded file {f_name}")
        except:
            runner_logger.error(f"Cannot download file {row['file_name']}")
            subresponses.append([row["id"], row["file_name"], "NOT uploaded", ""])
            continue  # skip rest of attempt since no file

        # check that file exists
        if not os.path.isfile(row["file_name"]):
            runner_logger.error(
                f"File {row['file_name']} not copied over or found from URL {row['file_url']}"
            )
            subresponses.append([row["id"], row["file_name"], "NOT uploaded", "File not copied from s3"])
            continue # ignore rest of function since file not downloaded
        else:  # proceed to uploaded with API
            runner_logger.info(
                f"Attempting upload of file {row['file_name']} (UUID: {row['id']}), file_size {round(row['file_size']/(1024**3), 2)} GB ...."
            )
            try:
                # check if part size uploads file in < 1000 connections
                if row['file_size'] / (part_size * 1024 * 1024) > 1000:
                    #calculate needed part size
                    adequate_part_size = round(row['file_size'] / 1000 / 1024 / 1024) + 2 
                    runner_logger.info(f"Part size too small to upload successfully, updating part size to {adequate_part_size} MB for this file.")
                    chunk_size = int(adequate_part_size * 1024 * 1024)
                else:
                    chunk_size = int(part_size * 1024 * 1024)

                #upload files with gdc-client to maximize efficient upload
                response = ShellOperation(
                    commands=[
                        f"{gdc_client_exe_path} upload {row['id']} -t {token_file} -c {chunk_size} -n {n_process}"
                    ],
                    stream_output=False,
                ).run()

                # check uploads results from streamed output
                if f"upload finished for file {row['id']}" in response[-1]:
                    runner_logger.info(f"Upload finished for file {row['id']}")
                    subresponses.append([row["id"], row["file_name"], "uploaded", "success"])
                else:
                    runner_logger.warning(f"Upload not successful for file {row['id']}")
                    subresponses.append([row["id"], row["file_name"], "NOT uploaded", "Failure duing upload"])
            except Exception as e:
                runner_logger.error(
                    f"Upload of file {row['file_name']} (UUID: {row['id']}) failed due to exception: {e}"
                )
                subresponses.append([row["id"], row["file_name"], "NOT uploaded", e])

            # delete file from VM
            if os.path.exists(f_name):
                os.remove(f_name)
                runner_logger.info(f"The file {f_name} has been removed.")
            else:
                runner_logger.warning(
                    f"The file {f_name} does not exist, cannot remove."
                )

            # check if file deleted from VM
            if os.path.exists(f_name):
                runner_logger.error(
                    f"The file {f_name} still exists, error removing."
                )

    return subresponses

DropDownChoices = Literal["upload_files", "remove_old_working_dirs", "check_status"]

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
    node_type: str,
    runner: str,
    secret_name_path: str,
    secret_key_name: str,
    upload_part_size_mb: int,
    n_processes: int,
    process_type: DropDownChoices,
):
    """CCDI Pipeline to Upload files to GDC

    Args:
        bucket (str): Bucket name of where the manifest is located in and the response output goes to
        project_id (str): GDC Project ID to submit to (e.g. CCDI-MCI, TARGET-AML)
        manifest_path (str): File path of the CCDI file manifest in bucket
        gdc_client_path (str): Path to GDC client to download to VM
        node_type (str): Node type to submit to GDC (e.g. submitted_aligned_reads, clinical_supplement, etc.)
        runner (str): Unique runner name
        secret_name_path (str): Path to AWS secrets manager where token hash stored
        secret_key_name (str): Authentication token string secret key name for file upload to GDC
        upload_part_size_mb (int): The upload part size in MB
        n_processes (int): The number of client connections to upload the files smaller than 7 GB
        process_type (str): Select whether to upload files, remove previous working dir instances from VM or check GDC API status
    """

    # runner_logger setup

    runner_logger = get_run_logger()

    runner_logger.info(">>> Running GDC_FILE_UPLOAD.py ....")

    if process_type == "remove_old_working_dirs": # remove previous GDC_file_upload working dirs to clear space

        runner_logger.info(
            ShellOperation(
                commands=[
                    "rm -r /usr/local/data/GDC_file_upload_*",
                    "ls -l /usr/local/data/",  # confirm removal of GDC_file_upload working dirs
                ]
            ).run()
        )
    
    elif process_type == "check_status":

        runner_logger.info(f">>> Checking GDC API status ....")

        # check that GDC API status is OK
        runner_logger.info(requests.get("https://api.gdc.cancer.gov/status").text)

        # check that GDC API status is OK
        runner_logger.info(requests.get("https://api.gdc.cancer.gov/v0/submissions").text)

        # check that GDC API status is OK
        runner_logger.info(requests.get("https://api.gdc.cancer.gov/v0/projects").text)
    
    
    elif process_type == "upload_files":

        # setup env
        token_path, token_dir, gdc_client_exe_path, working_dir = env_setup(bucket, gdc_client_path, project_id, secret_key_name, secret_name_path)

        # download the input manifest file
        file_dl(bucket, manifest_path)

        # extract file name before the workflow starts
        file_name = os.path.basename(manifest_path)

        runner_logger.info(f">>> Reading input file {file_name} ....")

        file_metadata = read_input(file_name)

        # chdir to working path
        os.chdir(working_dir)
        
        # store results of uploads here
        responses = []

        #TODO: perform query for UUIDs and files already uploaded to GDC
        already_uploaded = retrieve_current_nodes(
            project_id=project_id,
            node_type=node_type,
            secret_name_path=secret_name_path,
            secret_key_name=secret_key_name,
        )

        # compare md5sum and file_name to already uploaded files
        already_uploaded_df = pd.DataFrame(already_uploaded)

        print(already_uploaded_df.to_dict(orient="records"))

        matching_uuid(file_metadata, already_uploaded_df)


        # number of files to query S3 uploads and then upload consecutively in a flow
        chunk_size = 20

        runner_logger.info(f">>> Uploading files in manifest {file_name} ....")

        #exclude for testing for now
        """for chunk in range(0, len(file_metadata), chunk_size):
            # query against indexd for the bucket URL of the file
            runner_logger.info(
                f"Matching UUIDs for chunk {round(chunk/chunk_size)+1} of {len(range(0, len(file_metadata), chunk_size))}"
            )

            # grab S3 URL data to download files to VM
            file_metadata_s3 = matching_uuid(file_metadata[chunk:chunk+chunk_size])

            runner_logger.info(
                f"Uploading files in chunk {round(chunk/chunk_size)+1} of {len(range(0, len(file_metadata), chunk_size))}"
            )
            subresponses = uploader_handler(
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
        )"""

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

        # change back to starting dir
        os.chdir(token_dir)

        # remove working dir
        if os.path.exists(working_dir):
            try:
                ShellOperation(
                    commands=[
                        f"rm -r {working_dir}",
                    ]
                ).run()
            except Exception as e:
                runner_logger.error(f"Cannot remove working path {working_dir}: {e}.")
        else:
            runner_logger.warning(f"The path {working_dir} does not exist, cannot remove.")

    else:
        runner_logger.error(f"The submitted process_type {process_type} not one of ['upload_files', 'remove_old_working_dirs']")

