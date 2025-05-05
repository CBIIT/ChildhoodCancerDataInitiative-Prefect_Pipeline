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
from prefect_shell import ShellOperation
import pandas as pd
from time import sleep
from typing import Literal

# prefect dependencies
import boto3
from botocore.exceptions import ClientError
from prefect import flow, get_run_logger
from src.utils import get_time, file_dl, folder_ul, get_secret

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
        runner_logger.error(f"Error reading and parsing file {f_name}, check that headers exist: ['id', 'md5sum', 'file_size', 'file_name'].")
        sys.exit(1)

    if len(file_metadata) == 0:
        runner_logger.error(f"Error reading and parsing file {f_name}; empty file")
        sys.exit(1)

    return file_metadata


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

DropDownChoices = Literal["upload_files", "remove_old_working_dirs"]

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
        runner (str): Unique runner name
        secret_name_path (str): Path to AWS secrets manager where token hash stored
        secret_key_name (str): Authentication token string secret key name for file upload to GDC
        upload_part_size_mb (int): The upload part size in MB
        n_processes (int): The number of client connections to upload the files smaller than 7 GB
        process_type (str): Select whether to upload files or remove previous working dir instances from VM
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
    
    elif process_type == "upload_files":

        runner_logger.info(f">>> Setting up env ....")

        dt = get_time()

        token_dir = os.getcwd()  # directory where token and gdc-client will be stored

        working_dir = f"/usr/local/data/GDC_file_upload_{project_id}_{dt}"

        # make dir to download files from DCF to VM, to then upload to GDC
        if not os.path.exists(working_dir):
            os.mkdir(working_dir)

        # check that GDC API status is OK
        runner_logger.info(requests.get("https://api.gdc.cancer.gov/status").text)

        # download the input manifest file
        file_dl(bucket, manifest_path)

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

        # extract file name before the workflow starts
        file_name = os.path.basename(manifest_path)

        runner_logger.info(f">>> Reading input file {file_name} ....")

        file_metadata = read_input(file_name)

        # chdir to working path
        os.chdir(working_dir)
        
        # store results of uploads here
        responses = []

        # number of files to query S3 uploads and then upload consecutively in a flow
        chunk_size = 20

        runner_logger.info(f">>> Uploading files in manifest {file_name} ....")

        for chunk in range(0, len(file_metadata), chunk_size):
            # query against indexd for the bucket URL of the file
            runner_logger.info(
                f"Grabbing s3 URL metadata for chunk {round(chunk/chunk_size)+1} of {len(range(0, len(file_metadata), chunk_size))}"
            )

            # grab S3 URL data to download files to VM
            file_metadata_s3 = retrieve_s3_url(file_metadata[chunk:chunk+chunk_size])

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