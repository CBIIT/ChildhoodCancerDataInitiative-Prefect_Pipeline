import os
import sys
from src.utils import CheckCCDI, set_s3_session_client, get_logger, get_date, get_time
from botocore.exceptions import ClientError
from urllib.parse import urlparse
from shutil import copy
import numpy as np
import pandas as pd
import json
import hashlib
from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from typing import TypeVar

DataFrame = TypeVar("DataFrame")

def parse_file_url_in_cds(url: str) -> tuple:
    parsed_url = urlparse(url)
    bucket_name = parsed_url.netloc
    object_key = parsed_url.path
    if object_key[0] == "/":
        object_key = object_key[1:]
    else:
        pass
    return bucket_name, object_key


def calculate_object_md5sum(s3_client, url) -> str:
    """Calculate md5sum of an object using url
    This function was modified based on https://github.com/jmwarfe/s3-md5sum/blob/main/s3-md5sum.py

    Example of url:
    s3://example-bucket/folder1/folder2/test_file.fastq.gz
    """
    bucket_name, object_key = parse_file_url_in_cds(url)
    # get obejct
    obj = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    obj_body = obj["Body"]

    # Initialize MD5 hash object
    md5_hash = hashlib.md5()
    # Read the object in chunks and update the MD5 hash
    for chunk in iter(lambda: obj_body.read(1024 * 1024), b""):
        md5_hash.update(chunk)
    return md5_hash.hexdigest()


def copy_object_parameter(url_in_cds: str, dest_bucket_path: str) -> dict:
    """Returns a dict that can be used as parameter for copy object using s3 client

    Example
    mydict=copy_object_parameter(origin_bucket=ccdi-validation, object_key="QL/file2.txt",
                                 dest_bucket_path="my-source-bucket/new_release")
    Expected Return
    {
        'Bucket': 'my-source-bucket',
        'CopySource': 'ccdi-validation/QL/file2.txt',
        'Key': 'new_release/QL/file2.txt'
     }
    """
    origin_bucket, object_key = parse_file_url_in_cds(url=url_in_cds)
    dest_bucket, dest_prefix = dest_bucket_path.split("/", 1)
    copysource = os.path.join(origin_bucket, object_key)
    dest_key = os.path.join(dest_prefix, object_key)
    param_dict = {"Bucket": dest_bucket, "CopySource": copysource, "Key": dest_key}
    return param_dict


def dest_object_url(url_in_cds: str, dest_bucket_path: str) -> str:
    """Returns an url of object in destination bucket
    object_key is the path of object in original bucket(without bucket name)

    Example:
    dest_url =  dest_object_url(object_key="QL/inputs/file1.txt", dest_bucket_path="new-bucket/release5")
    Expected Return
    "s3://new-bucket/release5/QL/inputs/file1.txt"
    """
    orgin_bucket, object_key = parse_file_url_in_cds(url=url_in_cds)
    dest_url = os.path.join("s3://", dest_bucket_path, object_key)
    return dest_url


@task(name="Copy an object file", tags=["concurrency-test"])
def copy_file_task(copy_parameter: dict, s3_client, logger) -> str:
    try:
        s3_client.copy_object(**copy_parameter)
        transfer_status = "Success"
    except ClientError as ex:
        transfer_status = "Fail"
        ex_code = ex.response["Error"]["Code"]
        ex_message = ex.response["Error"]["Message"]
        if ex_code == "NoSuchKey":
            object_name = ex.response["Error"]["Key"]
            logger.error(ex_code + ":" + ex_message + " " + object_name)
            #runner_logger.error(ex_code + ":" + ex_message + " " + object_name)
        elif ex_code == "NoSuchBucket":
            bucket_name = ex.response["Error"]["Code"]["BucketName"]
            logger.error(
                ex_code + ":" + ex_message + " Bucket name: " + bucket_name
            )
            #runner_logger.error(ex_code + ":" + ex_message + " Bucket name: " + bucket_name)
        else:
            logger.error(
                "Error info:\n" + json.dumps(ex.response["Error"], indent=4)
            )
            #runner_logger.error("Error info:\n" + json.dumps(ex.response["Error"], indent=4))
    #finally:
    #    s3_client.close()
    return transfer_status    


@flow(task_runner=ConcurrentTaskRunner(), name="Copy Files Concurrently")
def copy_file_flow(copy_parameter_list: list[dict], logger) -> list:
    """Copy of list of file concurrently"""
    s3_client = set_s3_session_client()
    transfer_status_list =  copy_file_task.map(copy_parameter_list, s3_client, logger)
    s3_client.close()
    return [i.result() for i in transfer_status_list]


@task(name="Compare md5sum values", tags=["concurrency-test"])
def compare_md5sum_task(first_url: str, second_url:str, s3_client) -> tuple:
    """Compares the md5sum of two objects"""
    first_md5sum = calculate_object_md5sum(s3_client=s3_client, url=first_url)
    second_md5sum = calculate_object_md5sum(s3_client=s3_client, url=second_url)
    #s3_client.close()
    if first_md5sum == second_md5sum:
        return (first_md5sum, second_md5sum, "Pass")
    else:
        return (first_md5sum, second_md5sum, "Fail")

@flow(task_runner=ConcurrentTaskRunner(), name="Compare md5sum Concurrently")
def compare_md5sum_flow(first_url_list: list[str], second_url_list: list[str]) -> list:
    """Compare md5sum of two list of urls concurrently"""
    s3_client = set_s3_session_client()
    compare_list = compare_md5sum_task.map(first_url_list, second_url_list, s3_client=s3_client)
    s3_client.close()
    return [i.result() for i in compare_list]


def add_md5sum_results(transfer_df: DataFrame, md5sum_results: list[tuple]) -> DataFrame:
    """Adds md5sum comparison results to df"""
    transfer_df["md5sum_check"] = [""] * transfer_df.shape[0]
    transfer_df["md5sum_before_cp"] = [""] * transfer_df.shape[0]
    transfer_df["md5sum_after_cp"] = [""] * transfer_df.shape[0]
    md5sum_df = pd.DataFrame(md5sum_results, columns=["first_md5sum","second_md5sum","md5sum_check"])
    transfer_df.loc[transfer_df["transfer_status"] == "Success", "md5sum_before_cp"] = md5sum_df["first_md5sum"].tolist()
    transfer_df.loc[transfer_df["transfer_status"] == "Success", "md5sum_after_cp"] = (
        md5sum_df["second_md5sum"].tolist()
    )
    transfer_df.loc[transfer_df["transfer_status"] == "Success", "md5sum_check"] = (
        md5sum_df["md5sum_check"].tolist()
    )
    return transfer_df

def list_to_chunks(mylist: list, chunk_len: int) -> list:
    """Break a list into a list of chunks"""
    chunks = [mylist[i * chunk_len : (i + 1) * chunk_len] for i in range((len(mylist) + chunk_len - 1) // chunk_len)]
    return chunks


@flow(
    name="Move Manifest Files",
    log_prints=True,
    flow_run_name="move-manifest-files-" + f"{get_time()}",
)
def move_manifest_files(manifest_path: str, dest_bucket_path: str):
    """Checks file node sheets and replaces the "file_url_in_cds"
    with a new url in prod bucket
    Returns a new manifest with new
    """
    # create a runner logger
    runner_logger = get_run_logger()

    # create logger
    logger = get_logger(loggername="file_mover_workflow", log_level="info")
    logger_filename = "file_mover_workflow_" + get_date() + ".log"

    # Create output file
    ccdi_file = CheckCCDI(ccdi_manifest=manifest_path)
    # identify file nodes
    file_nodes = ccdi_file.find_file_nodes()

    # create output file
    output_name = (
        os.path.basename(manifest_path).rsplit(".", 1)[0]
        + "_FileMoved"
        + get_date()
        + "."
        + os.path.basename(manifest_path).rsplit(".", 1)[1]
    )
    copy(src=manifest_path, dst=output_name)

    # create an empty df
    transfer_df = pd.DataFrame(
        columns=[
            "node",
            "url_before_cp",
            "url_after_cp",
            "transfer_status",
            "cp_object_parameter",
        ]
    )
    # we assume file nodes list exists
    for node in file_nodes:
        node_df = ccdi_file.read_sheet_na(sheetname=node)
        # in case this is an empty sheet and only "type" column is populated with one entry
        node_df_rmna = node_df.drop(columns=["type"]).dropna(axis=0, how="all")
        if node_df_rmna.shape[0] >= 1:
            logger.info(
                f"Number of file objects in node {node}: {node_df_rmna.shape[0]}"
            )
            runner_logger.info(
                f"Number of file objects in node {node}: {node_df_rmna.shape[0]}"
            )
            node_file_urls = node_df["file_url_in_cds"].tolist()
            new_node_file_urls = [
                dest_object_url(url_in_cds=i, dest_bucket_path=dest_bucket_path)
                for i in node_file_urls
            ]
            node_file_param_dict = [
                copy_object_parameter(url_in_cds=i, dest_bucket_path=dest_bucket_path)
                for i in node_file_urls
            ]
            node_transfer_df = pd.DataFrame(
                {
                    "node": [node] * node_df.shape[0],
                    "url_before_cp": node_file_urls,
                    "url_after_cp": new_node_file_urls,
                    "cp_object_parameter": node_file_param_dict,
                    "transfer_status": [np.nan] * node_df.shape[0],
                }
            )
            # concatenate node_transfer_df to transfer_df
            transfer_df = pd.concat([transfer_df, node_transfer_df], ignore_index=True)
            # replace the old url with new url in the output
            node_df["file_url_in_cds"] = new_node_file_urls
            with pd.ExcelWriter(
                output_name, mode="a", engine="openpyxl", if_sheet_exists="overlay"
            ) as writer:
                node_df.to_excel(
                    writer, sheet_name=node, index=False, header=False, startrow=1
                )
        else:
            logger.info(f"Number of file objects in node {node}: 0")
            runner_logger.info(f"Number of file objects in node {node}: 0")
    logger.info(
        f"A CCDI Excel manifest with new object urls was generated {output_name}"
    )
    runner_logger.info(
        f"A CCDI Excel manifest with new object urls was generated {output_name}"
    )

    runner_logger.info(f"transfer_df counts: {transfer_df.shape[0]}")
    # for index, row in transfer_df.iterrows():
    #    runner_logger.info(json.dumps(row["cp_object_parameter"], indent=4))

    # File transfer starts
    logger.info(f"Start transfering files to destination bucket {dest_bucket_path}")
    runner_logger.info(
        f"Start transfering files to destination bucket {dest_bucket_path}"
    )
    transfer_parameter_list = transfer_df["cp_object_parameter"].tolist()
    # break transfer_parameter_list into chunks with 50
    transfer_chuncks = list_to_chunks(mylist=transfer_parameter_list, chunk_len=100)
    runner_logger.info(f"Copying file will be processed into {len(transfer_chuncks)} chunks")
    transfer_status_list = []
    for h in transfer_chuncks:
        h_transfer_status_list = copy_file_flow(h, logger)
        transfer_status_list.extend(h_transfer_status_list)

    # transfer_status_list = copy_file_flow(transfer_parameter_list, logger)
    transfer_df["transfer_status"] = transfer_status_list
    # if there is failed transfer
    if "Fail" in transfer_df["transfer_status"].value_counts().keys():
        failed_transfer_counts = transfer_df["transfer_status"].value_counts()["Fail"]
        success_transfer_counts = transfer_df.shape[0] - failed_transfer_counts
        logger.warning(f"Failed to transfer {failed_transfer_counts} files")
        runner_logger.warning(f"Failed to transfer {failed_transfer_counts} files")
        logger.info(f"Successfully transferred {success_transfer_counts} files")
        runner_logger.info(f"Successfully transferred {success_transfer_counts} files")
    else:
        logger.info(f"Successfully transferred {transfer_df.shape[0]} files")
        runner_logger.info(f"Successfully transferred {transfer_df.shape[0]} files")

    # Check md5sum of file before transfer and after transfer
    # md5sum check only checks files which have been successfully copied between buckets
    logger.info("Start checking md5sum before and after transfer")
    runner_logger.info("Start checking md5sum before and after transfer")
    # only checking md5sum for all success transfers. Tasks will be running concurrently
    urls_before_transfer = transfer_df.loc[
        transfer_df["transfer_status"] == "Success", "url_before_cp"
    ].tolist()
    urls_after_transfer = transfer_df.loc[
        transfer_df["transfer_status"] == "Success", "url_after_cp"
    ].tolist()
    # url list needs to be break into chunks
    urls_before_chunks = list_to_chunks(mylist=urls_before_transfer, chunk_len=100)
    urls_after_chunks = list_to_chunks(mylist=urls_after_transfer, chunk_len=100)
    md5sum_compare_result = []
    runner_logger.info(
        f"Md5sum check will be processed into {len(urls_before_chunks)} chunks"
    )
    for j in range(len(urls_before_chunks)):
        j_md5sum_compare_result = compare_md5sum_flow(first_url_list=urls_before_chunks[j], second_url_list=urls_after_chunks[j])
        md5sum_compare_result.extend(j_md5sum_compare_result)

    # add md5sum comparison result to transfer_df
    transfer_df = add_md5sum_results(
        transfer_df=transfer_df, md5sum_results=md5sum_compare_result
    )
    # log md5sum comparison check
    passed_md5sum_check_ct = sum(transfer_df["md5sum_check"] == "Pass")
    failed_md5sum_check_ct = sum(transfer_df["md5sum_check"] == "Fail")
    logger.info(f"md5sum check passed files: {passed_md5sum_check_ct}")
    runner_logger.info(f"md5sum check passed files: {passed_md5sum_check_ct}")
    if failed_md5sum_check_ct >= 1:
        logger.warning(f"md5sum check failed files count: {failed_md5sum_check_ct}")
        runner_logger.warning(f"md5sum check failed files count: {failed_md5sum_check_ct}")
    else:
        pass

    # write transfer summary table
    mover_summary_table = (
        os.path.basename(manifest_path).rsplit(".", 1)[0]
        + "_FileMover_Summary_" + get_date() + ".tsv"
    )
    transfer_df[
        [
            "node",
            "url_before_cp",
            "url_after_cp",
            "transfer_status",
            "md5sum_check",
            "md5sum_before_cp",
            "md5sum_after_cp",
        ]
    ].to_csv(mover_summary_table, sep="\t", index=False)
    logger.info(f"File mover summary table was created {mover_summary_table}")
    runner_logger.info(f"File mover summary table was created {mover_summary_table}")
    del transfer_df
    return output_name, logger_filename, mover_summary_table
