import os
from src.utils import (
    CheckCCDI,
    set_s3_session_client,
    get_logger,
    get_date,
    get_time,
    calculate_object_md5sum_new,
    file_ul,
)
from botocore.exceptions import (
    ClientError,
    ReadTimeoutError,
    ConnectTimeoutError,
    ResponseStreamingError,
)
from urllib.parse import urlparse
from shutil import copy
import numpy as np
import pandas as pd
import json
from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from prefect.cache_policies import NO_CACHE
from typing import TypeVar
import ast
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

DataFrame = TypeVar("DataFrame")


def parse_file_url(url: str) -> tuple:
    """Parse an s3 uri into bucket name and key"""
    # add s3:// if missing
    if not url.startswith("s3://"):
        url = "s3://" + url
    else:
        pass
    parsed_url = urlparse(url)
    bucket_name = parsed_url.netloc
    object_key = parsed_url.path
    # this is in case url is only bucket name, such as s3://my-bucket
    if object_key == "":
        object_key = "/"
    else:
        pass
    if object_key[0] == "/":
        object_key = object_key[1:]
    else:
        pass
    return bucket_name, object_key


def copy_object_parameter(url_in_cds: str, dest_bucket_path: str) -> dict:
    """Returns a dict that can be used as parameter for copy object using s3 client

    Example
    mydict=copy_object_parameter(url_in_cds="s3://ccdi-validation/QL/file2.txt",
        dest_bucket_path="my-source-bucket/new_release")
    Expected Return
    {
        'Bucket': 'my-source-bucket',
        'CopySource': 'ccdi-validation/QL/file2.txt',
        'Key': 'new_release/QL/file2.txt'
    }
    """
    origin_bucket, object_key = parse_file_url(url=url_in_cds)
    if "/" not in dest_bucket_path:
        dest_bucket_path = dest_bucket_path + "/"
    else:
        pass
    dest_bucket, dest_prefix = dest_bucket_path.split("/", 1)
    copysource = os.path.join(origin_bucket, object_key)
    dest_key = os.path.join(dest_prefix, object_key)
    param_dict = {"Bucket": dest_bucket, "CopySource": copysource, "Key": dest_key}
    return param_dict


def dest_object_url(url_in_cds: str, dest_bucket_path: str) -> str:
    """Returns an url of object in destination bucket
    object_key is the path of object in original bucket(without bucket name)

    Example:
    dest_url =  dest_object_url(url_in_cds="s3://ccdi-validation/QL/inputs/file1.txt", dest_bucket_path="new-bucket/release5")
    Expected Return
    "s3://new-bucket/release5/QL/inputs/file1.txt"
    """
    orgin_bucket, object_key = parse_file_url(url=url_in_cds)
    dest_url = os.path.join("s3://", dest_bucket_path, object_key)
    return dest_url


def copy_large_file(copy_parameter: dict, file_size: int, s3_client, logger) -> None:
    # collect source bucket, source key, destination bucket, and destination key
    source_bucket, source_key = copy_parameter["CopySource"].split("/", 1)
    copy_source = copy_parameter["CopySource"]
    destination_bucket = copy_parameter["Bucket"]
    destination_key = copy_parameter["Key"]

    # Define the size of each part (100MB)
    part_size = 100 * 1024 * 1024

    # Calculate the number of parts required
    num_parts = int(file_size / part_size) + 1

    # Initialize the multipart upload and get upload_id
    upload_id = s3_client.create_multipart_upload(
        Bucket=destination_bucket, Key=destination_key
    )["UploadId"]

    # Initialize parts list
    parts = []

    def upload_part(part_number):
        start_byte = (part_number - 1) * part_size
        end_byte = min(part_number * part_size - 1, file_size - 1)
        byte_range = f"bytes={start_byte}-{end_byte}"

        response = s3_client.upload_part_copy(
            Bucket=destination_bucket,
            Key=destination_key,
            CopySource={"Bucket": source_bucket, "Key": source_key},
            PartNumber=part_number,
            UploadId=upload_id,
            CopySourceRange=byte_range,
        )

        return {"PartNumber": part_number, "ETag": response["CopyPartResult"]["ETag"]}

    try:
        # Upload parts concurrently
        with ThreadPoolExecutor(max_workers=50) as executor:
            future_to_part = {executor.submit(upload_part, part_number): part_number for part_number in range(1, num_parts + 1)}
            for future in as_completed(future_to_part):
                part_number = future_to_part[future]
                try:
                    part = future.result()
                    parts.append(part)
                except Exception as e:
                    logger.error(f"Error uploading part {part_number}: {e}")
                    raise

        # Sort parts by PartNumber before completing the multipart upload
        parts.sort(key=lambda x: x["PartNumber"])

        # Complete the multipart upload
        s3_client.complete_multipart_upload(
            Bucket=destination_bucket,
            Key=destination_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

        logger.info(
            f"Multipart transferred successfully from {copy_source} to {destination_bucket}/{destination_key}"
        )
        print(
            f"Multipart transferred successfully from {copy_source} to {destination_bucket}/{destination_key}"
        )
        transfer_status = "Success"
    except Exception as e:
        # Abort the multipart upload if an error occurs
        s3_client.abort_multipart_upload(
            Bucket=destination_bucket, Key=destination_key, UploadId=upload_id
        )
        logger.info(f"Multipart copy Failed to copy file {copy_source}: {e}")
        print(f"Multipart copy Failed to copy file {copy_source}: {e}")
        transfer_status = "Fail"
    return transfer_status


def copy_file_by_size(
    copy_parameter: dict, file_size: int, s3_client, logger, runner_logger
):
    """Copy objects between two locations defined by copy_parameter

    Uses regular s3_client.copy_object for files less then 5GB, and
    Multipart upload for files larger than 5GB

    runner_logger is only used for error messages. logger is used for both regular
    info and error/warning messages
    """
    copy_source = copy_parameter["CopySource"]
    # test if file_size larger than 5GB, 5*1024*1024*1024
    if file_size > 5368709120:
        # runner_logger.info(
        #    f"File size {file_size} of {copy_source} larger than 5GB. Start multipart upload process"
        # )
        logger.info(
            f"File size {file_size} of {copy_source} larger than 5GB. Start multipart upload process"
        )
        transfer_status = copy_large_file(
            copy_parameter=copy_parameter,
            file_size=file_size,
            s3_client=s3_client,
            logger=logger,
        )
    else:
        # runner_logger.info(
        #    f"File size {file_size} of {copy_source} less than 5GB. Copy object file using copy_object of s3_client object"
        # )
        logger.info(
            f"File size {file_size} of {copy_source} less than 5GB. Copy object file using copy_object of s3_client object"
        )
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
                runner_logger.error(ex_code + ":" + ex_message + " " + object_name)
            elif ex_code == "NoSuchBucket":
                bucket_name = ex.response["Error"]["Code"]["BucketName"]
                logger.error(
                    ex_code + ":" + ex_message + " Bucket name: " + bucket_name
                )
                runner_logger.error(
                    ex_code + ":" + ex_message + " Bucket name: " + bucket_name
                )
            else:
                logger.error(
                    "Error info:\n" + json.dumps(ex.response["Error"], indent=4)
                )
                runner_logger.error(
                    "Error info:\n" + json.dumps(ex.response["Error"], indent=4)
                )
    return transfer_status


@task(
    name="Copy an object file",
    tags=["{concurrency_tag}"],
    retries=3,
    retry_delay_seconds=1,
    log_prints=True,
    cache_policy=NO_CACHE,
)
def copy_file_task(copy_parameter: dict, s3_client, logger, runner_logger, concurrency_tag) -> str:
    """Copy objects between two locations defined by copy_parameter

    It checks if the file has been transferred (Key and object Content length/size) before trasnfer process
    Function copy_file_by_size is executed only if
        - The file hasn't been transferred
        - the destination object size doesn't match to the source size
    """
    copy_source = copy_parameter["CopySource"]
    source_bucket, source_key = copy_source.split("/", 1)
    object_response = s3_client.head_object(Bucket=source_bucket, Key=source_key)
    file_size = object_response["ContentLength"]

    # check if the destination object has been copied or no
    try:
        dest_object = s3_client.head_object(
            Bucket=copy_parameter["Bucket"], Key=copy_parameter["Key"]
        )
        if dest_object["ContentLength"] == file_size:
            logger.info(
                f"File {copy_source} had already been copied to destination bucket path. Skip"
            )
            print(
                f"File {copy_source} had already been copied to destination bucket path. Skip"
            )
            transfer_status = "Success"
        else:
            # if the destin object size is different from source, copy the object
            transfer_status = copy_file_by_size(
                copy_parameter=copy_parameter,
                file_size=file_size,
                s3_client=s3_client,
                logger=logger,
                runner_logger=runner_logger,
            )

    except Exception as ex:
        # if the destin object does not exist, copy oject
        transfer_status = copy_file_by_size(
            copy_parameter=copy_parameter,
            file_size=file_size,
            s3_client=s3_client,
            logger=logger,
            runner_logger=runner_logger,
        )
    return transfer_status


@flow(task_runner=ConcurrentTaskRunner(), name="Copy Files Concurrently")
def copy_file_flow(copy_parameter_list: list[dict], logger, runner_logger, concurrency_tag) -> list:
    """Copy of list of file concurrently"""
    s3_client = set_s3_session_client()
    
    # Create an empty list to store the tasks.
    transfer_status_list = []
    
    for params in copy_parameter_list:
        # Submit the task with a delay of 0.25 seconds
        transfer_status_list.append(copy_file_task.submit(params, s3_client, logger, runner_logger, concurrency_tag))
        
        # Throttle task submission with a 0.25-second delay
        time.sleep(0.25)
    
    s3_client.close()
    
    return [i.result() for i in transfer_status_list]


@task(
    name="Compare md5sum values",
    tags=["{concurrency_tag}"],
    retries=3,
    retry_delay_seconds=1,
    log_prints=True,
    cache_policy=NO_CACHE,
)
def compare_md5sum_task(first_url: str, second_url: str, s3_client, logger, concurrency_tag) -> tuple:
    """Compares the md5sum of two objects

    compare_md5sum_task can return three status for comparison
    "Pass", "Fail", and "Error"
    """
    try:
        first_md5sum = calculate_object_md5sum_new(s3_client=s3_client, url=first_url)
        second_md5sum = calculate_object_md5sum_new(s3_client=s3_client, url=second_url)
        if first_md5sum == second_md5sum:
            return (first_url, first_md5sum, second_md5sum, "Pass")
        else:
            return (first_url, first_md5sum, second_md5sum, "Fail")
    except ClientError as ec:
        logger.error(
            f"ClientError occurred while calculating md5sum of {first_url} and {second_url}: {ec}"
        )
        return (first_url, "", "", "Error")
    except ReadTimeoutError as er:
        logger.error(
            f"ReadTimeoutError occurred while calculating  md5sum of {first_url} and {second_url}: {er}"
        )
        return (first_url, "", "", "Error")
    except ConnectTimeoutError as econ:
        logger.error(
            f"ConnectTimeoutError occurred while calculating  md5sum of {first_url} and {second_url}: {econ}"
        )
        return (first_url, "", "", "Error")
    except ResponseStreamingError as erres:
        logger.error(
            f"ConnectTimeoutError occurred while calculating  md5sum of {first_url} and {second_url}: {erres}"
        )
        return (first_url, "", "", "Error")
    except Exception as ex:
        logger.error(
            f"Error occurred while calculating  md5sum of {first_url} and {second_url}: {ex}"
        )
        return (first_url, "", "", "Error")


@flow(task_runner=ConcurrentTaskRunner(), name="Compare md5sum Concurrently")
def compare_md5sum_flow(first_url_list: list[str], second_url_list: list[str], concurrency_tag: str) -> list:
    """Compare md5sum of two list of urls concurrently"""
    s3_client = set_s3_session_client()
    runner_logger = get_run_logger()

    # Create an empty list to store the tasks.
    compare_list = []

    for i in range(len(first_url_list)):
        # Submit the task with a delay of 0.25 seconds
        compare_list.append(
            compare_md5sum_task.submit(
                first_url_list[i], second_url_list[i], s3_client, runner_logger, concurrency_tag
            )
        )

        # Throttle task submission with a 0.25-second delay
        time.sleep(0.25)
    # Wait for all tasks to complete and collect the results        

    s3_client.close()
    return [i.result() for i in compare_list]


def add_md5sum_results(
    transfer_df: DataFrame, md5sum_results: list[tuple]
) -> DataFrame:
    """Adds md5sum comparison results to df"""
    transfer_df["md5sum_check"] = [""] * transfer_df.shape[0]
    transfer_df["md5sum_before_cp"] = [""] * transfer_df.shape[0]
    transfer_df["md5sum_after_cp"] = [""] * transfer_df.shape[0]
    md5sum_df = pd.DataFrame(
        md5sum_results, columns=["first_md5sum", "second_md5sum", "md5sum_check"]
    )
    transfer_df.loc[transfer_df["transfer_status"] == "Success", "md5sum_before_cp"] = (
        md5sum_df["first_md5sum"].tolist()
    )
    transfer_df.loc[transfer_df["transfer_status"] == "Success", "md5sum_after_cp"] = (
        md5sum_df["second_md5sum"].tolist()
    )
    transfer_df.loc[transfer_df["transfer_status"] == "Success", "md5sum_check"] = (
        md5sum_df["md5sum_check"].tolist()
    )
    return transfer_df


def list_to_chunks(mylist: list, chunk_len: int) -> list:
    """Break a list into a list of chunks"""
    chunks = [
        mylist[i * chunk_len : (i + 1) * chunk_len]
        for i in range((len(mylist) + chunk_len - 1) // chunk_len)
    ]
    return chunks

def int_results_recorder(transfer_df: DataFrame, md5sum_results: list[list]) -> DataFrame:
    """Record the intermediate results of md5sum check"""
    int_df = pd.DataFrame(md5sum_results)
    int_df.columns = ["url_before_cp", "md5sum_before_cp", "md5sum_after_cp", "md5sum_check"]
    transfer_parse = transfer_df[transfer_df.url_before_cp.isin(int_df.url_before_cp)]
    int_df = transfer_parse.merge(int_df, on="url_before_cp")
    return int_df

@flow(
    name="Move Manifest Files",
    log_prints=True,
    flow_run_name="move-manifest-files-" + f"{get_time()}",
)
def move_manifest_files(manifest_path: str, dest_bucket_path: str, intermediate_out: str, bucket: str) -> tuple:
    """Checks file node sheets and replaces the "file_url"
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

    # create filename for summary table
    # write transfer summary table
    mover_summary_table = (
        os.path.basename(manifest_path).rsplit(".", 1)[0]
        + "_FileMover_Summary_"
        + get_date()
        + ".tsv"
    )

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
            node_file_urls = node_df["file_url"].tolist()
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
            node_df["file_url"] = new_node_file_urls
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
    logger.info(f"transfer_df counts: {transfer_df.shape[0]}")

    # drop duplicates need to convert cp_object_parameter into str first
    transfer_df["cp_object_parameter"] = transfer_df["cp_object_parameter"].astype(str)
    transfer_df.drop_duplicates(ignore_index=True, keep="first", inplace=True)
    transfer_df["cp_object_parameter"] = transfer_df["cp_object_parameter"].apply(ast.literal_eval)
    
    runner_logger.info(f"unique uri transfer counts: {transfer_df.shape[0]}")
    logger.info(f"unique uri transfer counts: {transfer_df.shape[0]}")

    # File transfer starts
    logger.info(f"Start transfering files to destination bucket {dest_bucket_path}")
    runner_logger.info(
        f"Start transfering files to destination bucket {dest_bucket_path}"
    )
    transfer_parameter_list = transfer_df["cp_object_parameter"].tolist()
    # break transfer_parameter_list into chunks with 50
    transfer_chuncks = list_to_chunks(mylist=transfer_parameter_list, chunk_len=100)
    runner_logger.info(
        f"Copying file will be processed into {len(transfer_chuncks)} chunks"
    )
    transfer_status_list = []
    for h in transfer_chuncks:
        try:
            h_transfer_status_list = copy_file_flow(h, logger, runner_logger)
            transfer_status_list.extend(h_transfer_status_list)


        except Exception as ex:
            logger.error(f"Error occurred while copying files: {ex}")
            runner_logger.error(f"Error occurred while copying files: {ex}")
            transfer_status_list.extend(["Fail"] * len(h))

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

    try:
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
        chunk_len = 100
        int_results = [] #record int results here

        urls_before_chunks = list_to_chunks(mylist=urls_before_transfer, chunk_len=chunk_len)
        urls_after_chunks = list_to_chunks(mylist=urls_after_transfer, chunk_len=chunk_len)

        md5sum_compare_result = []

        logger.info(
            f"Md5sum check will be processed into {len(urls_before_chunks)} chunks"
        )
        runner_logger.info(
            f"Md5sum check will be processed into {len(urls_before_chunks)} chunks"
        )
        for j in range(len(urls_before_chunks)):
            j_md5sum_compare_result = compare_md5sum_flow(
                first_url_list=urls_before_chunks[j],
                second_url_list=urls_after_chunks[j],
            )
            # add logging info on the md5sum check progress
            logger.info(f"md5sum check completed: {j+1}/{len(urls_before_chunks)}")
            runner_logger.info(
                f"md5sum check completed: {j+1}/{len(urls_before_chunks)}"
            )
            md5sum_compare_result.extend([i[1:] for i in j_md5sum_compare_result])

            # record the intermediate results
            intermediate_file_name = f"{os.path.basename(manifest_path).replace('.xlsx', '')}_intermediate_md5sum_check.tsv"
            int_results.extend(j_md5sum_compare_result)
            int_transfer_df = int_results_recorder(transfer_df, int_results)
            int_transfer_df[
                [
                    "node",
                    "url_before_cp",
                    "url_after_cp",
                    "transfer_status",
                    "md5sum_check",
                    "md5sum_before_cp",
                    "md5sum_after_cp",
                ]
            ].to_csv(intermediate_file_name, sep="\t", index=False)
            
            file_ul(
                bucket=bucket,
                output_folder=intermediate_out,
                sub_folder="",
                newfile=intermediate_file_name
            )

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
            logger.error(f"md5sum check failed files count: {failed_md5sum_check_ct}")
            runner_logger.error(
                f"md5sum check failed files count: {failed_md5sum_check_ct}"
            )
        else:
            pass

        # write transfer summary table
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
        runner_logger.info(
            f"File mover summary table was created {mover_summary_table}"
        )
        del transfer_df
    except Exception as ex:
        # for interrupted md5sum comparison, we fill the unchecked comparison with
        # status of "". Therefore the comparison results can have 4 kinds outcomes
        # "Pass", "Fail", "Error", and ""
        # in case there is an exception didn't get captured
        logger.error(f"Fail to finish md5sum check for all files. {ex}")
        runner_logger.error(f"Fail to finish md5sum check for all files. {ex}")
        try:
            len_md5sum_compare_result = len(md5sum_compare_result)
            missing_len = len(urls_before_transfer) - len_md5sum_compare_result
            md5sum_compare_result.extend([("", "", "")] * missing_len)
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
                logger.error(
                    f"md5sum check failed files count: {failed_md5sum_check_ct}"
                )
                runner_logger.error(
                    f"md5sum check failed files count: {failed_md5sum_check_ct}"
                )
            else:
                pass

            # write transfer summary table
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
            runner_logger.info(
                f"File mover summary table was created {mover_summary_table}"
            )
            del transfer_df

        except NameError:
            transfer_df[
                [
                    "node",
                    "url_before_cp",
                    "url_after_cp",
                    "transfer_status",
                ]
            ].to_csv(mover_summary_table, sep="\t", index=False)
            logger.info(f"File mover summary table was created {mover_summary_table}")
            runner_logger.info(
                f"File mover summary table was created {mover_summary_table}"
            )
            del transfer_df

    return output_name, logger_filename, mover_summary_table
