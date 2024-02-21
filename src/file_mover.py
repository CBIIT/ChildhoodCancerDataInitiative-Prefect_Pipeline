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
from prefect import flow


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


def compare_md5sum(first_url: str, second_url: str, s3_client) -> bool:
    """Compares the md5sum of two objects"""
    first_md5sum = calculate_object_md5sum(s3_client=s3_client, url=first_url)
    second_md5sum = calculate_object_md5sum(s3_client=s3_client, url=second_url)
    if first_md5sum == second_md5sum:
        return first_md5sum, second_md5sum, True
    else:
        return (first_md5sum, second_md5sum, False)


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
    logger.info(
        f"A CCDI Excel manifest with new object urls was generated {output_name}"
    )

    # File transfer starts
    logger.info(f"Start transfering files to destination bucket {dest_bucket_path}")
    s3_client = set_s3_session_client()
    success_transfer_ct = 0
    fail_transfer_ct = 0
    for index, row in transfer_df.iterrows():
        row_cp_parameter = row["cp_object_parameter"]
        try:
            response = s3_client.copy_object(**row_cp_parameter)
            transfer_df.loc[index, "transfer_status"] = "Success"
            success_transfer_ct += 1
        except ClientError as ex:
            ex_code = ex.response["Error"]["Code"]
            ex_message = ex.response["Error"]["Message"]
            failed_file = row["url_before_cp"]
            if ex_code == "NoSuchKey":
                object_name = ex.response["Error"]["Key"]
                logger.error(ex_code + ":" + ex_message + " " + object_name)
            elif ex_code == "NoSuchBucket":
                bucket_name = ex.response["Error"]["Code"]["BucketName"]
                logger.error(
                    ex_code + ":" + ex_message + " Bucket name: " + bucket_name
                )
            else:
                logger.error(
                    "Error info:\n" + json.dumps(ex.response["Error"], indent=4)
                )
            transfer_df.loc[index, "transfer_status"] = "Fail"
            fail_transfer_ct += 1
    logger.info(f"Successfully transferred {success_transfer_ct} files")
    if fail_transfer_ct >= 1:
        logger.warning(f"Failed to transfer {fail_transfer_ct} files")
    else:
        pass

    # Check md5sum of file before transfer and after transfer
    # md5sum check only checks files which have been successfully copied between buckets
    logger.info("Start checking md5sum before and after transfer")
    transfer_df["md5sum_check"] = [""] * transfer_df.shape[0]
    transfer_df["md5sum_before_cp"] = [""] * transfer_df.shape[0]
    transfer_df["md5sum_after_cp"] = [""] * transfer_df.shape[0]
    for index, row in transfer_df.iterrows():
        if row["transfer_status"] == "Fail":
            pass
        else:
            url_before = row["url_before_cp"]
            url_after = row["url_after_cp"]
            before_md5sum, after_md5sum, compare_result = compare_md5sum(
                first_url=url_before, second_url=url_after, s3_client=s3_client
            )
            if compare_result:
                transfer_df.loc[index, "md5sum_check"] = "Pass"
            else:
                transfer_df.loc[index, "md5sum_check"] = "Fail"
            transfer_df.loc[index, "md5sum_before_cp"] = before_md5sum
            transfer_df.loc[index, "md5sum_after_cp"] = after_md5sum
    passed_md5sum_check_ct = sum(transfer_df["md5sum_check"] == "Pass")
    failed_md5sum_check_ct = sum(transfer_df["md5sum_check"] == "Fail")
    logger.info(f"md5sum check passed files: {passed_md5sum_check_ct}")
    if failed_md5sum_check_ct >= 1:
        logger.warning(f"md5sum check failed files: {failed_md5sum_check_ct}")
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
    del transfer_df
    return output_name, logger_filename, mover_summary_table
