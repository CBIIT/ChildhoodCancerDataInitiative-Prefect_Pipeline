from prefect import flow, task, get_run_logger
import os
import sys

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.utils import (
    get_time,
    get_date,
    calculate_list_md5sum,
    calculate_list_size,
    file_ul,
)
from src.file_mover import list_to_chunks
import pandas as pd


@flow(
    name="Calculate md5sum and size values",
    log_prints=True,
    flow_run_name="{runner}-" + f"{get_time()}",
)
def fetch_size_md5sum(bucket: str, runner: str, s3uri_list: list[str]) -> None:
    logger = get_run_logger()
    today_date = get_date()
    logger.info(f"Number of objects: {len(s3uri_list)}")
    if len(s3uri_list) <= 100:
        size_list = calculate_list_size(s3uri_list=s3uri_list)
        md5sum_list = calculate_list_md5sum(s3uri_list=s3uri_list)
    else:
        chunk_list = list_to_chunks(mylist=s3uri_list, chunk_len=100)
        logger.info(
            f"Fetching objects size and md5sum will be processed in {len(chunk_list)} chunks"
        )
        size_list = []
        md5sum_list = []
        process_bar = 1
        for i in chunk_list:
            i_size_list = calculate_list_size(s3uri_list=i)
            i_md5sum_list = calculate_list_md5sum(s3uri_list=i)
            size_list.append(i_size_list)
            md5sum_list.append(i_md5sum_list)
            logger.info(f"Progress: {process_bar}/{len(chunk_list)}")
            process_bar += 1
    # creates a pandas df and writes it into a tsv file
    return_df = pd.DataFrame(
        {"S3_URI": s3uri_list, "Size": size_list, "md5sum": md5sum_list}
    )
    output_folder = os.path.join(runner, "fetch_size_md5sum_outputs_" + get_time())
    output_filename = "fetch_size_md5sum_" + today_date + ".tsv"
    return_df.to_csv(output_filename, sep="\t", index=False)
    logger.info(f"Created summary file: {output_filename}")

    # upload summary table to bucket
    file_ul(
        bucket=bucket,
        output_folder=output_folder,
        sub_folder="",
        newfile=output_filename,
    )
    logger.info(f"Uploaded file {output_filename} to bucket {bucket} folder {output_folder}")
    logger.info("Workflow finished")
