from prefect import flow, get_run_logger, pause_flow_run, task
from prefect.input import RunInput
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
    list_to_chunks,
    set_s3_session_client,
    list_dir_content_uri
)
from src.read_buckets import paginate_parameter
import pandas as pd


class DirectoryListInput(RunInput):
    dir_list: list[str]


class UriListInput(RunInput):
    url_list: list[str]


@flow(
    name="Calculate md5sum and size of url list",
    log_prints=True,
    flow_run_name="{runner}-" + f"{get_time()}",
)
def fetch_size_md5sum_with_urls(
    s3uri_list: list[str]
) -> None:
    logger = get_run_logger()
    today_date = get_date()
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
            size_list.extend(i_size_list)
            md5sum_list.extend(i_md5sum_list)
            logger.info(f"Progress: {process_bar}/{len(chunk_list)}")
            process_bar += 1
    # creates a pandas df and writes it into a tsv file
    return_df = pd.DataFrame(
        {"S3_URI": s3uri_list, "Size": size_list, "md5sum": md5sum_list}
    )
    output_filename = "fetch_size_md5sum_" + today_date + ".tsv"
    return_df.to_csv(output_filename, sep="\t", index=False)
    logger.info(f"Created summary file: {output_filename}")
    return output_filename


@flow(
    name="Get object size and md5sum",
    log_prints=True,
    flow_run_name="{runner}-" + f"{get_time()}",
)
def get_size_md5sum(bucket: str, runner: str, dir_or_uri: str) -> None:
    logger = get_run_logger()

    time_rightnow = get_time()
    output_folder = os.path.join(runner, "fetch_size_md5sum_outputs_" + time_rightnow)

    # create a uri list
    if dir_or_uri == "dir":
        logger.info(
            "You have one or more s3 directories to fetch for object size and md5sum"
        )
        dir_inputs = pause_flow_run(
            wait_for_input=DirectoryListInput.with_initial_data(
                description=(
                    f"""
**Please provide a list of directories**

- **dir_list**: e.g., s3-bucket/example_folder
"""
                )
            )
        )
        uri_list =  []
        for dir in dir_inputs:
            dir_content_list =  list_dir_content_uri(dir_path=dir)
            logger.info(f"Objects found in {dir}: {len(dir_content_list)}")
            uri_list.extend(dir_content_list)
    elif dir_or_uri == "uri":
        logger.info("You have one or more s3 uri to fetch for object size and md5sum")
        uri_list = pause_flow_run(
            wait_for_input=UriListInput.with_initial_data(
                description=(
                    f"""
**Please provide a list of uri**

- **uri_list**: e.g., example-bucket/folder1/test_file.txt
"""
                )
            )
        )
    else:
        logger.error("You must answer between dir or uri")
        raise ValueError(f"Invalid value for dir_or_uri was received: {dir_or_uri}")

    # start fetching object size and calculate md5sum with uri_list
    logger.info(f"Number of objects to report: {len(uri_list)}")
    output_file =  fetch_size_md5sum_with_urls(s3uri_list=uri_list)

    # upload summary table to bucket
    file_ul(
        bucket=bucket,
        output_folder=output_folder,
        sub_folder="",
        newfile=output_file,
    )
    logger.info(
        f"Uploaded file {output_file} to bucket {bucket} folder {output_folder}"
    )
    logger.info("Workflow finished")
