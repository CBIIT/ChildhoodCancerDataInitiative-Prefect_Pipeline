from prefect import flow, get_run_logger, pause_flow_run, task
from prefect.input import RunInput
import os
import sys
from typing import Literal, TypeVar

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.utils import (
    get_time,
    get_date,
    calculate_list_md5sum,
    calculate_list_md5sum_consecutively,
    calculate_list_size,
    file_ul,
    file_dl,
    list_to_chunks,
    set_s3_session_client,
    parse_file_url
)
from src.file_remover import paginate_parameter
import pandas as pd


class DirectoryListInput(RunInput):
    dir_list: list[str]


class UriListInput(RunInput):
    uri_list: list[str]

class FileInput(RunInput):
    file_bucket_path : str

DropDownChoices = Literal["s3_directory", "list_of_s3_uri", "file_containing_s3_uri"]
ConcurrencyDropDownChoices = Literal["yes","no"]
Md5sumDropDownChoices = Literal["Run md5sum", "Don't run md5sum"]
DataFrame = TypeVar("DataFrame")


@task(name="list URI of all objects under a s3 bucket dir", log_prints=True)
def list_dir_content_uri(dir_path: str) -> list[str]:
    s3 = set_s3_session_client()
    s3_paginator = s3.get_paginator("list_objects_v2")
    operation_parameters = paginate_parameter(bucket_path=dir_path)
    bucket_name = operation_parameters["Bucket"]
    pages = s3_paginator.paginate(**operation_parameters)
    return_list = []
    for page in pages:
        if "Contents" in page.keys():
            for obj in page["Contents"]:
                obj_key = obj["Key"]
                obj_uri = os.path.join(bucket_name, obj_key)
                return_list.append(obj_uri)
        else:
            pass
    s3.close()
    return return_list


@flow(
    name="Calculate md5sum and size of uri list",
    log_prints=True,
)
def fetch_size_md5sum_with_urls(s3uri_list: list[str], if_concurrency: str, Md5sumDropDownChoices: Md5sumDropDownChoices) -> DataFrame:
    """Returns a dataframe containing 3 columns of s3_uri, size, and md5sum

    Args:
        s3uri_list (list[str]): A list of s3 uri. Each uri starts with s3://

    Returns:
        DataFrame: A pandas DataFrame that contains 3 columns, s3_uri, size, and md5sum
    """    
    size_list = calculate_list_size(s3uri_list=s3uri_list)
    if Md5sumDropDownChoices == "Run md5sum":
        if if_concurrency == "yes":
            md5sum_list = calculate_list_md5sum(s3uri_list=s3uri_list)
        else:
            md5sum_list = calculate_list_md5sum_consecutively(s3uri_list=s3uri_list)
    elif Md5sumDropDownChoices == "Don't run md5sum":
        md5sum_list = ["SKIP"] * len(s3uri_list)
    else:
        raise ValueError(f"Invalid value for Md5sumDropDownChoices was received: {Md5sumDropDownChoices}")
    # fix uri if s3:// is missing
    s3uri_list =  ["s3://" + i if not i.startswith("s3://") else i for i in s3uri_list]
    # creates a pandas df and writes it into a tsv file
    return_df = pd.DataFrame(
        {"s3_uri": s3uri_list, "size": size_list, "md5sum": md5sum_list}
    )
    return return_df


@flow(
    name="Get object size and md5sum",
    log_prints=True,
    flow_run_name="{runner}-" + f"{get_time()}",
)
def get_size_md5sum(bucket: str, runner: str, input_type: DropDownChoices, run_concurrency: ConcurrencyDropDownChoices, Md5sumDropDownChoices: Md5sumDropDownChoices) -> None: 
    """Pipeline that calculates objects size and md5sum

    Args:
        bucket (str): Bucekt name where the output goes to
        runner (str):
        input_type (DropDownChoices): The type of input you can provide. Acceptable values are s3_directory, list_of_s3_uri, file_containing_s3_uri. If your list of uri is long (over 100), we recommond to put them in a file so the flow can read them through a file
        run_concurrency (ConcurrencyDropDownChoices): If you would like to run tasks concurrently
        Md5sumDropDownChoices (Md5sumDropDownChoices): If you would like to run md5sum calculation
    """
    logger = get_run_logger()

    # create a uri list
    if input_type == "s3_directory":
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
        uri_list = []
        for dir in dir_inputs.dir_list:
            dir_content_list = list_dir_content_uri(dir_path=dir)
            logger.info(f"Objects found in {dir}: {len(dir_content_list)}")
            uri_list.extend(dir_content_list)
    elif input_type == "list_of_s3_uri":
        logger.info("You have one or more s3 uri to fetch for object size and md5sum")
        uri_inputs = pause_flow_run(
            wait_for_input=UriListInput.with_initial_data(
                description=(
                    f"""
**Please provide a list of uri**

- **uri_list**: e.g., example-bucket/folder1/test_file.txt
"""
                )
            )
        )
        uri_list = uri_inputs.uri_list
    elif input_type == "file_containing_s3_uri":
        logger.info("You have a file containing s3 uri to fetch for object size and md5sum")
        file_input = pause_flow_run(
            wait_for_input=FileInput.with_initial_data(
                description=(
                    f"""
**Please provide the bucket path of the file containing s3 uri**

**ATTENTION**: No column name needed in the file. Please make sure only one s3 uri per line.

- **file_bucket_path**: e.g., s3://ccdi-validation/QL/input/test_file.txt             
"""
                )
            )
        )
        file_bucket_path = file_input.file_bucket_path
        filename =  os.path.basename(file_bucket_path)
        bucket_name, obj_key = parse_file_url(url=file_bucket_path)
        file_dl(bucket=bucket_name, filename = obj_key)

        uri_df = pd.read_csv(filename, header=None, names=["uri_list"])
        uri_list = uri_df["uri_list"].tolist()
    else:
        logger.error("Unrecognized input type")
        raise ValueError(f"Invalid value for input type was received: {input_type}")

    # start fetching object size and calculate md5sum with uri_list
    # print(uri_list)
    today_date = get_date()
    output_folder = os.path.join(runner, "fetch_size_md5sum_outputs_" + get_time())
    output_file = "fetch_size_md5sum_" + today_date + ".tsv"
    logger.info(f"Number of objects to report: {len(uri_list)}")
    if len(uri_list) > 100:
        uri_chunk_list = list_to_chunks(mylist=uri_list, chunk_len=100)
        logger.info(
            f"size and md5sum calculation is going to be executed in {len(uri_chunk_list)} chuncks"
        )
        result_df =  pd.DataFrame(columns=["s3_uri","size","md5sum"])
        progress = 1
        for i in uri_chunk_list:
            i_df =  fetch_size_md5sum_with_urls(s3uri_list=i, if_concurrency=run_concurrency, Md5sumDropDownChoices=Md5sumDropDownChoices)
            result_df =  pd.concat([result_df, i_df], ignore_index=True)
            logger.info(f"Progress: {progress}/{len(uri_chunk_list)}")
            # writing that result to output_file and upload to bucket. 
            # in case the flow failed in the middle
            # this is going to be overwritten till all the objs has been checked
            result_df.to_csv(output_file, sep="\t", index=False)
            file_ul(
                bucket=bucket,
                output_folder=output_folder,
                sub_folder="",
                newfile=output_file,
            )
            logger.info(
                f"Uploaded file {output_file} to bucket {bucket} folder {output_folder}"
            )
            progress += 1
    else:
        result_df = fetch_size_md5sum_with_urls(
            s3uri_list=uri_list, if_concurrency=run_concurrency, Md5sumDropDownChoices=Md5sumDropDownChoices
        )
        result_df.to_csv(output_file, sep="\t", index=False)
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
