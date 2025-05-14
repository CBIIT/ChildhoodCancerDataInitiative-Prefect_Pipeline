from prefect import flow, task, get_run_logger
import os
import sys
import pandas as pd
from typing import TypeVar
from botocore.errorfactory import ClientError
from prefect.cache_policies import NO_CACHE

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.utils import get_time, file_dl, file_ul, get_date, set_s3_session_client, get_logger
from src.file_mover import copy_object_parameter, dest_object_url, copy_file_flow, parse_file_url, compare_md5sum_flow
from src.file_remover import objects_deletion, retrieve_objects_from_bucket_path

DataFrame = TypeVar("DataFrame")

@task
def create_file_mover_metadata(tsv_df: DataFrame, new_bucket_folder: str) -> DataFrame:
    """Add additional columns to tsv_df with copy parameter dict and destination path

    Args:
        tsv_df (DataFrame): A dataframe with one col of uri
        new_bucket_folder (str):  

    Returns:
        DataFrame: Dataframe which contains original url, dest uri, copy parameter which can be used for file copying
    """    
    uri_list = tsv_df["original_uri"].tolist()
    #newfolder = newfolder.strip("/")
    copy_param_list = []
    dest_list = []
    new_bucket, new_folder = parse_file_url(url=new_bucket_folder)
    for i in uri_list:
        #i_bucket, _ = parse_file_url(url=i)
        #i_dest = i_bucket + "/" + newfolder
        i_dest = new_bucket + "/" + new_folder
        i_copy_parameter = copy_object_parameter(url_in_cds=i, dest_bucket_path=i_dest)
        i_dest_path = dest_object_url(url_in_cds=i, dest_bucket_path=i_dest)
        copy_param_list.append(i_copy_parameter)
        dest_list.append(i_dest_path)
    tsv_df["dest_uri"] =  dest_list
    tsv_df["copy_parameter"] =  copy_param_list
    return tsv_df

def generate_copy_parameter(original_uri: str, dest_uri: str) -> dict:
    """create s3 copy parameter dict used to copy file object from original_uri to dest_uri. The copy parameter dict is used in the copy_object function.

    Args:
        original_uri (str): s3 uri of the original file object. The format should be s3://{bucket-name}/{file-path}/{file-name}
        dest_uri (str): s3 uri of the destination file object. The format should be s3://{bucket-name}/{file-path}/{file-name}

    Returns:
        dict: Dictionary of s3 copy parameter. param_dict = {"Bucket": dest_bucket, "CopySource": copysource, "Key": dest_key}
    """
    param_dict = {}
    original_bucket, original_keypath = parse_file_url(url=original_uri)
    copysource = os.path.join(original_bucket, original_keypath)
    dest_bucket, dest_keypath = parse_file_url(url=dest_uri)
    param_dict["Bucket"] = dest_bucket
    param_dict["CopySource"] = copysource
    param_dict["Key"] = dest_keypath
    return param_dict


def check_if_directory(s3_client, uri_path: str) -> None:
    bucket, keypath = parse_file_url(url=uri_path)
    try:
        s3_client.head_object(Bucket=bucket, Key=keypath)
        if_dir = "object"
    except ClientError as e:
        err_code = e.response["Error"]["Code"]
        err_message = e.response["Error"]["Message"]
        print(f"{err_code}: {err_message} {uri_path}")
        try:
            result = s3_client.list_objects(Bucket=bucket, Prefix=keypath, MaxKeys=1)
            if 'Contents' in result:
                if_dir = "directory"
            else:
                if_dir = "invalid"
        except ClientError as err:
            err_code = err.response["Error"]["Code"]
            err_message = err.response["Error"]["Message"]
            print(f"{err_code}: {err_message} {uri_path}")

    print(if_dir)
    return if_dir

@task(
        name="If Directory",
        log_prints=True,
        cache_policy=NO_CACHE,
)
def identify_obj_dir(uri_list: list, logger) -> list:
    obj_list = []
    s3_client = set_s3_session_client()
    for uri in uri_list:
        uri_check =  check_if_directory(s3_client=s3_client, uri_path=uri)
        if uri_check == "object":
            obj_list.append(uri)
            logger.info(f"uri {uri} is an object")
        elif uri_check ==  "directory":
            logger.info(f"uri {uri} is a directory")
            uri_item_list = retrieve_objects_from_bucket_path(bucket_folder_path=uri)
            uri_path_list = ["s3://" + i["Bucket"] + "/" + i["Key"] for i in uri_item_list]
            obj_list.extend(uri_path_list)
        else:
            logger.error(f"uri {uri} is not valid. Neither obj nor dir")
            print(f"uri {uri} is not valid. Neither obj nor dir")

    return obj_list


@task(
    name="If uri valid",
    log_prints=True,
    cache_policy=NO_CACHE,
)
def identify_obj_uri_valid(filename: str, col_name: str, logger) -> list:
    obj_list = []
    s3_client = set_s3_session_client()
    file_df = pd.read_csv(filename, sep="\t", header=0)
    raw_n_rows = file_df.shape[0]
    file_df.drop_duplicates(inplace=True)
    if file_df.shape[0] != raw_n_rows:
        logger.warning(f"There are duplicate rows in the input file. Duplicates were removed. {file_df.shape} out of {raw_n_rows} rows were kept")
    else:
        pass
    uri_list = file_df[col_name].tolist()
    invalid_uri_list = []
    for uri in uri_list:
        uri_check = check_if_directory(s3_client=s3_client, uri_path=uri)
        if uri_check == "object":
            obj_list.append(uri)
            #logger.info(f"uri {uri} is an object")
        elif uri_check == "directory":
            logger.error(f"uri {uri} is a directory")
            print(f"uri {uri} is a directory")
            invalid_uri_list.append(uri)
        else:
            logger.error(f"uri {uri} is not valid. Neither obj nor dir")
            print(f"uri {uri} is not valid. Neither obj nor dir")
            invalid_uri_list.append(uri)

    return obj_list, invalid_uri_list


@flow(
    name="file mover and delete",
    log_prints=True,
    flow_run_name="file-mover-delete-{runner}-" + f"{get_time()}",
)
def file_mover_delete(bucket: str, runner: str, obj_list_tsv_path: str, move_to_bucket_folder: str) -> None:
    """Moves objects listed in a tsv file to a new folder in the same bucket

    Args:
        bucket (str): Bucket of where tsv lives and output goes to.
        runner (str): unique runner name
        obj_list_tsv_path (str): A file contains a column of s3 uri (s3://{bucket-name}/{file-path}). NO header needed
        move_to_bucket_folder (str): Bucker folder name of where the obj will be moved to. An example of bucker folder can be s3://dst-bucket/newfolder
    """
    current_time = get_time()
    # create logger
    runner_logger = get_run_logger()
    logger = get_logger(loggername="file_mover_delete_workflow", log_level="info")
    logger_filename = "file_mover_delete_workflow_" + get_date() + ".log"

    file_dl(bucket=bucket, filename = obj_list_tsv_path)
    runner_logger.info(f"Downloaded list of s3 uri file: {obj_list_tsv_path}")
    tsv_name = os.path.basename(obj_list_tsv_path)
    tsv_df = pd.read_csv(tsv_name, sep="\t", header=None, names =  ["original_uri"])
    logger.info(f"{tsv_df.shape[0]} items were found in file {tsv_name}")
    runner_logger.info(f"{tsv_df.shape[0]} items were found in file {tsv_name}")

    # identify if the uri in the tsv file dir or obj
    uri_list = identify_obj_dir(uri_list=tsv_df["original_uri"].tolist(), logger=logger)
    tsv_df = pd.DataFrame({"original_uri": uri_list})
    logger.info(f"A total of {tsv_df.shape[0]} objects will be moved")
    runner_logger.info(f"A total of {tsv_df.shape[0]} objects will be moved")

    runner_logger.info("Creating destination s3 uri")
    meta_df = create_file_mover_metadata(tsv_df=tsv_df, new_bucket_folder=move_to_bucket_folder)

    # copy file to the dest uri
    runner_logger.info("Start moving files")
    s3_client = set_s3_session_client()
    copy_parameter_list = meta_df["copy_parameter"].tolist()
    ## KEEP BELOW UNTIL MERGED TO PROD JUST IN CASE
    """copy_status = []
    for copy_parameter  in copy_parameter_list:
        item_status = copy_file_task(copy_parameter=copy_parameter, s3_client=s3_client, logger=logger, runner_logger=runner_logger)
        copy_status.append(item_status)"""
    copy_status = []
    for chunk in range(0, len(copy_parameter_list), 500):
        runner_logger.info(f"Copying {chunk} to {chunk+500} files")
        int_copy_status = copy_file_flow(
            copy_parameter_list=copy_parameter_list[chunk:chunk+500],logger=logger, runner_logger=runner_logger, concurrency_tag="file_mover_delete_copy")
        copy_status.extend(int_copy_status)
    

    # compare md5sum
    runner_logger.info("Start comparing md5sum before and after copy")
    ## KEEP BELOW UNTIL MERGED TO PROD JUST IN CASE
    """first_md5sum = []
    second_md5sum = []
    compare_md5sum_status = []
    for i in range(meta_df.shape[0]):
        original_uri_i = meta_df["original_uri"][i]
        dest_uri_i = meta_df["dest_uri"][i]
        i_original_url, i_original_md5sum, i_dest_md5sum, comparison_result = compare_md5sum_task(first_url=original_uri_i, second_url=dest_uri_i, s3_client=s3_client, logger=logger)
        
        first_md5sum.append(i_original_md5sum)
        second_md5sum.append(i_dest_md5sum)
        compare_md5sum_status.append(comparison_result)
    


    meta_df["copy_status"] = copy_status
    meta_df["original_md5sum"] = first_md5sum
    meta_df["dest_md5sum"] = second_md5sum
    meta_df["md5sum_check"] = compare_md5sum_status"""
    # compare md5sum
    first_url_list = meta_df["original_uri"].tolist()
    second_url_list = meta_df["dest_uri"].tolist()
    md5sum_results = []

    for chunk in range(0, len(first_url_list), 500):
        runner_logger.info(f"Comparing md5sum for {chunk} to {chunk+500} files")
        int_md5sum_results = compare_md5sum_flow(
            first_url_list=first_url_list[chunk:chunk+500],
            second_url_list=second_url_list[chunk:chunk+500],
            concurrency_tag="file_mover_delete_md5sum",
        )
        md5sum_results.extend(int_md5sum_results)

    """md5sum_results = compare_md5sum_flow(
        first_url_list=first_url_list,
        second_url_list=second_url_list,
        concurrency_tag="file_mover_delete_md5sum",
    )"""

    meta_df["copy_status"] = copy_status
    meta_df["original_md5sum"] = [result[1] for result in md5sum_results]
    meta_df["dest_md5sum"] = [result[2] for result in md5sum_results]
    meta_df["md5sum_check"] = [result[3] for result in md5sum_results]


    # upload files to bucket
    output_folder = os.path.join(runner, "file_mover_delete_outputs_" + current_time)
    meta_output = f"file_mover_delete_manifest_{get_date()}.tsv"
    meta_df.to_csv(meta_output, sep="\t", index=False)
    file_ul(bucket=bucket, output_folder=output_folder, sub_folder="", newfile=meta_output)
    file_ul(bucket=bucket, output_folder=output_folder, sub_folder="", newfile=logger_filename)
    runner_logger.info(f"Uploaded file {meta_output} to the bucket {bucket} folder {output_folder}")

    if len(meta_df["md5sum_check"].unique().tolist()) == 1 and meta_df["md5sum_check"].unique().tolist()[0] == "Pass":
        runner_logger.info("All files passed md5sum check")
        runner_logger.info("Start deleting file under original_uri column")
        deletion_summary, deletion_counts_df = objects_deletion(
            manifest_file_path=meta_output,
            delete_column_name="original_uri",
        )
        runner_logger.info(deletion_counts_df.to_markdown(index=False, tablefmt="rst"))
        file_ul(
            bucket=bucket,
            output_folder=output_folder,
            sub_folder="",
            newfile=deletion_summary,
        )
        runner_logger.info(f"Uploaded deletion summary file {deletion_summary} to bucket {bucket}, folder {output_folder}")
    else:
        runner_logger.warning("Not all files passed md5sum check. Therefore no deletion is performed")
        print(meta_df[["original_uri", "md5sum_check"]][meta_df["md5sum_check"] != "Pass"])

    return None

@flow(
    name="file mover and delete alt",
    log_prints=True,
    flow_run_name="file-mover-delete-alt-{runner}-" + f"{get_time()}",
)
def file_mover_delete_complete_input(bucket: str, runner: str, obj_tsv_file_path: str) -> None:
    """Moves objects listed in a tsv file to a new location based on the tsv input file.
    The tsv contains two columns: original_uri and dest_uri. The dest_uri is the new location of the file. This is a generic file mover worklfow,
    which covers use cases when the runner doesn't want to preserve the original file path strucutre.

    Args:
        bucket (str): Bucket of where output goes to.
        runner (str): unique runner name
        obj_tsv_file_path (str): A tsv file s3 uri (s3://{bucket-name}/{file-path}/{tsv-name}.tsv) which contains two columns, original_uri and dest_uri. Valid s3 uri format is expected under each column, e.g., s3://{bucket-name}/{file-path}/{file-name}).
    """
    current_time = get_time()
    # create logger
    runner_logger = get_run_logger()
    logger = get_logger(loggername="file_mover_delete_workflow", log_level="info")
    logger_filename = "file_mover_delete_workflow_" + get_date() + ".log"

    # uplload folder path
    upload_folder  = os.path.join(runner, "file_mover_delete_outputs_" + current_time)

    tsv_bucket, tsv_keypath = parse_file_url(url=obj_tsv_file_path)
    file_dl(bucket=tsv_bucket, filename=tsv_keypath)
    runner_logger.info(f"Downloaded list of s3 uri file: {obj_tsv_file_path}")
    tsv_name = os.path.basename(obj_tsv_file_path)
    tsv_df = pd.read_csv(tsv_name, sep="\t", header=0)
    # check if the tsv file has two columns
    if tsv_df.columns.tolist() != ["original_uri", "dest_uri"]:
        runner_logger.error(f"The input tsv file {tsv_name} does not have the correct columns. The columns should be original_uri and dest_uri")
        raise ValueError(f"The input tsv file {tsv_name} does not have the correct columns. The columns should be original_uri and dest_uri")
    else:
        pass
    logger.info(f"{tsv_df.shape[0]} items were found in file {tsv_name}")
    runner_logger.info(f"{tsv_df.shape[0]} items were found in file {tsv_name}")

    # identify if the uri in the tsv file dir or obj
    uri_list, invalid_uri_list = identify_obj_uri_valid(filename=tsv_name, col_name="original_uri", logger=logger)
    if len(invalid_uri_list) > 0:
        file_ul(bucket=bucket, output_folder=upload_folder, sub_folder="", newfile=logger_filename)
        logger.error(f"Not all uri in the tsv file are valid. Only {len(uri_list)} uri are valid, Please check {logger_filename} file in bucket {bucket} at {upload_folder}")
        raise ValueError(f"Not all uri in the tsv file are valid. Only {len(uri_list)} uri are valid. Please check {logger_filename} file in bucket {bucket} at {upload_folder} and fix the invalid uri")
    else:
        logger.info(f"All uri under original_uri col in file {tsv_name} are valid. {len(uri_list)} unique uri are valid")
        runner_logger.info(
            f"All uri under original_uri col in file {tsv_name} are valid. {len(uri_list)} unique uri are valid"
        )

    # create copy parameter
    copy_parameter_list = []
    tsv_df.drop_duplicates(inplace=True)
    logger.info(f"After removing duplicates, a total of {tsv_df.shape[0]} objects will be moved")
    runner_logger.info(
        f"After removing duplicates, a total of {tsv_df.shape[0]} objects will be moved"
    )
    for i in uri_list:
        # in case one file needs to be moved to multiplei destinations
        i_dest_uri_list = tsv_df[tsv_df["original_uri"] == i]["dest_uri"].values.tolist()
        for dest_uri in i_dest_uri_list:
            dest_uri_parameter = generate_copy_parameter(original_uri=i, dest_uri=dest_uri)
            copy_parameter_list.append(dest_uri_parameter)

    # start copy files
    copy_status = []
    for chunk in range(0, len(copy_parameter_list), 500):
        runner_logger.info(f"Copying {chunk} to {chunk+500} files")
        int_copy_status = copy_file_flow(
            copy_parameter_list=copy_parameter_list[chunk : chunk + 500],
            logger=logger,
            runner_logger=runner_logger,
            concurrency_tag="file-mover-delete-alt-copy",
        )
        copy_status.extend(int_copy_status)
    print(copy_status)

    # compare md5sum
    # compare md5sum
    first_url_list = tsv_df["original_uri"].tolist()
    second_url_list = tsv_df["dest_uri"].tolist()
    md5sum_results = []
    for chunk in range(0, len(first_url_list), 500):
        runner_logger.info(f"Comparing md5sum for {chunk} to {chunk+500} files")
        int_md5sum_results = compare_md5sum_flow(
            first_url_list=first_url_list[chunk : chunk + 500],
            second_url_list=second_url_list[chunk : chunk + 500],
            concurrency_tag="file-mover-delete-alt-md5sum",
        )
        md5sum_results.extend(int_md5sum_results)

    tsv_df["copy_status"] = copy_status
    tsv_df["original_md5sum"] = [result[1] for result in md5sum_results]
    tsv_df["dest_md5sum"] = [result[2] for result in md5sum_results]
    tsv_df["md5sum_check"] = [result[3] for result in md5sum_results]

    # upload tsv with copy status and md5sum check results to the bucket
    file_mover_delete_output = f"file_mover_delete_manifest_{get_date()}.tsv"
    tsv_df.to_csv(file_mover_delete_output, sep="\t", index=False)
    file_ul(bucket=bucket, output_folder=upload_folder, sub_folder="", newfile=file_mover_delete_output)
    # the new upload will replace the old one
    file_ul(bucket=bucket, output_folder=upload_folder, sub_folder="", newfile=logger_filename)

    if (
        len(tsv_df["md5sum_check"].unique().tolist()) == 1
        and tsv_df["md5sum_check"].unique().tolist()[0] == "Pass"
    ):
        runner_logger.info("All files passed md5sum check")
        runner_logger.info("Start deleting file under original_uri column")
        deletion_summary, deletion_counts_df = objects_deletion(
            manifest_file_path=file_mover_delete_output,
            delete_column_name="original_uri",
        )
        runner_logger.info(deletion_counts_df.to_markdown(index=False, tablefmt="rst"))
        file_ul(
            bucket=bucket,
            output_folder=upload_folder,
            sub_folder="",
            newfile=deletion_summary,
        )
        runner_logger.info(
            f"Uploaded deletion summary file {deletion_summary} to bucket {bucket}, folder {upload_folder}"
        )
    else:
        runner_logger.warning(
            "Not all files passed md5sum check. Therefore no deletion is performed"
        )
        print(
            tsv_df[["original_uri", "md5sum_check"]][tsv_df["md5sum_check"] != "Pass"]
        )

    return None
