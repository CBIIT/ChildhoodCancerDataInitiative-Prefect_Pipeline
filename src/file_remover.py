from prefect import flow, task, get_run_logger
from src.utils import set_s3_session_client
from src.read_buckets import paginate_parameter
from src.file_mover import parse_file_url_in_cds
from botocore.exceptions import ClientError
from dataclasses import dataclass
from prefect.input import RunInput
import os
import hashlib
import pandas as pd
import numpy as np
from typing import TypeVar
from src.utils import get_time


DataFrame = TypeVar("DataFrame")


class FlowPathInput(RunInput):
    have_manifest: str


class ManifestPathInput(RunInput):
    bucket: str
    manifest_tsv_path: str
    delete_column_name: str
    runner: str


class NoManifestPathInput(RunInput):
    prod_bucket_path: str
    staging_bucket_path: str
    workflow_output_bucket: str
    runner: str

class ObjectDeletionInput(RunInput):
    proceed_to_delete: str

@dataclass
class InputDescriptionMD:
    """dataclass for wait for input description MD"""
    have_manifest_md: str = (
        """
**Welcome to the File Remover Flow!**
Today's Date: {current_time}

Do you have a manifest of s3 URI endpoints to be deleted :
- **have_manifest**: y/n

"""
    )
    manifest_inputs_md: str = (
        """
**Please provide inputs as shown below**

- **bucket**: bucket name of where the manifest lives
- **manifest_tsv_path**: path of the manifest(tsv) in the bucket
- **delete_column_name**: column name of s3 uri to be deleted
- **runner**: your runner id

"""
    )
    no_manifest_inputs_md: str = (
        """
**Please provide inputs as shown below**

- **prod_bucket_path**: bucket path containig files you would like to keep
- **staging_bucket_path**: bucket path contaings duplciated object under prod bucket path that you would like to delete
- **runner**: your runner id

"""
    )
    object_deletion_md: str = (
        """
**Please provide inputs as shown below**

!!CAUTION!!

Make sure you have reviewed the manifest {manifest_file} under bucker {bucket} folder {folder}

- **proceed_to_delete**: y/n

"""
    )


def list_to_chunks(mylist: list, chunk_len: int) -> list:
    """Break a list into a list of chunks"""
    chunks = [
        mylist[i * chunk_len : (i + 1) * chunk_len]
        for i in range((len(mylist) + chunk_len - 1) // chunk_len)
    ]
    return chunks


def count_success_fail(deletion_status: list) -> tuple:
    count_success = deletion_status.count("Success")
    count_fail = deletion_status.count("Fail")
    return count_success, count_fail

@task(retries=3, retry_delay_seconds=0.5)
def if_object_exists(key_path: str, bucket: str, s3_client, logger) -> None:
    """Retrives the metadata of an object without returning the
    object itself

    To use HEAD, you must have the s3:GetObject permission
    """
    try:
        object_meta = s3_client.head_object(Bucket=bucket, Key=key_path)
        if_exist = True
    except ClientError as err:
        err_code = err.response["Error"]["Code"]
        err_message = err.response["Error"]["Message"]
        logger.error(
            f"Error occurred while fetching metadata of {key_path} from bucjet {bucket}: {err_code} {err_message}"
        )
        if_exist = False
    return if_exist


def parse_bucket_folder_path(bucket_folder_path: str) -> tuple:
    """Extract bucket name and folder path from a bucket path str

    Example: "ccdi-staging/sub_folder1/sub_folder2"
    Return values: ccdi-staging, "subfolder1/sub_folder2"
    """
    # remove s3:// if observed
    if bucket_folder_path.startswith("s3://"):
        bucket_folder_path = bucket_folder_path[5:]
    else:
        pass

    bucket_folder_path = bucket_folder_path.strip("/")
    if "/" in bucket_folder_path:
        path_list = bucket_folder_path.split("/", 1)
        bucket, folder_path = path_list
    else:
        bucket = bucket_folder_path
        folder_path = ""
    return bucket, folder_path


@task
def construct_staging_bucket_key(
    object_prod_bucket_key: str, prod_bucket_path: str, staging_bucket_path: str
) -> str:
    """Reconstruct the object key in staging bucket

    Example:
        object_prid_bucket_key: release_2/sub_dir1/sub_dir2/example.cram
        prod_bucket_path: prod_bucket/release_2
        staging_bucket_path: staging_bucket/staging_folder
    Returns:
        staging_folder/sub_dir1/sub_dir2/example.cram
    """
    _, prod_prefix = parse_bucket_folder_path(bucket_folder_path=prod_bucket_path)
    object_prod_bucket_key = object_prod_bucket_key.strip("/")
    # remove the prefix part in prod bucket
    object_without_prod_prefix = object_prod_bucket_key[len(prod_prefix) :].strip("/")
    # parse staging bucket path to staging bucket name, staging prefix
    staging_bucket, staging_prefix = parse_bucket_folder_path(
        bucket_folder_path=staging_bucket_path
    )
    # concatenate with staging bucket path
    object_staging_bucket_key = os.path.join(staging_prefix, object_without_prod_prefix)
    return object_staging_bucket_key


@task(retries=3, retry_delay_seconds=0.5)
def get_md5sum(object_key: str, bucket_name: str, s3_client) -> str:
    """
    Calculate md5sum of an object using url
    This function was modified based on https://github.com/jmwarfe/s3-md5sum/blob/main/s3-md5sum.py
    The new one reads specific byte range from file as a chunk to avoid empty chunk
    aws server getting time out

    expect inputs: bucket_name, object_key
    example: "ccdi-staging", "sub_dir1/sub_dir2/object_file.txt"
    """
    # get obejct
    object_size = s3_client.get_object(Bucket=bucket_name, Key=object_key)[
        "ContentLength"
    ]

    # specify a chunk size to get object, for intance 1Gb
    chunk_size = 1073741824

    chunk_start = 0
    chunk_end = chunk_start + chunk_size - 1

    # Initialize MD5 hash object
    md5_hash = hashlib.md5()
    while chunk_start <= object_size:
        # Read specific byte range from file as a chunk. We do this because AWS server times out and sends
        # empty chunks when streaming the entire file.
        if body := s3_client.get_object(
            Bucket=bucket_name, Key=object_key, Range=f"bytes={chunk_start}-{chunk_end}"
        ).get("Body"):
            # update md5 hash one MB at a time
            for small_chunk in iter(lambda: body.read(1024 * 1024), b""):
                md5_hash.update(small_chunk)
            chunk_start += chunk_size
            chunk_end += chunk_size
    return md5_hash.hexdigest()


@flow(name="Calculate objects md5sum ")
def objects_md5sum(list_keys: list[str], bucket_name: str) -> list[str]:
    """Get a list of md5sum using a list of keys and static bucket name

    Example:
        list_keys = ["folder1/folder2/file1.txt","folder1/folder2/file2.txt","folder1/folder2/file3.txt"]
        bucket_name = "ccdi-staging"
    """
    s3_client = set_s3_session_client()
    logger = get_run_logger()
    md5sum_futures = get_md5sum.map(list_keys, bucket_name, s3_client=s3_client)
    md5sum_list = [i.result() for i in md5sum_futures]
    logger.info(f"md5sum list return is: {*md5sum_list,}")
    s3_client.close()
    return md5sum_list


@flow
def objects_staging_key(
    object_prod_key_list: list[str], prod_bucket_path: str, staging_bucket_path: str
) -> list[str]:
    """Returns a list of proposed keys of objects in staging bucket, given object paths in prod bucket,
    prod bucket name and staging bucket path
    """
    staging_keys_future = construct_staging_bucket_key.map(
        object_prod_key_list, prod_bucket_path, staging_bucket_path
    )
    staging_keys_list = [i.result() for i in staging_keys_future]
    return staging_keys_list


@flow
def objects_if_exist(key_path_list: list[str], bucket: str, logger) -> list:
    """Returns a list of boolean indicating if the object exists

    This flow takes logger input so the parent flow can log objects
    aren't existed
    """
    s3_client = set_s3_session_client()
    if_exist_future = if_object_exists.map(key_path_list, bucket, s3_client, logger)
    if_exist_list = [i.result() for i in if_exist_future]
    s3_client.close()
    return if_exist_list


@task(
    name="Delete Single S3 Object",
    retries=3,
    retry_delay_seconds=0.5,
    tags=["concurrency-test"],
)
def delete_single_object_by_uri(object_uri: str, s3_client, logger) -> str:
    bucket_name, object_key = parse_file_url_in_cds(url=object_uri)
    try:
        s3_client.delete_object(Bucket=bucket_name, Key=object_key)
        delete_status = "Success"
    except ClientError as err:
        logger.info(f"Fail to delete object {object_uri}: {err}")
        delete_status = "Fail"
    return delete_status


@flow(name="Delete S3 Objects")
def delete_objects_by_uri(uri_list, logger) -> None:
    s3_client = set_s3_session_client()
    delete_responses =  delete_single_object_by_uri.map(uri_list, s3_client, logger)
    delete_status_list =  [i.result() for i in delete_responses]
    s3_client.close()
    return delete_status_list


@flow
def retrieve_objects_from_bucket_path(bucket_folder_path: str) -> list[dict]:
    """Returns a list of dict for object files located under bucket_folder_path

    List item example:
        {
            "Bucket": "prod_ccdi",
            "Key": "release_folder/subfolder1/subfolder2/file.txt",
            "Size": 1234
        }
    """
    logger = get_run_logger()
    bucket, folder_prefix = parse_bucket_folder_path(
        bucket_folder_path=bucket_folder_path
    )
    lookup_parameters = paginate_parameter(bucket_path=bucket_folder_path)
    s3 = set_s3_session_client()
    s3_paginator = s3.get_paginator("list_objects_v2")
    pages = s3_paginator.paginate(**lookup_parameters)

    bucket_object_dict_list = []
    for page in pages:
        if "Contents" in page.keys():
            for obj in page["Contents"]:
                obj_dict = {
                    "Bucket": bucket,
                    "Key": obj["Key"],
                    "Size": obj["Size"],
                }
                bucket_object_dict_list.append(obj_dict)

        else:
            logger.info(f"No object file found under {bucket_folder_path}")
            break
    s3.close()
    return bucket_object_dict_list


@flow
def find_missing_objects(
    manifest_df: DataFrame, file_object_list: list[dict]
) -> DataFrame:
    """ Adds a column of object keys in staging bucket if the object
    in prod bucket can't be found under staging bucket path.
    """
    # find nonexist files
    not_found_df = manifest_df.loc[manifest_df["Staging_If_Exist"]==False, ["Key", "Size", "md5sum"]]
    not_found_df["Missing_Object_Candidate_Keys"] =""

    # add file basename to file_object_list
    file_object_list_new = []
    for i in range(len(file_object_list)):
        i_dict = file_object_list[i]
        i_dict["Filename"] = os.path.basename(i_dict['Key'])
        file_object_list_new.append(i_dict)

    for index, row in not_found_df.iterrows():
        row_basename = os.path.basename(row['Key'])
        row_missing_candidates = []
        for k in file_object_list_new:
            if k['Filename'] == row_basename and k['Size'] == row['Size']:
                s3_client = set_s3_session_client()
                k_md5sum = get_md5sum(object_key=k['Key'], bucket_name=k['Bucket'], s3_client=s3_client)
                s3_client.close()
                if k_md5sum == row['md5sum']:
                    row_missing_candidates.append(k['Key'])
                else:
                    pass
            else:
                pass
        not_found_df.loc[index, "Missing_Object_Candidate_Keys"] = row_missing_candidates

    manifest_df["Missing_Object_Candidate_Keys"] = ""
    manifest_df.loc[
        manifest_df["Staging_If_Exist"] == False, "Missing_Object_Candidate_Keys"
    ] = not_found_df["Missing_Object_Candidate_Keys"].tolist()

    return manifest_df


@flow
def create_matching_object_manifest(prod_bucket_path: str, staging_bucket_path: str, runner: str) -> None:
    # get output name
    output_manifest_name = runner + "_matching_objects_manifest_" + get_time() + ".tsv"

    # create logger
    logger = get_run_logger()

    # get prod bucket and prod prefix
    prod_bucket, prod_prefix = parse_bucket_folder_path(
        bucket_folder_path=prod_bucket_path
    )
    staging_bucket, staging_prefix = parse_bucket_folder_path(
        bucket_folder_path=staging_bucket_path
    )

    # create a list of dicts for objects in under prod_bucket_path
    logger.info(f"Reading object files under prod bucket path: {prod_bucket_path}")
    objects_prod_list = retrieve_objects_from_bucket_path(
        bucket_folder_path=prod_bucket_path
    )
    logger.info(f"Files found under prod bucket path: {len(objects_prod_list)}")
    objects_prod_key_list = [i["Key"] for i in objects_prod_list]
    # calculate md5sum of pbjects in prod bucket. If more than 100 objects, split them into chunks
    logger.info("Start calculating md5sum of objects under prod bucket path")
    if len(objects_prod_key_list) > 100:
        objects_prod_key_chunks = list_to_chunks(
            mylist=objects_prod_key_list, chunk_len=100
        )
        logger.info(
            f"md5sum calculation will be processed in {len(objects_prod_key_chunks)} chunks"
        )
        objects_prod_md5sum = []
        for i in range(len(objects_prod_key_chunks)):
            i_md5sum_list = objects_md5sum(
                list_keys=objects_prod_key_chunks[i], bucket_name=prod_bucket
            )
            logger.info(
                f"md5sum calculation progress: {i+1}/{len(objects_prod_key_chunks)}"
            )
            objects_prod_md5sum.extend(i_md5sum_list)
        logger.info("md5sum calculation of prod keys finished")
    else:
        objects_prod_md5sum = objects_md5sum(
            list_keys=objects_prod_key_list, bucket_name=prod_bucket
        )
        logger.info("md5sum calculation of prod keys finished")

    # reconstruct the key in staging bucket
    # This is before validating whether the key in staging bucket exists or not
    # objects_staging_key_list example ["sub_dir/subdir2/file.txt", "dir1/dir2/file.fastq", ...]
    logger.info(f"Start reconstructing staging object keys given the info of staging bucket path: {staging_bucket_path}")
    if len(objects_prod_key_list) > 100:
        objects_prod_key_chunks = list_to_chunks(
            mylist=objects_prod_key_list, chunk_len=100
        )
        logger.info(
            f"Object staging keys reconstruction will be processed in {len(objects_prod_key_chunks)} chunks"
        )
        objects_staging_key_list = []
        for j in range(len(objects_prod_key_chunks)):
            j_staging_key_list = objects_staging_key(
                object_prod_key_list=objects_prod_key_chunks[j],
                prod_bucket_path=prod_bucket_path,
                staging_bucket_path=staging_bucket_path,
            )
            logger.info(
                f"Object staging keys reconstruction progress: {j+1}/{len(objects_prod_key_chunks)}"
            )
            objects_staging_key_list.extend(j_staging_key_list)
        logger.info("Object staging keys reconstruction finished")
    else:
        objects_staging_key_list = objects_staging_key(
            object_prod_key_list=objects_prod_key_list,
            prod_bucket_path=prod_bucket_path,
            staging_bucket_path=staging_bucket_path,
        )
        logger.info("Object staging keys reconstruction finished")

    # check if staging key exists
    logger.info(f"Start checking if objects exist under staging bucket path: {staging_bucket_path}")
    if len(objects_staging_key_list) > 100:
        objects_staging_key_chunks = list_to_chunks(mylist=objects_staging_key_list, chunk_len=100)
        logger.info(
            f"Checking if object staging keys exist will be processed in {len(objects_staging_key_chunks)} chunks"
        )
        if_staging_objects_exist = []
        for h in range(len(objects_staging_key_chunks)):
            h_staging_if_exist = objects_if_exist(
                key_path_list=objects_staging_key_chunks[h], bucket=staging_bucket, logger=logger
            )
            logger.info(
                f"Checking if object staging keys exist progress: {h+1}/{len(objects_staging_key_chunks)}"
            )
            if_staging_objects_exist.extend(h_staging_if_exist)
        logger.info("Checking if object staging keys exist finished")
    else:
        if_staging_objects_exist = objects_if_exist(
            key_path_list=objects_staging_key_list,
            bucket=staging_bucket,
            logger=logger
        )
        logger.info("Checking if object staging keys exist finished")

    logger.info(
        f"files exist under staging bucket path: {sum(if_staging_objects_exist)} / {len(if_staging_objects_exist)}"
    )

    # add prod_md5sum value and proposed staging key, and if the staging key is valid
    # each object dict should have keys: Bucket, Key, Size, Staging_Bucket, Staging_Key, Staging_If_Exist
    manifest_list = []
    for i in range(len(objects_prod_list)):
        i_dict = objects_prod_list[i]
        i_dict["md5sum"] = objects_prod_md5sum[i]
        i_dict["Staging_Bucket"] = staging_bucket
        i_dict["Staging_Key"] = objects_staging_key_list[i]
        i_dict["Staging_If_Exist"] = if_staging_objects_exist[i]
        manifest_list.append(i_dict)
    del objects_prod_list

    # turn manifest_list into a pd dataframe
    manifest_df = pd.DataFrame(manifest_list)
    if sum(manifest_df["Staging_If_Exist"]) < manifest_df.shape[0]:
        missing_staging_keys = manifest_df.loc[
            manifest_df["Staging_If_Exist"] == False, "Staging_Key"
        ].tolist()
        missing_count = manifest_df.shape[0] - sum(manifest_df["Staging_If_Exist"])
        logger.error(
            f"{missing_count} objects not found in staging bucket {staging_bucket_path}:\n{*missing_staging_keys,}"
        )
    else:
        logger.info(
            f"All objects were found under staging bucket path: {staging_bucket_path}"
        )

    # check the md5sum of staging key if the object exists
    logger.info("Start calculating md5sum of objects in staging bucket path if they exist")
    staging_exist_key = manifest_df.loc[
        manifest_df["Staging_If_Exist"] == True, "Staging_Key"
    ].tolist()
    if len(staging_exist_key)>100:
        staging_exist_key_chunks = list_to_chunks(mylist=staging_exist_key, chunk_len=100)
        logger.info(
            f"md5sum calculation will be processed in {len(staging_exist_key_chunks)} chunks"
        )
        objects_staging_md5sum = []
        for k in range(len(staging_exist_key_chunks)):
            logger.info(
                f"md5sum calculation progress: {k+1}/{len(staging_exist_key_chunks)}"
            )
            k_staging_md5sum = objects_md5sum(list_keys=staging_exist_key_chunks[k], bucket_name=staging_bucket)
            objects_staging_md5sum.extend(k_staging_md5sum)
        logger.info("md5sum calculation of staging keys finished")
    else:
        objects_staging_md5sum = objects_md5sum(list_keys=staging_exist_key, bucket_name=staging_bucket)
        logger.info("md5sum calculation of staging keys finished")
    # add staging md5sum values to df
    manifest_df["Staging_md5sum"] = ""
    manifest_df.loc[manifest_df["Staging_If_Exist"] == True, "Staging_md5sum"] = (
        objects_staging_md5sum
    )

    # check if staging md5sum value == prod md5sum
    manifest_df["md5sum_check"] = ""
    # compare md5sum vlaues between prod key and staging key
    manifest_df.loc[
        manifest_df["md5sum"] == manifest_df["Staging_md5sum"], "md5sum_check"
    ] = "Pass"
    passed_md5sum_counts = manifest_df[manifest_df["md5sum_check"]=="Pass"].shape[0]
    logger.info(f"Files passed md5sum checks: {passed_md5sum_counts}")

    # look for missing objects if there are missing ones
    if sum(manifest_df["Staging_If_Exist"]) < manifest_df.shape[0]:
        # if there is object missing, search for entire bucket
        logger.info(f"Not all files can be found under staging bucket path {staging_bucket_path}. Start looking for missing objects in staging bucket {staging_bucket}")
        staging_objects_list = retrieve_objects_from_bucket_path(
            bucket_folder_path=staging_bucket
        )
        manifest_df = find_missing_objects(
            manifest_df=manifest_df, file_object_list=staging_objects_list
        )
        logger.info("Searching for missing objects finished")
    else:
        pass

    # construct full path of staging object
    manifest_df["Staging_S3_URI"] = (
        "s3://" + manifest_df["Staging_Bucket"] + "/" + manifest_df["Staging_Key"]
    )

    # write manifest into file
    logger.info(f"Writing output manifest {output_manifest_name}")
    manifest_df.to_csv(output_manifest_name, sep="\t", index=False)
    return output_manifest_name


@flow
def objects_deletion(manifest_file_path: str, delete_column_name: str, runner: str):
    logger = get_run_logger()

    # read manifest file
    logger.info(f"Reading manifest file {manifest_file_path}")
    manifest_df =  pd.read_csv(manifest_file_path, sep='\t', header=0)

    # check if the delete_column_name can be found in the manifest tsv
    manifest_columns = manifest_df.columns.tolist()
    if delete_column_name not in manifest_columns:
        raise KeyError(f"Column name {delete_column_name} not found in manifest file {manifest_file_path}")
    else:
        pass

    # filter delete_column_name if check_md5sum column is present
    if "md5sum_check" in manifest_columns:
        delete_uri_list = manifest_df.loc[manifest_df["md5sum_check"]=="Pass", delete_column_name].tolist()
    else:
        delete_uri_list = manifest_df[delete_column_name]
    logger.info(f"Number of objects to be deleted: {len(delete_uri_list)}")

    if len(delete_uri_list) > 100:
        delete_chunks =  list_to_chunks(delete_uri_list, 100)
        logger.info(f"Objects deletion will be performed in {len(delete_chunks)} chunks")
        delete_status = []
        for i in range(len(delete_chunks)):
            i_delete_list = delete_chunks[i]
            i_delete_status = delete_objects_by_uri(uri_list=i_delete_list, logger=logger)
            logger.info(f"Objects deletion progress: {i+1}/{len(delete_chunks)}")
            delete_status.extend(i_delete_status)
    else:
        delete_status = delete_objects_by_uri(uri_list=delete_uri_list, logger=logger)
        logger.info("Objects deletion finished")

    success_count, fail_count = count_success_fail(deletion_status=delete_status)
    deletion_counts_df = pd.DataFrame({"Success":[success_count], "Fail":[fail_count]})
    if fail_count >= 1:
        logger.warning(f"Fail to delete files: {fail_count}/{len(delete_status)}")
    else:
        pass
    logger.info(f"Deleted files: {success_count}/{len(delete_status)}")

    # prepare for file deletion output
    delete_output = "objects_deletion_summary_" + get_time() + ".tsv"
    logger.info(f"Writing objects deletion summary table to: {delete_output}")
    delete_dict = {"s3_uri": delete_uri_list, "delete_status" : delete_status}
    delete_df = pd.DataFrame(delete_dict)
    delete_df.to_csv(delete_output, sep='\t', index=False)
    logger.info("Deleting objects finished!")
    return delete_output, deletion_counts_df
