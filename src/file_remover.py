from prefect import flow, task, get_run_logger
from src.utils import set_s3_session_client
from src.read_buckets import paginate_parameter
from botocore.errorfactory import ClientError
import os
import hashlib
import pandas as pd
import numpy as np
from typing import TypeVar


DataFrame = TypeVar("DataFrame")


@task
def if_object_exists(key_path: str, bucket: str, logger) -> None:
    """Retrives the metadata of an object without returning the
    object itself

    To use HEAD, you must have the s3:GetObject permission
    """
    s3_client = set_s3_session_client()
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
    finally:
        s3_client.close()
    return if_exist


def parse_bucket_folder_path(bucket_folder_path: str) -> tuple:
    """Extract bucket name and folder path from a bucket path str

    Example: "ccdi-staging/sub_folder1/sub_folder2"
    Return values: ccdi-staging, "subfolder1/sub_folder2"
    """
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
        staging_bucket, staging_folder/sub_dir1/sub_dir2/example.cram
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
    return staging_bucket, object_staging_bucket_key


@task
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

    # specify a chunk size to get object, for intance 1GB
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


@flow
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
    #logger = get_run_logger()
    staging_keys_future = construct_staging_bucket_key.map(
        object_prod_key_list, prod_bucket_path, staging_bucket_path
    )
    staging_keys_list = [i.result() for i in staging_keys_future]
    #for h in staging_keys_list:
    #    logger.info(f"staging bucket: {h[0]}\nobject key: {h[1]}")
    return staging_keys_list


@flow
def objects_if_exist(key_path_list: list[str], bucket:str, logger) -> list:
    """Returns a list of boolean indicating if the object exists
    
    This flow takes logger input so the parent flow can log objects
    aren't existed
    """
    if_exist_future = if_object_exists.map(key_path_list, bucket, logger)
    if_exist_list = [i.result() for i in if_exist_future]
    return if_exist_list


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
    bucket, folder_prefix =  parse_bucket_folder_path(bucket_folder_path=bucket_folder_path)
    lookup_parameters = paginate_parameter(bucket_path=bucket_folder_path)
    s3 = set_s3_session_client()
    s3_paginator = s3.get_paginator("list_objects_v2")
    pages = s3_paginator.paginate(**lookup_parameters)

    bucket_object_dict_list = []
    for page in pages:
        if "Contents" in page.keys():
            for obj in page['Contents']:
                obj_dict = {
                    "Bucket": bucket,
                    "Key": obj['Key'],
                    "Size": obj['Size'],
                }
                bucket_object_dict_list.append(obj_dict)

        else:
            logger.info(f"No object file found under {bucket_folder_path}")
            break
    return bucket_object_dict_list


@flow
def find_missing_objects(manifest_df: DataFrame, file_object_list: list[dict]) -> DataFrame:
    manifest_df["Missing_Object_Candidate_Keys"] = np.nan
    for index, row in manifest_df.iterrows():
        if row["Staging_If_Exist"] == False:
            row_filename = os.path.basename(row['Key'])
            row_md5sum = row['md5sum']

    return None


@flow
def create_object_manifest(prod_bucket_path: str, staging_bucket_path: str) -> None:
    # create logger
    logger = get_run_logger()

    # get prod bucket and prod prefix
    prod_bucket, prod_prefix = parse_bucket_folder_path(bucket_folder_path=prod_bucket_path)
    staging_bucket, staging_prefix = parse_bucket_folder_path(bucket_folder_path=staging_bucket_path)

    # create a list of dicts for objects in under prod_bucket_path
    objects_prod_list = retrieve_objects_from_bucket_path(bucket_folder_path=prod_bucket_path)
    objects_prod_key_list =  [i['Key'] for i in objects_prod_list]
    objects_prod_md5sum = objects_md5sum(list_keys=objects_prod_key_list, bucket_name=prod_bucket)
    # reconstruct the key in staging bucket
    # This is before validating whether the key in staging bucket exists or not

    objects_staging_key_list = objects_staging_key(
        object_prod_key_list=objects_prod_key_list,
        prod_bucket_path=prod_bucket_path,
        staging_bucket_path=staging_bucket_path
    )

    # add prod_md5sum value and proposed staging key to each object dict
    # each object dict should have keys: Bucket, Key, Size, Staging_Bucket, Staging_Key,
    for i in range(len(objects_prod_list)):
        i_dict = objects_prod_list[i]
        i_dict["md5sum"] = objects_prod_md5sum[i]
        i_dict["Staging_Bucket"] = staging_bucket
        i_dict["Staging_Key"] = objects_staging_key_list[i]
    # turn object_prod_list into a pd dataframe
    manifest_df = pd.DataFrame(objects_prod_list)

    # check if staging keys exist in staging bucket
    if_staging_objects_exist = objects_if_exist(
        key_path_list=manifest_df["Staging_Key"], bucket=staging_bucket, logger=logger
    )
    manifest_df["Staging_If_Exist"] = if_staging_objects_exist
    if sum(manifest_df["Staging_If_Exist"]) < manifest_df.shape[0]:
        missing_staging_keys = manifest_df.loc[manifest_df["Staging_If_Exist"]==False, "Staging_Key"].tolist()
        missing_count = manifest_df.shape[0] - sum(manifest_df["Staging_If_Exist"])
        logger.error(f"{missing_count} objects not found in staging bucket {staging_bucket_path}:\n{*missing_staging_keys,}")
    else:
        logger.info(f"All objects were found under staging bucket path: {staging_bucket_path}")

    # check the md5sum of staging key if the object exists
    staging_exist_key = manifest_df.loc[
        manifest_df["Staging_If_Exist"] == True, "Staging_Key"
    ].tolist()
    objects_staging_md5sum = objects_md5sum(list_keys=staging_exist_key, bucket_name=staging_bucket)
    manifest_df["Staging_md5sum"]=""
    manifest_df.loc[manifest_df["Staging_If_Exist"] == True, "Staging_md5sum"] = (
        objects_staging_md5sum
    )
    manifest_df["md5sum_check"] = ""
    # compare md5sum vlaues between prod key and staging key
    manifest_df.loc[manifest_df["md5sum"]==manifest_df["Staging_md5sum"], "md5sum_check"] = "Pass"
    
    # look for missing objects if there are missing ones
    if sum(manifest_df["Staging_If_Exist"]) < manifest_df.shape[0]:
        # if there is object missing, search for entire bucket
        staging_objects_list = retrieve_objects_from_bucket_path(bucket_folder_path=staging_bucket)
