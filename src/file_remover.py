from prefect import flow, task, get_run_logger
from src.utils import set_s3_session_client
from botocore.errorfactory import ClientError
import os
import hashlib

def if_object_exists(bucket: str, key_path: str, logger) -> None:
    """Retrives the metadata of an object without returning the
    object itself

    To use HEAD, you must have the s3:GetObject permission
    """
    s3_client= set_s3_session_client()
    try:
        object_meta = s3_client.head_object(Bucket=bucket,Key=key_path)
        if_exist = True
    except ClientError as err:
        err_code = err.response["Error"]["Code"]
        err_message = err.response["Error"]["Message"]
        logger.error(f"Error occurred while fetching metadata of {key_path} from bucjet {bucket}: {err_code} {err_message}")
        if_exist = False
    finally:
        s3_client.close()
    return if_exist

def parse_bucket_folder_path(bucket_folder_path:str) -> tuple:
    """Extract bucket name and folder path from a bucket path str
    
    Example: "ccdi-staging/sub_folder1/sub_folder2"
    Return values: ccdi-staging, "subfolder1/sub_folder2"
    """
    bucket_folder_path =  bucket_folder_path.strip("/")
    if "/" in bucket_folder_path:
        path_list =  bucket_folder_path.split("/", 1)
        bucket, folder_path =  path_list
    else:
        bucket = bucket_folder_path
        folder_path = ""
    return bucket, folder_path

def construct_staging_key(object_prod_bucket_key: str, prod_bucket_path: str, staging_bucket_path: str) -> str:
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
    object_without_prod_prefix = object_prod_bucket_key[len(prod_prefix):].strip("/")
    # parse staging bucket path to staging bucket name, staging prefix
    staging_bucket, staging_prefix = parse_bucket_folder_path(
        bucket_folder_path=staging_bucket_path
    )
    # concatenate with staging bucket path
    object_staging_bucket_key = os.path.join(staging_prefix, object_without_prod_prefix)
    return staging_bucket, object_staging_bucket_key

@task
def get_md5sum(object_key: str, bucket: str) -> str:
    """
    Calculate md5sum of an object using url
    This function was modified based on https://github.com/jmwarfe/s3-md5sum/blob/main/s3-md5sum.py

    expect inputs: bucket_name, object_key
    example: "ccdi-staging", "sub_dir1/sub_dir2/object_file.txt"
    """
    s3_client = set_s3_session_client()
    # get obejct
    obj = s3_client.get_object(Bucket=bucket, Key=object_key)
    obj_body = obj["Body"]

    # Initialize MD5 hash object
    md5_hash = hashlib.md5()
    # Read the object in chunks and update the MD5 hash
    for chunk in iter(lambda: obj_body.read(1024 * 1024), b""):
        md5_hash.update(chunk)
    s3_client.close()
    return md5_hash.hexdigest()

@flow
def objects_md5sum(list_keys: list[str], bucket_name: str):
    """Get a list of md5sum using a list of keys and static bucket name

    Example: 
        list_keys = ["folder1/folder2/file1.txt","folder1/folder2/file2.txt","folder1/folder2/file3.txt"]
        bucket_name = "ccdi-staging"
    """
    logger = get_run_logger()
    md5sum_futures = get_md5sum.map(list_keys, bucket_name)
    md5sum_list = [i.result() for i in md5sum_futures]
    logger.info(f"md5sum list return is: {*md5sum_list,}")
    return md5sum_list
