"""
This module provides utility functions for interacting with AWS S3 using the boto3 library.
It includes functionalities for creating S3 clients, loading and parsing manifest files,
tagging S3 objects, and uploading data to S3 in CSV format.

Functions:
- get_client(config: Config) -> S3Client:
    Creates and returns a boto3 S3 client instance using the provided configuration.

- load_manifest(config: Config, client: S3Client) -> List[Dict[str, Any]]:
    Loads a manifest file from an S3 bucket, parses its contents as a CSV, and returns
    a list of dictionaries representing the rows.

- tag_objects(enriched_manifest: List[Dict[str, Any]], client: S3Client) -> List[Dict[str, Any]]:
    Tags S3 objects specified in the enriched manifest with registration and release
    status indicators.

- upload_object(data: List[Dict[str, Any]], ftype: FileType, config: Config, client: S3Client) ->
    PutObjectOutputTypeDef:
    Uploads a CSV file to an S3 bucket using the provided data, file type, and configuration.
"""

import csv
from io import StringIO
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError
from mypy_boto3_s3 import S3Client
from mypy_boto3_s3.type_defs import PutObjectOutputTypeDef

from src.utils.models import Config, FileType


def get_client(config: Config) -> S3Client:
    """
    Returns a boto3 S3 client instance. If a profile is provided, it uses that profile
    to create the session. Otherwise, it uses the default session.

    Args:
        profile (Optional[str]): The name of the AWS profile to use. If None, the
            default profile is used.

    Returns:
        S3Client: A boto3 S3 client instance.
    """
    if config.boto.profile_name:
        session = boto3.Session(profile_name=config.boto.profile_name)
    else:
        session = boto3.Session()
    return session.client("s3")


def load_manifest(client: S3Client, config: Config) -> List[Dict[str, Any]]:
    """
    Loads a manifest file from an S3 bucket and parses its contents into a list of dictionaries.
    The method retrieves the manifest file specified by the bucket and key in the configuration,
    reads its content, and parses it as a CSV file. Each row in the CSV is converted into a
    dictionary, where the keys are the column headers.

    Args:
        client (S3Client): The S3 client instance used to interact with AWS S3.
        config (Config): The configuration object containing S3 bucket and key
            information.
    Returns:
        list[dict]: A list of dictionaries representing the rows in the manifest file.
    Raises:
        ValueError: If the bucket name is not provided in the configuration.
        ClientError: If there is an error retrieving the manifest file from S3.
    """

    if not config.manifest.bucket:
        raise ValueError("Bucket name is not provided in the configuration.")

    try:
        result = client.get_object(
            Bucket=config.manifest.bucket,
            Key=config.manifest.key,
        )
        manifest = result["Body"].read().decode("utf-8").splitlines()
        reader = csv.DictReader(manifest)
        response = [row for row in reader]
        return response
    except ClientError as err:
        raise err


def tag_objects(
    client: S3Client, enriched_manifest: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    This function iterates over a list of records (`enriched_manifest`) and tags the
    objects specified in each record with indicators of release and registration
    status. The tags applied are `"kf_registered"` and `"kf_released"` with their
    respective values from the record in the enriched manifest.

    The enriched manifest is expected to contain the following keys:
    - `nci_bucket` (str): The S3 bucket where the object is stored.
    - `nci_key` (str): The key of the object in the S3 bucket.
    - `kf_registered` (bool): A boolean indicating if the object is registered.
    - `kf_released` (bool): A boolean indicating if the object is released.

    The function returns a list of dictionaries similar to the input `enriched_manifest`,
    with an additional key `tagged` indicating whether the tagging operation was
    successful for each record. If an error occurs during the tagging process,
    the `tagged` key will be set to `False`, and the `tag_error` key will contain
    the error message.

    Args:
        enriched_manifest (List[Dict[str, Any]]): A list of dictionaries representing
            the enriched manifest with S3 object information and registration/release
            status.
        client (S3Client): The S3 client instance used to interact with AWS S3.

    Returns:
        response (List[Dict[str, Any]]): A list of dictionaries representing the enriched manifest
            with an additional key `tagged` indicating the success of the tagging operation, and
            `tag_error` indicating any error that occurred during the tagging process.
    """
    response: List[Dict[str, Any]] = []
    for record in enriched_manifest:

        try:
            result = client.put_object_tagging(
                Bucket=record["nci_bucket"],
                Key=record["nci_key"],
                Tagging={
                    "TagSet": [
                        {
                            "Key": "kf_registered",
                            "Value": str(record["kf_registered"]),
                        },
                        {
                            "Key": "kf_released",
                            "Value": str(record["kf_released"]),
                        },
                    ]
                },
            )
            if result["ResponseMetadata"]["HTTPStatusCode"] == 200:
                record["tagged"] = True
                record["tagging_error"] = None
        except ClientError as err:
            record["tagged"] = False
            record["tag_error"] = err.response.get("Error", {}).get("Code")
            continue
        response.append(record)
    return response


def upload_object(
    client: S3Client, data: List[Dict[str, Any]], ftype: FileType, config: Config
) -> PutObjectOutputTypeDef:
    """
    Uploads a CSV file to an S3 bucket. The file is created from the provided data,
    which is a list of dictionaries. The function uses the boto3 S3 client to upload
    the file to the specified bucket and key. The content type of the file is set to
    "text/csv".
    The function raises a ValueError if the bucket name is not provided in the
    configuration. It also raises a ClientError if there is an error during the
    upload process.

    Args:
        client (S3Client): The S3 client instance used to interact with AWS S3.
        data (List[Dict[str, Any]]): A list of dictionaries representing the data to be
            uploaded as a CSV file.
        ftype (FileType): The type of file being uploaded (e.g., RAW_MANIFEST,
            ENRICHED_MANIFEST, REPORT).
        config (Config): The configuration object containing S3 bucket and key
            information.

    Returns:
        PutObjectOutputTypeDef: The response from the S3 client after the upload
            operation.
    Raises:
        ValueError: If the bucket name is not provided in the configuration.
        ClientError: If there is an error during the upload process.
    """
    if not config.manifest.bucket:
        raise ValueError("Bucket name is not provided in the configuration.")

    if ftype == FileType.RAW_MANIFEST:
        key = config.manifest.key
    elif ftype == FileType.ENRICHED_MANIFEST:
        key = config.manifest.enriched_manifest_key
    elif ftype == FileType.REPORT:
        key = config.manifest.report_key
    else:
        raise ValueError(f"Unsupported file type: {ftype}")

    try:
        csv_buffer = StringIO()
        writer = csv.DictWriter(csv_buffer, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
        response = client.put_object(
            Bucket=config.manifest.bucket,
            Key=key,
            Body=csv_buffer.getvalue(),
            ContentType="text/csv",
        )
        return response
    except ClientError as err:
        raise err


def list_all_objects(
    client: S3Client, bucket: str, prefix: Optional[str] = None
) -> List[str]:
    """
    Lists all the objects in an S3 Bucket with the specified prefix. If no prefix
    is provided, it lists all objects in the bucket. The function uses the
    `list_objects_v2` method of the S3 client to retrieve the objects and
    handles pagination if there are many objects. The function returns a list of
    object keys.

    Args:
        client (S3Client): The S3 client instance used to interact with AWS S3.
        bucket (str): The name of the S3 bucket.
        prefix (Optional[str]): The prefix to filter the objects. If None, all
            objects in the bucket are listed.

    Returns:
        List[str]: A list of object keys in the S3 bucket that match the prefix.

    Raises:
        ClientError: If there is an error retrieving the objects from S3.
    """
    response: List[str] = []
    paginator = client.get_paginator("list_objects_v2")

    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix or ""):
            if "Contents" in page:
                for obj in page["Contents"]:
                    key = obj.get("Key")
                    if key is not None:
                        response.append(key)
    except ClientError as err:
        raise err
    return response


def list_object_tags(client: S3Client, bucket: str, key: str) -> Dict[str, Any]:
    """
    Lists the tags for a specific object in an S3 bucket. The function uses the
    `get_object_tagging` method of the S3 client to retrieve the tags for the
    specified object. It returns a dictionary containing the bucket name, object
    key, and the values of the "kf_released" and "kf_registered" tags if they
    exist. If the tags do not exist, their values will be None.

    Args:
        client (S3Client): The S3 client instance used to interact with AWS S3.
        bucket (str): The name of the S3 bucket.
        key (str): The key of the object in the S3 bucket.
    Returns:
        response (Dict[str, Any]): A dictionary containing the bucket name, object key,
            and the values of the "kf_released" and "kf_registered" tags.
    Raises:
        ClientError: If there is an error retrieving the tags for the object.
    """
    response: Dict[str, Any] = {
        "bucket": bucket,
        "key": key,
        "kf_released": None,
        "kf_registered": None,
    }
    try:
        result = client.get_object_tagging(Bucket=bucket, Key=key)
        for tag in result["TagSet"]:
            if tag["Key"] == "kf_released":
                response["kf_released"] = tag["Value"]
            elif tag["Key"] == "kf_registered":
                response["kf_registered"] = tag["Value"]
    except ClientError as err:
        raise err
    return response


def inventory_object_tags(
    client: S3Client, bucket: str, prefix: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Lists the tags for all objects in an S3 bucket with the specified prefix.
    The function uses the `list_all_objects` function to retrieve all object keys
    in the bucket and then calls the `list_object_tags` function for each object
    to retrieve its tags. It returns a list of dictionaries, each containing the
    bucket name, object key, and the values of the "kf_released" and
    "kf_registered" tags for each object.

    Args:
        client (S3Client): The S3 client instance used to interact with AWS S3.
        bucket (str): The name of the S3 bucket.
        prefix (Optional[str]): The prefix to filter the objects. If None, all
            objects in the bucket are listed.
    Returns:
        response (List[Dict[str, Any]]): A list of dictionaries, each containing the
            bucket name, object key, and the values of the "kf_released" and
            "kf_registered" tags for each object.
    Raises:
        ClientError: If there is an error retrieving the tags for the objects.
    """
    response: List[Dict[str, Any]] = []

    try:
        objects = list_all_objects(client, bucket, prefix)
        for obj in objects:
            tags = list_object_tags(client, bucket, obj)
            response.append(tags)
    except ClientError as err:
        raise err
    return response
