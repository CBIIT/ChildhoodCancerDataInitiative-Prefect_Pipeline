import sys
import os
import mock
import pytest
from unittest.mock import MagicMock
from prefect.testing.utilities import prefect_test_harness
from botocore.exceptions import ClientError

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.file_remover import (
    parse_bucket_folder_path,
    construct_staging_bucket_key,
    count_success_fail,
    if_object_exists,
    delete_single_object_by_uri,
)


def test_parse_bucket_folder_path():
    """test for parse_bucket_folder_path"""
    test_bucket_path_one = "test-bucket/sub_folder1/subfolder_2/subfolder_3/"
    test_bucket_path_two = "test-bucket/sub_folder4"
    one_bucket, one_folder_path = parse_bucket_folder_path(
        bucket_folder_path=test_bucket_path_one
    )
    _, two_folder_path = parse_bucket_folder_path(
        bucket_folder_path=test_bucket_path_two
    )
    assert one_bucket == "test-bucket"
    assert one_folder_path == "sub_folder1/subfolder_2/subfolder_3"
    assert two_folder_path == "sub_folder4"


def test_construct_staging_bucket_key():
    """test for construct_staging_bucket_key"""
    object_prod_key = "release_2/sub_dir1/sub_dir2/example.cram"
    prod_bucket_path = "prod_bucket/release_2"
    staging_bucket_path = "staging_bucket/staging_folder"
    staging_key = construct_staging_bucket_key.fn(
        object_prod_bucket_key=object_prod_key,
        prod_bucket_path=prod_bucket_path,
        staging_bucket_path=staging_bucket_path,
    )
    assert staging_key == "staging_folder/sub_dir1/sub_dir2/example.cram"


def test_count_success_fail():
    """test for count_success_fail"""
    test_list = ["Success", "Success", "Fail", "Fail", "Success"]
    count_success, count_fail = count_success_fail(deletion_status=test_list)
    assert count_success == 3
    assert count_fail == 2


def test_if_object_exists_true():
    s3_client = MagicMock()
    logger = MagicMock()
    s3_client.head_object.return_value = {"ContentLength": 123, "Expiration": "string"}
    if_exist = if_object_exists.fn(
        bucket="test-bucket",
        key_path="test_folder/file.txt",
        s3_client=s3_client,
        logger=logger,
    )
    assert if_exist


def test_if_object_exists_false():
    s3_client = MagicMock()
    logger = MagicMock()
    s3_client.head_object.side_effect = ClientError(
        operation_name="InvalidKey",
        error_response={
            "Error": {"Code": "TestErrorCode", "Message": "This is a custom message"}
        },
    )
    if_exist = if_object_exists.fn(
        bucket="test-bucket",
        key_path="test_folder/file.txt",
        s3_client=s3_client,
        logger=logger,
    )
    assert not if_exist


def test_delete_single_object_by_uri_success():
    """test for delete_single_object_by_uri"""
    s3_client = MagicMock()
    logger = MagicMock()
    s3_client.delete_object.return_value = {
        "DeleteMarker": True,
        "VersionId": "string",
        "RequestCharged": "requester",
    }
    delete_status = delete_single_object_by_uri.fn(
        object_uri="s3://test-bucket/folder/file.txt",
        s3_client=s3_client,
        logger=logger,
    )
    assert delete_status == "Success"


def test_delete_single_object_by_uri_fail():
    """test for delete_single_object_by_uri"""
    s3_client = MagicMock()
    logger = MagicMock()
    s3_client.delete_object.side_effect = ClientError(
        operation_name="InvalidKey",
        error_response={
            "Error": {"Code": "TestErrorCode", "Message": "This is a custom message"}
        },
    )
    delete_status = delete_single_object_by_uri.fn(
        object_uri="s3://test-bucket/folder/file.txt",
        s3_client=s3_client,
        logger=logger,
    )
    assert delete_status == "Fail"
