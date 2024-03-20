import sys
import os
import mock
import pytest
from unittest.mock import MagicMock
from prefect.testing.utilities import prefect_test_harness

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.file_remover import parse_bucket_folder_path, construct_staging_bucket_key


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
