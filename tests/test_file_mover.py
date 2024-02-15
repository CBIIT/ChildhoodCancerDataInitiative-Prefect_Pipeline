import sys
import os
import mock
import pytest
from unittest.mock import MagicMock

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.file_mover import (
    parse_file_url_in_cds,
    dest_object_url,
    compare_md5sum,
)


def test_parse_url():
    """test for parse_file_url_in_cds()"""
    bucket_name, object_key = parse_file_url_in_cds(
        "s3://mytest-bucket/folder_1/folder2/test_object.fastq"
    )
    assert bucket_name == "mytest-bucket"
    assert object_key == "folder_1/folder2/test_object.fastq"


def test_dest_object_url():
    """test for dest_object_url"""
    dest_url = dest_object_url(
        url_in_cds="s3://origin-bucket/subdir1/subdir/test_object.fastq",
        dest_bucket_path="dest-bucket/test_dir",
    )
    assert dest_url == "s3://dest-bucket/test_dir/subdir1/subdir/test_object.fastq"


@mock.patch("src.utils.set_s3_session_client", autospec=True)
@mock.patch("src.file_mover.calculate_object_md5sum", autospec=True)
def test_compare_md5sum_same(mock_md5sum_calculator, mock_s3_client):
    """test for compare_md5sum"""
    mock_md5sum_calculator.side_effect = [
        "94da06b53eddd226f8e637e3ec85b7ce",
        "94da06b53eddd226f8e637e3ec85b7ce",
    ]
    s3_client = mock_s3_client.return_value
    first_md5sum, _ , compare_result = compare_md5sum(
        first_url="myfirst_url", second_url="mysecond_url", s3_client=s3_client
    )
    assert compare_result
    assert first_md5sum == "94da06b53eddd226f8e637e3ec85b7ce"


@mock.patch("src.utils.set_s3_session_client", autospec=True)
@mock.patch("src.file_mover.calculate_object_md5sum", autospec=True)
def test_compare_md5sum_different(mock_md5sum_calculator, mock_s3_client):
    """test for compare_md5sum"""
    mock_md5sum_calculator.side_effect = [
        "94da06b53eddd226f8e637e3ec85b7ce",
        "94da06b53eddd226f8e637e3ec85b7cg",
    ]
    s3_client = mock_s3_client.return_value
    _ , second_md5sum, compare_result = compare_md5sum(
        first_url="myfirst_url", second_url="mysecond_url", s3_client=s3_client
    )
    assert not compare_result
    assert second_md5sum == "94da06b53eddd226f8e637e3ec85b7cg"
