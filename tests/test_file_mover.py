import sys
import os
import mock
import pytest
import pandas as pd
from unittest.mock import MagicMock

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.file_mover import (
    parse_file_url_in_cds,
    dest_object_url,
    compare_md5sum_task,
    copy_object_parameter,
    add_md5sum_results,
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
@mock.patch("src.file_mover.calculate_object_md5sum_new", autospec=True)
def test_compare_md5sum_same(mock_md5sum_calculator, mock_s3_client):
    """test for compare_md5sum"""
    mock_md5sum_calculator.side_effect = [
        "94da06b53eddd226f8e637e3ec85b7ce",
        "94da06b53eddd226f8e637e3ec85b7ce",
    ]
    s3_client = mock_s3_client.return_value
    logger = MagicMock()
    first_md5sum, _, compare_result = compare_md5sum_task.fn(
        first_url="myfirst_url",
        second_url="mysecond_url",
        s3_client=s3_client,
        logger=logger,
    )
    assert compare_result == "Pass"
    assert first_md5sum == "94da06b53eddd226f8e637e3ec85b7ce"


@mock.patch("src.utils.set_s3_session_client", autospec=True)
@mock.patch("src.file_mover.calculate_object_md5sum_new", autospec=True)
def test_compare_md5sum_different(mock_md5sum_calculator, mock_s3_client):
    """test for compare_md5sum"""
    mock_md5sum_calculator.side_effect = [
        "94da06b53eddd226f8e637e3ec85b7ce",
        "94da06b53eddd226f8e637e3ec85b7cg",
    ]
    s3_client = mock_s3_client.return_value
    logger = MagicMock()
    _, second_md5sum, compare_result = compare_md5sum_task.fn(
        first_url="myfirst_url",
        second_url="mysecond_url",
        s3_client=s3_client,
        logger=logger,
    )
    assert compare_result == "Fail"
    assert second_md5sum == "94da06b53eddd226f8e637e3ec85b7cg"


def test_copy_object_parameter():
    """test for copy_object_parameter"""
    mydict = copy_object_parameter(
        url_in_cds="s3://ccdi-validation/QL/file2.txt",
        dest_bucket_path="my-source-bucket/new_release",
    )
    assert mydict["Bucket"] == "my-source-bucket"
    assert mydict["CopySource"] == "ccdi-validation/QL/file2.txt"
    assert mydict["Key"] == "new_release/QL/file2.txt"


def test_add_md5sum_results():
    """test for add_md5sum_results"""
    transfer_df = pd.DataFrame(
        {
            "node": ["sequencing_file"] * 5,
            "url_before_cp": ["test_url_beofre"] * 5,
            "url_after_cp": ["test_url_after"] * 5,
            "transfer_status": ["Success", "Success", "Fail", "Fail", "Success"],
        }
    )
    md5sum_check_results = [
        (
            "493022b0283b527427fa55f8c57e33c1",
            "493022b0283b527427fa55f8c57e33c1",
            "Pass",
        ),
        (
            "147f568ba31a25b1771fa46c5dd93a7a",
            "147f568ba31a25b1771fa46c5dd93a7b",
            "Fail",
        ),
        ("", "", "Error"),
    ]
    added_transfer_df = add_md5sum_results(
        transfer_df=transfer_df, md5sum_results=md5sum_check_results
    )
    assert added_transfer_df.shape[1] == 7
    assert added_transfer_df.loc[1, "md5sum_check"] == "Fail"
    assert added_transfer_df.loc[2, "md5sum_before_cp"] == ""
    assert added_transfer_df.loc[4, "md5sum_check"] == "Error"
