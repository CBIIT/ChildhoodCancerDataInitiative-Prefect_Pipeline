import os
import sys
import datetime
import pytest
import pandas as pd
import mock
from prefect.testing.utilities import prefect_test_harness

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.read_buckets import (
    extract_obj_info,
    count_df,
    single_bucket_content_str,
    read_bucket_content,
    paginate_parameter,
)


@pytest.fixture
def single_object_return():
    object_dict = {
        "Key": "my/mock/dir/file.txt",
        "Size": 12345,
        "LastModified": datetime.date(2024, 1, 2),
    }
    return object_dict


@pytest.fixture
def single_object_noext_return():
    object_dict = {
        "Key": "my/mock/dir/file_no_extension",
        "Size": 10897,
        "LastModified": datetime.date(2024, 3, 4),
    }
    return object_dict


@pytest.fixture
def single_object_secext_return():
    object_dict = {
        "Key": "my/mock/dir/file_compressed.fastq.gz",
        "Size": 67340,
        "LastModified": datetime.date(2024, 2, 3),
    }
    return object_dict


@pytest.fixture
def paginate_return_iter():
    iterator_list = iter(
        [
            {
                "IsTruncated": True,
                "Contents": [
                    {
                        "Key": "my/mock/dir/file.txt",
                        "Size": 12345,
                        "LastModified": datetime.date(2023, 11, 12),
                    },
                    {
                        "Key": "my/mock/dir/file.txt",
                        "Size": 23456,
                        "LastModified": datetime.date(2023, 11, 13),
                    },
                ],
            },
            {
                "IsTruncated": False,
                "Contents": [
                    {
                        "Key": "my/mock/dir/file_no_extension",
                        "Size": 34567,
                        "LastModified": datetime.date(2023, 11, 13),
                    },
                    {
                        "Key": "my/mock/dir/file.fastq",
                        "Size": 45678,
                        "LastModified": datetime.date(2023, 11, 14),
                    },
                ],
            },
        ]
    )
    return iterator_list


def test_extract_obj_info_w_extension(single_object_return):
    object_ext, object_size, modified_date = extract_obj_info(single_object_return)
    assert object_ext == ".txt"
    assert object_size == 12345
    assert modified_date == "2024-01-02"


def test_extract_obj_info_wo_extension(single_object_noext_return):
    object_ext, object_size, modified_date = extract_obj_info(
        single_object_noext_return
    )
    assert object_ext == "missing ext"
    assert object_size == 10897
    assert modified_date == "2024-03-04"


def test_extract_obj_info_w_secondext(single_object_secext_return):
    object_ext, object_size, modified_date = extract_obj_info(
        single_object_secext_return
    )
    assert object_ext == ".fastq.gz"
    assert object_size == 67340
    assert modified_date == "2024-02-03"


def test_count_df_add_value():
    mydict = {"mykey": [1]}
    newdict = count_df(mydict=mydict, newitem="mykey")
    assert newdict["mykey"][0] == 2


def test_count_df_add_key():
    mydict = {"mykey1": [3], "mykey2": [2]}
    newdict = count_df(mydict=mydict, newitem="mykey3")
    assert newdict["mykey2"][0] == 2
    assert newdict["mykey3"][0] == 1


def test_single_bucket_content_str():
    ext_dict = {"Extension": [".txt", ".pdf", "fastq"], "Count": [2, 3, 4]}
    date_dict = {"Date": ["2023-12-23", "2023-12-31"], "Count": [5, 6]}
    test_str = single_bucket_content_str(
        bucket_name="test_bucket",
        bucket_size=34503214,
        file_count=789,
        ext_dict=pd.DataFrame(ext_dict),
        date_dict=pd.DataFrame(date_dict),
    )
    assert "34.5 MB" in test_str
    assert "| .txt        |       2 |\n" in test_str
    assert "| 2023-12-23 |       5 |\n" in test_str
    assert "789\n" in test_str


@mock.patch("src.read_buckets.set_s3_session_client", autospec=True)
def test_read_bucket_content(mock_s3_client, paginate_return_iter):
    with prefect_test_harness():
        # run the flow on a mocked s3 client return
        s3_client = mock_s3_client.return_value
        mock_paginator = s3_client.get_paginator.return_value
        mock_paginator.paginate.return_value = paginate_return_iter

        bucket_size, file_count, file_ext_df, modified_date_df = read_bucket_content(
            bucket="mock_bucket"
        )
        assert bucket_size == 12345 + 23456 + 34567 + 45678
        assert file_count == 4
        assert file_ext_df.shape[0] == 3
        assert modified_date_df.shape[0] == 3

@pytest.mark.parametrize(
        "bucket_path,expected",
        [("my-bucket",{"Bucket":"my-bucket", "Prefix":""}),
         ("my-bucket/test_subdir/",{"Bucket":"my-bucket","Prefix":"test_subdir"}),
         ("my-bucket/subdir/more_subdir/",{"Bucket":"my-bucket", "Prefix":"subdir/more_subdir"})
        ]
)
def test_paginate_parameter(bucket_path, expected):
    assert paginate_parameter(bucket_path=bucket_path) == expected
    