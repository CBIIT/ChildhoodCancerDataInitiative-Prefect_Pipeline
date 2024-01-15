import os
import sys
import datetime
import pytest
import pandas as pd

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.read_buckets import extract_obj_info, count_df, single_bucket_content_str


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
    print(test_str)
    assert "34.5 MB" in test_str
    assert "| .txt        |       2 |\n" in test_str
    assert "| 2023-12-23 |       5 |\n" in test_str
    assert "789\n" in test_str
