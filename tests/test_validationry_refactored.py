import os
import sys
import mock
import pytest
import pandas as pd
from unittest.mock import MagicMock
import numpy as np
from pathlib import Path
from botocore.exceptions import ClientError


parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.utils import CheckCCDI
from src.s3_validationry_refactored import (
    if_template_valid,
    if_string_float,
    if_string_int,
    cleanup_manifest_nodes,
    validate_required_properties_one_sheet,
    validate_whitespace_one_sheet,
    validate_terms_value_sets_one_sheet,
    validate_integer_numeric_checks_one_sheet,
    validate_unique_key_one_sheet,
    extract_object_file_meta,
    check_file_size_zero,
    check_file_md5sum_regex,
    check_file_basename,
    validate_cross_links_single_sheet,
    validate_key_id_single_sheet,
    count_buckets,
    check_buckets_access,
    validate_single_manifest_obj_in_bucket,
    validate_unique_guid_str,
)


@mock.patch("pandas.ExcelFile")
def test_if_template_valid_pass(mock_ExcelFile):
    """test if_template_valid pass"""
    mock_file = MagicMock()
    mock_ExcelFile.return_value = mock_file
    mock_file.sheet_names = [
        "Dictionary",
        "Terms and Value Sets",
        "Test sheet",
    ]
    template_valid = if_template_valid(template_path="test_file")
    assert template_valid == None


@mock.patch("pandas.ExcelFile")
def test_if_template_valid_fail(mock_ExcelFile):
    """test if_template_valid fail"""
    mock_file = MagicMock()
    mock_ExcelFile.return_value = mock_file
    mock_file.sheet_names = [
        "Dictionary",
        "Test sheet",
    ]
    with pytest.raises(
        ValueError,
        match='Template must include sheet "Dictionary" and "Terms and Value Sets"',
    ):
        if_template_valid(template_path="test_file")


def test_if_string_float():
    """test for if_string_floa
    float(3) won't raise ValueError
    """
    pass_string = if_string_float("2.3")
    fail_again_string = if_string_float("test")
    assert pass_string
    assert fail_again_string is False


def test_if_string_int():
    """test for if_string_int"""
    pass_string = if_string_int("3")
    fail_string = if_string_int("3.2")
    assert pass_string
    assert fail_string is False


@mock.patch("src.s3_validationry_refactored.CheckCCDI")
def test_cleanup_manifest_nodes(mock_checkccdi):
    """test for cleanup_manifest_nodes task"""
    mock_file_object = MagicMock()
    logger = MagicMock()
    mock_checkccdi.return_value = mock_file_object
    mock_file_object.get_sheetnames.return_value = [
        "README and INSTRUCTIONS",
        "apple",
        "Dictionary",
        "pear",
        "Terms and Value Sets",
        "grape",
    ]
    mock_file_object.read_sheet_na.side_effect = [
        pd.DataFrame({"type": [], "apple_id": []}),
        pd.DataFrame(
            {
                "type": ["pear", "pear"],
                "pear_id": ["pear_1", "pear_2"],
                "pear_color": ["green", "yellow"],
            }
        ),
        pd.DataFrame(
            {
                "type": ["grape"] * 3,
                "grape_id": ["grape_1", "grape_2", "grape_3"],
                "grape_color": ["purple", "purple", "green"],
            }
        ),
    ]
    template_node_list = ["grape", "apple", "pear"]
    ordered_nodes = cleanup_manifest_nodes.fn(
        file_path="test_file_path", template_node_list=template_node_list, logger=logger
    )
    assert ordered_nodes == ["grape", "pear"]


def test_validate_required_properties_one_sheet():
    """test for validate_required_properties_one_sheet task"""
    file_object = MagicMock()
    test_df = pd.DataFrame(
        {
            "type": ["type"] * 25,
            "required_prop1": ["anything"] * 8 + [np.nan] * 3 + ["anything"] * 14,
            "required_prop2": ["anything"] * 21 + [np.nan] * 4,
            "nonrequired_prop1": ["anything"] * 7 + [np.nan] * 3 + ["anything"] * 15,
        }
    )
    file_object.read_sheet_na.return_value = test_df
    return_str = validate_required_properties_one_sheet.fn(
        node_name="test_node",
        checkccdi_object=file_object,
        required_properties=["required_prop1", "required_prop2"],
    )
    assert "test_node" in return_str
    assert "10,11,12" in return_str
    assert "required_prop2" in return_str
    assert "nonrequired_prop1" not in return_str


def test_validate_whitespace_one_sheet():
    """task for validate_whitespace_one_sheet"""
    file_object = MagicMock()
    test_df = pd.DataFrame(
        {
            "type": ["test_type"] * 3,
            "prop1": ["apple", "orange", "pear   "],
            "prop2": ["   milk", "juice", "wine"],
        }
    )
    file_object.read_sheet_na.return_value = test_df
    return_str = validate_whitespace_one_sheet.fn(
        node_name="test_node", checkccdi_object=file_object
    )
    assert "prop1" in return_str
    assert "prop2" in return_str
    assert "4" in return_str


# Dir containing some files
FIXTURE_DIR = Path(__file__).parent.resolve() / "test_files"
ALL_FILES = pytest.mark.datafiles(
    FIXTURE_DIR / "CCDI_Submission_Template_v1.7.2.xlsx",
    FIXTURE_DIR
    / "CCDI_Submission_Template_v1.7.2_20Exampler_w_missing_value_additional_error.xlsx",
)


@ALL_FILES
def test_validate_terms_value_sets_one_sheet(datafiles):
    """test for validate_terms_value_sets_one_sheet task"""
    for item in datafiles.iterdir():
        if "Exampler" in item.name:
            file_path = str(item)
        else:
            temp_path = str(item)
    temp_object = CheckCCDI(temp_path)
    file_object = CheckCCDI(file_path)
    validate_str = validate_terms_value_sets_one_sheet.fn(
        node_name="study_personnel",
        checkccdi_object=file_object,
        template_object=temp_object,
    )
    assert "free strings allowed" in validate_str
    assert "personnel_type" in validate_str
    assert "wasteful_fantastic_6" in validate_str
    assert "institution" not in validate_str


@ALL_FILES
def test_validate_terms_value_sets_one_sheet_no_error(datafiles):
    """test for validate_terms_value_sets_one_sheet task"""
    for item in datafiles.iterdir():
        if "Exampler" in item.name:
            file_path = str(item)
        else:
            temp_path = str(item)
    temp_object = CheckCCDI(temp_path)
    file_object = CheckCCDI(file_path)
    validate_str = validate_terms_value_sets_one_sheet.fn(
        node_name="study_funding",
        checkccdi_object=file_object,
        template_object=temp_object,
    )
    assert "\n\tstudy_funding\n\t----------\n\t\n" == validate_str


@ALL_FILES
def test_validate_integer_numeric_checks_one_sheet(datafiles):
    """test for validate_integer_numeric_checks_one_sheet task"""
    for item in datafiles.iterdir():
        if "Exampler" in item.name:
            file_path = str(item)
        else:
            temp_path = str(item)
    temp_object = CheckCCDI(temp_path)
    file_object = CheckCCDI(file_path)
    validate_str = validate_integer_numeric_checks_one_sheet.fn(
        node_name="single_cell_sequencing_file",
        file_object=file_object,
        template_object=temp_object,
    )
    assert "coverage" in validate_str


@ALL_FILES
def test_validate_unique_key_one_sheet(datafiles):
    """test for validate_unique_key_one_sheet task"""
    for item in datafiles.iterdir():
        if "Exampler" in item.name:
            file_path = str(item)
        else:
            temp_path = str(item)
    temp_object = CheckCCDI(temp_path)
    file_object = CheckCCDI(file_path)
    validate_str = validate_unique_key_one_sheet.fn(
        node_name="study_personnel",
        file_object=file_object,
        template_object=temp_object,
    )
    assert "obeisant_pathetic_26" in validate_str


@ALL_FILES
def test_check_file_size_zero(datafiles):
    """test for check_file_size_zero"""
    for item in datafiles.iterdir():
        if "Exampler" in item.name:
            file_path = str(item)
        else:
            temp_path = str(item)
    file_object = CheckCCDI(file_path)
    file_df = extract_object_file_meta(
        nodes_list=[
            "radiology_file",
            "sequencing_file",
        ],
        file_object=file_object,
    )
    print_str = check_file_size_zero(file_df=file_df)
    assert print_str.count("radiology_file") == 2
    assert print_str.count("sequencing_file") == 1


@ALL_FILES
def test_check_file_md5sum_regex(datafiles):
    """test for check_file_md5sum_regex"""
    for item in datafiles.iterdir():
        if "Exampler" in item.name:
            file_path = str(item)
        else:
            temp_path = str(item)
    file_object = CheckCCDI(file_path)
    file_df = extract_object_file_meta(
        nodes_list=[
            "pathology_file",
            "single_cell_sequencing_file",
        ],
        file_object=file_object,
    )
    print_str = check_file_md5sum_regex(file_df=file_df)
    assert print_str.count("pathology_file") == 1
    assert "heartbreaking_whimsical_13" in print_str
    assert "gkfjvhvjhf7" in print_str


@ALL_FILES
def test_check_file_basename(datafiles):
    """test for check_file_basename"""
    for item in datafiles.iterdir():
        if "Exampler" in item.name:
            file_path = str(item)
        else:
            temp_path = str(item)
    file_object = CheckCCDI(file_path)
    file_df = extract_object_file_meta(
        nodes_list=[
            "single_cell_sequencing_file",
        ],
        file_object=file_object,
    )
    print_str = check_file_basename(file_df=file_df)
    assert print_str.count("single_cell_sequencing_file") == 20


@ALL_FILES
def test_validate_cross_links_single_sheet(datafiles):
    """test for validate_cross_links_single_sheet task"""
    for item in datafiles.iterdir():
        if "Exampler" in item.name:
            file_path = str(item)
        else:
            temp_path = str(item)
    file_object = CheckCCDI(file_path)
    print_str = validate_cross_links_single_sheet.fn(
        node_name="single_cell_sequencing_file", file_object=file_object
    )
    assert "diligent_overwrought_80" in print_str
    assert print_str.count("pdx.pdx_id") == 1


@ALL_FILES
def test_validate_key_id_single_sheet(datafiles):
    """test for validate_key_id_single_sheet task"""
    for item in datafiles.iterdir():
        if "Exampler" in item.name:
            file_path = str(item)
        else:
            temp_path = str(item)
    file_object = CheckCCDI(file_path)
    temp_object = CheckCCDI(temp_path)
    print_str = validate_key_id_single_sheet.fn(
        node_name="pdx", file_object=file_object, template_object=temp_object
    )
    assert print_str.count("pdx") == 3
    assert "diligent_overwrought_80&" in print_str

@ALL_FILES
def test_validate_unique_guid_str(datafiles):
    """test for validate_unique_guid_str task"""
    for item in datafiles.iterdir():
        if "Exampler" in item.name:
            file_path = str(item)
        else:
            temp_path = str(item)
    file_object = CheckCCDI(file_path)
    report_str = validate_unique_guid_str.fn(node_list=["radiology_file"], file_object=file_object)
    assert report_str.count("radiology_file") == 2
    assert "dg.4DFC/4f709698-24f0-4673-8827-5ee95b9efe9e" in report_str


def test_count_buckets():
    test_df = pd.DataFrame(
        {
            "type": ["test_type"] * 4,
            "file_url": [
                "s3://bucket1/folder/file1",
                "s3://bucket1/folder/file2",
                "s3://bucket2/folder/file1",
                "s3://bucket3/folder/file3",
            ],
        }
    )
    bucket_list = count_buckets(df_file=test_df)
    print(bucket_list)
    assert len(bucket_list) == 3
    assert "bucket3" in bucket_list


@mock.patch("src.s3_validationry_refactored.set_s3_session_client")
def test_check_buckets_access_all_success(mock_s3client):
    s3_client = MagicMock()
    mock_s3client.return_value = s3_client
    s3_client.head_bucket.side_effect = [True, True]
    invalid_buckets = check_buckets_access(bucket_list=["test-bucket1", "test-bucket2"])
    assert len(invalid_buckets["bucket"]) == 0


@mock.patch("src.s3_validationry_refactored.set_s3_session_client")
def test_check_buckets_access_with_fail(mock_s3client):
    s3_client = MagicMock()
    mock_s3client.return_value = s3_client
    s3_client.head_bucket.side_effect = [
        True,
        ClientError(
            operation_name="InvalidKey",
            error_response={
                "Error": {
                    "Code": "TestErrorCode",
                    "Message": "This is a custom message",
                }
            },
        ),
    ]
    invalid_buckets = check_buckets_access(bucket_list=["test-bucket1", "test-bucket2"])
    assert len(invalid_buckets["bucket"]) == 1
    assert (
        invalid_buckets["error_message"][0] == "TestErrorCode This is a custom message"
    )


def test_validate_single_manifest_obj_in_bucket_success():
    s3_client = MagicMock()
    s3_client.head_object.return_value = {
        "Key": "folder/file.txt",
        "ContentLength": "123",
    }
    if_exist, file_size = validate_single_manifest_obj_in_bucket.fn(
        s3_uri="s3://test-bucket/folder/file.txt", s3_client=s3_client
    )
    assert if_exist
    assert file_size == "123"


def test_validate_single_manifest_obj_in_bucket_fail():
    s3_client = MagicMock()
    s3_client.head_object.side_effect = ClientError(
        operation_name="InvalidKey",
        error_response={
            "Error": {
                "Code": "TestErrorCode",
                "Message": "This is a custom message",
            }
        },
    )
    if_exist, file_size = validate_single_manifest_obj_in_bucket.fn(
        s3_uri="s3://test-bucket/folder/file.txt", s3_client=s3_client
    )
    assert not if_exist
    assert np.isnan(file_size)
