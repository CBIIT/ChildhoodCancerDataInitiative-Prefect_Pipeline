import os
import sys
import mock
import pytest
import pandas as pd
from unittest.mock import MagicMock
import numpy as np

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.s3_validationry_refactored import (
    if_template_valid,
    if_string_float,
    if_string_int,
    cleanup_manifest_nodes,
    validate_required_properties_one_sheet
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
    mock_file_object = MagicMock()
    logger = MagicMock()
    mock_checkccdi.return_value = mock_file_object
    mock_file_object.get_sheetnames.return_value = [
            "README and INSTRUCTIONS",
            "apple",
            "Dictionary",
            "pear",
            "Terms and Value Sets",
            "grape"
        ]
    mock_file_object.read_sheet_na.side_effect = [
        pd.DataFrame({
            "type":[],
            "apple_id":[]
        }), 
        pd.DataFrame({
            "type":["pear","pear"],
            "pear_id":["pear_1","pear_2"],
            "pear_color": ["green","yellow"]
        }), 
        pd.DataFrame({
            "type":["grape"]*3,
            "grape_id":["grape_1","grape_2","grape_3"],
            "grape_color": ["purple","purple","green"]
        })
    ]
    template_node_list = ["grape","apple","pear"]
    ordered_nodes = cleanup_manifest_nodes.fn(file_path="test_file_path", template_node_list=template_node_list, logger=logger)
    assert ordered_nodes == ["grape","pear"]


def test_validate_required_properties_one_sheet():
    file_object=MagicMock()
    test_df = pd.DataFrame(
        {
            "type": ["type"] * 25,
            "required_prop1": ["anything"] * 8 + [np.nan] * 3 + ["anything"] * 14,
            "required_prop2": ["anything"] * 21 + [np.nan] * 4,
            "nonrequired_prop1": ["anything"] * 7 + [np.nan] * 3 + ["anything"] * 15
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
