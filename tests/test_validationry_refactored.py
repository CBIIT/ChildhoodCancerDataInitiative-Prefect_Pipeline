import os
import sys
import mock
import pytest
import pandas as pd
from unittest.mock import MagicMock

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.s3_validationry_refactored import (
    if_template_valid,
    if_string_float,
    if_string_int,
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
    print(fail_again_string)
    assert pass_string
    assert fail_again_string is False

def test_if_string_int():
    """test for if_string_int"""
    pass_string = if_string_int("3")
    fail_string = if_string_int("3.2")
    assert pass_string
    assert fail_string is False
