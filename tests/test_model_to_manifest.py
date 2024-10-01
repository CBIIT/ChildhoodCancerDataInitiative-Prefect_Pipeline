import mock
import sys
import os
import pytest
import pandas as pd
import random
from openpyxl import Workbook

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.create_submission import GetCCDIModel, ManifestSheet
import logging
import numpy as np


@pytest.fixture
def getccdimodel_object():
    """GetCCDIModel object for testing purpose"""
    obj = GetCCDIModel(
        model_file="test_model", prop_file="test_prop", term_file="test_term"
    )
    return obj


@pytest.fixture
def getmanifestsheet_object():
    """Get ManifestSheet object for testing purpose"""
    obj = ManifestSheet()
    return obj


test_prop_parametrize_list = [
    (
        # test enum type and example value with enum item less that 5
        {
            "Desc": "test description 1",
            "Term": [{"Origin": "caDSR", "Code": "112233", "Value": "test caDSR"}],
            "Enum": ["Yes", "No", "Unknown", "Not Reported"],
            "Req": False,
            "Strict": True,
            "Private": False,
        },
        (
            "test description 1",
            "enum",
            ["Yes", "No", "Unknown", "Not Reported"],
            False,
            "112233",
        ),
    ),
    (
        # test string;enum type
        {
            "Desc": "test description 2",
            "Term": [
                {
                    "Origin": "caDSR",
                    "Code": "223344",
                    "Value": "test caDSR",
                }
            ],
            "Enum": [
                "Within 5 Minutes",
                "6-30 Minutes",
                "31-60 Minutes",
                "After 60 Minutes",
                "Unknown",
            ],
            "Req": False,
            "Strict": False,
            "Private": False,
        },
        (
            "test description 2",
            "string;enum",
            [
                "Within 5 Minutes",
                "6-30 Minutes",
                "31-60 Minutes",
                "After 60 Minutes",
                "Unknown",
            ],
            False,
            "223344",
        ),
    ),
    (
        # test array[string;enum]
        {
            "Desc": "test description 3",
            "Term": [
                {
                    "Origin": "caDSR",
                    "Code": "334455",
                    "Value": "Tobacco Use Smoking Products Ever Used Regularly Type",
                }
            ],
            "Type": {
                "value_type": "list",
                "item_type": [
                    "Cigarettes",
                    "Cigar",
                ],
            },
            "Req": False,
            "Strict": False,
            "Private": False,
        },
        (
            "test description 3",
            "array[string;enum]",
            [
                "Cigarettes",
                "Cigar",
            ],
            False,
            "334455",
        ),
    ),
]


def test_getccdimodel_init(getccdimodel_object):
    assert getccdimodel_object.model_file == "test_model"
    assert getccdimodel_object.prop_file == "test_prop"
    assert getccdimodel_object.term_file == "test_term"


@pytest.mark.parametrize("prop_dict,expected", test_prop_parametrize_list)
def test_read_each_prop(getccdimodel_object, prop_dict, expected):
    assert getccdimodel_object._read_each_prop(prop_dict=prop_dict) == expected


def test_get_cde_version(getccdimodel_object):
    test_prop_term_dict = {
        "test_prop": {
            "Origin": "caDSR",
            "Definition": "test definition.",
            "Code": 23456789,
            "Version": "2",
            "Value": "Ethnic Group Category Text",
        }
    }
    assert (
        getccdimodel_object._get_prop_cde_version(
            prop_name="test_prop", term_dict=test_prop_term_dict
        )
        == "2"
    )
    assert pd.isna(
        getccdimodel_object._get_prop_cde_version(
            prop_name="test_prop_notfound", term_dict=test_prop_term_dict
        )
    )


def test_get_sorted_node_list(getccdimodel_object):
    preferred_order = getccdimodel_object.node_preferred_order
    test_node_list = random.sample(preferred_order, len(preferred_order))
    test_node_list_addition = random.sample(preferred_order, len(preferred_order)) + [
        "nodeA",
        "nodeB",
    ]
    assert (
        getccdimodel_object._get_sorted_node_list(test_node_list)
        == getccdimodel_object.node_preferred_order
    )
    assert (
        getccdimodel_object._get_sorted_node_list(test_node_list_addition)[:-2]
        == getccdimodel_object.node_preferred_order
    )


def test_manifestsheet_init(getmanifestsheet_object):
    workbook = getmanifestsheet_object.workbook
    assert isinstance(workbook, Workbook)
    assert (
        getmanifestsheet_object.release_api
        == "https://api.github.com/repos/CBIIT/ccdi-model/releases"
    )


def test_manifestsheet_get_sheets_order(getmanifestsheet_object):
    getmanifestsheet_object.workbook.create_sheet("sheetA")
    getmanifestsheet_object.workbook.create_sheet("sheetB")
    getmanifestsheet_object.workbook.create_sheet("sheetC")
    del getmanifestsheet_object.workbook["Sheet"]
    print(getmanifestsheet_object.workbook.sheetnames)
    order_index = getmanifestsheet_object._get_sheets_order(
        expected_name_order=["sheetC", "sheetB", "sheetA"]
    )
    assert order_index == [2, 1, 0]


def test_manifestsheet_if_prop_required(getmanifestsheet_object):
    prop_dict_test = pd.DataFrame(
        {
            "Property": ["propertyA", "propertyB", "propertyC"],
            "Node": ["Node1", "Node2", "Node3"],
            "Required": [np.nan, "Node2", "Node3"],
        }
    )
    if_propA_required = getmanifestsheet_object._if_prop_required(
        prop_name="propertyA", node_name="Node1", prop_dict_df=prop_dict_test, logger=logging
    )
    if_propB_required = getmanifestsheet_object._if_prop_required(
        prop_name="propertyB", node_name="Node2", prop_dict_df=prop_dict_test, logger=logging
    )
    assert not if_propA_required
    assert if_propB_required


@mock.patch("src.create_submission.requests", autospec=True)
def test_manifestsheet_release_api_return(mock_requests, getmanifestsheet_object):
    release_data_test = [
        {
            "name": "v0.1.1: test title 2",
            "html_url": "https://github.com/test/test_repo/releases/tag/0.1.1",
        },
        {
            "name": "v0.1.0: test title 1",
            "html_url": "https://github.com/test/test_repo/releases/tag/0.1.0",
        },
        
    ]
    mock_requests_get =  mock_requests.get.return_value
    mock_requests_get.json.return_value = release_data_test
    version_list, title_list, tag_url_list = getmanifestsheet_object.release_api_return()
    assert version_list == ["v0.1.0","v0.1.1"]
    assert title_list == ["test title 1", "test title 2"]
    assert tag_url_list == [
        "https://github.com/test/test_repo/releases/tag/0.1.0",
        "https://github.com/test/test_repo/releases/tag/0.1.1",
    ]
