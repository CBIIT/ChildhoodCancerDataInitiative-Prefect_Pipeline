import pytest
import os
import sys
import pandas as pd
from prefect import flow, task
from unittest.mock import MagicMock

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.update_ccdi_template import dropSheets, getNodeProp, populate_template_workbook


@pytest.fixture
def workbook_dict_example():
    workbook_dict = {
        "sheet1": pd.DataFrame({"property1": [1, 2, 3], "property2": [1, 2, 3]}),
        "sheet2": pd.DataFrame(
            {
                "property3": [1, 2, 3, 4],
                "property4": [1, 2, 3, 4],
                "property5": [1, 2, 3, 4],
                "property8": [1, 2, 3, 4],
            }
        ),
    }
    return workbook_dict


@pytest.fixture
def template_dict_example():
    template_dict = {
        "sheet1": pd.DataFrame({"property1": [], "property2": []}),
        "sheet2": pd.DataFrame(
            {
                "property3": [],
                "property5": [],
            }
        ),
        "sheet3": pd.DataFrame(
            {
                "property6": [],
                "property7": [],
                "property4": [],
            }
        ),
    }
    return template_dict


def test_dropSheet():
    test_list = [
        "keep_sheet1",
        "README and INSTRUCTIONS",
        "Dictionary",
        "keep_sheet2",
        "Terms and Value Sets",
    ]
    filtered_list = dropSheets(test_list)
    assert len(filtered_list) == 2
    assert "keep_sheet1" in filtered_list


def test_getNodeProp(workbook_dict_example):
    node_prop = getNodeProp.fn(workbook_dict=workbook_dict_example, dict_type="test")
    print(node_prop[node_prop["property"] == "property4"]["node"])
    assert node_prop.shape[0] == 6
    assert node_prop["source"][2] == "test"
    assert (
        node_prop[node_prop["property"] == "property4"]["node"].tolist()[0] == "sheet2"
    )


def test_populate_template_workbook(workbook_dict_example, template_dict_example):
    """Tests if template dict is populated properly"""
    workbook_node_prop = getNodeProp.fn(
        workbook_dict=workbook_dict_example, dict_type="workbook"
    )
    template_node_prop = getNodeProp.fn(
        workbook_dict=template_dict_example, dict_type="template"
    )
    logger_mock = MagicMock()
    populated_template_dict, changed_property_df = populate_template_workbook.fn(
        workbook_dict=workbook_dict_example,
        template_dict=template_dict_example,
        workbook_node_prop=workbook_node_prop,
        template_node_prop=template_node_prop,
        logger=logger_mock,
    )
    assert populated_template_dict["sheet1"]["property1"].tolist() == [1, 2, 3]
    assert (
        changed_property_df[changed_property_df["property"] == "property4"].shape[0]
        == 1
    )
    assert (
        changed_property_df[changed_property_df["property"] == "property8"]["change"].tolist()[0]
        == "Not transfered"
    )
    assert len(populated_template_dict["sheet3"]["property6"].dropna()) == 0
