import os
import sys
import pytest
from unittest.mock import MagicMock
import mock
import pandas as pd
import numpy as np

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.manifest_liftover import (
    remove_index_cols,
    find_nonempty_nodes,
    multiple_mapping_summary_cleanup,
    tags_validation,
    liftover_tags,
)


@pytest.fixture
def test_mapping_df():
    mapping_df = pd.DataFrame(
        {
            "lift_from_version": ["0.1.0", "0.1.0"],
            "lift_from_node": ["from_node", "from_node"],
            "lift_from_property": ["from_property1", "from_property2"],
            "lift_to_version": ["0.2.0", "0.2.0"],
            "lift_to_node": ["lift_to_node", "lift_to_node"],
            "lift_to_property": ["to_property1", "to_property2"],
        }
    )
    return mapping_df


def test_remove_index_cols():
    test_col_list = ["col1", "type", "id", "id.sample", "col2"]
    cleaned_col = remove_index_cols(col_list=test_col_list)
    assert cleaned_col == ["col1", "col2"]


def test_find_nonempty_nodes():
    ccdi_mock = MagicMock()
    ccdi_mock.get_sheetnames.return_value = [
        "README and INSTRUCTIONS",
        "sheet_1",
        "Dictionary",
        "sheet_2" "Terms and Value Sets",
    ]
    sheet_df_list = [
        pd.DataFrame(
            {
                "type": ["sheet_1", "sheet_1"],
                "sample.sample_id": ["sample1", "sampl2"],
                "id": [np.nan, np.nan],
                "test_col": ["abc", "def"],
                "id.sample": [np.nan, np.nan],
            }
        ),
        pd.DataFrame(
            {
                "type": ["sheet_2"],
                "study.study_id": [np.nan],
                "test_col2": [np.nan],
                "id": [np.nan],
                "id.study": [np.nan],
            }
        ),
    ]
    ccdi_mock.read_sheet_na.side_effect = sheet_df_list
    nonempety_node_list = find_nonempty_nodes(checkccdi_object=ccdi_mock)
    assert nonempety_node_list == ["sheet_1"]


def test_multiple_mapping_summary_cleanup(test_mapping_df):

    cleaned_df = multiple_mapping_summary_cleanup(
        df=test_mapping_df, manifest_version="1.2.3", template_version="2.3.4"
    )
    assert len(cleaned_df.columns) == 4
    assert cleaned_df.columns[0] == "1.2.3_node"
    assert cleaned_df.columns[3] == "2.3.4_property"


@mock.patch("src.manifest_liftover.CheckCCDI")
def test_tags_validation_pass(mock_checkccdi):
    mock_file_object = MagicMock()
    mock_checkccdi.return_value = mock_file_object
    mock_file_object.get_version.return_value = "test_version"
    logger = MagicMock()
    tag_validation = tags_validation.fn(
        manifest_path="any_path", tag="test_version", logger=logger
    )
    assert tag_validation == True


@mock.patch("src.manifest_liftover.CheckCCDI")
def test_tags_validation_fail(mock_checkccdi):
    mock_file_object = MagicMock()
    mock_checkccdi.return_value = mock_file_object
    mock_file_object.get_version.return_value = "fail_version"
    logger = MagicMock()
    tag_validation = tags_validation.fn(
        manifest_path="any_path", tag="test_version", logger=logger
    )
    assert tag_validation == False


@mock.patch("pandas.read_csv", autospec=True)
def test_liftover_tags_wo_error(mock_read_csv, test_mapping_df):
    mock_read_csv.side_effect = [test_mapping_df]
    lift_from_tag, lift_to_tag = liftover_tags.fn(liftover_mapping_path="test_anypath")
    assert lift_from_tag == "0.1.0"
    assert lift_to_tag == "0.2.0"


@mock.patch("pandas.read_csv", autospec=True)
def test_liftover_tags_wo_error(mock_read_csv):
    problematic_mapping_df = pd.DataFrame(
        {
            "lift_from_version": ["0.1.0", "0.1.0"],
            "lift_from_node": ["from_node", "from_node"],
            "lift_from_property": ["from_property1", "from_property2"],
            "lift_to_version": ["0.2.0", "0.3.0"],
            "lift_to_node": ["lift_to_node", "lift_to_node"],
            "lift_to_property": ["to_property1", "to_property2"],
        }
    )
    mock_read_csv.side_effect = [problematic_mapping_df]
    with pytest.raises(ValueError) as excinfo:
        lift_from_tag, lift_to_tag = liftover_tags.fn(liftover_mapping_path="test_anypath")
    assert "('0.2.0', '0.3.0')" in str(excinfo.value)
