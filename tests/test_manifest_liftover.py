import os
import sys
import pytest
from unittest.mock import MagicMock
import mock
import pandas as pd
import numpy as np
from pathlib import Path

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.manifest_liftover import (
    remove_index_cols,
    find_nonempty_nodes,
    find_unlifted_properties,
    multiple_mapping_summary_cleanup,
    tags_validation,
    liftover_tags,
    mapping_coverage
)
from src.utils import CheckCCDI


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
    """test remove_index_cols"""
    test_col_list = ["col1", "type", "id", "id.sample", "col2"]
    cleaned_col = remove_index_cols(col_list=test_col_list)
    assert cleaned_col == ["col1", "col2"]


def test_find_nonempty_nodes():
    """test find_nonempty_nodes"""
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
    """test multiple_mapping_summary_cleanup"""
    cleaned_df = multiple_mapping_summary_cleanup(
        df=test_mapping_df, manifest_version="1.2.3", template_version="2.3.4"
    )
    assert len(cleaned_df.columns) == 4
    assert cleaned_df.columns[0] == "1.2.3_node"
    assert cleaned_df.columns[3] == "2.3.4_property"


@mock.patch("src.manifest_liftover.CheckCCDI")
def test_tags_validation_pass(mock_checkccdi):
    """test tags_validation with True"""
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
    """test tags_validation with False"""
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
    """test for liftover_tags without error"""
    mock_read_csv.side_effect = [test_mapping_df]
    lift_from_tag, lift_to_tag = liftover_tags.fn(liftover_mapping_path="test_anypath")
    assert lift_from_tag == "0.1.0"
    assert lift_to_tag == "0.2.0"


@mock.patch("pandas.read_csv", autospec=True)
def test_liftover_tags_w_error(mock_read_csv):
    """test for liftover_tags with ValueError"""
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


# Dir containing some files
FIXTURE_DIR = Path(__file__).parent.resolve() / "test_files"
ALL_FILES = pytest.mark.datafiles(
    FIXTURE_DIR / "liftover_1.7.2_to_1.8.0.tsv",
    FIXTURE_DIR / "CCDI_Submission_Template_v1.7.2.xlsx",
    FIXTURE_DIR / "liftover_error_1.7.2_to_1.8.0.tsv",
    FIXTURE_DIR / "CCDI_v1.7.2_20Exampler.xlsx",
)

@ALL_FILES
def test_mapping_coverage(datafiles):
    """test for mapping coverage"""
    for item in datafiles.iterdir():
        if "liftover_1.7.2" in item.name:
            mapping_filepath = str(item)
        elif "Submission_Template_v1.7.2" in item.name:
            manifest_filepath = str(item)
        else:
            pass
    manifest_object = CheckCCDI(ccdi_manifest=manifest_filepath)
    mapping_df = pd.read_csv(mapping_filepath, sep="\t")
    missing_prop_df, extra_prop_df = mapping_coverage.fn(mapping_df=mapping_df, manifest_object=manifest_object, node_colname="lift_from_node", prop_colname="lift_from_property")
    assert missing_prop_df.columns.tolist() == ["node","prop"]
    assert extra_prop_df.empty


@ALL_FILES
def test_mapping_coverage_error(datafiles):
    """test for mapping coverage with missing prop and extra prop"""
    for item in datafiles.iterdir():
        if "liftover_error" in item.name:
            mapping_filepath = str(item)
        elif "Submission_Template_v1.7.2" in item.name:
            manifest_filepath = str(item)
        else:
            pass
    manifest_object = CheckCCDI(ccdi_manifest=manifest_filepath)
    mapping_df = pd.read_csv(mapping_filepath, sep="\t")
    missing_prop_df, extra_prop_df = mapping_coverage.fn(
        mapping_df=mapping_df,
        manifest_object=manifest_object,
        node_colname="lift_from_node",
        prop_colname="lift_from_property",
    )
    assert missing_prop_df["prop"].tolist() == ["study_short_title"]
    assert missing_prop_df.shape[0] == 1
    assert extra_prop_df["prop"].tolist() == ["test_extra_1", "test_extra_2"]

@ALL_FILES
def test_find_unlifted_properties(datafiles):
    """test for find_unlifted_properties"""
    for item in datafiles.iterdir():
        if "liftover_1.7.2_to_1.8.0.tsv" in item.name:
            mapping_filepath = str(item)
        elif "CCDI_v1.7.2_20Exampler.xlsx" in item.name:
            manifest_filepath = str(item)
        else:
            pass
    manifest_object = CheckCCDI(ccdi_manifest=manifest_filepath)
    nonempty_nodes = find_nonempty_nodes(manifest_object)
    unlifted_props = find_unlifted_properties(mapping_file=mapping_filepath, nonempty_nodes=nonempty_nodes, checkccdi_object=manifest_object)
    assert sum(unlifted_props["node"]=="pathology_file") == 2
    assert sum(unlifted_props["property"] == "diagnosis_verification_status") == 2
