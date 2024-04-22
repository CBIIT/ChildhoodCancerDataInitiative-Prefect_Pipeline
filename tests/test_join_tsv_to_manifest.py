import sys
import os
import mock
import pandas as pd
import pytest
from unittest.mock import MagicMock
import pyfakefs

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.join_tsv_to_manifest import (
    check_same_study_files,
    check_subfolder,
    find_missing_cols,
    find_id_cols,
    find_parent_id_cols,
    unpack_folder_list,
)


@pytest.fixture
def test_tsv_dfs():
    first_df = pd.DataFrame(
        {
            "id": ["phs000123::" + str(i) for i in range(10)],
            "node": ["test_node_one"] * 10,
        }
    )
    second_df = pd.DataFrame(
        {
            "id": ["phs000123::" + str(i) for i in range(10, 20)],
            "node": ["test_node_two"] * 10,
        }
    )

    return (first_df, second_df)


@pytest.fixture
def test_tsv_dfs_fail():
    first_df = pd.DataFrame(
        {
            "id": ["phs000123::" + str(i) for i in range(10)],
            "node": ["test_node_one"] * 10,
        }
    )
    second_df = pd.DataFrame(
        {
            "id": ["phs000124::" + str(i) for i in range(10, 20)],
            "node": ["test_node_two"] * 10,
        }
    )

    return (first_df, second_df)


def test_find_missing_cols():
    test_tsv_cols = ["apple", "orange", "grape", "watermelon"]
    test_sheet_cols = [
        "pear",
        "apple",
        "kiwi",
        "pineapple",
        "orange",
        "grape",
        "watermelon",
    ]
    missing_cols = find_missing_cols(tsv_cols=test_tsv_cols, sheet_cols=test_sheet_cols)
    assert missing_cols == ["pear", "kiwi", "pineapple"]


def test_find_id_cols():
    test_cols = [
        "id_happy",
        "enchanting.id",
        "belligerent_id.name",
        "workable_test.id",
        "nutritious_tangible",
    ]
    id_cols = find_id_cols(col_list=test_cols)
    assert id_cols == ["enchanting.id", "workable_test.id"]


def test_find_parent_id_cols():
    test_cols = ["koala.id", "lion.id", "tiger.id"]
    parent_ids = find_parent_id_cols(id_cols=test_cols)
    assert parent_ids == ["koala.koala_id", "lion.lion_id", "tiger.tiger_id"]


@mock.patch("pandas.read_csv", autospec=True)
def test_check_same_study_files(mock_read_csv, test_tsv_dfs):
    mock_read_csv.side_effect = test_tsv_dfs
    logger = MagicMock()
    study_accession = check_same_study_files(
        file_list=["testfile_1", "test_file_2"], logger=logger
    )
    assert study_accession == "phs000123"


@mock.patch("pandas.read_csv", autospec=True)
def test_check_same_study_files_fail(mock_read_csv, test_tsv_dfs_fail):
    mock_read_csv.side_effect = test_tsv_dfs_fail
    logger = MagicMock()
    file_list = ["testfile_1", "test_file_2"]
    with pytest.raises(
        ValueError,
        match="More than one study accession were found in a given file list: \(\'testfile_1\', \'test_file_2\'\)",
    ):
        check_same_study_files(file_list=file_list, logger=logger)

def test_check_subfolder_all_tsv(fs):
    fs.create_file("/home/x/alpha.tsv")
    fs.create_file("/home/x/beta.tsv")
    logger=MagicMock()
    if_subfolder = check_subfolder(folder_path="/home/x/", logger=logger)
    assert if_subfolder == "single"


def test_check_subfolder_all_subfolder(fs):
    fs.create_dir("/home/y/alpha_study/")
    fs.create_dir("/home/y/beta_study/")
    logger = MagicMock()
    if_subfolder = check_subfolder(folder_path="/home/y/", logger=logger)
    assert if_subfolder == "multiple"


def test_check_subfolder_fail(fs):
    fs.create_dir("/home/z/alpha_study/")
    fs.create_dir("/home/z/beta_study/")
    fs.create_file("/home/z/alpha.tsv")
    fs.create_file("/home/z/beta.tsv")
    logger = MagicMock()
    with pytest.raises(
        ValueError,
        match="Please provide a bucket folder path that only contains tsv files or subfolders",
    ):
        check_subfolder(folder_path="/home/z/", logger=logger)

def test_unpack_folder_list(fs):
    fs.create_file("/home/z/alpha_study/alpha_1.tsv")
    fs.create_file("/home/z/alpha_study/alpha_2.tsv")
    fs.create_file("/home/z/beta_study/beta_1.tsv")
    fs.create_file("/home/z/beta_study/beta_2.tsv")
    logger = MagicMock()
    unpacked_list = unpack_folder_list(
        folder_path_list=["/home/z/alpha_study/", "/home/z/beta_study/"]
    )
    assert unpacked_list[0] == [
        "/home/z/alpha_study/alpha_1.tsv",
        "/home/z/alpha_study/alpha_2.tsv",
    ]
    assert len(unpacked_list) == 2
