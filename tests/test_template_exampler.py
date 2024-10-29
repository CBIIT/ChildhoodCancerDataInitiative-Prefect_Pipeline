import os
import sys
import pytest
import mock
import re
import pandas as pd

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.template_exampler import (
    GetFakeValue,
    create_linkage,
    columns_to_populate,
    if_linking_prop,
    get_property_type,
)


# tests for GetFakeValue
@pytest.fixture
def my_getfakevalue():
    return GetFakeValue()


@pytest.fixture
def test_enum_list():
    test_list = ["option1", "option2", "option3", "option4", "option5"]
    return test_list


@pytest.fixture
def test_populated_df():
    test_dict = {
        "study": pd.DataFrame(
            {
                "type": ["study"],
                "study_id": ["sample_study"],
                "phs_accession": ["1234567"],
                "id": [pd.NA],
            }
        ),
        "publication": pd.DataFrame(
            {
                "type": ["publication", "publication", "publication"],
                "study.study_id": [pd.NA, pd.NA, pd.NA],
                "publication_id": ["abe", "def", "ghi"],
                "pubmed_id": ["soccer", "football", "badminton"],
                "id": [pd.NA, pd.NA, pd.NA],
                "study.id": [pd.NA, pd.NA, pd.NA],
            }
        ),
        "participant": pd.DataFrame(
            {
                "type": ["participant", "participant", "participant"],
                "study.study_id": [
                    pd.NA,
                    pd.NA,
                    pd.NA,
                ],
                "publication.publication_id": [pd.NA, pd.NA, pd.NA],
                "particpant_id": ["pid1", "pid2", "pid3"],
                "race": ["White", "American Indian", "Asian"],
                "id": [pd.NA, pd.NA, pd.NA],
                "study.id": [pd.NA, pd.NA, pd.NA],
                "publication.id": [pd.NA, pd.NA, pd.NA],
            }
        ),
    }
    return test_dict


@pytest.fixture
def test_prop_dict_df():
    test_dict_df = pd.DataFrame(
        {
            "Property": ["consent", "race", "medical_history_category"],
            "Node": ["study", "participant", "medical_history"],
            "Type": ["string", "array[string;enum]", "string;enum"],
        }
    )
    return test_dict_df


@mock.patch("src.template_exampler.RandomWord", autospec=True)
def test_getfakevalue_get_word_lib(mock_randomword, my_getfakevalue):
    test_word_lib = my_getfakevalue.get_word_lib()
    mock_randomword.assert_called_once()


def test_getfakevalue_get_fake_md5sum(my_getfakevalue):
    test_md5sum = my_getfakevalue.get_fake_md5sum()
    find_list = re.findall(r"([a-fA-F\d]{32})", test_md5sum)
    assert len(find_list) == 1


def test_getfakevalue_get_fake_uuid(my_getfakevalue):
    test_uuid = my_getfakevalue.get_fake_uuid()
    assert "dg.4DFC/" in test_uuid


@mock.patch("src.template_exampler.RandomWord", autospec=True)
def test_getfakevalue_get_fake_str(mock_randomword, my_getfakevalue):
    mock_randomword.random_words.return_value = ["miniature", "industrious"]
    myfakestr = my_getfakevalue.get_fake_str(random_words=mock_randomword)
    fake_list = myfakestr.split("_")
    assert len(fake_list) == 3
    assert fake_list[:2] == ["miniature", "industrious"]
    assert int(fake_list[2]) <= 100


def test_getfakevalue_get_random_int(my_getfakevalue):
    random_int = my_getfakevalue.get_random_int()
    assert isinstance(random_int, int)
    assert random_int < 1000000


def test_getfakevalue_get_random_number(my_getfakevalue):
    random_float = my_getfakevalue.get_random_number()
    assert isinstance(random_float, float)
    assert random_float < 1000000

def test_getfakevalue_get_random_age(my_getfakevalue):
    random_age = my_getfakevalue.get_random_age()
    assert isinstance(random_age, int)
    assert random_age < 32850

def test_getfakevalue_random_enum_single(my_getfakevalue, test_enum_list):
    random_enum_single = my_getfakevalue.get_random_enum_single(test_enum_list)
    assert random_enum_single in test_enum_list


def test_getfakevalue_random_enum_single_empty(my_getfakevalue):
    random_enum_single = my_getfakevalue.get_random_enum_single(enum_list=[])
    assert random_enum_single == ""


@mock.patch("src.template_exampler.random", autospec=True)
@mock.patch("src.template_exampler.RandomWord", autospec=True)
def test_getfakevalue_random_str_list(mock_randomword, mock_random, my_getfakevalue):
    # fake_random_words = mock_randomword.random_words.return_value
    mock_randomword.random_words.side_effect = [
        ["apple", "orange"],
        ["happy", "sad"],
        ["laugh", "speak"],
    ]
    mock_random.randint.return_value = 3
    test_fake_str_list = my_getfakevalue.get_random_string_list(
        random_words=mock_randomword
    )
    assert len(test_fake_str_list.split(";")) <= 3
    assert len(test_fake_str_list.split(";")[0].split("_")) == 3
    assert len(test_fake_str_list.split(";")[2].split("_")) == 3


def test_getfakevalue_get_random_enum_list(my_getfakevalue, test_enum_list):
    test_enum_list = my_getfakevalue.get_random_enum_list(test_enum_list)
    assert len(test_enum_list.split(";")) >= 1
    assert test_enum_list.split(";")[0] in test_enum_list


def test_getfakevalue_get_random_enum_list_empty(my_getfakevalue):
    test_enum_list = my_getfakevalue.get_random_enum_list(enum_list=[])
    assert test_enum_list == ""


@mock.patch("src.template_exampler.random", autospec=True)
@mock.patch("src.template_exampler.RandomWord", autospec=True)
def test_getfakevalue_get_enum_string_list(
    mock_randomword, mock_random, my_getfakevalue, test_enum_list
):
    mock_randomword.random_words.return_value = ["new", "old"]
    mock_random.randint.side_effect = [2, 1]
    mock_random.sample.return_value = ["apple", "orange"]
    test_enum_str_list_str = my_getfakevalue.get_random_enum_string_list(
        enum_list=test_enum_list, random_words=mock_randomword
    )
    test_enum_str_list = test_enum_str_list_str.split(";")
    assert len(test_enum_str_list) == 3
    assert test_enum_str_list[0] == "apple"
    assert "new_old_" in test_enum_str_list[2]


@mock.patch("src.template_exampler.random", autospec=True)
@mock.patch("src.template_exampler.RandomWord", autospec=True)
def test_getfakevalue_enum_or_str(
    mock_randomword, mock_random, my_getfakevalue, test_enum_list
):
    mock_random.randint.return_value = 0
    mock_random.choice.return_value = "option1"
    enum_or_str = my_getfakevalue.get_random_str_or_enum(
        enum_list=test_enum_list, random_words=mock_randomword
    )
    assert enum_or_str in test_enum_list


@mock.patch("src.template_exampler.random", autospec=True)
def test_create_linkage(mock_random, test_populated_df):
    mock_random.choice.side_effect = [
        "study.study_id",
        "publication.publication_id",
        "study.study_id",
    ]
    linkage_created_dict = create_linkage.fn(populated_dfs=test_populated_df)
    assert linkage_created_dict["publication"]["study.study_id"].tolist() == [
        "sample_study",
        "sample_study",
        "sample_study",
    ]
    assert linkage_created_dict["participant"][
        "publication.publication_id"
    ].tolist() == ["", "def", ""]


def test_columns_to_populate():
    test_cols = [
        "type",
        "study.study_id",
        "anything_id",
        "meta_1",
        "meta2",
        "id",
        "study.id",
    ]
    filter_cols = columns_to_populate(full_col_list=test_cols)
    assert len(filter_cols) == 3
    assert "study.id" not in filter_cols


def test_if_linking_prop():
    test_true_str = "sample.sample_id"
    test_false_str_first = "ethnicity"
    test_false_str_second = "study.id"
    assert if_linking_prop(test_true_str)
    assert not if_linking_prop(test_false_str_first)
    assert not if_linking_prop(test_false_str_second)


def test_get_property_type(test_prop_dict_df):
    test_type_first = get_property_type(
        dict_df=test_prop_dict_df, property_name="race", sheet_name="participant"
    )
    test_type_second = get_property_type(
        dict_df=test_prop_dict_df,
        property_name="medical_history_category",
        sheet_name="medical_history",
    )
    assert test_type_first == "array[string;enum]"
    assert test_type_second == "string;enum"
