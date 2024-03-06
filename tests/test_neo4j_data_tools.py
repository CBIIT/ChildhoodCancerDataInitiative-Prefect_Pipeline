import sys
import os
import mock
import pytest
from unittest.mock import MagicMock

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from prefect.testing.utilities import prefect_test_harness
from src.neo4j_data_tools import (
    Neo4jCypherQuery,
    cypher_query_parameters,
    compare_id_input_db,
    validate_df_to_count_summary,
    validate_df_to_id_summary,
    list_type_files
)
import pandas as pd


@pytest.fixture
def myNeo4jQuery():
    return Neo4jCypherQuery()


@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    with prefect_test_harness():
        yield


@pytest.fixture
def test_validate_df():
    validate_df = pd.DataFrame(
        {
            "study_id": ["phs000123"] * 10,
            "node": ["node_" + str(i) for i in range(1, 11)],
            "DB_count": [91, 171, 129, 1, 487, 2082, 1, 1, 4, 157],
            "tsv_count": [92, 171, 122, 1, 488, 2082, 1, 1, 4, 148],
            "count_check": [
                "Unequal",
                "Equal",
                "Unequal",
                "Equal",
                "Unequal",
                "Equal",
                "Equal",
                "Equal",
                "Equal",
                "Unequal",
            ],
            "id_check": [
                "Fail",
                "Pass",
                "Fail",
                "Pass",
                "Fail",
                "Pass",
                "Pass",
                "Pass",
                "Pass",
                "Pass",
            ],
            "db_missing_id": [""] * 10,
        }
    )
    return validate_df


def test_neo4jcypher_main_cypher_query(myNeo4jQuery):
    node_label = "test_node"
    query_str = myNeo4jQuery.main_cypher_query.format(node_label=node_label)
    assert "test_node" in query_str


def test_neo4jcypher_node_id_cypher_query_query(myNeo4jQuery):
    study_id = "phs000123"
    node_label = "test_node"
    query_str = myNeo4jQuery.node_id_cypher_query_query.format(
        study_id=study_id, node=node_label
    )
    assert "phs000123" in query_str
    assert "test_node" in query_str


def test_neo4jcypher_all_nodes_entries_study_cypher_query(myNeo4jQuery):
    study_id = "phs000456"
    query_str = myNeo4jQuery.all_nodes_entries_study_cypher_query.format(
        study_id=study_id
    )
    assert "phs000456" in query_str


@mock.patch("src.neo4j_data_tools.get_aws_parameter", autospec=True)
def test_cypher_query_parameters(mock_get_parameter):
    """test cypher_query_parameter"""
    mock_get_parameter.side_effect = [
        {
            "Parameter": {"Value": "test_uri"},
            "ResponseMetadata": {
                "HTTPStatusCode": 200,
            },
        },
        {
            "Parameter": {"Value": "test_username"},
            "ResponseMetadata": {
                "HTTPStatusCode": 200,
            },
        },
        {
            "Parameter": {"Value": "test_password"},
            "ResponseMetadata": {
                "HTTPStatusCode": 200,
            },
        },
    ]
    logger = MagicMock()

    uri_value, username_value, password_value = cypher_query_parameters.fn(
        uri_parameter="uri",
        username_parameter="username",
        password_parameter="password",
        logger=logger,
    )
    assert uri_value == "test_uri"
    assert username_value == "test_username"
    assert password_value == "test_password"


def test_compare_id_input_db():
    """test task compare_id_input_db()"""
    test_db_id_pulled_dict = {
        "test_study_1": {
            "node_1": ["id_1", "id_2", "id_3", "id_4"],
            "node_2": ["id_5", "id_6", "id_7"],
        },
        "test_study_2": {"node_3": ["id_8", "id_9", "id_10", "id_11", "id_12"]},
    }
    test_parsed_tsv_file_df = pd.DataFrame(
        {
            "study_id": ["test_study_1", "test_study_1", "test_study_2"],
            "node": ["node_1", "node_2", "node_3"],
            "tsv_count": [3, 3, 6],
            "tsv_id": [
                ["id_1", "id_2", "id_3"],
                ["id_5", "id_6", "id_7"],
                ["id_8", "id_9", "id_10", "id_11", "id_12", "id_13", "id_14"],
            ],
        }
    )
    logger = MagicMock()
    test_comparison_df = compare_id_input_db.fn(
        db_id_pulled_dict=test_db_id_pulled_dict,
        parsed_tsv_file_df=test_parsed_tsv_file_df,
        logger=logger,
    )
    assert test_comparison_df.loc[1, "count_check"] == "Equal"
    assert test_comparison_df.loc[0, "count_check"] == "Unequal"
    assert test_comparison_df.loc[0, "id_check"] == "Pass"
    assert test_comparison_df.loc[2, "count_check"] == "Unequal"
    assert test_comparison_df.loc[2, "id_check"] == "Fail"
    assert test_comparison_df.loc[2, "db_missing_id"] == "id_13;id_14"


def test_validate_df_to_count_summary(test_validate_df):
    """test task validate_df_to_count_summary()"""
    count_summary_df = validate_df_to_count_summary(validate_df=test_validate_df)
    assert count_summary_df.columns.tolist() == ["Study ID","Entries Count Check","Node Count"]
    assert count_summary_df["Entries Count Check"].tolist() == ["Equal","Unequal"]
    assert count_summary_df["Node Count"].tolist() == [6, 4]


def test_validate_df_to_id_summary(test_validate_df):
    """test task validate_df_to_id_summary"""
    id_summary_df = validate_df_to_id_summary(validate_df=test_validate_df)
    assert id_summary_df.columns.tolist() == ["Study ID","Entries ID Check","Node Count"]
    assert id_summary_df["Entries ID Check"].tolist() == ["Fail", "Pass"]
    assert id_summary_df["Node Count"].tolist() == [3, 7]

@mock.patch("os.listdir", autospec=True)
def test_list_type_files(mock_os_listdir):
    mock_os_listdir.return_value = ["test_1.tsv","test_2.csv","test_3.txt"]
    csv_file_list = list_type_files(file_dir="./test_folder/", file_type=".csv")
    tsv_file_list = list_type_files(file_dir="./test_folder/", file_type=".tsv")
    assert csv_file_list == ["./test_folder/test_2.csv"]
    assert tsv_file_list == ["./test_folder/test_1.tsv"]
