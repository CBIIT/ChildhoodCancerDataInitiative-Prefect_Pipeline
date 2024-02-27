import sys
import os
import mock
import pytest
from unittest.mock import MagicMock

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.neo4j_data_tools import Neo4jCypherQuery


@pytest.fixture
def myNeo4jQuery():
    return Neo4jCypherQuery()

def test_neo4jcypher_main_cypher_query(myNeo4jQuery):
    node_label =  "test_node"
    query_str = myNeo4jQuery.main_cypher_query.format(node_label=node_label)
    assert "test_node" in query_str


def test_neo4jcypher_node_id_cypher_query_query(myNeo4jQuery):
    study_id = "phs000123"
    node_label = "test_node"
    query_str = myNeo4jQuery.node_id_cypher_query_query.format(study_id=study_id, node=node_label)
    assert "phs000123" in query_str
    assert "test_node" in query_str


def test_neo4jcypher_all_nodes_entries_study_cypher_query(myNeo4jQuery):
    study_id = "phs000456"
    query_str = myNeo4jQuery.all_nodes_entries_study_cypher_query.format(
        study_id=study_id
    )
    assert "phs000456" in query_str
