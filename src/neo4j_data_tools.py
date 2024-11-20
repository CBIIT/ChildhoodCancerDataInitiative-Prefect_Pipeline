from src.utils import get_date
from dataclasses import dataclass
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact
from prefect.task_runners import ConcurrentTaskRunner
from neo4j import GraphDatabase
import pandas as pd
import numpy as np
import csv
import os
from typing import TypeVar, Dict, List
import boto3
import json
import tempfile
import traceback
from botocore.exceptions import ClientError


DataFrame = TypeVar("DataFrame")


@dataclass
class Neo4jCypherQuery:
    """Dataclass for Cypher Query"""

    study_cypher_query: str = (
        """
MATCH (startNode:study)
WITH startNode, properties(startNode) AS props
UNWIND keys(props) AS propertyName
RETURN  startNode.id AS startNodeId,
    labels(startNode) AS startNodeLabels,
    propertyName AS startNodePropertyName,
    startNode[propertyName] AS startNodePropertyValue,
    startNode.study_id as dbgap_accession 
"""
    )
    main_cypher_query_per_study_node: str = (
        """
MATCH (startNode:{node_label})-[:of_{node_label}]-(linkedNode)-[*0..5]-(study:study {{study_id:"{study_accession}"}})
WITH study, startNode, linkedNode, properties(startNode) AS props
UNWIND keys(props) AS propertyName
RETURN startNode.id AS startNodeId, 
labels(startNode) AS startNodeLabels, 
propertyName AS startNodePropertyName, 
startNode[propertyName] AS startNodePropertyValue, 
linkedNode.id AS linkedNodeId, 
labels(linkedNode) AS linkedNodeLabels, 
study.study_id AS dbgap_accession
"""
    )
    unique_nodes_query: str = (
        """
MATCH (n)
RETURN DISTINCT labels(n) AS uniqueNodes
"""
    )
    study_list_cypher_query: str = (
        """
MATCH (n:study)
RETURN
    n.study_id as study_id
"""
    )
    all_nodes_entries_study_cypher_query: str = (
        """
MATCH (study:study {{study_id: "{study_id}"}})-[*1..7]-(node)
RETURN labels(node) AS NodeLabel, COUNT(node) AS NodeCount
"""
    )
    node_id_cypher_query_query: str = (
        """
MATCH (study:study{{study_id:"{study_id}"}})-[*0..7]-(node:{node})
RETURN node.id AS id
"""
    )

# dataclass for stats query pipeline
@dataclass
class StatsNeo4jCypherQuery:
    """Dataclass for Stat Related Cypher Queries"""

    #######################
    # STUDY LEVEL QUERIES #
    #######################

    # Query to obtain all unique studies in the database
    stats_get_unique_study_query: str = (
        """
MATCH (s:study)
WITH s.study_id as study_id, s.study_name as study_name
RETURN DISTINCT study_id, study_name
"""
    )

    # Querty to obtain the study PI
    stats_get_pi_query: str = (
        """
MATCH (study:study {{study_id: "{study_id}"}})-[*0..2]-(n:study_personnel {{personnel_type: "PI"}})
WITH labels(n) AS NodeType, COLLECT(n.personnel_name) AS Value
RETURN
    NodeType,
    Value
"""
    )

    # Querty to obtain the study institution based on PI
    stats_get_institution_query: str = (
        """
MATCH (study:study {{study_id: "{study_id}"}})-[*0..2]-(n:study_personnel {{personnel_type: "PI"}})
WITH labels(n) AS NodeType, n.institution AS Value
RETURN
    NodeType,
    Value
"""
    )

    # Querty to obtain prescence of clinical data in study
    stats_get_study_clinical: str = (
        """
OPTIONAL MATCH (study:study {{study_id: "{study_id}"}})-[*0..4]-(n:clinical_measure_file)
WITH
    CASE 
        WHEN n IS NOT NULL THEN 'Yes'
        ELSE 'No'
    END AS Value,
    CASE
        WHEN n IS NOT NULL THEN labels(n)
        ELSE ['clinical_measure_file']
    END AS NodeType
RETURN DISTINCT
    NodeType,
    Value
"""
    )

    # Querty to obtain prescence of pathology data in study
    stats_get_study_pathology: str = (
        """
OPTIONAL MATCH (study:study {{study_id: "{study_id}"}})-[*0..4]-(n:pathology_file)
WITH
    CASE 
        WHEN n IS NOT NULL THEN 'Yes'
        ELSE 'No'
    END AS Value,
    CASE
        WHEN n IS NOT NULL THEN labels(n)
        ELSE ['pathology_file']
    END AS NodeType
RETURN DISTINCT
    NodeType,
    Value
"""
    )

    # Querty to obtain prescence of radiology data in study
    stats_get_study_radiology: str = (
        """
OPTIONAL MATCH (study:study {{study_id: "{study_id}"}})-[*0..4]-(n:radiology_file)
WITH
    CASE 
        WHEN n IS NOT NULL THEN 'Yes'
        ELSE 'No'
    END AS Value,
    CASE
        WHEN n IS NOT NULL THEN labels(n)
        ELSE ['radiology_file']
    END AS NodeType
RETURN DISTINCT
    NodeType,
    Value
"""
    )

    # Querty to obtain prescence of methylation_array data in study
    stats_get_study_methylation_array: str = (
        """
OPTIONAL MATCH (study:study {{study_id: "{study_id}"}})-[*0..4]-(n:methylation_array_file)
WITH
    CASE 
        WHEN n IS NOT NULL THEN 'Yes'
        ELSE 'No'
    END AS Value,
    CASE
        WHEN n IS NOT NULL THEN labels(n)
        ELSE ['methylation_array_file']
    END AS NodeType
RETURN DISTINCT
    NodeType,
    Value
"""
    )

    # Querty to obtain prescence of cytogenomic data in study
    stats_get_study_cytogenomic: str = (
        """
OPTIONAL MATCH (study:study {{study_id: "{study_id}"}})-[*0..4]-(n:cytogenomic_file)
WITH
    CASE 
        WHEN n IS NOT NULL THEN 'Yes'
        ELSE 'No'
    END AS Value,
    CASE
        WHEN n IS NOT NULL THEN labels(n)
        ELSE ['cytogenomic_file']
    END AS NodeType
RETURN DISTINCT
    NodeType,
    Value
"""
    )

    # Query to get file count in study
    stats_get_study_file_count: str = (
        """
WITH ['study_level_file'] AS NodeType
OPTIONAL MATCH (study:study {{study_id: "{study_id}"}})-[*0..4]-(n)
WHERE n.file_size IS NOT NULL
WITH count(n) as Value, NodeType
RETURN DISTINCT
    NodeType,
    Value
"""
    )

    # Query to get file size in study, based on unique file_urls
    stats_get_study_file_size: str = (
        """
WITH ['study_level'] AS NodeType
MATCH (study:study {{study_id: "{study_id}"}})-[*0..4]-(n)
WHERE n.file_size IS NOT NULL AND n.file_url IS NOT NULL
WITH n.file_url AS FileURL, 
    COLLECT(DISTINCT n.file_size) AS FileSizes,
    NodeType
WITH FileURL, REDUCE(totalSize = 0, size IN FileSizes | totalSize + size) AS TotalFileSize, NodeType
RETURN SUM(TotalFileSize) AS Value, NodeType
"""
    )

    # Query to get the unique buckets found in each study
    stats_get_study_buckets: str = (
        """
WITH ['study'] as NodeType
MATCH (study:study {{study_id: "{study_id}"}})-[*0..5]-(n)
WHERE n.file_url IS NOT NULL
WITH n.file_url AS fileUrl, NodeType
WITH COLLECT(DISTINCT substring(fileUrl, 0, apoc.text.indexOf(fileUrl, "/", 5) + 1)) AS Value, NodeType
RETURN 
    NodeType,
    Value
"""
    )

    ######################
    # NODE LEVEL QUERIES #
    ######################

    # Query for study nodes
    stats_get_study_nodes: str = (
        """
MATCH (study:study {{study_id: "{study_id}"}})-[*0..4]-(n)
UNWIND labels(n) AS NodeLabel
RETURN DISTINCT NodeLabel
"""
    )

    # Query to get all records per study in the database, with escaped curly braces
    stats_get_study_node_counts: str = (
        """
MATCH (study:study {{study_id: "{study_id}"}})-[*0..4]-(n:{node})
RETURN labels(n) AS NodeType, COUNT(n) AS Value
"""
    )

    # Query to get file size
    stats_get_study_node_file_size: str = (
        """
MATCH (study:study {{study_id: "{study_id}"}})-[*0..4]-(n:{node})
WHERE n.file_size IS NOT NULL AND n.md5sum IS NOT NULL
WITH n.md5sum AS md5, n.file_size AS fileSize, labels(n) as NodeType
WITH md5, NodeType, MIN(fileSize) AS uniqueFileSize
RETURN
    NodeType,
    SUM(uniqueFileSize) AS Value
"""
    )

    # Query to get library strategies
    stats_get_study_library_strategy: str = (
        """
MATCH (study:study {{study_id: "{study_id}"}})-[*0..4]-(n:{node})
WHERE n.file_size IS NOT NULL AND n.library_strategy IS NOT NULL
WITH study, COLLECT(DISTINCT n.library_strategy) AS Value, labels(n) as NodeType   
RETURN NodeType, Value
"""
    )

    # Get file count by sequencing file library strategy
    stats_get_study_library_strategy_count: str = (
        """
MATCH (study:study {{study_id: "{study_id}"}})-[*0..4]-(n:{node})
WHERE n.file_size IS NOT NULL AND n.library_strategy IS NOT NULL
WITH [n.library_strategy] AS NodeType, COUNT(n) AS Value
RETURN NodeType, Value
"""
    )

    # Get file count by sequencing file library strategy
    stats_get_study_library_strategy_size: str = (
        """
MATCH (study:study {{study_id: "{study_id}"}})-[*0..4]-(n:{node})
WHERE n.file_size IS NOT NULL AND n.library_strategy IS NOT NULL
WITH [n.library_strategy] AS NodeType, sum(n.file_size) AS Value
RETURN NodeType, Value
"""
    )


def get_aws_parameter(parameter_name: str, logger) -> Dict:
    """Returns response from calling simple system manager with a
    parameter name
    """
    # create simple system manager (SSM) client
    ssm_client = boto3.client("ssm")

    try:
        parameter_response = ssm_client.get_parameter(Name=parameter_name)
        logger.info(f"Fetching aws parameter {parameter_name} Done")
        # logger.info(
        #    f"Parameter info:\n{json.dumps(parameter_response, indent=4, default=str)}"
        # )
    except ClientError as err:
        ex_code = err.response["Error"]["Code"]
        ex_message = err.response["Error"]["Message"]
        logger.error(ex_code + ":" + ex_message)
        raise
    except Exception as error:
        logger.error(f"Fetching aws parameter {parameter_name} FAILED")
        logger.error("General exception noted.", exc_info=True)
        raise

    return parameter_response


def list_to_chunks(mylist: list, chunk_len: int) -> list:
    """Break a list into a list of chunks"""
    chunks = [
        mylist[i * chunk_len : (i + 1) * chunk_len]
        for i in range((len(mylist) + chunk_len - 1) // chunk_len)
    ]
    return chunks


@task
def cypher_query_parameters(
    uri_parameter: str, username_parameter: str, password_parameter: str, logger
) -> tuple:
    """Return the value of 3 parameters, which are used to access neo4j DB"""
    uri_reponse = get_aws_parameter(parameter_name=uri_parameter, logger=logger)
    username_response = get_aws_parameter(
        parameter_name=username_parameter, logger=logger
    )
    password_response = get_aws_parameter(
        parameter_name=password_parameter, logger=logger
    )
    return (
        uri_reponse["Parameter"]["Value"],
        username_response["Parameter"]["Value"],
        password_response["Parameter"]["Value"],
    )


def export_to_csv_per_node(tx, node_label: str, cypher_query: str, output_directory: str):
    """Export query results to csv file per node of all studies present in DB"""
    # Run the main Cypher query with the specified node_label
    result = tx.run(cypher_query.format(node_label=node_label))

    output_file_path = os.path.join(output_directory, f"{node_label}_output.csv")

    with open(output_file_path, "w", newline="") as csvfile:
        csv_writer = csv.writer(csvfile)

        # Write header
        header = result.keys()
        csv_writer.writerow(header)

        # Write data rows
        for record in result:
            csv_writer.writerow(record.values())
    return None


def export_to_csv_per_node_per_study(
    tx, study_name: str, node_label: str, cypher_query: str, output_directory: str
) -> None:
    """Export query results to csv file per node per study present in DB"""
    # Run the main Cypher query with the specified node_label
    result = tx.run(cypher_query.format(node_label=node_label, study_accession=study_name))

    output_file_path = os.path.join(output_directory, f"{study_name}_{node_label}_output.csv")

    with open(output_file_path, "w", newline="") as csvfile:
        csv_writer = csv.writer(csvfile)

        # Write header
        header = result.keys()
        csv_writer.writerow(header)

        # Write data rows
        for record in result:
            csv_writer.writerow(record.values())
    return None

@task(name="Pull node data", task_run_name="pull_node_data_{node_label}")
def pull_data_per_node(
    driver, data_to_csv, node_label: str, query_str: str, output_dir: str
) -> None:
    """Exports DB data by a given node"""
    session = driver.session()
    try:
        session.execute_read(
            data_to_csv, node_label, query_str.format(node_label=node_label), output_dir
        )
    except:
        traceback.print_exc()
        raise
    finally:
        session.close()
    return None


@task(name="Pull node data per study", task_run_name="pull_node_data_{node_label}_{study_name}")
def pull_data_per_node_per_study(
    driver, data_to_csv, study_name: str, node_label: str, query_str: str, output_dir: str
) -> None:
    """Exports DB data by a given node and a given study"""
    session = driver.session()
    try:
        #session.execute_read(
        #    data_to_csv, study_name, node_label, query_str.format(node_label=node_label, study_accession=study_name), output_dir
        #3)
        session.execute_read(
            data_to_csv,
            study_name,
            node_label,
            query_str,
            output_dir,
        )
    except:
        traceback.print_exc()
        raise
    finally:
        session.close()
    return None


@flow(task_runner=ConcurrentTaskRunner(), log_prints=True)
def pull_nodes_loop(study_list: list, node_list: list, driver, out_dir: str, logger) -> None:
    """Loops through a list of node labels and pulls data from a neo4j DB"""
    cypher_phrase = Neo4jCypherQuery.main_cypher_query_per_study_node
    per_study_per_node_out_dir = os.path.join(os.path.dirname(out_dir), os.path.basename(out_dir) + "_per_study_per_study")
    print(per_study_per_node_out_dir)
    os.makedirs(per_study_per_node_out_dir, exist_ok=True)

    for study in study_list:
        for node_label in node_list:
            logger.info(f"Pulling from Node {node_label}")
            pull_data_per_node_per_study.submit(
                driver=driver,
                data_to_csv=export_to_csv_per_node_per_study,
                study_name=study,
                node_label=node_label,
                query_str=cypher_phrase,
                output_dir=per_study_per_node_out_dir,
            )
    return None


@flow(log_prints=True)
def combine_node_csv_all_studies(node_list: list[str], out_dir: str):
    """Look at csv query result files and combine the results from the same node together

    Args:
        folder_dir (str): folder that contains query result csv per node per study
        node_list (list[str]): unique node list
    """    
    # look at the out_dir and concatenate files for the same node,
    # so each node can have one csv file
    print("Below is the list of query results per study per node:")
    folder_dir = os.path.join(
        os.path.dirname(out_dir), os.path.basename(out_dir) + "_per_study_per_study"
    )
    print(os.listdir(folder_dir))
    files_list = [
        os.path.join(folder_dir, i)
        for i in os.listdir(folder_dir)
    ]

    for node_label in node_list:
        node_label_phrase =  "_" + node_label + "_output.csv"
        node_file_list  = [i for i in files_list if node_label_phrase in i]
        print(f"files belongs to node {node_label}: {*node_file_list,}")
        node_df = pd.DataFrame(columns=["startNodeId", "startNodeLabels", "startNodePropertyName", "startNodePropertyValue", "linkedNodeId", "linkedNodeLabels", "dbgap_accession"])
        for j in node_file_list:
            j_df= pd.read_csv(j)
            print(j_df.columns)
            print(j_df.head())
            if j_df.shape[0] == 0:
                pass
            else:
                node_df = pd.concat([node_df, j_df], ignore_index=True)
        node_df_filename = node_label + "_output.csv"
        node_df_dir = os.path.join(out_dir, node_df_filename)
        node_df.to_csv(node_df_dir, index=False)
    return None


@flow
def pull_study_node(driver, out_dir: str) -> None:
    """Pulls data for study node from a neo4j DB"""
    cypher_phrase = Neo4jCypherQuery.study_cypher_query
    pull_data_per_node(
        driver=driver,
        data_to_csv=export_to_csv_per_node,
        node_label="study",
        query_str=cypher_phrase,
        output_dir=out_dir,
    )
    return None


@task
def pull_uniq_nodes(driver) -> List:
    """Return a list of nodes in the neo4j DB"""
    session = driver.session()
    try:
        unique_nodes_response = session.run(Neo4jCypherQuery.unique_nodes_query)
        unique_nodes = [record["uniqueNodes"][0] for record in unique_nodes_response]
    except:
        traceback.print_exc()
        raise
    finally:
        session.close()
    return unique_nodes


@task
def pull_uniq_studies(driver) -> List:
    """Return a list of studies in DB"""
    session = driver.session()
    try:
        study_list_response = session.run(Neo4jCypherQuery.study_list_cypher_query)
        study_list = [record["study_id"] for record in study_list_response]
    except:
        traceback.print_exc()
        raise
    finally:
        session.close()
    return study_list


def export_node_counts_a_study(tx, study_id: str, output_dir: str) -> None:
    """Returns a csv which contains counts of entries of every node of a study

    Example content of csv file:
    study_id, node, DB_count
    phs000123, study_admin, 1
    ...
    """
    cypher_query = Neo4jCypherQuery.all_nodes_entries_study_cypher_query.format(
        study_id=study_id
    )
    # run the cypher query with specified study_id
    result = tx.run(cypher_query)
    output_filename = os.path.join(output_dir, f"{study_id}_nodes_entry_counts.csv")
    with open(output_filename, "w", newline="") as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(["study_id", "node", "DB_count"])
        for record in result:
            node_label = record["NodeLabel"][0]
            node_count = record["NodeCount"]
            csv_writer.writerow([study_id, node_label, node_count])
    return None


@task(
    name="Pull counts per node a study",
    task_run_name="pull_counts_per_node_study_{study_id}",
)
def pull_all_nodes_a_study(
    driver, export_to_csv, study_id: str, output_dir: str
) -> None:
    """Executes export_node_count_a_study"""
    session = driver.session()
    try:
        session.execute_read(export_to_csv, study_id, output_dir)
    except:
        traceback.print_exc()
        raise
    finally:
        session.close()
    return None


def export_node_ids_a_study(tx, study_id: str, node: str, output_dir: str) -> None:
    """Writes a csv which contains ids of all entries of a node of a study

    Example content of csv file:
    study_id, node, id
    phs000123, publication, phs000123::random_id
    """
    cypher_query = Neo4jCypherQuery.node_id_cypher_query_query.format(
        study_id=study_id, node=node
    )
    # run the cypher query with specified study and node
    result = tx.run(cypher_query)
    db_id_list = [record["id"] for record in result]
    # print(f"study {study_id} node {node} has ids: {*db_id_list,}")
    study_node_id_df = pd.DataFrame(columns=["study_id", "node", "id"])
    if len(db_id_list) == 0:
        study_node_id_df["id"] = [pd.NA]
    else:
        study_node_id_df["id"] = db_id_list
    study_node_id_df["study_id"] = study_id
    study_node_id_df["node"] = node
    output_filepath = os.path.join(output_dir, f"{study_id}_{node}_id_list.csv")
    study_node_id_df.to_csv(output_filepath, index=False)
    
    return None


@task(
    name="Pull ids a node a study",
    task_run_name="pull_ids_{node}_{study_id}",
    tags=["concurrency-test"],
)
def pull_ids_node_study(
    driver, export_ids_csv, study_id: str, node: str, output_dir: str
) -> None:
    """Executes export_node_ids_a_study"""
    session = driver.session()
    try:
        session.execute_read(export_ids_csv, study_id, node, output_dir)
    except:
        traceback.print_exc()
        raise
    finally:
        session.close()
    return None


@task
def parse_tsv_files(filelist: list) -> DataFrame:
    """Loops through all ingested tsv files downloaded from bucket
    and returns dataframe with columns of ["study_id","node","tsv_count","tsv_id"]
    """
    return_df = pd.DataFrame(columns=["study_id", "node", "tsv_count", "tsv_id"])
    for file in filelist:
        tsv_df = pd.read_csv(file, sep="\t", low_memory=False)
        tsv_study_id = (
            tsv_df["id"].str.split(pat="::", n=1, expand=True)[0].unique().tolist()[0]
        )
        tsv_node = tsv_df["type"].unique().tolist()[0]
        tsv_id_list = tsv_df["id"].tolist()
        tsv_id_count = tsv_df.shape[0]
        tsv_df_info = pd.DataFrame(
            {
                "study_id": [tsv_study_id],
                "node": [tsv_node],
                "tsv_count": [tsv_id_count],
                "tsv_id": [tsv_id_list],
            }
        )
        return_df = pd.concat([return_df, tsv_df_info], ignore_index=True)
    # filter out any node ==  "study"
    return_df = return_df[return_df["node"] != "study"].reset_index(drop=True)
    return return_df


def compare_id_input_db(
    db_id_pulled_dict: dict, parsed_tsv_file_df: DataFrame, logger
) -> DataFrame:
    comparison_df = parsed_tsv_file_df
    comparison_df["count_check"] = np.nan
    comparison_df["id_check"] = np.nan
    comparison_df["db_missing_id"] = np.nan
    for i in range(comparison_df.shape[0]):
        i_study_id = comparison_df.loc[i, "study_id"]
        i_node = comparison_df.loc[i, "node"]
        i_node_id = comparison_df.loc[i, "tsv_id"]
        i_node_id_count = comparison_df.loc[i, "tsv_count"]
        db_node_id_count = len(db_id_pulled_dict[i_study_id][i_node])
        db_node_id = db_id_pulled_dict[i_study_id][i_node]
        if i_node_id_count == db_node_id_count:
            comparison_df.loc[i, "count_check"] = "Equal"
        else:
            comparison_df.loc[i, "count_check"] = "Unequal"
            logger.warning(
                f"Study {i_study_id} node {i_node} ingestion file contains different number of entries compared to neo4j DB"
            )
        db_missing_ids = [i for i in i_node_id if i not in db_node_id]
        if len(db_missing_ids) > 0:
            comparison_df.loc[i, "db_missing_id"] = ";".join(db_missing_ids)
            comparison_df.loc[i, "id_check"] = "Fail"
            logger.error(
                f"Study {i_study_id} node {i_node} ingestion has ids not found in neo4j DB: {*db_missing_ids,}"
            )
        else:
            comparison_df.loc[i, "id_check"] = "Pass"
            logger.info(
                f"Study {i_study_id} node {i_node} ingestion has all ids found in neo4j DB"
            )
    return comparison_df


@flow(task_runner=ConcurrentTaskRunner(), log_prints=True)
def pull_node_ids_all_studies_write(
    driver, studies_dataframe: DataFrame, logger
) -> str:
    """Returns a temp folder that contains files of ids for each node each study"""
    # create a temp folder
    temp_folder_name = "db_ids_all_node_all_studies"
    os.mkdir(temp_folder_name)

    print(f"ingested studies dataframe has rows: {studies_dataframe.shape[0]}")
    print(f"ingested studies dataframe size: {studies_dataframe.size}")
    logger.info(
        f"ingested studies dataframe has rows of {studies_dataframe.shape[0]} and size of {studies_dataframe.size}"
    )
    study_id_list = studies_dataframe["study_id"].tolist()
    study_id_chunks = list_to_chunks(study_id_list, 50)
    node_list = studies_dataframe["node"].tolist()
    node_chunks = list_to_chunks(node_list, 50)
    for i in range(len(node_chunks)):
        # print(f"study_id_list: {*study_id_chunks[i],}")
        # print(f"node_list: {*node_chunks[i],}")
        pull_ids_node_study.map(
            driver,
            export_node_ids_a_study,
            study_id_chunks[i],
            node_chunks[i],
            temp_folder_name,
        )

    return temp_folder_name


@flow(log_prints=True)
def pull_node_ids_all_studies(driver, studies_dataframe: DataFrame, logger) -> Dict:
    """Returns a dictionary of db id list using study id and node name

    The function takes dataframe which contains columns of study_id and node as input.
    It loops each row, and pulls id list of a node in one study.
    """
    csv_folder = pull_node_ids_all_studies_write(
        driver=driver, studies_dataframe=studies_dataframe, logger=logger
    )
    ids_dict = {}
    csv_list = os.listdir(csv_folder)
    for file in csv_list:
        file_path = os.path.join(csv_folder, file)
        file_df = pd.read_csv(file_path, header=0)
        file_study = file_df["study_id"].unique().tolist()[0]
        if file_study not in ids_dict.keys():
            ids_dict[file_study] = {}
        else:
            pass
        file_node = file_df["node"].unique().tolist()[0]
        ids_dict[file_study][file_node] = file_df["id"].dropna().tolist()
    return ids_dict


@flow(task_runner=ConcurrentTaskRunner())
def pull_studies_loop_write(driver, study_list: list, logger) -> DataFrame:
    """Returns temp folder which contains counts all nodes(except study node)
    of all studies in a DB
    """
    # create a folder to keep all node entry counts per study
    temp_folder_name = "db_node_entry_counts_all_studies"
    os.mkdir(temp_folder_name)
    logger.info("Start pulling entry counts per node per study")
    for study in study_list:
        logger.info(f"Pulling entry counts per node for study {study}")
        pull_all_nodes_a_study.submit(
            driver=driver,
            export_to_csv=export_node_counts_a_study,
            study_id=study,
            output_dir=temp_folder_name,
        )

    return temp_folder_name


@flow
def pull_studies_loop(driver, study_list: list, logger) -> DataFrame:
    """Return a dataframe contains entry counts per node of all studies in DB"""
    csv_folder = pull_studies_loop_write(
        driver=driver, study_list=study_list, logger=logger
    )
    csv_filelist = os.listdir(csv_folder)
    count_df = pd.DataFrame(columns=["study_id", "node", "DB_count"])
    for file in csv_filelist:
        file_path = os.path.join(csv_folder, file)
        file_df = pd.read_csv(file_path, header=0)
        count_df = pd.concat([count_df, file_df], ignore_index=True)
    return count_df


@flow
def counts_DB_all_nodes_all_studies(
    uri_parameter: str, username_parameter: str, password_parameter: str
) -> Dict:
    logger = get_run_logger()

    logger.info("Getting uri, username and password parameter from AWS")
    # get uri, username, and password value
    uri, username, password = cypher_query_parameters(
        uri_parameter=uri_parameter,
        username_parameter=username_parameter,
        password_parameter=password_parameter,
        logger=logger,
    )

    # driver instance
    logger.info("Creating GraphDatabase driver using uri, username, and password")
    driver = GraphDatabase.driver(uri, auth=(username, password))

    # fetch all unique studies
    study_list = pull_uniq_studies(driver=driver)
    logger.info(f"Unique study list in DB: {*study_list,}")

    # Loop through each study and fetch counts of all nodes per study
    studies_dataframe = pull_studies_loop(
        driver=driver, study_list=study_list, logger=logger
    )

    driver.close()

    return studies_dataframe


@flow(log_prints=True)
def validate_DB_with_input_tsvs(
    uri_parameter: str,
    username_parameter: str,
    password_parameter: str,
    tsv_folder: str,
    studies_dataframe: DataFrame,
) -> DataFrame:
    logger = get_run_logger()

    logger.info("Getting uri, username and password parameter from AWS")
    # get uri, username, and password value
    uri, username, password = cypher_query_parameters(
        uri_parameter=uri_parameter,
        username_parameter=username_parameter,
        password_parameter=password_parameter,
        logger=logger,
    )

    # driver instance
    logger.info("Creating GraphDatabase driver using uri, username, and password")
    driver = GraphDatabase.driver(uri, auth=(username, password))

    # read trhough tsv folder and extrac study_id, node, id_count,
    # and id list from each file
    tsv_files = list_type_files(file_dir=tsv_folder, file_type=".tsv")
    ingested_studies_dataframe = parse_tsv_files(tsv_files)

    logger.info("pulled all ids based off the nodes and studies from tsv provided")
    db_id_list_all_studies = pull_node_ids_all_studies(
        driver=driver,
        studies_dataframe=ingested_studies_dataframe[["study_id", "node"]],
        logger=logger,
    )

    logger.info("Start comparing db pulled id with ids in tsv files")
    comparison_df = compare_id_input_db(
        db_id_pulled_dict=db_id_list_all_studies,
        parsed_tsv_file_df=ingested_studies_dataframe,
        logger=logger,
    )

    merged_summary_table = pd.merge(
        studies_dataframe, comparison_df, on=["study_id", "node"], how="outer"
    )
    merged_summary_table.drop(columns=["tsv_id"], inplace=True)

    # sort the df order based on study id and node
    merged_summary_table = merged_summary_table.sort_values(
        by=["study_id", "node"], ascending=True
    )
    # close driver
    driver.close()
    return merged_summary_table


def validate_df_to_count_summary(validate_df: DataFrame) -> DataFrame:
    count_summary_df = (
        validate_df.groupby(["study_id", "count_check"])["node"]
        .agg("count")
        .reset_index()
        .rename(
            columns={
                "study_id": "Study ID",
                "count_check": "Entries Count Check",
                "node": "Node Count",
            }
        )
    )
    return count_summary_df


def validate_df_to_id_summary(validate_df: DataFrame) -> DataFrame:
    id_summary_df = (
        validate_df.groupby(["study_id", "id_check"])["node"]
        .agg("count")
        .reset_index()
        .rename(
            columns={
                "study_id": "Study ID",
                "id_check": "Entries ID Check",
                "node": "Node Count",
            }
        )
    )
    return id_summary_df


@task
def neo4j_validation_md(
    count_summary_df: DataFrame, id_summary_df: DataFrame, runner=str
) -> None:
    count_df_str = count_summary_df.to_markdown(tablefmt="pipe", index=False)
    id_df_str = id_summary_df.to_markdown(tablefmt="pipe", index=False)
    markdown_report = f"""# CCDI Neo4j DB Validation Summary

## Entry Count Validation Summary

{count_df_str}

## Entry ID Validation Summary

{id_df_str}

"""
    create_markdown_artifact(
        key=f"neo4j-validation-report-{runner.lower().replace('_','-').replace(' ','-').replace('.','-').replace('/','-')}",
        markdown=markdown_report,
        description=f"Neo4j validation Report for {runner}",
    )
    return None


@flow
def query_db_to_csv(
    output_dir: str,
    uri_parameter: str,
    username_parameter: str,
    password_parameter: str,
) -> str:
    """It export one csv file for each unique node.
    Each csv file (per node) contains all the info of the node across all studies
    in DB
    """
    logger = get_run_logger()
    logger.info(
        f"Creating folder {output_dir} if not exists for writing data pulled from Neo4j DB"
    )
    # create the output dir if not exist
    os.makedirs(output_dir, exist_ok=True)

    logger.info("Getting uri, username and password parameter from AWS")
    # get uri, username, and password value
    uri, username, password = cypher_query_parameters(
        uri_parameter=uri_parameter,
        username_parameter=username_parameter,
        password_parameter=password_parameter,
        logger=logger,
    )

    # driver instance
    logger.info("Creating GraphDatabase driver using uri, username, and password")
    driver = GraphDatabase.driver(uri, auth=(username, password))

    # fetch unique nodes and unique studies
    logger.info("Fetching all unique nodes DB")
    unique_nodes = pull_uniq_nodes(driver=driver)
    unqiue_studies = pull_uniq_studies(driver=driver)
    logger.info(f"Nodes list: {*unique_nodes,}")

    # Iterate through each unique node and export data
    logger.info("Pulling data by each node")
    pull_nodes_loop(
       study_list = unqiue_studies, node_list=unique_nodes, driver=driver, out_dir=output_dir, logger=logger
    )

    # combine all csv of same node into single file
    combine_node_csv_all_studies(out_dir=output_dir, node_list=unique_nodes)

    # Obtain study node data
    logger.info("Pulling data from study node")
    pull_study_node(driver=driver, out_dir=output_dir)

    # close the driver
    logger.info("Closing GraphDatabase driver")
    driver.close()

    return output_dir


def list_type_files(file_dir: str, file_type: str) -> list:
    """Returns a list of matched file paths under a folder path"""
    file_list = os.listdir(file_dir)
    matched_files = [
        os.path.join(file_dir, file) for file in file_list if file.endswith(file_type)
    ]
    if len(matched_files) == 0:
        raise ValueError(
            f"No matched {file_type} files were found under folder {file_dir}"
        )
    else:
        pass
    return matched_files


@task(log_prints=True)
def pivot_long_df_wide_clean(file_path: str) -> DataFrame:
    """Pivot the long df to wider df
    It also removes quotes from column names and value
    """
    df_long = pd.read_csv(file_path)

    # Pivot the DataFrame to wide format
    df_wide = df_long.pivot(
        index="startNodeId",
        columns="startNodePropertyName",
        values="startNodePropertyValue",
    ).reset_index()

    df_wide = df_wide.merge(
        df_long[["startNodeId", "startNodeLabels"]].drop_duplicates(), on="startNodeId"
    )

    if "['study']" not in df_long["startNodeLabels"].unique().tolist():
        # Preserve relational columns by merging with the original DataFrame
        df_wide = df_wide.merge(
            df_long[["startNodeId", "linkedNodeId"]].drop_duplicates(), on="startNodeId"
        )
        df_wide = df_wide.merge(
            df_long[["startNodeId", "linkedNodeLabels"]].drop_duplicates(),
            on="startNodeId",
        )
        df_wide = df_wide.merge(
            df_long[["startNodeId", "dbgap_accession"]].drop_duplicates(),
            on="startNodeId",
        )

        df_wide["linkedNodeLabels"] = df_wide["linkedNodeLabels"].str.strip("['")
        df_wide["linkedNodeLabels"] = df_wide["linkedNodeLabels"].str.strip("']")

    else:
        pass

    # remove quotes from column names.
    # remove quotes and brackets from str value
    df_wide.columns = df_wide.columns.str.strip('"')
    df_wide.columns = df_wide.columns.str.strip("'")

    # Only fix the node label and nothing else.
    df_wide["startNodeLabels"] = df_wide["startNodeLabels"].str.strip("['")
    df_wide["startNodeLabels"] = df_wide["startNodeLabels"].str.strip("']")

    # removed as it was affecting acl property.
    # df_wide = df_wide.applymap(lambda x: x.strip("[") if isinstance(x, str) else x)
    # df_wide = df_wide.applymap(lambda x: x.strip("]") if isinstance(x, str) else x)
    # df_wide = df_wide.applymap(lambda x: x.strip('"') if isinstance(x, str) else x)
    # df_wide = df_wide.applymap(lambda x: x.strip("'") if isinstance(x, str) else x)

    # remove few columns
    df_wide["type"] = df_wide["startNodeLabels"]
    df_wide.drop(
        ["startNodeId", "created", "startNodeLabels", "uuid"], axis=1, inplace=True
    )
    return df_wide


@task(log_prints=True)
def wide_df_setup_link(df_wide: DataFrame) -> DataFrame:
    """Setup links in wide df"""
    print("setup links in wide df")
    if "study" not in df_wide["type"].unique().tolist():
        df_wide["linkedNodeLabels"] = df_wide["linkedNodeLabels"] + ".id"

        # Add [node].id columns
        df_wide_links = df_wide.pivot(
            index="id", columns="linkedNodeLabels", values="linkedNodeId"
        ).reset_index()

        # Add linkages back into data frame and drop extra columns
        df_wide_links = df_wide_links.merge(df_wide.drop_duplicates(), on="id")
        df_wide_links = df_wide_links.drop(["linkedNodeId", "linkedNodeLabels"], axis=1)
        df_wide = df_wide_links

        # below should only work for non study nodes

        df_wide["study"] = df_wide["dbgap_accession"]
        df_wide.drop(columns=["dbgap_accession"], inplace=True)
        if "updated" in df_wide.columns:
            df_wide.drop(columns=["updated"], inplace=True)
        else:
            pass
    else:
        # this is only for study node
        df_wide["study"] = df_wide["study_id"]
        if "updated" in df_wide.columns:
            df_wide.drop(columns=["updated"], inplace=True)
        else:
            pass
    return df_wide


@task
def write_wider_df_all(wider_df: DataFrame, output_dir: str, logger) -> None:
    """It subsets wider df per study, and writes each subset into
    its own study folder.
    Each wider df contains information of a specific node across all studies
    """
    # get node label
    node_label = wider_df["type"].unique().tolist()[0]
    logger.info(f"Writing node {node_label} tsv files for all studies")

    # export folder
    os.makedirs(output_dir, exist_ok=True)

    # loop through studies and export tsv per study for the node
    studies = wider_df["study"].unique().tolist()
    for study in studies:
        df_to_write = wider_df[wider_df["study"] == study]

        # create the output directory if not exist
        study_folder = os.path.join(output_dir, study)
        os.makedirs(study_folder, exist_ok=True)

        # node_study_tsv filename
        node_study_tsv_filename = study + "_" + node_label + ".tsv"

        df_to_write.to_csv(
            os.path.join(study_folder, node_study_tsv_filename),
            sep="\t",
            index=False,
        )

    return None


@flow
def convert_csv_to_tsv(db_pulled_outdir: str, output_dir: str) -> None:
    """Converts all csv exports from query_db_to_csv to tsv files per study"""
    logger = get_run_logger()
    # fetch a list of csv files under folder db_pulled_outdir
    csv_list = list_type_files(file_dir=db_pulled_outdir, file_type=".csv")

    logger.info(f"List of csv files under {db_pulled_outdir}: {*csv_list,}")

    # export folder for tsv files
    export_folder = os.path.join(output_dir, "export_" + get_date())
    os.makedirs(export_folder, exist_ok=True)
    logger.info(f"Creating the output for writing output tsv files: {export_folder} ")

    # writing tsv files
    for file_path in csv_list:
        logger.info(f"processing csv file: {file_path}")
        file_df = pd.read_csv(file_path)
        if file_df.shape[0] > 0:
            wider_df = pivot_long_df_wide_clean(file_path=file_path)
            wider_df = wide_df_setup_link(df_wide=wider_df)
            logger.info(f"Writing tsv files for all studies from file: {file_path}")
            write_wider_df_all(wider_df, output_dir=export_folder, logger=logger)
        else:
            pass
    return export_folder


# Functions for stats query pipeline
@flow(log_prints=True)
def stats_pull_graph_data_study(uri: str, username:str, password:str, query: str, query_topic: str):
    with GraphDatabase.driver(uri, auth=(username, password)) as driver:
        with driver.session() as session:
            # Initialize an empty list to store dataframes
            df_list = []
            # Fetch unique study_ids
            study_result = session.run(
                StatsNeo4jCypherQuery.stats_get_unique_study_query
            )
            study_info = [
                {
                    "study_id": record["study_id"],
                    "column_name": "study_name",
                    "value": record["study_name"],
                }
                for record in study_result
            ]
            df_study = pd.DataFrame(study_info)
            study_ids = [entry["study_id"] for entry in study_info]
            # Iterate through each unique study_id and fetch records
            for study_id in study_ids:
                print(study_id)
                # Format the query with the current study_id
                study_query = query.format(study_id=study_id)
                # Execute the query and fetch results
                records = session.run(study_query)
                # Convert the result to a dataframe
                df = pd.DataFrame(
                    [
                        {
                            "study_id": study_id,
                            "column_name": f"{record['NodeType'][0]}_{query_topic}",  # Assuming one label per node
                            "value": str(record["Value"]),
                        }
                        for record in records
                    ]
                )
                # Append the dataframe to the list
                df_list.append(df)
            # Concatenate all dataframes into one
            final_df = pd.concat(df_list, ignore_index=True)
            final_df = pd.concat([final_df, df_study], axis=0, ignore_index=True)
            final_df = final_df.drop_duplicates()
    return final_df

@flow(log_prints=True)
def stats_pull_graph_data_nodes(uri:str, username:str, password:str, query: str, query_topic: str):
    with GraphDatabase.driver(uri, auth=(username, password)) as driver:
        with driver.session() as session:
            # Initialize an empty list to store dataframes
            df_list = []
            # Fetch unique study_ids
            study_result = session.run(
                StatsNeo4jCypherQuery.stats_get_unique_study_query
            )
            study_info = [
                {
                    "study_id": record["study_id"],
                    "column_name": "study_name",
                    "value": record["study_name"],
                }
                for record in study_result
            ]
            df_study = pd.DataFrame(study_info)
            study_ids = [entry["study_id"] for entry in study_info]
            # Iterate through each unique study_id and fetch records
            for study_id in study_ids:
                print(study_id)
                # Get file nodes based on data base
                # Execute the query and fetch results
                node_query = StatsNeo4jCypherQuery.stats_get_study_nodes.format(
                    study_id=study_id
                )
                node_records = session.run(node_query)
                nodes = [record["NodeLabel"] for record in node_records]
                for node in nodes:
                    print(node)
                    # Format the query with the current study_id
                    node_query = query.format(study_id=study_id, node=node)
                    # Execute the query and fetch results
                    records = session.run(node_query)
                    # Convert the result to a dataframe
                    df = pd.DataFrame(
                        [
                            {
                                "study_id": study_id,
                                "column_name": f"{record['NodeType'][0]}_{query_topic}",  # Assuming one label per node
                                "value": str(record["Value"]),
                            }
                            for record in records
                        ]
                    )
                    # Append the dataframe to the list
                    df_list.append(df)
            # Concatenate all dataframes into one
            final_df = pd.concat(df_list, ignore_index=True)
            final_df = pd.concat([final_df, df_study], axis=0, ignore_index=True)
            final_df = final_df.drop_duplicates()
    return final_df
