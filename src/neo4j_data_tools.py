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
    startNode[propertyName] AS startNodePropertyValue
"""
    )
    main_cypher_query: str = (
        """
MATCH (startNode:{node_label})-[r]->(linkedNode)
WITH startNode, linkedNode, properties(startNode) AS props
UNWIND keys(props) AS propertyName
RETURN
    startNode.id AS startNodeId,
    labels(startNode) AS startNodeLabels,
    propertyName AS startNodePropertyName,
    startNode[propertyName] AS startNodePropertyValue,
    linkedNode.id AS linkedNodeId, 
    labels(linkedNode) AS linkedNodeLabels
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
MATCH (study:study {{study_id: "{study_id}"}})-[*]-(node)
RETURN labels(node) AS NodeLabel, COUNT(node) AS NodeCount
"""
    )
    node_id_cypher_query_query: str = (
        """
MATCH (study:study{{study_id:"{study_id}"}})-[*]-(node:{node})
RETURN node.id AS id
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
        logger.info(
            f"Parameter info:\n{json.dumps(parameter_response, indent=4, default=str)}"
        )
    except ClientError as err:
        ex_code = err.response["Error"]["Code"]
        ex_message = err.response["Error"]["Message"]
        logger.error(ex_code + ":" + ex_message)
        raise
    except Exception as error:
        logger.error(f"Get s3 parameter {parameter_name} FAILED")
        logger.error("General exception noted.", exc_info=True)
        raise

    return parameter_response


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


def export_to_csv(tx, node_label: str, cypher_query: str, output_directory: str):
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


@task
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


@flow(task_runner=ConcurrentTaskRunner())
def pull_nodes_loop(node_list: list, driver, out_dir: str, logger) -> None:
    """Loops through a list of node labels and pulls data from a neo4j DB"""
    cypher_phrase = Neo4jCypherQuery.main_cypher_query
    for node_label in node_list:
        logger.info(f"Pulling from Node {node_label}")
        pull_data_per_node.submit(
            driver=driver,
            data_to_csv=export_to_csv,
            node_label=node_label,
            query_str=cypher_phrase,
            output_dir=out_dir,
        )
    return None


@task
def pull_study_node(driver, out_dir: str) -> None:
    """Pulls data for study node from a neo4j DB"""
    cypher_phrase = Neo4jCypherQuery.study_cypher_query
    pull_data_per_node(
        driver=driver,
        data_to_csv=export_to_csv,
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


@task
def pull_all_nodes_a_study(driver, study_id: str) -> Dict:
    """Returns a dictionary of a record counts of all nodes (except study node) of a given study"""
    session = driver.sesssion()
    study_query = Neo4jCypherQuery.all_nodes_entries_study_cypher_query.format(
        study_id=study_id
    )
    print(study_query)
    study_dict = {}
    try:
        study_response = session.run(study_query)
        for record in study_response:
            node_label = record["NodeLabel"]
            node_count = record["NodeCount"]
            study_dict[node_label[0]] = node_count
    except:
        traceback.print_exc()
        raise
    finally:
        session.close()
    return study_dict


@task
def pull_node_id_a_study(driver, study_id: str, node: str) -> Dict:
    """return a dictionary of a node of a study"""
    session = driver.session()
    query_str = Neo4jCypherQuery.node_id_cypher_query_query.format(
        study_id=study_id, node=node
    )
    print(query_str)
    try:
        node_result = session.run(query_str)
        db_id_list = [node_record["id"] for node_record in node_result]
    except:
        traceback.print_exc()
        raise
    finally:
        session.close()
    return db_id_list


@flow(task_runner=ConcurrentTaskRunner())
def pull_node_ids_all_studies(driver, studies_dataframe: DataFrame) -> Dict:
    """Returns a dict which keeps track of all id of each node of all studies"""
    node_ids_dict = {}
    for index in range(studies_dataframe.shape[0]):
        study_id = studies_dataframe.loc[index, "study_id"]
        node = studies_dataframe.loc[index, "node"]
        study_node_ids = pull_node_id_a_study.submit(
            driver=driver, study_id=study_id, node=node
        )
        if study_id not in node_ids_dict.keys():
            node_ids_dict[study_id] = {}
        else:
            pass
        node_ids_dict[study_id][node] = study_node_ids
    return node_ids_dict


@flow(task_runner=ConcurrentTaskRunner())
def pull_studies_loop(driver, study_list: list, logger) -> DataFrame:
    """Returns a dataframe of record counts all nodes(except study node) of all studies in a DB"""
    studies_dataframe = pd.DataFrame(columns=["study_id", "node", "DB_count"])
    for study in study_list:
        logger.info(f"Pulling counts of each node of study {study}")
        study_dict = pull_all_nodes_a_study.submit(driver=driver, study_id=study)
        study_df = pd.DataFrame(columns=["study_id", "node", "DB_count"])
        study_df["study_id"] = [study] * len(study_dict.keys())
        index = 0
        for key, value in study_dict.items():
            study_df.loc[index, "node"] = key
            study_df.loc[index, "DB_count"] = value
            index += 1
        studies_dataframe = pd.concat([studies_dataframe, study_df], ignore_index=True)
    return studies_dataframe


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
        logger=logger
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


@flow
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
        logger=logger
    )

    # driver instance
    logger.info("Creating GraphDatabase driver using uri, username, and password")
    driver = GraphDatabase.driver(uri, auth=(username, password))

    db_id_list_all_studies = pull_node_ids_all_studies(
        driver=driver, studies_dataframe=studies_dataframe
    )
    tsv_files = list_type_files(file_dir=tsv_folder, file_type=".tsv")
    validate_df = studies_dataframe
    validate_df["tsv_count"] = np.nan
    validate_df["count_compare"] = np.nan
    validate_df["id_check"] = np.nan
    for tsv in tsv_files:
        tsv_df = pd.read_csv(tsv, sep="\t", low_memory=False)
        tsv_study_id = (
            tsv_df["id"].str.split(pat="::", n=1, expand=True)[0].unique().tolist()[0]
        )
        tsv_node = tsv_df["type"].unique().tolist()[0]
        # skip tsv file for study node tsv
        if tsv_node != "study":
            tsv_count = tsv_df.shape[0]
            tsv_id_list = tsv_df["id"].tolist()
            db_id_list = db_id_list_all_studies[tsv_study_id][tsv_node]
            validate_df.loc[
                (validate_df["study_id"] == tsv_study_id)
                & (validate_df["node"] == tsv_node),
                "tsv_count",
            ] = tsv_count
            node_db_count = validate_df.loc[
                (validate_df["study_id"] == tsv_study_id)
                & (validate_df["node"] == tsv_node),
                "DB_count",
            ]
            if node_db_count == tsv_count:
                validate_df.loc[
                    (validate_df["study_id"] == tsv_study_id)
                    & (validate_df["node"] == tsv_node),
                    "count_compare",
                ] = "Pass"
                logger.info(
                    f"Count check Passed for node {tsv_node} in study {tsv_study_id}"
                )
            else:
                validate_df.loc[
                    (validate_df["study_id"] == tsv_study_id)
                    & (validate_df["node"] == tsv_node),
                    "count_compare",
                ] = "Fail"
                logger.error(
                    f"Count discrepancy of node {tsv_node} in study {tsv_study_id}"
                )

            # check if tsv_id_list can be found in DB
            id_notfound = [id for id in tsv_id_list if id not in db_id_list]
            if len(id_notfound) >= 1:
                logger.error(
                    f"Ids in tsv of node {tsv_node} in study {tsv_study_id} not found in db: {*id_notfound,}"
                )
                validate_df.loc[
                    (validate_df["study_id"] == tsv_study_id)
                    & (validate_df["node"] == tsv_node),
                    "id_check",
                ] = "Fail"
            else:
                validate_df.loc[
                    (validate_df["study_id"] == tsv_study_id)
                    & (validate_df["node"] == tsv_node),
                    "id_check",
                ] = "Pass"
                logger.info(
                    f"All ids of node {tsv_node} in study {tsv_study_id} found in DB"
                )
        else:
            pass
    # sort the df order based on study id and node
    validate_df = validate_df.sort_values(by=["study_id", "node"], ascending=True)
    # close driver
    driver.close()
    return validate_df


@task
def neo4j_validation_md(validate_df: DataFrame, runner=str) -> None:
    validate_df_str = validate_df.to_markdown(tablefmt="pipe", index=False)
    markdown_report = f"""# CCDI Neo4j DB Validation Summary Table

{validate_df_str}

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
        logger=logger
    )

    # driver instance
    logger.info("Creating GraphDatabase driver using uri, username, and password")
    driver = GraphDatabase.driver(uri, auth=(username, password))

    # fetch unique nodes
    logger.info("Fetching all unique nodes DB")
    unique_nodes = pull_uniq_nodes(driver=driver)
    logger.info(f"Nodes list: {*unique_nodes,}")

    # Iterate through each unique node and export data
    logger.info("Pulling data by each node")
    pull_nodes_loop(
        node_list=unique_nodes, driver=driver, output_dir=output_dir, logger=logger
    )

    # Obtain study node data
    logger.info("Pulling data from study node")
    pull_study_node(driver=driver, out_dir=output_dir)

    # close the driver
    logger.info("Closing GraphDatabase driver")
    driver.close()

    return output_dir


def read_node_tsv(filepath: str) -> tuple:
    node_df = pd.read_csv(filepath, sep="\t", low_memory=False)
    study_id = (
        node_df["id"].str.split(pat="::", n=1, expand=True)[0].unique().tolist()[0]
    )
    node_name = node_df["type"].unique().tolist()[0]
    node_count = node_df.shape[0]
    id_list = node_df["id"].tolist()
    return study_id, node_name, node_count, id_list


def list_type_files(file_dir: str, file_type: str) -> list:
    """Returns a list of matched file paths under a folder path"""
    file_list = os.listdir(file_dir)
    matched_files = [
        os.join.path(file_dir, file) for file in file_list if file.endswith(file_type)
    ]
    if len(matched_files) == 0:
        raise ValueError(
            f"No matched {file_type} files were found under folder {file_dir}"
        )
    else:
        pass
    return matched_files


@task
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

    else:
        pass

    # remove quotes from column names.
    # remove quotes and brackets from str value
    df_wide.columns = df_wide.columns.str.strip('"')
    df_wide.columns = df_wide.columns.str.strip("'")

    df_wide = df_wide.applymap(lambda x: x.strip("[") if isinstance(x, str) else x)
    df_wide = df_wide.applymap(lambda x: x.strip("]") if isinstance(x, str) else x)
    df_wide = df_wide.applymap(lambda x: x.strip('"') if isinstance(x, str) else x)
    df_wide = df_wide.applymap(lambda x: x.strip("'") if isinstance(x, str) else x)

    # remove few columns
    df_wide["type"] = df_wide["startNodeLabels"]
    df_wide.drop(
        ["startNodeId", "created", "startNodeLabels", "uuid"], axis=1, inplace=True
    )
    return df_wide


@task
def wide_df_setup_link(df_wide: DataFrame) -> DataFrame:
    """Setup links in wide df"""
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

    else:
        pass
    id_name = df_wide["type"].unique().tolist()[0] + "_id"
    df_wide[["study", id_name]] = df_wide["id"].str.split("::", n=1, expand=True)
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
    logger.info("Creating the output for writing output tsv files ")
    export_folder = os.path.join(output_dir, "export_" + get_date())
    os.makedirs(export_folder, exist_ok=True)

    # writing tsv files
    for file_path in csv_list:
        wider_df = pivot_long_df_wide_clean(file_path=file_path)
        wider_df = wide_df_setup_link(df_wide=wider_df)
        logger.info(f"Writing tsv files for all studies from file: {file_path}")
        write_wider_df_all(wider_df, output_dir=export_folder)
    return export_folder
