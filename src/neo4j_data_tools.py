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


@flow
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
    study_node_id_df = pd.DataFrame(columns=["study_id", "node", "id"])
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


@task
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
    """
        if studies_dataframe.shape[0] > 50:
        uniq_nodes =  studies_dataframe['node'].unique().tolist()
        df_list = []
        # break studies_dataframe based on uniq value of nodes
        for i in uniq_nodes:
            i_df =  studies_dataframe[studies_dataframe["node"]==i].reset_index(drop=True)
            df_list.append(i_df)

        # loop through each df in df_list
        # we limit the task concurrency by the number of studies
        for df in df_list:
            print(df)
            for index in range(df.shape[0]):
                study_id = studies_dataframe.loc[index, "study_id"]
                node = studies_dataframe.loc[index, "node"]
                logger.info(f"Pulling ids for node {node} study {study_id}")
                pull_ids_node_study.submit(
                    driver,
                    export_ids_csv=export_node_ids_a_study,
                    study_id=study_id,
                    node=node,
                    output_dir=temp_folder_name,
                )
    else:
        for index in range(studies_dataframe.shape[0]):
            study_id = studies_dataframe.loc[index, "study_id"]
            node = studies_dataframe.loc[index, "node"]
            logger.info(f"Pulling ids for node {node} study {study_id}")
            pull_ids_node_study.submit(
                driver,
                export_ids_csv=export_node_ids_a_study,
                study_id=study_id,
                node=node,
                output_dir=temp_folder_name,
            )
    """
    study_id_list = studies_dataframe['study_id'].tolist()
    study_id_chunks = list_to_chunks(study_id_list, 10)
    node_list = studies_dataframe['node'].tolist()
    node_chunks = list_to_chunks(node_list, 10)
    for i in range(len(node_list)):
        print(f"study_id_list: {*study_id_chunks[i],}")
        print(f"node_list: {*node_chunks[i],}")
        pull_ids_node_study.map(
            driver,
            export_node_ids_a_study,
            study_id_chunks[i],
            node_chunks[i],
            temp_folder_name,
        )

    return temp_folder_name


@flow
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
        ids_dict[file_study][file_node] = file_df["id"].tolist()
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
    for index, row in ingested_studies_dataframe.iterrows():
        print(row)
    

    db_id_list_all_studies = pull_node_ids_all_studies(
        driver=driver, studies_dataframe=ingested_studies_dataframe, logger=logger
    )

    comparison_df = compare_id_input_db(
        db_id_pulled_dict=db_id_list_all_studies,
        parsed_tsv_file_df=ingested_studies_dataframe,
        logger=logger,
    )

    merged_summary_table = pd.merge(
        studies_dataframe, comparison_df, on=["study_id", "node"], how="left"
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

    # fetch unique nodes
    logger.info("Fetching all unique nodes DB")
    unique_nodes = pull_uniq_nodes(driver=driver)
    logger.info(f"Nodes list: {*unique_nodes,}")

    # Iterate through each unique node and export data
    logger.info("Pulling data by each node")
    pull_nodes_loop(
        node_list=unique_nodes, driver=driver, out_dir=output_dir, logger=logger
    )

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
    export_folder = os.path.join(output_dir, "export_" + get_date())
    os.makedirs(export_folder, exist_ok=True)
    logger.info(f"Creating the output for writing output tsv files: {export_folder} ")

    # writing tsv files
    for file_path in csv_list:
        wider_df = pivot_long_df_wide_clean(file_path=file_path)
        wider_df = wide_df_setup_link(df_wide=wider_df)
        logger.info(f"Writing tsv files for all studies from file: {file_path}")
        write_wider_df_all(wider_df, output_dir=export_folder, logger=logger)
    return export_folder
