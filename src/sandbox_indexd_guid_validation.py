from utils import CCDI_Tags, CheckCCDI, get_time, get_date, folder_ul, file_ul
from neo4j_data_tools import export_to_csv, pull_data_per_node, cypher_query_parameters
from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from neo4j import GraphDatabase
import os
from pathlib import Path
import pandas as pd


@flow(task_runner=ConcurrentTaskRunner())
def pull_guid_meta_nodes_loop(study_accession: str, node_list: list, driver, out_dir: str, logger) -> None:
    """Loops through a list of node labels and pulls data of a given study from a neo4j DB"""
    phs_accession =  study_accession
    guid_meta_query =  f"""
MATCH (s:study)-[*1..7]-(f:{{node_label}})
WHERE s.dbgap_accession = "{phs_accession}"
RETURN f.dcf_indexd_guid as guid, f.acl as acl, f.file_url as url, f.md5sum as md5sum, f.file_size as file_size
"""

    for node_label in node_list:
        logger.info(f"Pulling from Node {node_label}")
        pull_data_per_node.submit(
            driver=driver,
            data_to_csv=export_to_csv,
            node_label=node_label,
            query_str=guid_meta_query,
            output_dir=out_dir,
        )
    return None


@task
def concatenate_csv_files(folder_name: str) -> str:
    """Merge all csv files of guid meta from one study

    Args:
        folder_name (str): folder name containg several csv files of guid meta data

    Returns:
        str: output filename
    """    
    file_list = [os.path.join(folder_name, i) for i in os.listdir(folder_name)]
    
    for index in range(len(file_list)):
        if index == 0:
            combined_df =  pd.read_csv(file_list[index])
        else:
            file_df = pd.read_csv(file_list[index])
            combined_df = pd.concat([combined_df, file_df], ignore_index=True)
    phs_accession = folder_name.split("_")[0]
    output_name = phs_accession + "_guid_meta_in_sandbox.tsv"
    combined_df.to_csv(output_name, sep="\t", index=False)
    return output_name


@flow
def query_guid_meta_sandbox(phs_accession: str, data_model_tag: str, bucket: str, runner: str) -> None:
    """Download guid metadata of all guids associated with a single study in sandbox

    Args:
        phs_accession (str): dbGaP accession of a study, e.g., phs002504
        data_model_tag (str): ccdi data model tag
        bucket (str): bucket name where output uploads to
        runner (str): unique runner name
    """    
    current_time = get_time()
    logger = get_run_logger()
    manifest_download = CCDI_Tags().download_tag_manifest(tag=data_model_tag, logger=logger)
    file_nodes = CheckCCDI(ccdi_manifest=manifest_download).find_file_nodes()

    uri_parameter = "uri"
    username_parameter = "username"
    password_parameter = "password"

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

    # start pulling guid metadata from sandbox of a given study
    foldername = phs_accession + "_guid_csv"
    # create foldername folder if not exist
    Path(foldername).mkdir(parents=True, exist_ok=True)
    pull_guid_meta_nodes_loop(
        study_accession=phs_accession,
        node_list=file_nodes,
        driver=driver,
        out_dir=foldername,
        logger=logger
    )

    # combined all sandbox guid metadata into one tsv
    tsv_guid_meta = concatenate_csv_files(folder_name=foldername)

    # upload the folder to s3 bucket
    output_folder = os.path.join(runner,"sandbox_guid_pull_" + current_time)
    # folder_ul(local_folder=foldername, bucket=bucket, destination=output_folder, sub_folder="")
    file_ul(
        bucket=bucket, output_folder=output_folder, sub_folder="", newfile=tsv_guid_meta
    )
    
    # check sandbox guid against indexd record
    
