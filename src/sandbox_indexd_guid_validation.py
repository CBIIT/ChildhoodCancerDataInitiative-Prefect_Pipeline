from utils import CCDI_Tags, CheckCCDI, get_time, file_ul
from neo4j_data_tools import export_to_csv, pull_data_per_node, cypher_query_parameters
from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from neo4j import GraphDatabase
import os
from pathlib import Path
import pandas as pd
import numpy as np
import requests


@flow(task_runner=ConcurrentTaskRunner(), name="Pull guid metadata from sandbox")
def pull_guid_meta_nodes_loop(
    study_accession: str, node_list: list, driver, out_dir: str, logger
) -> None:
    """Pulls guids metadata from sandbox

    Args:
        study_accession (str): dbGaP accession number
        node_list (list): a list of node names
        driver (_type_): graph database driver
        out_dir (str): output directory name
        logger (_type_): logger object
    """    
    phs_accession = study_accession
    guid_meta_query = f"""
MATCH (s:study)-[*1..7]-(f:{{node_label}})
WHERE s.dbgap_accession = "{phs_accession}"
RETURN f.dcf_indexd_guid as guid, f.acl as acl, f.authz as authz, f.file_url as url, f.md5sum as md5sum, f.file_size as file_size
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


@task(name="Create combined guid metadata tsv", log_prints=True)
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
            combined_df = pd.read_csv(file_list[index])
        else:
            file_df = pd.read_csv(file_list[index])
            combined_df = pd.concat([combined_df, file_df], ignore_index=True)

    phs_accession = folder_name.split("_")[0]
    print(f"A total of file records in study {phs_accession}: {combined_df.shape[0]}")
    # remove duplicates if any
    combined_df.drop_duplicates(inplace=True)
    print(
        f"A Total of file records in study {phs_accession} after removing duplicates: {combined_df.shape[0]}"
    )
    output_name = phs_accession + "_guid_meta_in_sandbox.tsv"
    combined_df.to_csv(output_name, sep="\t", index=False)
    return output_name


@task(name="Validate sandbox guid metadata against indexd record")
def check_guid_meta_against_indexd(file_name: str) -> str:
    """Check sandbox guid metadata against indexd record

    Args:
        file_name (str): file name of guid meta tsv file of a study

    Returns:
        str: output file name
    """
    tsv_df = pd.read_csv(file_name, sep="\t")
    api_url = "https://nci-crdc.datacommons.io/index/index?ids={guid}"
    guid_exist = []
    md5sum_indexd = []
    url_indexd = []
    acl_indexd = []
    authz_indexd = []
    size_indexd = []

    for guid in tsv_df["guid"].tolist():
        api_response = requests.get(api_url.format(guid=guid))
        records = api_response.json()["records"]
        if len(records) == 0:
            guid_exist.append("No")
            md5sum_indexd.append("")
            url_indexd.append("")
            acl_indexd.append("")
            authz_indexd.append("")
            size_indexd.append("")
        else:
            record = records[0]
            guid_exist.append("Yes")
            acl_indexd.append(str(record["acl"]))  # this one should be a list
            authz_indexd.append(str(record["authz"])) # this one should be a list
            url_indexd.append(record["urls"][0])  # assume there is only one url
            md5sum_indexd.append(record["hashes"]["md5"])
            size_indexd.append(record["size"])
    tsv_df["indexd_guid_exist"] = guid_exist
    tsv_df["indexd_acl"] = acl_indexd
    tsv_df["indexd_authz"] = authz_indexd
    tsv_df["indexd_md5sum"] = md5sum_indexd
    tsv_df["indexd_url"] = url_indexd
    tsv_df["indexd_size"] = size_indexd
    tsv_df["acl_check"] = np.where(
        tsv_df["acl"] == tsv_df["indexd_acl"], "Pass", "Fail"
    )
    tsv_df["authz_check"] = np.where(
        tsv_df["authz"] == tsv_df["indexd_authz"], "Pass", "Fail"
    )
    tsv_df["md5sum_check"] = np.where(
        tsv_df["md5sum"] == tsv_df["indexd_md5sum"], "Pass", "Fail"
    )
    tsv_df["url_check"] = np.where(
        tsv_df["url"] == tsv_df["indexd_url"], "Pass", "Fail"
    )
    tsv_df["size_check"] = np.where(
        tsv_df["file_size"] == tsv_df["indexd_size"], "Pass", "Fail"
    )
    output_name = (
        file_name.split(".")[0].split("_")[0] + "_guid_meta_check_against_indexd.tsv"
    )
    tsv_df.to_csv(output_name, sep="\t", index=False)
    return output_name


@flow(log_prints=True, name="Find inactive guids in indexd")
def find_ghost_indexd_records(file_name: str, phs_accession: str) -> str:
    """Find indexd records with an acl of a study that are not found in sandbox

    Args:
        file_name (str): A tsv file of all guids for a study
        phs_accession (str): dbGaP accession number

    Returns:
        str: a tsv of any guids that are associated with a given acl in indexd but not found in the file
    """
    tsv_df = pd.read_csv(file_name, sep="\t")
    all_guids = tsv_df["guid"].tolist()
    del tsv_df

    page = 0
    ghost_guid = []
    while True:
        endpoint = f"https://nci-crdc.datacommons.io/index/index?acl={phs_accession}&limit=100&page={page}"
        api_response = requests.get(endpoint)
        while api_response.status_code != 200:
            api_response = requests.get(endpoint)

        api_data = api_response.json()

        print(f"page: {page}")
        records = api_data["records"]
        for i in records:
            i_guid = i["did"]
            if i_guid in all_guids:
                pass
            else:
                i_size = i["size"]
                i_md5sum = i["hashes"]["md5"]
                i_url = i["urls"]
                i_acl = str(i["acl"])
                i_authz = str(i["authz"])
                ghost_guid.append(
                    {
                        "guid": i_guid,
                        "md5": i_md5sum,
                        "size": i_size,
                        "acl": i_acl,
                        "authz": i_authz,
                        "urls": i_url,
                    }
                )
        if len(records) < 100:
            break
        else:
            page += 1
    ghost_guid_df =  pd.DataFrame.from_records(ghost_guid)
    print(f"Found {len(ghost_guid)} indexd records with an acl of {phs_accession} that are not found in sandbox")
    output_name = phs_accession + "_indexd_records_unfound_in_sandbox.tsv"
    ghost_guid_df.to_csv(output_name, sep="\t", index=False)
    return output_name


@flow(name="Guid validation between sandbox and indexd record")
def guid_validation_between_sandbox_indexd(
    phs_accession: str, data_model_tag: str, bucket: str, runner: str
) -> None:
    """Download guid metadata of all guids associated with a single study in sandbox

    Args:
        phs_accession (str): dbGaP accession of a study, e.g., phs002504
        data_model_tag (str): ccdi data model tag
        bucket (str): bucket name where output uploads to
        runner (str): unique runner name
    """
    current_time = get_time()
    logger = get_run_logger()
    manifest_download = CCDI_Tags().download_tag_manifest(
        tag=data_model_tag, logger=logger
    )
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
    logger.info("Starting pulling guid metadata from Neo4j sandbox instance")
    pull_guid_meta_nodes_loop(
        study_accession=phs_accession,
        node_list=file_nodes,
        driver=driver,
        out_dir=foldername,
        logger=logger,
    )

    # combine all sandbox guid metadata csv files into one tsv
    tsv_guid_meta = concatenate_csv_files(folder_name=foldername)

    # create an output folder name where outputs will be uploaded to
    output_folder = os.path.join(runner, "sandbox_indexd_guid_validation_" + phs_accession + "_" + current_time)

    # check sandbox guid against indexd record
    logger.info(f"Checking if all guid metadata for study {phs_accession} matches to indexd record")
    guid_meta_check_output = check_guid_meta_against_indexd(file_name=tsv_guid_meta)
    # upload sandbox guid metadata validation file to the bucket
    file_ul(
        bucket=bucket,
        output_folder=output_folder,
        sub_folder="",
        newfile=guid_meta_check_output,
    )

    # find indexd record with an acl of phs_accession that are not found in sandbox
    logger.info(f"Finding any indexd records of acl {phs_accession} that are not found in sandbox")
    unused_guid_file = find_ghost_indexd_records(
        phs_accession=phs_accession, file_name=tsv_guid_meta
    )
    file_ul(
        bucket=bucket,
        output_folder=output_folder,
        sub_folder="",
        newfile=unused_guid_file
    )
    logger.info("Finished!")
