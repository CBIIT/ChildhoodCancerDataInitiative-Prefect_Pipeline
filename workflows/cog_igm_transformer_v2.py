"""Convert, parse and rehshape/update COG and IGM clinical report JSONs into TSVs"""

##############
#
# Env. Setup
#
##############

# builtin
import os
import sys
from typing import Literal
from datetime import datetime
import pandas as pd
import shutil
import logging

# utils
from src.cog_igm_utils import manifest_reader, json_downloader
from src.utils import get_time, folder_ul, file_dl, get_date
from prefect import flow, get_run_logger
from prefect_shell import ShellOperation

##############
#
# Functions
#
##############

FormParsing = Literal["cog_only", "igm_only", "cog_and_igm", "data_clean_up"]

@flow(
    name="COG IGM JSON Transform JSON2TSV v2 Helper Flow",
    log_prints=True,
    flow_run_name="json2tsv_flow-" + f"{get_time()}",
)
def json2tsv_flow(
    json_dir_path: str,
    output_path: str,
):
    """Run the json2tsv parsing functions

    Args:
        json_dir_path (str): Path to directory containing JSON files
        ouput_path (str): Path to output directory for TSVs
    """
    from MCI_JSON2TSV import json2tsv
    #import json2tsv.cog_utils
    #import json2tsv.igm_utils
    #import cog_igm_integration

    json2tsv(
        json_dir_path=json_dir_path,
        output_path=output_path,
    )

@flow(
    name="COG IGM JSON Transform JSON2TSV v2",
    log_prints=True,
    flow_run_name="cog_igm_transform-{runner}_" + f"{get_time()}",
)
def cog_igm_transform(
    bucket: str, runner: str, manifest_path: str, form_parsing: FormParsing, file_path: str = ""
):
    """CCDI data curation pipeline

    Args:
        bucket (str): Bucket name of where the manifest is located in and the output goes to
        runner (str): Unique runner name where manifest located and where to direct outputs to
        manifest_path (str): File path of the CCDI study manifest
        form_parsing (str): Select whether to parse out form level TSVs for COG, or parse variant sections of IGM or parse both OR perform data clean up previously downloaded JSONs
        file_path (str): optional; FULL path to the working directory where the clinical files are located, avoid redownloading files if they are already present, e.g. /usr/local/data/COG_IGM_Transform_working_20251006_T132846

    """

    # create a logging object
    runner_logger = get_run_logger()
    
    print(logging.getLogger().handlers)
    
    # create logger for log file
    logger = logging.getLogger("COG_IGM_JSON2TSV")
    
    log_filename = "COG_IGM_JSON2TSV_" + get_date() + ".log"
    
    # logging config
    logging.basicConfig(
        filename="COG_IGM_JSON2TSV_" + get_date() + ".log",
        encoding="utf-8",
        filemode="w",
        level=logging.INFO,
        format=">>> %(name)s - %(asctime)s - %(levelname)s - %(message)s\n",
        force=True,
    )

    logger.info(f"Logs beginning at {get_time()}")

    start_time = datetime.now()

    dt = get_time()

    runner_logger.info(">>> Running cog_igm_transformer.py ....")
    
    running_dir = os.getcwd()
    
    # move json2tsv contents to current working directory
    ShellOperation(
            commands=[
                "mv ../ChildhoodCancerDataInitiative-MCI_JSON2TSV-logging_force/src/* ./",  # show data directory contents
            ]
        ).run()
    
    # confirm json2tsv directory moved and contents
    runner_logger.info(
        ShellOperation(
            commands=[
                "ls -l .",
                #"ls -l ./json2tsv"  # show data directory contents
            ]
        ).run()
    )
    
    if form_parsing not in ["cog_only", "igm_only", "cog_and_igm", "data_clean_up"]:
        raise ValueError(f"form_parsing must be one of {FormParsing}, got {form_parsing} instead.")

    if form_parsing == 'data_clean_up':
        runner_logger.info(
        ShellOperation(
            commands=[
                "rm -r /usr/local/data/COG_IGM_Transform_*",
                "ls -l /usr/local/data/",  # confirm removal of COG_IGM_Transform working dirs
            ]
        ).run()
        )
        runner_logger.info(">>> Data clean up completed, exiting workflow ....")
        return None
    
    
    # create working dir name
    # check if file_path is provided and contains files, if not create a new working dir
    if file_path != "" and os.path.exists(file_path) and len(os.listdir(file_path)) != 0:
        working_path = file_path
        working_dir = working_path.split("/")[-1]
    else:
        working_dir = f"COG_IGM_Transform_working_{dt}"
        working_path = f"/usr/local/data/{working_dir}"
        get_run_logger().info(f"Working path: {working_path}")
        if not os.path.exists(working_path):
            os.mkdir(working_path)

    # create output dir for logs etc
    output_dir = f"COG_IGM_Transform_output_{dt}"
    output_path = f"/usr/local/data/{working_dir}/{output_dir}"
    if not os.path.exists(output_path):
        os.mkdir(output_path)

    # change to working dir
    os.chdir(working_path)
    
    # download the manifest file
    try:
        file_dl(bucket, manifest_path)
    except Exception as e:
        runner_logger.error(f"Cannot download manifest from path {manifest_path}: {e}")
        sys.exit(1)

    # print to logger name of working dir
    runner_logger.info(f"Working directory: {working_path}")
    
    # load in the manifest
    manifest_df, local_manifest_path = manifest_reader(manifest_path, form_parsing)

    # TODO download files and handle dupes with renaming
    # check for duplicate file_names
    dups = manifest_df[manifest_df["file_name"].duplicated(keep=False)]["file_name"].to_list()
    if not [i for i in os.listdir(os.getcwd()) if i.endswith('.json')]: # check if dir empty, if so download JSONs

        # chunked downloading of JSON files
        chunk_size = 200

        # download JSON files
        for chunk in range(0, len(manifest_df), chunk_size):
            runner_logger.info(f"Downloading JSON chunk {chunk//chunk_size+1} of {len(manifest_df)//chunk_size+1}")
            json_downloader(manifest_df[chunk:chunk+chunk_size], dups, logger)
    
    # chdir back to running dir
    os.chdir(running_dir)
    
    #run json2tsv parsing
    json2tsv_flow(
        json_dir_path=working_path,
        output_path=output_path,
    )
    
    # move log file to output dir and shutdown logging
    shutil.move(log_filename, f"{output_path}/{log_filename.replace(get_date(), dt)}")
    #os.rename(cog_transform_log, f"{output_path}/{cog_transform_log.replace(get_date(), dt)}")

    # copy manifest file to output dir
    shutil.copy(local_manifest_path, f"{output_path}/{os.path.basename(local_manifest_path).replace('.xlsx', '' + '_COG_IGM' + '_' + dt + '.xlsx')}")

    
    # upload output dir
    folder_ul(
        local_folder=f"{output_path}",
        bucket=bucket,
        destination=runner + "/",
        sub_folder="",
    )
