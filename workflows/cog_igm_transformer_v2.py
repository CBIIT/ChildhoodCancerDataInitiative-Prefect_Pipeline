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

# utils
from src.cog_igm_utils import manifest_reader, cog_igm_json2tsv
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

    start_time = datetime.now()

    dt = get_time()

    runner_logger.info(">>> Running cog_igm_transformer.py ....")
    
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
    
    # download the manifest file
    try:
        file_dl(bucket, manifest_path)
    except Exception as e:
        runner_logger.error(f"Cannot download manifest from path {manifest_path}: {e}")
        sys.exit(1)
    
    # load in the manifest
    #manifest_df, local_manifest_path = manifest_reader(manifest_path)
    
    # show directory structure for debugging
    runner_logger.info(
        ShellOperation(
            commands=[
                "ls -l .",  # show data directory contents
            ]
        ).run()
    )