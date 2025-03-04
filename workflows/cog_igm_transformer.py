"""Convert, parse and rehshape/update COG and IGM clinical report JSONs into TSVs"""


##############
#
# Env. Setup
#
##############

#builtin
import logging
import os
import sys
from typing import Literal
from datetime import datetime

# utils
from src.cog_igm_utils import manifest_reader, cog_igm_json2tsv
from src.utils import get_time, folder_ul, file_dl, get_logger, get_date
from prefect import flow, get_run_logger
from prefect_shell import ShellOperation

##############
#
# Functions
#
##############

FormParsing = Literal["cog_only", "igm_only", "cog_and_igm"]

@flow(
    name="COG IGM JSON Transform",
    log_prints=True,
    flow_run_name="cog_igm_transform-{runner}_" + f"{get_time()}",
)
def cog_igm_transform(
    bucket: str,
    runner: str,
    manifest_path: str,
    form_parsing: FormParsing
):
    """CCDI data curation pipeline

    Args:
        bucket (str): Bucket name of where the manifest is located in and the output goes to
        runner (str): Unique runner name where manifest located and where to direct outputs to
        manifest_path (str): File path of the CCDI study manifest
        form_parsing (str): Select whether to parse out form level TSVs for COG, or parse variant sections of IGM or parse both 

    """
    
    # create a logging object
    runner_logger = get_run_logger()
    
    start_time = datetime.now()

    dt = get_time()

    runner_logger.info(">>> Running cog_igm_transformer.py ....")

    # clean up previous working dir
    runner_logger.info(
            ShellOperation(
                commands=[
                    "rm -r /usr/local/data/COG_IGM_Transform_*",
                    "ls -l /usr/local/data/",  # confirm removal of COG_IGM_Transform working dirs
                ]
            ).run()
        )

    #init logging
    #logger = logging.get_logger("COG_IGM_JSON2TSV")
    #log_filename = f"COG_IGM_JSON2TSV_{dt}.log"
    logger = get_logger(loggername="COG_IGM_JSON2TSV", log_level="info")
    log_filename = "COG_IGM_JSON2TSV_" + get_date() + ".log"

    # logging config
    """logging.basicConfig(
        filename=log_filename,
        encoding="utf-8",
        filemode="w",
        level=logging.INFO,
        format="%(name)s - %(levelname)s - %(message)s",
    )"""

    # download the manifest file
    try:
        file_dl(bucket, manifest_path)
    except Exception as e:
        runner_logger.error(f"Cannot download manifest from path {manifest_path}: {e}")
        sys.exit(1)

    # load in the manifest 
    manifest_df = manifest_reader(manifest_path)

    #create working dir name
    working_dir = f"COG_IGM_Transform_working"
    working_path = f"/usr/local/data/{working_dir}"
    if not os.path.exists(working_path):
        os.mkdir(working_path)

    #create output dir for logs etc
    output_dir = f"COG_IGM_Transform_output_{dt}"
    output_path = f"/usr/local/data/{working_dir}/{output_dir}"
    if not os.path.exists(output_path):
        os.mkdir(output_path)

    # change to working dir
    os.chdir(working_path)

    #perform parsing
    cog_success_count, cog_error_count, igm_success_count, igm_error_count = cog_igm_json2tsv(manifest_df, form_parsing, working_path, output_path, dt)

    end_time = datetime.now()
    time_diff = end_time - start_time
    runner_logger.info(f"\n\t>>> Time to Completion: {time_diff}")
    runner_logger.info(f"\t>>> # COG JSON Files Successfully Transformed: {cog_success_count}")
    if cog_error_count > 0:
        runner_logger.info(
            f"\t>>> # COG JSON Files NOT Transformed (Errors): {cog_error_count}, check log file {output_path}/JSON2TSV_{get_time}.log for errors"
        )
    else:
        runner_logger.info(f"\t>>> # COG JSON Files NOT Transformed (Errors): {cog_error_count}")
    runner_logger.info(f"\t>>> # IGM JSON Files Successfully Transformed: {igm_success_count}")
    if igm_error_count > 0:
        runner_logger.info(
            f"\t>>> # IGM JSON Files NOT Transformed (Errors): {igm_error_count}, check log file {output_path}/JSON2TSV_{get_time}.log for errors \n"
        )
    else:
        runner_logger.info(f"\t>>> # IGM JSON Files NOT Transformed (Errors): {igm_error_count}")
    runner_logger.info(
        f"\t>>> Check log file {output_path}/JSON2TSV_{get_time}.log for additional information\n"
    )

    # move log file to output dir and shutdown logging
    os.rename(log_filename, f"{output_path}/{log_filename}")
    logging.shutdown()

    #upload output dir 
    folder_ul(
            local_folder=f"{output_dir}",
            bucket=bucket,
            destination=runner + "/",
            sub_folder="",
        )
