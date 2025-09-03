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
    name="COG IGM JSON Transform",
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
        file_path (str): optional; FULL path to the working directory where the clinical files are located, avoid redownloading files if they are already present

    """

    # create a logging object
    runner_logger = get_run_logger()

    start_time = datetime.now()

    dt = get_time()

    runner_logger.info(">>> Running cog_igm_transformer.py ....")

    if form_parsing == 'data_clean_up':
        runner_logger.info(
        ShellOperation(
            commands=[
                "rm -r /usr/local/data/COG_IGM_Transform_*",
                "ls -l /usr/local/data/",  # confirm removal of COG_IGM_Transform working dirs
            ]
        ).run()
    )
        
    else:
    
        # clean up previous working dir
        """runner_logger.info(
            ShellOperation(
                commands=[
                    "rm -r /usr/local/data/COG_IGM_Transform_*",
                    "ls -l /usr/local/data/",  # confirm removal of COG_IGM_Transform working dirs
                ]
            ).run()
        )"""

        # download the manifest file
        try:
            file_dl(bucket, manifest_path)
        except Exception as e:
            runner_logger.error(f"Cannot download manifest from path {manifest_path}: {e}")
            sys.exit(1)


        # load in the manifest
        manifest_df, local_manifest_path = manifest_reader(manifest_path) 

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

        # print to logger name of working dir
        runner_logger.info(f"Working directory: {working_path}")

        # perform parsing
        (
            cog_success_count,
            cog_error_count,
            igm_success_count,
            igm_error_count,
            log_filename,
            cog_transform_log,
        ) = cog_igm_json2tsv(manifest_df, local_manifest_path, form_parsing, working_path, output_path, dt)

        end_time = datetime.now()
        time_diff = end_time - start_time
        runner_logger.info(f"\n\t>>> Time to Completion: {time_diff}")
        runner_logger.info(
            f"\t>>> # COG JSON Files Successfully Transformed: {cog_success_count}"
        )
        if cog_error_count > 0:
            runner_logger.info(
                f"\t>>> # COG JSON Files NOT Transformed (Errors): {cog_error_count}, check log file {output_path}/JSON2TSV_{get_time}.log for errors"
            )
        else:
            runner_logger.info(
                f"\t>>> # COG JSON Files NOT Transformed (Errors): {cog_error_count}"
            )
        runner_logger.info(
            f"\t>>> # IGM JSON Files Successfully Transformed: {igm_success_count}"
        )
        if igm_error_count > 0:
            runner_logger.info(
                f"\t>>> # IGM JSON Files NOT Transformed (Errors): {igm_error_count}, check log file {output_path}/JSON2TSV_{get_time}.log for errors \n"
            )
        else:
            runner_logger.info(
                f"\t>>> # IGM JSON Files NOT Transformed (Errors): {igm_error_count}"
            )
        runner_logger.info(
            f"\t>>> Check log file {output_path}/JSON2TSV_{dt}.log for additional information\n"
        )

        # move log file to output dir and shutdown logging
        os.rename(log_filename, f"{output_path}/{log_filename.replace(get_date(), dt)}")
        #os.rename(cog_transform_log, f"{output_path}/{cog_transform_log.replace(get_date(), dt)}")

        # copy manifest file to output dir
        shutil.copy(local_manifest_path, f"{output_path}/{os.path.basename(local_manifest_path).replace('.xlsx', '' + '_COG_IGM' + '_' + dt + '.xlsx')}")

        # upload output dir
        folder_ul(
            local_folder=f"{output_dir}",
            bucket=bucket,
            destination=runner + "/",
            sub_folder="",
        )

