from prefect import flow, get_run_logger
import os
import sys
from datetime import date
parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.s3_ccdi_to_dbgap import CCDI_to_dbGaP
from src.utils import (
    get_time,get_manifest_phs,
    file_dl,
    folder_dl,
    CCDI_DCC_Tags,
    check_ccdi_version,
    ccdi_wf_outputs_ul,
    identify_data_curation_log_file,
)


@flow(
    name="curation dbgap pipeline for DCC only",
    log_prints=True,
    flow_run_name="{runner}_" + f"{get_time()}",
)
def runner_dbgap_dcc(
    bucket: str,
    file_path: str,
    runner: str,
    template_tag: str = "",
    dbgap_previous_dir_path: str = "",
):
    """CCDI-DCC data curation pipeline that only include dbgap submission step

    Args:
        bucket (str): Bucket name of where the manifest is located in and the output goes to, such as "ccdi-validation"
        file_path (str): File path of the DCC manifest, such as "path/to/manifest.xlsx"
        runner (str): Unique runner name
        template_tag (str, optional): Tag of the DCC manifest template. Defaults to "" to use the latest version.
        dbgap_previous_dir_path (str, optional): Folder path of previous dbGaP submission folder, if applicable. Defaults to "".

    Raises:
        ValueError: Value Error occurs when the pipeline fails to proceed.
    """
    # create a logging object
    runner_logger = get_run_logger()

    # if not profile:
    # profile = "default"

    # download the manifest
    file_dl(bucket, file_path)

    # check the manifest version before the workflow starts
    manifest_version = check_ccdi_version(os.path.basename(file_path))
    runner_logger.info(f"The version of provided CCDI-DCC manifest is v{manifest_version}")

    # get study phs and create output_folder name
    phs_accession = get_manifest_phs(os.path.basename(file_path))
    output_folder = runner.rstrip("/") + "/" + phs_accession + "_outputs_" + get_time()

    # download CCDI-DCC template if not provided
    if template_tag != "":
        input_template = CCDI_DCC_Tags().download_tag_manifest(tag=template_tag, logger=runner_logger)
        runner_logger.info(f"A CCDI-DCC template manifest in tag {template_tag} was downloaded from GitHub repo")
    else:
        # template tag not provided, and download the template in the latest version tag
        input_template = CCDI_DCC_Tags().download_latest_tag_manifest(logger=runner_logger)
        latest_tag = CCDI_DCC_Tags().get_latest_tag()
        runner_logger.info(
            f"No CCDI-DCC tag was provided and the latest CCDI-DCC template {latest_tag} was downloaded from GitHub repo"
        )
    template_version = check_ccdi_version(input_template)
    runner_logger.info(f"The version of CCDI-DCC template version is v{template_version}")

    latest_manifest_version = CCDI_DCC_Tags().get_latest_tag()
    runner_logger.info(
        f"The current latest version of CCDI-DCC template is v{latest_manifest_version}"
    )

    # Check dcc version and manifest version
    if manifest_version == latest_manifest_version:
        # this can only happen when the user provides an old version of template in the bucket
        if template_version != latest_manifest_version:
            input_template = CCDI_DCC_Tags().download_latest_tag_manifest(logger=runner_logger)
            runner_logger.warning(
                f"CCDI-DCC Manifest was found in latest version {manifest_version}, while a version of {template_version} was provided as template tag. The workflow continues by downloading the newest CCDI-DCC template"
            )
        else:
            pass
    else:
        if latest_manifest_version is None:
            # This might happen when the github API limit being reached and we failed to get latest manifest version
            # the get_ccdi_latest_release() will return "unknown"
            runner_logger.warning("Fail to retrieve latest manifest version through GitHub API. The workflow will continue without this information")
        else:
            if manifest_version == template_version:
                output_folder = output_folder + "(OLD_VERSION_v" + manifest_version + ")"
                runner_logger.error(
                    f"An old version(v{manifest_version}) of CCDI-DCC manifest and CCDI-DCC template were provided. New version of CCDI-DCC template v{latest_manifest_version} is available"
                )
            else:
                runner_logger.error(
                    f"An old version of CCDI-DCC manifest was provided(v{manifest_version}). And no matching CCDI-DCC template was provided. Please provide a matching CCDI-DCC template in the same version of the manifest or update the CCDI-DCC manifest to the latest version v{latest_manifest_version}"
                )
                raise ValueError(
                    "CCDI-DCC manifest version is older version and doesn't match to the version of provided CCDI-DCC template"
                )

    input_file = os.path.basename(file_path)

    if dbgap_previous_dir_path != "":
        runner_logger.info(
            f"User provided dbGaP previous submission folder at s3 bucket {dbgap_previous_dir_path}"
        )
        try:
            folder_dl(bucket=bucket, remote_folder=dbgap_previous_dir_path)
            dbgap_pre_sub_dir = dbgap_previous_dir_path
            runner_logger.info(
                f"Downloaded dbGaP previous submission folder from s3 bucket: {dbgap_pre_sub_dir}"
            )
        except:
            runner_logger.info(
                "Problem occurred downloading dbGaP previous submission folder from s3 bucket. The workflow will continue without dbGaP previous submission input"
            )
            dbgap_pre_sub_dir = None
    else:
        runner_logger.info("No dbGaP previous submission folder provided")
        dbgap_pre_sub_dir = None

    # run CCDI-DCC to dbGaP
    runner_logger.info("Running CCDI-DCC to dbGaP submission file flow")
    try:
        (dbgap_output_folder, dbgap_out_log) = CCDI_to_dbGaP(
            manifest=input_file, pre_submission=dbgap_pre_sub_dir
        )
    except:
        dbgap_output_folder = None
        dbgap_out_log = identify_data_curation_log_file(
            start_str="CCDI_to_dbGaP_submission_"
        )
    runner_logger.info(f"Uploading outputs of dbGaP to bucket {bucket}")
    ccdi_wf_outputs_ul(
        bucket=bucket,
        output_folder=output_folder,
        output_path=dbgap_output_folder,
        output_log=dbgap_out_log,
        sub_folder="3_dbGaP_submisison_output",
    )
