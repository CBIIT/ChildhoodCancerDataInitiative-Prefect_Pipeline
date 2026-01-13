"""
This prefect pipeline works flow combines 7 tools
that catches error and validate the CCDI-DCC manifest before it
generates files for data ingestion and submission.

Authors: Sean Burke <sean.burke2@nih.gov>
         Qiong Liu <qiong.liu@nih.gov>
"""
from prefect import flow, get_run_logger
import os
import sys
from datetime import date

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.s3_ccdi_to_sra import CCDI_to_SRA
from src.s3_ccdi_to_dbgap import CCDI_to_dbGaP
from src.s3_catcherry import CatchERRy
from src.s3_validationry_refactored import ValidationRy_new
from src.dcc_tabbreaker import tabBreakeRy_dcc
from src.utils import (
    get_time,
    get_manifest_phs,
    file_dl,
    folder_dl,
    view_all_s3_objects,
    markdown_input_task,
    markdown_output_task,
    check_ccdi_version,
    dl_ccdi_template,
    dl_sra_template,
    get_ccdi_latest_release,
    ccdi_wf_inputs_ul,
    ccdi_wf_outputs_ul,
    identify_data_curation_log_file,
    ccdi_to_dcf_index,
    CCDI_DCC_Tags,
)


@flow(
    name="S3 Prefect Pipeline for DCC",
    log_prints=True,
    flow_run_name="{runner}_" + f"{get_time()}",
)
def runner_dcc(
    bucket: str,
    file_path: str,
    runner: str,
    template_tag: str = "",
    dbgap_previous_dir_path: str = "",
):
    """CCDI-DCC data curation pipeline

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

    # upload wf inputs into designated bucket
    ccdi_wf_inputs_ul(
        bucket=bucket,
        output_folder=output_folder,
        ccdi_manifest=input_file,
        ccdi_template=input_template,
    )

    # run CatchERR
    runner_logger.info("Running CatchERRy flow")
    try:
        (catcherr_out_file, catcherr_out_log) = CatchERRy(input_file, input_template)
    except:
        catcherr_out_file = None
        catcherr_out_log = (
            input_file[0:-5] + "_CatchERR" + date.today().strftime("%Y%m%d") + ".txt"
        )
    # upload CatchERR output and log
    runner_logger.info(f"Uploading outputs of CatchERR to bucket {bucket}")
    ccdi_wf_outputs_ul(
        bucket=bucket,
        output_folder=output_folder,
        output_path=catcherr_out_file,
        output_log=catcherr_out_log,
        sub_folder="1_CatchERR_output",
    )

    if catcherr_out_file is not None:

        # run ValidationRy
        runner_logger.info("Running ValidationRy flow")
        try:
            validation_out_file = ValidationRy_new(catcherr_out_file, input_template)
        except:
            validation_out_file = None
        # upload ValidationRy output
        runner_logger.info(f"Uploading outputs of ValidationRy to bucket {bucket}")
        ccdi_wf_outputs_ul(
            bucket=bucket,
            output_folder=output_folder,
            output_path=validation_out_file,
            output_log=None,
            sub_folder="2_ValidationRy_output",
        )

        # run CCDI-DCC to dbGaP
        runner_logger.info("Running CCDI-DCC to dbGaP submission file flow")
        try:
            (dbgap_output_folder, dbgap_out_log) = CCDI_to_dbGaP(
                manifest=catcherr_out_file, pre_submission=dbgap_pre_sub_dir
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

        # run CCDI-DCC to dcf index
        runner_logger.info("Running CCDI-DCC to DCF Index files flow")
        try:
            (dcf_index_file, dcf_index_log) = ccdi_to_dcf_index(ccdi_manifest=catcherr_out_file)
        except:
            dcf_index_file = None
            dcf_index_log = identify_data_curation_log_file(
                start_str="CCDI_to_DCF_Index_"
            )
        runner_logger.info(f"Uploading outputs of DCF index ofile to bucket {bucket}")
        ccdi_wf_outputs_ul(
            bucket=bucket,
            output_folder=output_folder,
            output_path=dcf_index_file,
            output_log=dcf_index_log,
            sub_folder="4_DCF_index_output"
        )

        # run CCDI-DCC to tabbreaker
        runner_logger.info("Running CCDI-DCC to TabBreaker flow")
        try:
            (tabbreaker_output_folder, tabbreaker_out_log) = tabBreakeRy_dcc(
                manifest=catcherr_out_file
            )
        except:
            tabbreaker_output_folder = None
            tabbreaker_out_log = identify_data_curation_log_file(
                start_str="DCC_TabBreaker_"
            )
        runner_logger.info(f"Uploading outputs of TabBreaker to bucket {bucket}")
        ccdi_wf_outputs_ul(
            bucket=bucket,
            output_folder=output_folder,
            output_path=tabbreaker_output_folder,
            output_log=tabbreaker_out_log,
            sub_folder="5_TabBreaker_output",
        )
    else:
        pass

    source_file_list = view_all_s3_objects(bucket)
    markdown_output_task(
        source_bucket=bucket,
        source_file_list=source_file_list,
        output_folder=output_folder,
        runner=runner,
    )


@flow(
    name="S3 Prefect Pipeline with SRA for DCC",
    log_prints=True,
    flow_run_name="{runner}_" + f"{get_time()}",
)
def runner_dcc_w_SRA(
    bucket: str,
    file_path: str,
    runner: str,
    template_tag: str = "",
    sra_template_path: str = "",
    sra_previous_file_path: str = "",
    dbgap_previous_dir_path: str = "",
):
    """CCDI-DCC data curation pipeline with SRA step

    Args:
        bucket (str): Bucket name of where the manifest is located in and the output goes to, such as "ccdi-validation"
        file_path (str): File path of the DCC manifest, such as "path/to/manifest.xlsx"
        runner (str): Unique runner name
        template_tag (str, optional): Tag of the DCC manifest template. Defaults to "" to use the latest version.
        sra_template_path (str, optional): SRA template file path in the s3 bucket. Defaults to "".
        sra_previous_file_path (str, optional): SRA previous submission file path in the s3 bucket. Defaults to "".
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
    runner_logger.info(
        f"The version of provided CCDI-DCC manifest is v{manifest_version}"
    )

    # get study phs and create output_folder name
    phs_accession = get_manifest_phs(os.path.basename(file_path))
    output_folder = runner.rstrip("/") + "/" + phs_accession + "_outputs_" + get_time()

    # download CCDI-DCC template if not provided
    if template_tag != "":
        input_template = CCDI_DCC_Tags().download_tag_manifest(
            tag=template_tag, logger=runner_logger
        )
        runner_logger.info(
            f"A CCDI-DCC template manifest in tag {template_tag} was downloaded from GitHub repo"
        )
    else:
        # template tag not provided, and download the template in the latest version tag
        input_template = CCDI_DCC_Tags().download_latest_tag_manifest(
            logger=runner_logger
        )
        latest_tag = CCDI_DCC_Tags().get_latest_tag()
        runner_logger.info(
            f"No CCDI-DCC tag was provided and the latest CCDI-DCC template {latest_tag} was downloaded from GitHub repo"
        )
    template_version = check_ccdi_version(input_template)
    runner_logger.info(
        f"The version of CCDI-DCC template version is v{template_version}"
    )

    latest_manifest_version = CCDI_DCC_Tags().get_latest_tag()
    runner_logger.info(
        f"The current latest version of CCDI-DCC template is v{latest_manifest_version}"
    )

    # Check dcc version and manifest version
    if manifest_version == latest_manifest_version:
        # this can only happen when the user provides an old version of template in the bucket
        if template_version != latest_manifest_version:
            input_template = CCDI_DCC_Tags().download_latest_tag_manifest(
                logger=runner_logger
            )
            runner_logger.warning(
                f"CCDI-DCC Manifest was found in latest version {manifest_version}, while a version of {template_version} was provided as template tag. The workflow continues by downloading the newest CCDI-DCC template"
            )
        else:
            pass
    else:
        if latest_manifest_version is None:
            # This might happen when the github API limit being reached and we failed to get latest manifest version
            # the get_ccdi_latest_release() will return "unknown"
            runner_logger.warning(
                "Fail to retrieve latest manifest version through GitHub API. The workflow will continue without this information"
            )
        else:
            if manifest_version == template_version:
                output_folder = (
                    output_folder + "(OLD_VERSION_v" + manifest_version + ")"
                )
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

    # download SRA template if not provided
    if sra_template_path != "":
        file_dl(bucket, sra_template_path)
        input_sra_template = os.path.basename(sra_template_path)
        runner_logger.info("An SRA template was provided")
    else:
        input_sra_template = dl_sra_template()
        runner_logger.info(
            "No SRA template was provided. A template was downloaded from GitHub repo"
        )

    # download sra previous submission or dbgap previous submission if provided
    if sra_previous_file_path != "":
        runner_logger.info(
            f"User provided SRA previous submission file at s3 bucket {sra_previous_file_path}"
        )
        try:
            file_dl(bucket=bucket, filename=sra_previous_file_path)
            sra_previous_submission = os.path.basename(sra_previous_file_path)
            runner_logger.info(
                f"Downloaded SRA previous submission from s3 bucket: {sra_previous_file_path}"
            )
        except:
            runner_logger.info(
                "Problem occurred downloading SRA previous submission from s3 bucket. The workflow will continue without SRA previous submission input"
            )
            sra_previous_submission = None
    else:
        runner_logger.info("No SRA previous submission file provided")
        sra_previous_submission = None

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

    # upload wf inputs into designated bucket
    ccdi_wf_inputs_ul(
        bucket=bucket,
        output_folder=output_folder,
        ccdi_manifest=input_file,
        ccdi_template=input_template,
        sra_template=input_sra_template,
    )

    # run CatchERR
    runner_logger.info("Running CatchERRy flow")
    try:
        (catcherr_out_file, catcherr_out_log) = CatchERRy(input_file, input_template)
    except:
        catcherr_out_file = None
        catcherr_out_log = (
            input_file[0:-5] + "_CatchERR" + date.today().strftime("%Y%m%d") + ".txt"
        )
    # upload CatchERR output and log
    runner_logger.info(f"Uploading outputs of CatchERR to bucket {bucket}")
    ccdi_wf_outputs_ul(
        bucket=bucket,
        output_folder=output_folder,
        output_path=catcherr_out_file,
        output_log=catcherr_out_log,
        sub_folder="1_CatchERR_output",
    )

    if catcherr_out_file is not None:

        # run ValidationRy
        runner_logger.info("Running ValidationRy flow")
        try:
            validation_out_file = ValidationRy_new(catcherr_out_file, input_template)
        except:
            validation_out_file = None
        # upload ValidationRy output
        runner_logger.info(f"Uploading outputs of ValidationRy to bucket {bucket}")
        ccdi_wf_outputs_ul(
            bucket=bucket,
            output_folder=output_folder,
            output_path=validation_out_file,
            output_log=None,
            sub_folder="2_ValidationRy_output",
        )

        # run CCDI to SRA
        runner_logger.info("Running CCDI to SRA submission file flow")
        try:
            (sra_out_file, sra_out_log) = CCDI_to_SRA(
                manifest=catcherr_out_file,
                template=input_sra_template,
                pre_submission=sra_previous_submission,
            )
        except:
            sra_out_file = None
            sra_out_log = identify_data_curation_log_file(
                start_str="CCDI_to_SRA_submission_"
            )
            # sra_out_log = "CCDI_to_SRA_submission_" + get_date() + ".log"
        runner_logger.info(f"Uploading outputs of SRA to bucket {bucket}")
        ccdi_wf_outputs_ul(
            bucket=bucket,
            output_folder=output_folder,
            output_path=sra_out_file,
            output_log=sra_out_log,
            sub_folder="3_SRA_submisison_output",
        )

        # run CCDI-DCC to dbGaP
        runner_logger.info("Running CCDI-DCC to dbGaP submission file flow")
        try:
            (dbgap_output_folder, dbgap_out_log) = CCDI_to_dbGaP(
                manifest=catcherr_out_file, pre_submission=dbgap_pre_sub_dir
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
            sub_folder="4_dbGaP_submisison_output",
        )

        # run CCDI-DCC to dcf index
        runner_logger.info("Running CCDI-DCC to DCF Index files flow")
        try:
            (dcf_index_file, dcf_index_log) = ccdi_to_dcf_index(
                ccdi_manifest=catcherr_out_file
            )
        except:
            dcf_index_file = None
            dcf_index_log = identify_data_curation_log_file(
                start_str="CCDI_to_DCF_Index_"
            )
        runner_logger.info(f"Uploading outputs of DCF index ofile to bucket {bucket}")
        ccdi_wf_outputs_ul(
            bucket=bucket,
            output_folder=output_folder,
            output_path=dcf_index_file,
            output_log=dcf_index_log,
            sub_folder="5_DCF_index_output",
        )

        # run CCDI-DCC to tabbreaker
        runner_logger.info("Running CCDI-DCC to TabBreaker flow")
        try:
            (tabbreaker_output_folder, tabbreaker_out_log) = tabBreakeRy_dcc(
                manifest=catcherr_out_file
            )
        except:
            tabbreaker_output_folder = None
            tabbreaker_out_log = identify_data_curation_log_file(
                start_str="DCC_TabBreaker_"
            )
        runner_logger.info(f"Uploading outputs of TabBreaker to bucket {bucket}")
        ccdi_wf_outputs_ul(
            bucket=bucket,
            output_folder=output_folder,
            output_path=tabbreaker_output_folder,
            output_log=tabbreaker_out_log,
            sub_folder="6_TabBreaker_output",
        )
    else:
        pass

    source_file_list = view_all_s3_objects(bucket)
    markdown_output_task(
        source_bucket=bucket,
        source_file_list=source_file_list,
        output_folder=output_folder,
        runner=runner,
    )


if __name__ == "__main__":
    bucket = "my-source-bucket"

    # test new version manifest and latest version template
    file_path = "inputs/test_file.xlsx"
    # template_path = "inputs/CCDI_Submission_Template_v1.7.1.xlsx"
    # sra_template_path = "path_to/sra_template/in/ccdi-curation/bucket"
    # sra_previous_file_path = "QL/phs002790_outputs_20240129_T113511/3_SRA_submisison_output/phs002790_SRA_submission.xlsx"
    # dbgap_previous_path = "QL/phs002790_outputs_20240129_T113511/4_dbGaP_submisison_output/phs002790_dbGaP_submission_2024-01-29"

    runner_dcc(
        bucket=bucket,
        file_path=file_path,
        # template_path=template_path,
        # sra_template_path=sra_template_path,
        runner="QL",
        # sra_previous_file_path=sra_previous_file_path,
        # dbgap_previous_dir_path=dbgap_previous_path,
    )
