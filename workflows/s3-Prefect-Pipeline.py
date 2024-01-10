"""
This prefect pipeline works flow combines 7 tools
that catches error and validate the CCDI manifest before it
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
from src.s3_validationry import ValidationRy
from src.s3_ccdi_to_cds import CCDI_to_CDS
from src.s3_ccdi_to_index import CCDI_to_IndexeRy
from src.s3_ccdi_to_tabbreakery import tabBreakeRy
from src.utils import (
    get_time,
    get_date,
    get_manifest_phs,
    file_dl,
    view_all_s3_objects,
    markdown_input_task,
    markdown_output_task,
    check_ccdi_version,
    dl_ccdi_template,
    dl_sra_template,
    get_ccdi_latest_release,
    ccdi_wf_inputs_ul,
    ccdi_wf_outputs_ul,
)


@flow(
    name="S3 Prefect Pipeline",
    log_prints=True,
    flow_run_name="{runner}_" + f"{get_time()}",
)
def runner(
    bucket: str,
    file_path: str,
    runner: str,
    template_path: str = "path_to/ccdi_template/in/s3/bucket",
    sra_template_path: str = "path_to/sra_template/in/s3/bucket",
):
    # create a logging object
    runner_logger = get_run_logger()

    # if not profile:
    # profile = "default"

    # download the manifest
    file_dl(bucket, file_path)

    # check the manifest version before the workflow starts
    manifest_version = check_ccdi_version(os.path.basename(file_path))
    runner_logger.info(f"The version of provided CCDI manifest is v{manifest_version}")

    # get study phs and create output_folder name
    phs_accession = get_manifest_phs(os.path.basename(file_path))
    output_folder = runner + "/" + phs_accession + "_outputs_" + get_time()

    # download CCDI template if not provided
    if template_path != "path_to/ccdi_template/in/s3/bucket":
        file_dl(bucket, template_path)
        input_template = os.path.basename(template_path)
        runner_logger.info("A CCDI template was provided")
    else:
        input_template = dl_ccdi_template()
        runner_logger.info(
            "No CCDI template was provided and a CCDI template is downloaded from GitHub repo"
        )
    template_version = check_ccdi_version(input_template)
    runner_logger.info(f"The version of CCDI template version is v{template_version}")

    latest_manifest_version = get_ccdi_latest_release()
    runner_logger.info(
        f"The current latest version of CCDI template is v{latest_manifest_version}"
    )

    # Check ccdi version and manifest version
    if manifest_version == latest_manifest_version:
        # this can only happen when the user provides an old version of template in the bucket
        if template_version != latest_manifest_version:
            input_template = dl_ccdi_template()
            runner_logger.warning(
                f"CCDI Manifest was found in latest version {manifest_version}, while a version of {template_version} template was provided. The workflow continues by downloading the newest CCDI template"
            )
        else:
            pass
    else:
        if manifest_version == template_version:
            output_folder = output_folder + "(OLD_VERSION_v" + manifest_version + ")"
            runner_logger.error(
                f"An old version(v{manifest_version}) of CCDI manifest and CCDI template were provided. New version of CCDI template v{latest_manifest_version} is available"
            )
        else:
            runner_logger.error(
                f"An old version of CCDI manifest was provided(v{manifest_version}). And no matching CCDI template was provided. Please provide a matching CCDI template in the same version of the manifest or update the CCDI manifest to the latest version v{latest_manifest_version}"
            )
            raise ValueError(
                "CCDI manifest version is older version and doesn't match to the version of provided CCDI template"
            )

    # download SRA template if not provided
    if sra_template_path != "path_to/sra_template/in/s3/bucket":
        file_dl(bucket, sra_template_path)
        input_sra_template = os.path.basename(sra_template_path)
        runner_logger.info("An SRA template was provided")
    else:
        input_sra_template = dl_sra_template()
        runner_logger.info(
            "No SRA template was provided. A template was downloaded from GitHub repo"
        )

    input_file = os.path.basename(file_path)

    # create an artifact markdown of workflow inputs
    markdown_input_task(
        source_bucket=bucket,
        manifest=input_file,
        template=input_template,
        sra_template=input_sra_template,
        runner=runner,
    )

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
        wf_step="CatchERR",
        sub_folder="1_CatchERR_output",
    )

    if catcherr_out_file is not None:
        # run ValidationRy
        runner_logger.info("Running ValidationRy flow")
        try:
            validation_out_file = ValidationRy(catcherr_out_file, input_template)
        except:
            validation_out_file = None
        # upload ValidationRy output
        runner_logger.info(f"Uploading outputs of ValidationRy to bucket {bucket}")
        ccdi_wf_outputs_ul(
            bucket=bucket,
            output_folder=output_folder,
            output_path=validation_out_file,
            output_log=None,
            wf_step="ValidationRy",
            sub_folder="2_ValidationRy_output",
        )

        # run CCDI to SRA
        runner_logger.info("Running CCDI to SRA submission file flow")
        try:
            (sra_out_file, sra_out_log) = CCDI_to_SRA(
                manifest=catcherr_out_file, template=input_sra_template
            )
        except:
            sra_out_file = None
            sra_out_log = "CCDI_to_SRA_submission_" + get_date() + ".log"
        runner_logger.info(f"Uploading outputs of SRA to bucket {bucket}")
        ccdi_wf_outputs_ul(
            bucket=bucket,
            output_folder=output_folder,
            output_path=sra_out_file,
            output_log=sra_out_log,
            wf_step="CCDI-to-SRA",
            sub_folder="3_SRA_submisison_output",
        )

        # run CCDI to dbGaP
        runner_logger.info("Running CCDI to dbGaP submission file flow")
        try:
            (dbgap_output_folder, dbgap_out_log) = CCDI_to_dbGaP(
                manifest=catcherr_out_file
            )
        except:
            dbgap_output_folder = None
            dbgap_out_log = "CCDI_to_dbGaP_submission_" + get_date() + ".log"
        runner_logger.info(f"Uploading outputs of dbGaP to bucket {bucket}")
        ccdi_wf_outputs_ul(
            bucket=bucket,
            output_folder=output_folder,
            output_path=dbgap_output_folder,
            output_log=dbgap_out_log,
            wf_step="CCDI-to-dbGaP",
            sub_folder="4_dbGaP_submisison_output",
        )

        # run CCDI to CDS
        runner_logger.info("Runnning CCDI to CDS conversion flow")
        try:
            (cds_output_file, cds_output_log) = CCDI_to_CDS(
                manifest_path=catcherr_out_file
            )
        except:
            cds_output_file = None
            cds_output_log = "CCDI_to_CDS_submission_" + get_date() + ".log"
        runner_logger.info(f"Uploading outputs of CDS to bucket {bucket}")
        ccdi_wf_outputs_ul(
            bucket=bucket,
            output_folder=output_folder,
            output_path=cds_output_file,
            output_log=cds_output_log,
            wf_step="CCDI-to-CDS",
            sub_folder="5_CDS_output",
        )

        # run CCDI to index
        runner_logger.info("Running CCDI to Index files flow")
        try:
            (index_out_file, index_out_log) = CCDI_to_IndexeRy(
                manifest_path=catcherr_out_file
            )
        except:
            index_out_file = None
            index_out_log = "CCDI_to_Index_" + get_date() + ".log"
        runner_logger.info(f"Uploading outputs of Index to bucket {bucket}")
        ccdi_wf_outputs_ul(
            bucket=bucket,
            output_folder=output_folder,
            output_path=index_out_file,
            output_log=index_out_log,
            wf_step="CCDI-to-index",
            sub_folder="6_Index_output",
        )

        # run CCDI to tabbreaker
        runner_logger.info("Running CCDI to TabBreaker flow")
        try:
            (tabbreaker_output_folder, tabbreaker_out_log) = tabBreakeRy(
                manifest=catcherr_out_file
            )
        except:
            tabbreaker_output_folder = None
            tabbreaker_out_log = "CCDI_to_TabBreakeRy_" + get_date() + ".log"
        runner_logger.info(f"Uploading outputs of TabBreaker to bucket {bucket}")
        ccdi_wf_outputs_ul(
            bucket=bucket,
            output_folder=output_folder,
            output_path=tabbreaker_output_folder,
            output_log=tabbreaker_out_log,
            wf_step="CCDI-to-TabBreaker",
            sub_folder="7_TabBreaker_output",
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
    file_path = "inputs/CCDI_Submission_Template_v1.7.2_10ExampleR20231228.xlsx"
    # template_path = "inputs/CCDI_Submission_Template_v1.7.1.xlsx"
    # sra_template_path = "path_to/sra_template/in/ccdi-curation/bucket"

    runner(
        bucket=bucket,
        file_path=file_path,
        # template_path=template_path,
        # sra_template_path=sra_template_path,
        runner="QL",
    )
