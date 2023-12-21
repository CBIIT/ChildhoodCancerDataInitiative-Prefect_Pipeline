"""
This prefect pipeline works flow combines four tools
that catches error and validate the CCDI manifest before it
generates files for dbGaP and SRA submission.

Authors: Sean Burke <sean.burke2@nih.gov>
         Qiong Liu <qiong.liu@nih.gov>
"""
from prefect import flow, get_run_logger
import os
import subprocess
from src.s3_ccdi_to_sra import CCDI_to_SRA
from src.s3_ccdi_to_dbgap import CCDI_to_dbGaP
from src.s3_catcherry import CatchERRy
from src.s3_validationry import ValidationRy
from src.s3_ccdi_to_cds import CCDI_to_CDS
from src.s3_ccdi_to_index import CCDI_to_IndexeRy
from src.utils import (
    get_time,
    get_manifest_phs,
    outputs_ul,
    file_dl,
    view_all_s3_objects,
    markdown_input_task,
    markdown_output_task,
    check_ccdi_version,
    dl_ccdi_template,
    dl_sra_template,
    get_ccdi_latest_release,
    folder_ul,
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

    # run CatchERR
    runner_logger.info("Running CatchERRy flow")
    (catcherr_out_file, catcherr_out_log) = CatchERRy(input_file, input_template)
    # run ValidationRy
    runner_logger.info("Running ValidationRy flow")
    validation_out_file = ValidationRy(catcherr_out_file, input_template)
    # run CCDI to SRA
    runner_logger.info("Running CCDI to SRA submission file flow")
    (sra_out_file, sra_out_log) = CCDI_to_SRA(
        manifest=catcherr_out_file, template=input_sra_template
    )
    # run CCDI to dbGaP
    runner_logger.info("Running CCDI to dbGaP submission file flow")
    (dbgap_output_folder, dbgap_out_log) = CCDI_to_dbGaP(manifest=catcherr_out_file)
    # run CCDI to CDS
    runner_logger.info("Runnning CCDI to CDS conversion flow")
    (cds_output_file, cds_output_log) = CCDI_to_CDS(manifest_path=catcherr_out_file)
    # run CCDI to index
    runner_logger.info("Running CCDO to Index files flow")
    (index_out_file, index_out_log) = CCDI_to_IndexeRy(manifest_path=catcherr_out_file)

    # upload all outputs to the source bucket
    runner_logger.info(
        f"Uploading workflow inputs and outputs to bucket {bucket} under folder {output_folder}"
    )
    outputs_ul(
        bucket=bucket,
        output_folder=output_folder,
        ccdi_manifest=input_file,
        ccdi_template=input_template,
        sra_template=input_sra_template,
        catcherr_file=catcherr_out_file,
        catcherr_log=catcherr_out_log,
        validation_log=validation_out_file,
        sra_file=sra_out_file,
        sra_log=sra_out_log,
        dbgap_folder=dbgap_output_folder,
        dbgap_log=dbgap_out_log,
        cds_file=cds_output_file,
        cds_log=cds_output_log,
        index_file=index_out_file,
        index_log=index_out_log,
    )

    source_file_list = view_all_s3_objects(bucket)
    markdown_output_task(
        source_bucket=bucket,
        source_file_list=source_file_list,
        output_folder=output_folder,
        runner=runner,
    )

    # run gaptools validaiton of dbgap submission
    create_gaptools_out = f"mkdir gaptools_out/"
    gaptools_up = f"bash ./dbgap-docker.bash up -i {dbgap_output_folder} -o gaptools_out/ -m {dbgap_output_folder}/metadata.json"
    gaptools_down = "bash ./dbgap-docker.bash down"
    runner_logger.info("Start running gaptool validation")
    subprocess.run(create_gaptools_out, shell=True)
    subprocess.run(gaptools_up, shell=True)
    subprocess.run(gaptools_down, shell=True)
    folder_ul(local_folder="gaptools_out", bucket=bucket,destination=output_folder, sub_folder="7_dbGaP_validation_report")


if __name__ == "__main__":
    bucket = "my-source-bucket"

    # test new version manifest and latest version template
    file_path = "inputs/CCDI_Submission_Template_v1.7.1_40ExampleR20231207_noguid.xlsx"
    # template_path = "inputs/CCDI_Submission_Template_v1.7.1.xlsx"
    # sra_template_path = "path_to/sra_template/in/ccdi-curation/bucket"

    runner(
        bucket=bucket,
        file_path=file_path,
        # template_path=template_path,
        # sra_template_path=sra_template_path,
        runner="QL",
    )
