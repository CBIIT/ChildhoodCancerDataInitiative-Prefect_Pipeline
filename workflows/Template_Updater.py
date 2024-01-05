"""
This workflow takes a manifest of CCDI, and updates it into
the most recent release of CCDI metadata manifest

Authors: Qiong Liu <qiong.liu@nih.gov>
         Sean Burke <sean.burke2@nih.gov>
"""
from prefect import flow, get_run_logger
import os
import sys

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.utils import (
    get_time,
    file_dl,
    check_ccdi_version,
    dl_ccdi_template,
    get_ccdi_latest_release,
    get_date,
    file_ul,
    markdown_template_updater,
)
from src.update_ccdi_template import updateManifest


@flow(
    name="Template Updater",
    log_prints=True,
    flow_run_name="{runner}_" + f"{get_time()}",
)
def updater(
    bucket: str,
    file_path: str,
    runner: str,
    template_path: str = "path_to/ccdi_template/in/s3/bucket",
) -> tuple:
    # create a logging object
    runner_logger = get_run_logger()

    # generate output folder name
    output_folder = runner + "/template_updater_outputs_" + get_time()

    # download the manifest
    file_dl(bucket, file_path)

    # check manifest version
    manifest_version = check_ccdi_version(os.path.basename(file_path))
    runner_logger.info(f"The version of provided CCDI manifest is v{manifest_version}")

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

    runner_logger.info(
        f"The manifest version will be updated from v{manifest_version} to v{template_version}"
    )

    try:
        updated_manifest, updated_log = updateManifest(
            manifest=os.path.basename(file_path),
            template=input_template,
            template_version=template_version,
        )
    except:
        updated_manifest = None
        updated_log = "Update_CCDI_manifest_" + get_date() + ".log"

    # Upload updated manifest if it's not None
    if updated_manifest is not None:
        file_ul(
            bucket=bucket,
            output_folder=output_folder,
            sub_folder="",
            newfile=updated_manifest,
        )
    else:
        pass
    # Upload workflow log
    file_ul(
        bucket=bucket,
        output_folder=output_folder,
        sub_folder="",
        newfile=updated_log,
    )

    # Generate markdown summary of this workflow
    markdown_template_updater(
        source_bucket=bucket,
        runner=runner,
        output_folder=output_folder,
        manifest=os.path.basename(file_path),
        manifest_version=manifest_version,
        template=input_template,
        template_version=template_version
    )


if __name__ == "__main__":
    bucket = "my-source-bucket"
    file_path = "inputs/CCDI_Submission_Template_v1.5.0_10ExampleR20240102.xlsx"

    updater(bucket=bucket, file_path=file_path, runner="QL")
