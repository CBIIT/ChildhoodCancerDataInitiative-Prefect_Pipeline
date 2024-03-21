import sys
import os
from prefect import flow, pause_flow_run, get_run_logger

import prefect
import boto3
parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)

from src.utils import get_time
from src.file_remover import InputDescriptionMD, FlowPathInput, ManifestPathInput, NoManifestPathInput


@flow(log_prints=True)
def run_file_remover():
    logger = get_run_logger()
    current_time = get_time()

    user_input = pause_flow_run(
        wait_for_input=FlowPathInput.with_initial_data(
            description=InputDescriptionMD.have_manifest_md.format(
                current_time=current_time
            ),
            have_manifest="y",
        )
    )

    if user_input.have_manifest == "y":
        logger.info("You have a manifest for File Remover")
        manifest_path_inputs = pause_flow_run(
            wait_for_input=ManifestPathInput.with_initial_data(
                description=InputDescriptionMD.manifest_inputs_md, bucket="ccdi-validation"
            )
        )

        logger.info(
            f"bucket: {manifest_path_inputs.bucket}"
            + "\n"
            + f"manifest_tsv_path: {manifest_path_inputs.manifest_tsv_path}"
            + "\n"
            + f"delete_column_name: {manifest_path_inputs.delete_column_name}"
            + "\n"
            + f"runner id: {manifest_path_inputs.runner}"
        )

    else:
        logger.info("You don't have a manifest. To proceed, please provide a prod bucket path and a staging bucket path")
        no_manifest_path_inputs = pause_flow_run(
            wait_for_input= NoManifestPathInput.with_initial_data(
                description=InputDescriptionMD.no_manifest_inputs_md
            )
        )
        logger.info(
            f"prod_bucket_path: {no_manifest_path_inputs.prod_bucket_path}"
              + "\n" + f"staging_bucket_path: {no_manifest_path_inputs.staging_bucket_path}"
              + "\n" + f"runner: {no_manifest_path_inputs.runner}"
        )


if __name__=="__main__":
    run_file_remover()
