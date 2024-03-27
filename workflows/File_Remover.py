import sys
import os
from prefect import flow, pause_flow_run, get_run_logger

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)

from src.utils import get_time, file_dl, file_ul
from src.file_remover import (
    InputDescriptionMD,
    FlowPathInput,
    ManifestPathInput,
    NoManifestPathInput,
    ObjectDeletionInput,
    objects_deletion,
    create_matching_object_manifest,
)


@flow(name="File Remover Pipeline", log_prints=True)
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
                description=InputDescriptionMD.manifest_inputs_md,
                bucket="ccdi-validation",
            )
        )

        logger.info(
            "Inputs received:" + "\n"
            f"bucket: {manifest_path_inputs.bucket}"
            + "\n"
            + f"manifest_tsv_path: {manifest_path_inputs.manifest_tsv_path}"
            + "\n"
            + f"delete_column_name: {manifest_path_inputs.delete_column_name}"
            + "\n"
            + f"runner id: {manifest_path_inputs.runner}"
        )

        # download the manifest from bucket
        file_dl(
            bucket=manifest_path_inputs.bucket,
            filename=manifest_path_inputs.manifest_tsv_path,
        )
        logger.info(
            f"Downloaded manifest {manifest_path_inputs.manifest_tsv_path} from bucket {manifest_path_inputs.bucket}"
        )
        manifest_file = os.path.basename(manifest_path_inputs.manifest_tsv_path)

        # start deleting objects in manifest
        logger.info("Start objects deletion process")
        deletion_summary, deletion_counts_df = objects_deletion(
            manifest_file_path=manifest_file,
            delete_column_name=manifest_path_inputs.delete_column_name,
            runner=manifest_path_inputs.runner,
        )
        logger.info(deletion_counts_df.to_markdown(index=False, tablefmt="rst"))
        logger.info(
            f"Objects deletion finished and a summary table has been generated {deletion_summary}"
        )

        # upload the deletion summary to bucket
        output_folder = (
            manifest_path_inputs.runner + "/" + "file_remover_summary_" + get_time()
        )
        file_ul(
            bucket=manifest_path_inputs.bucket,
            output_folder=output_folder,
            sub_folder="",
            newfile=deletion_summary,
        )
        logger.info(
            f"Uploaded summary table to bucket {manifest_path_inputs.bucket} under folder {output_folder}"
        )
        logger.info("File Remover workflow finished!")

    else:
        logger.info(
            "You don't have a manifest. To proceed, please provide a prod bucket path and a staging bucket path"
        )
        no_manifest_path_inputs = pause_flow_run(
            wait_for_input=NoManifestPathInput.with_initial_data(
                description=InputDescriptionMD.no_manifest_inputs_md
            )
        )
        logger.info(
            "Inputs received:" + "\n"
            f"prod_bucket_path: {no_manifest_path_inputs.prod_bucket_path}"
            + "\n"
            + f"staging_bucket_path: {no_manifest_path_inputs.staging_bucket_path}"
            + "\n"
            + f"workflow_output_bucket: {no_manifest_path_inputs.workflow_output_bucket}"
            + "\n"
            + f"runner: {no_manifest_path_inputs.runner}"
        )

        logger.info(
            f"Start generating a manifest using provided prod_bucket_path {no_manifest_path_inputs.prod_bucket_path} and staging_bucket_path {no_manifest_path_inputs.staging_bucket_path}"
        )
        manifest_file = create_matching_object_manifest(
            prod_bucket_path=no_manifest_path_inputs.prod_bucket_path,
            staging_bucket_path=no_manifest_path_inputs.staging_bucket_path,
            runner=no_manifest_path_inputs.runner,
        )
        logger.info("Generated a manifest for objects deletion")

        # upload the manifest to bucket
        # upload the deletion summary to bucket
        output_folder = (
            no_manifest_path_inputs.runner + "/" + "file_remover_summary_" + get_time()
        )
        file_ul(
            bucket=no_manifest_path_inputs.workflow_output_bucket,
            output_folder=output_folder,
            sub_folder="",
            newfile=manifest_file,
        )
        logger.info(
            f"Uploaded manifest {manifest_file} to bucket {no_manifest_path_inputs.workflow_output_bucket} under folder {output_folder}. Please check the manifest before deletion."
        )
        deletion_input = pause_flow_run(
            wait_for_input=ObjectDeletionInput.with_initial_data(
                description=InputDescriptionMD.object_deletion_md.format(
                    manifest_file=manifest_file,
                    bucket=no_manifest_path_inputs.workflow_output_bucket,
                    folder=output_folder,
                ),
                proceed_to_delete="n",
            )
        )
        if deletion_input.proceed_to_delete == "y":
            logger.info("Start objects deletion process")
            deletion_summary, deletion_counts_df = objects_deletion(
                manifest_file_path=manifest_file,
                delete_column_name="Staging_S3_URI",
                runner=no_manifest_path_inputs.runner,
            )
            logger.info(
                deletion_counts_df.to_markdown(index=False, tablefmt="rst")
            )
            logger.info(f"Objects deletion finished and a summary table has been generated {deletion_summary}")

            # upload deletion_summary to bucket
            file_ul(
                bucket=no_manifest_path_inputs.workflow_output_bucket,
                output_folder=output_folder,
                sub_folder="",
                newfile=deletion_summary,
            )
            logger.info(f"Uploaded deletion summaru table {deletion_summary} to bucket {no_manifest_path_inputs.workflow_output_bucket} under folder {output_folder}")
            logger.info("File Remover workflow finished!")
        else:
            logger.info(
                f"You chose not to proceed with deletion this time.\nYou can restart the flow whenever you're ready.\nManifest location: {no_manifest_path_inputs.workflow_output_bucket}/{output_folder}/{manifest_file}"
            )
            logger.info("File Remover workflow finished!")

if __name__ == "__main__":
    run_file_remover()
