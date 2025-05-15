import os
import sys
from prefect import flow, get_run_logger, pause_flow_run

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.mci_monthly_release import (
    download_diff_files,
    find_newly_added,
    ProceedtoMergeInput,
    MCIInputDescriptionMD,
)
from src.utils import file_dl, CCDI_Tags, get_time, file_ul, get_date
from src.file_mover import parse_file_url
from src.submission_cruncher import concatenate_submissions


@flow(name="MCI monthly release manifest", log_prints=True)
def mci_release_manifest(
    bucket: str,
    mci_manifests_bucket_path: str,
    template_tag: str,
    previous_pull_list_path: str,
    runner: str,
) -> None:
    """Pipeline that finds newly added manifests in a given bucket folder and combines them into a single CCDI manifest

    Args:
        bucket (str): Bucket name of where output goes to
        mci_manifests_bucket_path (str): Bucket path where newly added manifests are
        template_tag (str): A CCDI template tag to use for combining new manifests
        previous_pull_list_path (str): A file path in the given bucket that contains a list of previously pulled file names
        runner (str): Unique runner name
    """    
    logger = get_run_logger()

    manifest_bucket, manifest_folder = parse_file_url(
        url=mci_manifests_bucket_path
    )
    logger.info(
        f"The workflow will scan the bucket {manifest_bucket} folder {manifest_folder} for newly added manifests"
    )

    # output folder name to upload files
    output_folder = os.path.join(runner, "MCI_monthly_release_" + get_time())

    # download previous template. bucket here is mostly likely to be ccdi-validation
    file_dl(bucket=bucket, filename=previous_pull_list_path)
    prev_pull_list = os.path.basename(previous_pull_list_path)
    logger.info(f"Downlaoded previously pulled manifest list: {prev_pull_list}")

    # read the bucket path and identify the diff
    staging_bucket, latest_pull_filename, diff_list, diff_filename = find_newly_added(
        bucket_path=mci_manifests_bucket_path, prev_pulled_list=prev_pull_list
    )
    logger.info(f"Newly added manifests counts: {len(diff_list)}")

    # upload the last pull, latest pull and diff
    file_ul(
        bucket=bucket,
        output_folder=output_folder,
        sub_folder="",
        newfile=prev_pull_list,
    )
    file_ul(
        bucket=bucket,
        output_folder=output_folder,
        sub_folder="",
        newfile=latest_pull_filename,
    )
    file_ul(
        bucket=bucket,
        output_folder=output_folder,
        sub_folder="",
        newfile=diff_filename,
    )
    logger.info(
        f"Uploaded 3 files to bucket {bucket} folder {output_folder}:\n- {prev_pull_list}\n- {latest_pull_filename}\n- {diff_filename}"
    )

    print(diff_list)

    proceed_input = pause_flow_run(
        wait_for_input=ProceedtoMergeInput.with_initial_data(
            description=MCIInputDescriptionMD.proceed_to_merge_md.format(
                today_date=get_date(),
                bucket=bucket,
                output_folder=output_folder,
                diff_filename=diff_filename,
            ),
            proceed_to_merge="y/n",
        )
    )

    if proceed_input.proceed_to_merge == "y":
        logger.info("Start downloading newly added manifest files")
        # download the diff manifests
        # this returns a folder name downloading_folder which contains all the newly added manifests since last release
        downloading_folder = download_diff_files(
            bucket=staging_bucket, diff_file_list=diff_list
        )

        # download the template of a given tag
        template_name = CCDI_Tags().download_tag_manifest(
            tag=template_tag, logger=logger
        )

        # run submission cruncher
        # list all the files under submission_folder_path and filter list based on the file extension
        manifest_files = os.listdir(downloading_folder)
        manifest_files = [
            os.path.join(downloading_folder, i)
            for i in manifest_files
            if i.endswith(".xlsx")
        ]

        if len(manifest_files) > 0:
            logger.info(
                f"{len(manifest_files)} xlsx files were found in folder {downloading_folder}"
            )

            # concatenate submission files
            logger.info("Start merging submission files")
            output_file = concatenate_submissions(
                xlsx_list=manifest_files,
                template_file=template_name,
                logger=logger,
            )

            # upload the output to the bucket
            file_ul(
                bucket=bucket,
                output_folder=output_folder,
                sub_folder="",
                newfile=output_file,
            )
            logger.info(
                f"Uploaded merged manifest {output_file} to bucket {bucket} folder {output_folder}"
            )
        else:
            logger.warning(f"No xlsx file found under folder {downloading_folder}")

    else:
        logger.info(
            "You decided not to merge all the diff manifests identified. The workflow finished!"
        )

    return None
