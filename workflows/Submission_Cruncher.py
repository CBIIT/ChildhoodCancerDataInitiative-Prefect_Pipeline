from prefect import flow, get_run_logger
import os
import sys

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.submission_cruncher import concatenate_submissions
from typing import Literal
from src.utils import (
    get_time,
    folder_dl,
    file_ul,
    CCDI_DCC_Tags,
    CCDI_Tags,
)
DropDownChoices = Literal["ccdi-dcc", "ccdi"]
@flow(
    name="Submission Cruncher Flow",
    log_prints=True,
    flow_run_name="{runner}_" + f"{get_time()}",
)
def submission_cruncher(
    bucket: str,
    submission_folder_path: str,
    runner: str,
    template_source: DropDownChoices,
    template_tag: str,
) -> None:
    """Pipeline that combines all manifests in a bucket folder path into a single manifest

    Args:
        bucket (str): Bucket name of where the manifests located in and the output goes to
        submission_folder_path (str): A folder path of the manifests folder
        runner (str): Unique runner name
        template_source (DropDownChoices): The source of the manifest template, either "ccdi-dcc" or "ccdi".
        template_tag (str): The tag of the data model to use.
    """    
    runner_logger = get_run_logger()

    # dl submission_folder_path
    runner_logger.info(
        f"Downloading folder {submission_folder_path} from bucket {bucket}"
    )
    folder_dl(bucket=bucket, remote_folder=submission_folder_path)

    # download the template of a given tag
    if template_source == "ccdi-dcc":
        template = CCDI_DCC_Tags().download_tag_manifest(
            tag=template_tag, logger=runner_logger
        )
    elif template_source == "ccdi":
        template = CCDI_Tags().download_tag_manifest(
            tag=template_tag, logger=runner_logger
        )
    else:
        runner_logger.error(
            f"Invalid template source {template_source}. Please choose either ccdi-dcc or ccdi"
        )
        raise ValueError(
            f"Invalid template source {template_source}. Please choose either ccdi-dcc or ccdi"
        )

    # list all the files under submission_folder_path and filter list based on the file extension
    submission_files = os.listdir(submission_folder_path)
    submission_files = [
        os.path.join(submission_folder_path, i)
        for i in submission_files
        if i.endswith(".xlsx")
    ]

    if len(submission_files) > 0:
        runner_logger.info(
            f"{len(submission_files)} xlsx files were found in folder {submission_folder_path}"
        )

        # concatenate submission files
        runner_logger.info("Start merging submission files")
        output_file = concatenate_submissions(
            xlsx_list=submission_files, template_file=template, logger=runner_logger
        )

        # upload the output to the bucket
        output_folder = os.path.join(runner, "Submission_Cruncher_output_" + get_time())
        file_ul(
            bucket=bucket,
            output_folder=output_folder,
            sub_folder="",
            newfile=output_file,
        )
        runner_logger.info(
            f"Uploaded output {output_file} to bucket {bucket} folder {output_folder}"
        )
    else:
        runner_logger.warning(
            f"No xlsx file found under folder {submission_folder_path}."
        )
