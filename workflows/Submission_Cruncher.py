from prefect import flow, get_run_logger
import os
import sys

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.submission_cruncher import concatenate_submissions
from src.utils import get_time, folder_dl, file_dl, file_ul, dl_ccdi_template


@flow(
    name="Submission Cruncher Flow",
    log_prints=True,
    flow_run_name="{runner}_" + f"{get_time()}",
)
def submission_cruncher(
    bucket: str,
    submission_folder_path: str,
    runner: str,
    template_path: str = "default_to_latest",
) -> None:
    runner_logger = get_run_logger()

    # dl submission_folder_path
    runner_logger.info(
        f"Downloading folder {submission_folder_path} from bucket {bucket}"
    )
    folder_dl(bucket=bucket, remote_folder=submission_folder_path)

    # dl template
    if template_path != "default_to_latest":
        runner_logger.info(f"User provided a template path in bucket {bucket}")
        runner_logger.info(f"Downloading folder {template_path} from bucket {bucket}")
        file_dl(bucket=bucket, filename=template_path)
        template = os.path.basename(template_path)
    else:
        runner_logger.info(f"User didn't provided a template path in bucket {bucket}")
        runner_logger.info("Downloading the latest CCDI template release")
        template = dl_ccdi_template()

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
