from prefect import flow, task, get_run_logger
import os
import sys

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.file_mover import move_manifest_files
from src.utils import get_time, file_dl, file_ul


@flow(
    name="File Mover",
    log_prints=True,
    flow_run_name="file-mover-{runner}-" + f"{get_time()}",
)
def file_mover(bucket: str, file_path: str, runner: str, dest_bucket_path: str) -> None:
    """Pipeline that moves file objects in a CCDI manifest to a designated bucket folder, and generates a new CCDI manifest with updated S3 uri

    Args:
        bucket (str): Bucket name of where the manifest located at and output goes to
        file_path (str): File path of CCDI manifest
        runner (str): Unique runner name
        dest_bucket_path (str): Destination bucket path, e.g., dest-bucket-name/somefolder
    """    
    # create a logging object
    runner_logger = get_run_logger()

    # download the manifest
    file_dl(bucket, file_path)
    runner_logger.info(f"Downloaded manifest from bucket {bucket} at {file_path}")

    manifest_path = os.path.basename(file_path)

    # Start file moving and generating new files
    modified_manifest, mover_log, summary_table =  move_manifest_files(manifest_path=manifest_path, dest_bucket_path=dest_bucket_path)

    # upload files to bucket
    output_folder = os.path.join(runner, "file_mover_outputs_" + get_time())
    file_ul(
        bucket=bucket,
        output_folder=output_folder,
        sub_folder="",
        newfile=modified_manifest
    )
    runner_logger.info(f"Uploaded {modified_manifest} to bucket {bucket} folder {output_folder}")
    file_ul(
        bucket=bucket,
        output_folder=output_folder,
        sub_folder="",
        newfile=mover_log,
    )
    runner_logger.info(
        f"Uploaded {mover_log} to bucket {bucket} folder {output_folder}"
    )
    file_ul(
        bucket=bucket,
        output_folder=output_folder,
        sub_folder="",
        newfile=summary_table,
    )
    runner_logger.info(
        f"Uploaded {summary_table} to bucket {bucket} folder {output_folder}"
    )
    runner_logger.info(f"File mover workflow has FINISHED! Modified CCDI manifest, summary table tsv, and workflow log can be found at {output_folder}")


if __name__=="__main__":
    bucket="my-source-bucket"
    file_path="inputs/testing_manifest.xlsx"
    runner="QL"
    dest_bucket_path = "my-dest-bucket/new-release"

    file_mover(bucket=bucket, file_path=file_path, runner=runner, dest_bucket_path=dest_bucket_path)
