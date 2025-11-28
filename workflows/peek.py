from prefect import flow, task, get_run_logger
import boto3
import os
import sys

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.utils import get_time, file_dl, file_ul


@flow(
    name="Simple File Mover",
    log_prints=True,
    flow_run_name="file-mover-simple-{runner}-" + f"{get_time()}",
)
def file_mover_simple(bucket: str, file_path: str, runner: str, dest_bucket: str) -> None:
    """Pipeline that moves a file object from one bucket to another.

    Args:
        bucket (str): Bucket name of where the manifest located at and output goes to
        file_path (str): File path of file
        runner (str): Unique runner name
        dest_bucket_path (str): Destination bucket path, e.g., dest-bucket-name/somefolder
    """    
    # create a logging object
    runner_logger = get_run_logger()
    s3 = boto3.client('s3')
    buckets = s3.list_buckets()
    canid = buckets['Owner']['ID']
    runner_logger.info(f"Your MC is {canid}.")
    # download the file
    file_dl(bucket, file_path)
    runner_logger.info(f"Downloaded file from bucket {bucket} at {file_path}")

    filename = os.path.basename(file_path)

    # upload files to bucket
    output_folder = os.path.join(runner, "file_mover_simple_outputs_" + get_time())
    file_ul(
        bucket=dest_bucket,
        output_folder=output_folder,
        sub_folder="",
        newfile=filename
    )
    runner_logger.info(
        f"Uploaded {filename} to bucket {bucket} folder {output_folder}"
    )
    runner_logger.info(f"File mover simple workflow has FINISHED! File can be found at {output_folder}")


if __name__=="__main__":
    bucket="my-source-bucket"
    file_path="inputs/testing_manifest.xlsx"
    runner="MAJ"
    dest_bucket_path = "my-dest-bucket/new-release"

    file_mover_simple(bucket=bucket, file_path=file_path, runner=runner, dest_bucket_path=dest_bucket_path)
