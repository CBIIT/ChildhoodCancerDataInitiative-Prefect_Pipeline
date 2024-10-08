from prefect import flow, task, get_run_logger
import os
import sys


parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.utils import (
    get_time,
)
from src.read_buckets import (
    read_bucket_content,
    bucket_reader_md,
    single_bucket_content_str,
)
from botocore.exceptions import ClientError
import json
import traceback


@flow(
    name="Bucket Reader",
    log_prints=True,
    flow_run_name="bucket-reader-{runner}-" + f"{get_time()}",
)
def reader(buckets: list[str], runner: str) -> None:
    """Pipeline that reads contents of a list of bucket names/paths

    Args:
        buckets (list[str]): A list of bucket name/path, e.g., ["my-first-bucket", "my-second-bucket/subdir"]. Click "Add item" to add more list item.
        runner (str): Unique runner name
    """
    # create a logging object
    runner_logger = get_run_logger()

    md_str = ""

    for bucket in buckets:
        runner_logger.info(f"Start reading bucket {bucket}")
        try:
            bucket_size, file_count, file_ext_df, modified_date_df = read_bucket_content(
                bucket
            )
            # logger warning if bucket is empty without objects
            if bucket_size == 0:
                runner_logger.warning(f"Bucket {bucket} is EMPTY")
            else:
                pass
            single_bucket_summary = single_bucket_content_str(bucket_name=bucket, bucket_size=bucket_size, file_count=file_count, ext_dict=file_ext_df, date_dict=modified_date_df)
            md_str = md_str + single_bucket_summary
        except ClientError as ex:
            ex_code = ex.response["Error"]["Code"]
            ex_message = ex.response["Error"]["Message"]
            runner_logger.error(
                ex_code + ":" + ex_message + " Bucket name: " + bucket
            )
            runner_logger.error(
                "Additional info:\n" + json.dumps(ex.response["Error"], indent=4)
            )
        except Exception as error:
            runner_logger.error(f"Fetching contents in bucket {bucket} Failed: {type(error).__name__}, {error}")
            traceback.print_exc()

    runner_logger.info("Generating markdown output")
    if md_str != "":
        bucket_reader_md(
            tablestr=md_str,
            runner=runner.lower().replace("_", "-").replace(" ", "-").replace(".", "-"),
        )
    else:
        runner_logger.warning("No bucket contents summary was generated")

    runner_logger.info("Workflow finished!")


if __name__ == "__main__":
    mylist = [
        "my-source-bucket",
        "my-second-bucket",
    ]
    reader(*mylist, runner="Qiong Liu.try")
