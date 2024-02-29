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


@flow(
    name="Bucket Reader",
    log_prints=True,
    flow_run_name="bucket-reader-{runner}-" + f"{get_time()}",
)
def reader(buckets: list[str], runner: str):
    # create a logging object
    runner_logger = get_run_logger()

    md_str = ""

    for bucket in buckets:
        runner_logger.info(f"Start reading bucket {bucket}")
        bucket_size, file_count, file_ext_df, modified_date_df = read_bucket_content(
            bucket
        )
        single_bucket_summary = single_bucket_content_str(bucket_name=bucket, bucket_size=bucket_size, file_count=file_count, ext_dict=file_ext_df, date_dict=modified_date_df)
        md_str = md_str + single_bucket_summary

    runner_logger.info("Generating markdown output")
    bucket_reader_md(
        tablestr=md_str,
        runner=runner.lower().replace("_", "-").replace(" ", "-").replace(".", "-"),
    )

    runner_logger.info("Workflow finished!")


if __name__ == "__main__":
    mylist = [
        "my-source-bucket",
        "my-second-bucket",
    ]
    reader(*mylist, runner="Qiong Liu.try")
