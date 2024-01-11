from prefect import flow, task
import os
import sys

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.utils import (
    get_time,
)
from src.read_buckets import read_bucket_content, bucket_reader_md, single_bucket_content_str


@flow(
    name="Bucket Reader",
    log_prints=True,
    flow_run_name="bucket-reader-{runner}-" + f"{get_time()}",
)
def reader(*buckets, runner: str):

    md_str = ""

    for bucket in buckets:
        bucket_df = read_bucket_content(bucket)
        single_bucket_summary = single_bucket_content_str(bucket_df=bucket_df, bucket_name=bucket)
        md_str = md_str + single_bucket_summary
    
    bucket_reader_md(tablestr=md_str, runner=runner.lower())


if __name__ == "__main__":
    mylist = [
        "my-source-bucket", "my-second-bucket",
    ]
    reader(*mylist, runner="QL")
