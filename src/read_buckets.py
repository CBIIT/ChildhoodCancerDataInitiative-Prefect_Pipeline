from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from typing import Dict, TypeVar
import os
import pandas as pd
import humanize
from src.utils import get_time, set_s3_session_client


DataFrame = TypeVar("DataFrame")


def extract_obj_info(bucket_object: Dict) -> Dict:
    object_name = bucket_object["Key"]
    object_filename = os.path.basename(object_name)
    object_ext = os.path.splitext(object_filename)[-1]
    object_size = bucket_object["Size"]
    modified_date = bucket_object["LastModified"].strftime("%Y-%m-%d")
    object_dict = {
        "filename": [object_filename],
        "size": [object_size],
        "file_ext": [object_ext],
        "last_modified_date": [modified_date],
    }
    return pd.DataFrame(object_dict)


@flow(
    name="Read Bucket Content",
    log_prints=True,
    flow_run_name="read-bucket-content-" + f"{get_time()}",
)
def read_bucket_content(bucket):
    s3 = set_s3_session_client()
    bucket_df = pd.DataFrame(
        {"filename": [], "size": [], "file_ext": [], "last_modified_date": []}
    )
    bucket_objects = s3.list_objects_v2(Bucket=bucket)
    for obj in bucket_objects["Contents"]:
        obj_dict = extract_obj_info(obj)
        bucket_df = pd.concat([bucket_df, obj_dict], ignore_index=True)
    return bucket_df


def single_bucket_content_str(bucket_df: DataFrame, bucket_name: str) -> str:
    bucket_size = humanize.naturalsize(bucket_df["size"].sum())
    bucket_file_no = bucket_df.shape[0]
    ext_breakdown_df = (
        bucket_df.groupby(["file_ext"])
        .size()
        .sort_values(ascending=True)
        .reset_index(name="Count")
        .rename(columns={"file_ext": "Extension"})
    )
    bucket_ext_str = ext_breakdown_df.to_markdown(tablefmt="pipe", index=False)
    date_breakdown_df = (
        bucket_df.groupby(["last_modified_date"])
        .size()
        .sort_values(ascending=True)
        .reset_index(name="Count")
        .rename(columns={"last_modified_date": "Date"})
    )
    bucket_date_str = date_breakdown_df.to_markdown(tablefmt="pipe", index=False)
    single_bucket_str = f"""## {bucket_name}

### Bucket Size

{bucket_size}

### File Counts

{bucket_file_no}

### File Extension Breakdown Table

{bucket_ext_str}

### Last Modified Date Breakdown Table

{bucket_date_str}

---
    
"""
    return single_bucket_str


@task
def bucket_reader_md(tablestr: str, runner: str) -> None:
    markdown_report = f"""# Bucket Reader Report

{tablestr}

"""
    create_markdown_artifact(
        key=f"bucket-reader-report-{runner}",
        markdown=markdown_report,
        description=f"Bucker Reader Report for {runner}",
    )
