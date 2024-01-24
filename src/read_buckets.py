from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from typing import Dict, TypeVar
import os
import pandas as pd
import humanize
import time
from src.utils import get_time, set_s3_session_client


DataFrame = TypeVar("DataFrame")


def extract_obj_info(bucket_object: Dict) -> Dict:
    object_name = bucket_object["Key"]
    object_filename = os.path.basename(object_name)
    object_ext = os.path.splitext(object_filename)[-1]
    if (object_ext is None) or (object_ext == ""):
        object_ext = "missing ext"
    object_size = bucket_object["Size"]
    modified_date = bucket_object["LastModified"].strftime("%Y-%m-%d")
    return (object_ext, object_size, modified_date)


def count_df(mydict: Dict, newitem: str) -> Dict:
    if newitem in mydict.keys():
        mydict[newitem][0] += 1
    else:
        mydict[newitem] = []
        mydict[newitem].append(1)
    return mydict


@flow(
    name="Read Bucket Content",
    log_prints=True,
    flow_run_name="read-bucket-content-" + f"{get_time()}",
)
def read_bucket_content(bucket):
    s3 = set_s3_session_client()
    s3_paginator = s3.get_paginator("list_objects_v2")

    bucket_size = 0
    file_count = 0
    file_ext = {}
    modified_date = {}
    pages = s3_paginator.paginate(Bucket=bucket)
    for page in pages:
        file_count += len(page["Contents"])
        for obj in page["Contents"]:
            obj_ext, obj_size, obj_date = extract_obj_info(obj)
            file_ext = count_df(mydict=file_ext, newitem=obj_ext)
            modified_date = count_df(mydict=modified_date, newitem=obj_date)
            bucket_size += obj_size

    file_ext_df = (
        pd.DataFrame(file_ext)
        .transpose()
        .reset_index()
        .rename(columns={"index": "Extension", 0: "Count"})
    ).sort_values(by=["Count"], ascending=False)
    modified_date_df = (
        pd.DataFrame(modified_date)
        .transpose()
        .reset_index()
        .rename(columns={"index": "Date", 0: "Count"})
    ).sort_values(by=["Count"], ascending=False)

    return bucket_size, file_count, file_ext_df, modified_date_df


def single_bucket_content_str(
    bucket_name: str, bucket_size: int, file_count: int, ext_dict: Dict, date_dict: Dict
) -> str:
    bucket_size = humanize.naturalsize(bucket_size)
    bucket_ext_str = ext_dict.to_markdown(tablefmt="pipe", index=False)
    bucket_date_str = date_dict.to_markdown(tablefmt="pipe", index=False)
    single_bucket_str = f"""## {bucket_name}

### Bucket Size

{bucket_size}

### File Counts

{file_count}

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
        key=f"bucket-reader-report-{runner.lower().replace('_','-').replace(' ','-').replace('.','-').replace('/','-')}",
        markdown=markdown_report,
        description=f"Bucker Reader Report for {runner}",
    )
