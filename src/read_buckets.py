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
    object_basename, object_ext = os.path.splitext(object_filename)
    if "." in object_basename and object_ext in [".gz", ".zip"]:
        object_ext = os.path.splitext(object_basename)[-1] + object_ext
    else:
        pass
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


def paginate_parameter(bucket_path: str) -> tuple:
    if bucket_path.startswith("s3://"):
        bucket_path = bucket_path[5:]
    else:
        pass
    # make sure path ends with "/" even only bucket name
    if not bucket_path.endswith("/"):
        bucket_path = bucket_path + "/"
    else:
        pass
    #bucket_path = bucket_path.strip("/")
    if "/" not in bucket_path:
        bucket_name = bucket_path
        prefix = ""
    else:
        bucket_path_splitted = bucket_path.split("/", 1)
        bucket_name = bucket_path_splitted[0]
        prefix = bucket_path_splitted[1]
    return {"Bucket": bucket_name, "Prefix": prefix}


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
    operation_parameters = paginate_parameter(bucket)
    pages = s3_paginator.paginate(**operation_parameters)
    for page in pages:
        if "Contents" in page.keys():
            file_count += len(page["Contents"])
            for obj in page["Contents"]:
                obj_ext, obj_size, obj_date = extract_obj_info(obj)
                file_ext = count_df(mydict=file_ext, newitem=obj_ext)
                modified_date = count_df(mydict=modified_date, newitem=obj_date)
                bucket_size += obj_size
        else:
            pass

    # returns empty df of file_ext_df and modified_date_df if bucket is empty
    if file_ext != {}:
        file_ext_df = (
            pd.DataFrame(file_ext)
            .transpose()
            .reset_index()
            .rename(columns={"index": "Extension", 0: "Count"})
        ).sort_values(by=["Count"], ascending=False)
    else:
        file_ext_df = pd.DataFrame({})
    
    if modified_date != {}:
        modified_date_df = (
            pd.DataFrame(modified_date)
            .transpose()
            .reset_index()
            .rename(columns={"index": "Date", 0: "Count"})
        ).sort_values(by=["Count"], ascending=False)
    else:
        modified_date_df = pd.DataFrame({})

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
