from prefect import flow, task, get_run_logger
from prefect.input import RunInput
import os
import pandas as pd
from src.utils import set_s3_session_client, get_date
from src.read_buckets import paginate_parameter
from dataclasses import dataclass


class ProceedtoMergeInput(RunInput):
    proceed_to_merge: str


@dataclass
class MCIInputDescriptionMD:
    """dataclass for wait for input description MD"""

    proceed_to_merge_md: str = (
        """
**Please Review Newly Added Manifest List!**
Today's Date: *{today_date}*
Diff file bucket: *{bucket}*
Diff file path: *{output_folder}/{diff_filename}*

Do you want to create a mega-merged manifest of all the diff manifests identified:
- **proceed_to_merge**: y/n

"""
    )


@flow(name="Read MCI staging folder", log_prints=True)
def read_mci_staing_folder(bucket_path: str):
    s3_client = set_s3_session_client()
    s3_paginator = s3_client.get_paginator("list_objects_v2")

    operation_parameters = paginate_parameter(bucket_path)
    pages = s3_paginator.paginate(**operation_parameters)
    bucket_name = operation_parameters["Bucket"]
    download_list=[]
    for page in pages:
        if "Contents" in page.keys():
            for obj_dict in page["Contents"]:
                object_key=obj_dict["Key"]
                object_basename = os.path.basename(object_key)
                # This only exclude
                if object_basename.startswith(".") or "P_____" in object_basename:
                    pass
                else:
                    download_list.append({"object_key":object_key, "filename":object_basename})
        else:
            pass
    print(f"Files in bucket path {bucket_path} counts: {len(download_list)}")
    # print out the objects found in the most recent pull
    run_date = get_date()
    latest_pull_name = run_date + "_mci_ccdi_pull.txt"
    latest_obj_str = "\n".join([i["filename"] for i in download_list])
    with open(latest_pull_name, "w") as pull_file:
        pull_file.write(latest_obj_str)
    print(
        f"Writing filenames of objects found in bucket path {bucket_path} into latest_pull_name"
    )
    s3_client.close()
    return bucket_name, download_list, latest_pull_name


@flow(name="find newly added", )
def find_newly_added(download_list: list[dict], prev_pulled_list:str):
    prev_file_list = pd.read_csv(prev_pulled_list, header=None)[0].tolist()
    diff_list = [i for i in download_list if i["filename"] not in prev_file_list]
    print(f"Newly added file counts: {len(diff_list)}")
    # create a file containing only diff filenames
    run_date= get_date()
    diff_filename = run_date + "_mci_ccdi_diff.txt"
    diff_filename_str = "\n".join([i["filename"] for i in diff_list])
    with open(diff_filename, "w") as diff_file:
        diff_file.write(diff_filename_str)
    return diff_list, diff_filename


@flow(name="download diff files", log_prints=True)
def download_diff_files(bucket: str, diff_file_list: list[dict]):
    s3_client = set_s3_session_client()
    downloading_folder = "newly_added_manifests/"
    # create the folder for downloaded files 
    os.makedirs(downloading_folder[:-1], exist_ok=True)
    
    download_file_list = []
    for h in diff_file_list:
        h_filename = h["filename"]
        h_dst = downloading_folder + h_filename
        h_key = h["object_key"]
        h_dict = {"object_key":h_key, "download_dst": h_dst}
        download_file_list.append(h_dict)
    print(bucket)
    print(diff_file_list)
    print(f"Downloading {len(diff_file_list)} files")
    print([i["object_key"] for i in download_file_list])
    print([i["download_dst"] for i in download_file_list])
    progress = 1
    for i in download_file_list:
        i_key = i["object_key"]
        i_filename = i["download_dst"]
        s3_client.download_file(bucket, i_key, i_filename)
        if progress % 20 == 0:
            print(f"Downloading progress: {progress}/{len(diff_file_list)}")
        else:
            pass
        progress += 1
    s3_client.close()
    return downloading_folder
