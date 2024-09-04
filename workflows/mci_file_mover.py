from prefect import flow, task, get_run_logger
import os
import sys
import pandas as pd
from typing import TypeVar

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.utils import get_time, file_dl, file_ul, get_date
from src.file_mover import copy_object_parameter, dest_object_url, copy_file_task, parse_file_url

DataFrame = TypeVar("DataFrame")

@task
def create_file_mover_metadata(tsv_df: DataFrame, newfolder: str) -> DataFrame:
    """Add additional columns to tsv_df with copy parameter dict and destination path

    Args:
        tsv_df (DataFrame): A dataframe with one col of uri
        newfolder (str): folder name to be used in the destination obj path

    Returns:
        DataFrame: Dataframe which contains original url, dest uri, copy parameter which can be used for file copying
    """    
    uri_list = tsv_df["original_uri"]
    newfolder = newfolder.strip("/")
    copy_param_list = []
    dest_list = []
    for i in uri_list:
        i_bucket, _ = parse_file_url(url=i)
        i_dest = i_bucket + "/" + newfolder
        i_copy_parameter = copy_object_parameter(url_in_cds=i, dest_bucket_path=i_dest)
        i_dest_path = dest_object_url(url_in_cds=i, dest_bucket_path=i_dest)
        copy_param_list.append(i_copy_parameter)
        dest_list.append(i_dest_path)
    tsv_df["dest_uri"] =  dest_list
    tsv_df["copy_parameter"] =  copy_param_list
    
    return tsv_df


@flow(
    name="mci file mover",
    log_prints=True,
    flow_run_name="mci-file-mover-{runner}-" + f"{get_time()}",
)
def mci_file_mover(runner: str, obj_list_tsv_path: str, move_to_folder: str, bucket: str = "ccdi-validation") -> None:
    """Moves objects listed in a tsv file to a new folder in the same bucket

    Args:
        runner (str): unique runner name
        obj_list_tsv_path (str): A file contains a column of s3 uri (s3://{bucket-name}/{file-path}). NO header needed
        move_to_folder (str): Folder name of where the obj will be moved to. New uri will be s3://{bucket-name}/{dest-folder}/{file-path}
        bucket (str, optional): Bucket of where tsv lives and output goes to. Defaults to "ccdi-validation".
    """
    current_time = get_time()

    tsv_name = file_dl(bucket=bucket, filename = obj_list_tsv_path)
    tsv_df = pd.read_csv(tsv_name, sep="\t", header=None, names =  ["original_uri"])

    meta_df = create_file_mover_metadata(tsv_df=tsv_df, newfolder=move_to_folder)
    # upload files to bucket
    output_folder = os.path.join(runner, "mci_file_mover_outputs_" + current_time)
    meta_output = f"mci_file_mover_manifest_{get_date()}.tsv"
    meta_df.to_csv(meta_output, sep="\t", index=False)
    file_ul(bucket=bucket, output_folder=output_folder, sub_folder="", newfile=meta_output)

    return None
