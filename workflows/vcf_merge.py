"""Script to merge VCF files"""

# read in fiule that has s3 urls of files to download and merge
# the above may be done recursively

import os
import sys

import pandas as pd
from fuc import pyvcf
from datetime import datetime


# prefect dependencies
import boto3
from botocore.exceptions import ClientError
from prefect import flow, get_run_logger
from src.utils import get_time, file_dl, folder_ul

def read_input(file_path: str):
    """Read in file with s3 URLs of VCFs to merge and participant IDs 

    Args:
        file_path (str): path to input file that contains required cols

    Returns:
        pd.DataFrame: DataFrame with extracted necessary metadata
    """

    runner_logger = get_run_logger()

    f_name = os.path.basename(file_path)

    try:
        file_metadata = pd.read_csv(f_name, sep="\t")
    except:
        runner_logger.error(f"Error reading and parsing file {f_name}.")
        sys.exit(1)

    if "s3_url" not in file_metadata.columns:
        runner_logger.error(f"Error reading and parsing file {f_name}: no column named 's3_url'.")
        sys.exit(1)
    
    if "patient_id" not in file_metadata.columns:
        runner_logger.error(f"Error reading and parsing file {f_name}: no column named 'patient_id'.")
        sys.exit(1)

    if len(file_metadata) == 0:
        runner_logger.error(f"Error reading and parsing file {f_name}; empty file")
        sys.exit(1)

    return file_metadata

@flow(
    name="vcf_merge_download_vcfs",
    log_prints=True,
    flow_run_name="vcf_merge_download_vcfs_" + f"{get_time()}",
) 
def download_handler(df: pd.DataFrame):
    """Function to handle downloading VCF files
    """

    runner_logger = get_run_logger()

    for index, row in df.iterrows():
        f_bucket = row["s3_url"].split("/")[2]
        f_path = "/".join(row["s3_url"].split("/")[3:])

        # trying to re-use file_dl() function
        file_dl(f_bucket, f_path)

        # extract file name
        f_name = os.path.basename(f_path)

        # check that file exists
        if not os.path.isfile(f_name):
            runner_logger.error(
                f"File {f_name} not copied over or found from URL {row['s3_url']}"
            )
        else:
            pass
    
    return None


        
@flow(
    name="vcf_merge_merge_vcfs",
    log_prints=True,
    flow_run_name="vcf_merge_merge_vcfs_" + f"{get_time()}",
) 
def merging(df: pd.DataFrame):
    """Function to call fuc libray to merge a set of VCFs
    """

    runner_logger = get_run_logger()

    vcf_to_merge = []
    not_merged = []

    #need to log which VCFs produced which merged VCF for accounting purposes
    for index, row in df.iterrows():
        
        f_name = os.path.basename(row["s3_url"])

        if os.path.exists(f_name):
            vcf_to_merge.append(vcf_to_merge)
        else:
            not_merged.append(f_name)

    if len(vcf_to_merge) > 1:
        runner_logger.info(f"Running merge of {len(vcf_to_merge)} VCF files....")
        try:
            vcf_dfs = [] #list to store VCF DFs
            for vcf in vcf_to_merge:
                #read in VCF
                vcf_df = pyvcf.VcfFrame.from_file(vcf).df

                #change column names to temp sample names >>> TODO: need actual sample names? where to find?
                vcf_df = vcf_df.rename(columns={"tumor" : "tumor"+row["patient_id"], "normal" : "normal"+row["patient_id"]})
                
                #append to list of vcf_dfs
                vcf_dfs.append(vcf_df)

            merged_vcf = pyvcf.merge(vcf_dfs, how='outer')
            runner_logger.info("Merge Complete!")
        except:
            runner_logger.error("Issue merging VCF files.")
            merged_vcf = ""
    else:
        runner_logger.error("No VCF files provided to merge. Exiting process.")
        sys.exit(1)


    return vcf_to_merge, not_merged, merged_vcf


@flow(
    name="vcf_merge_remove_temp_vcfs",
    log_prints=True,
    flow_run_name="vcf_merge_remove_temp_vcfs_" + f"{get_time()}",
)
def delete_handler(df: pd.DataFrame):

    runner_logger = get_run_logger()

    for index, row in df.iterrows():
        f_name = os.path.basename(row["s3_url"])

        # delete file
        if os.path.exists(f_name):
            os.remove(f_name)
        else:
            runner_logger.warning(f"The file {f_name} does not exist, cannot remove.")
        
    return None


@flow(
    name="VCF Merge",
    log_prints=True,
    flow_run_name="{runner}_" + f"{get_time()}",
)
def runner(bucket: str,
    manifest_path: str,
    runner: str,
    chunk_size: int,
):
    """CCDI Pipeline to Upload files to GDC

    Args:
        bucket (str): Bucket name of where the manifest is located in and the response output goes to
        file_path (str): File path of the CCDI file manifest in bucket
        runner (str): Unique runner name
        chunk_size (str): Integer for the number of files that should be merged at a time
    """
    
    runner_logger = get_run_logger()

    dt = get_time()

    # download manifest file
    file_dl(bucket, manifest_path)

    # read in manifest file
    file_metadata = read_input(manifest_path)

    # mkdir for outputs
    
    os.mkdir(f"VCF_merge_{chunk_size}_{dt}")

    runner_logger.info(">>> Running VCF_MERGE.py ....")

    for chunk in range(0, len(file_metadata), chunk_size):
        runner_logger.info(f"Working on chunk {round(chunk/chunk_size)+1} of {len(range(0, len(file_metadata), chunk_size))}...")

        # download VCFs that need to be merged
        runner_logger.info(f"Downloading VCFs in chunk {round(chunk/chunk_size)+1}")
        download_handler(file_metadata[chunk:chunk+chunk_size])

        # merge VCFs
        runner_logger.info(f"Merging VCFs in chunk {round(chunk/chunk_size)+1}...")
        vcf_to_merge, not_merged, merged_vcf = merging(file_metadata[chunk:chunk+chunk_size])

        # save merged file and log files
        runner_logger.info(f"Saving merged VCFs and info for chunk {round(chunk/chunk_size)+1}")
        with open(f"VCF_merge_{chunk_size}_{dt}/vcfs_merged_in_{chunk}.txt", "w+") as w:
            w.write("\n".join(vcf_to_merge))
        w.close()

        if len(not_merged) > 0:
            with open(f"VCF_merge_{chunk_size}_{dt}/vcf_not_merged_{chunk}.txt", "w+") as w:
                w.write("\n".join(not_merged))
            w.close()

        merged_vcf.to_csv(f"VCF_merge_{chunk_size}_{dt}/merged_file_{chunk}.vcf", sep="\t", index=False)


        # delete VCFs
        runner_logger.info(f"Removing constituent VCFs in chunk {round(chunk/chunk_size)+1}")
        delete_handler(file_metadata[chunk:chunk+chunk_size])


    # dl folder to somewhere else
    folder_ul(
        local_folder=f"VCF_merge_file_upload_{chunk_size}_{dt}",
        bucket=bucket,
        destination=runner + "/",
        sub_folder="",
    )