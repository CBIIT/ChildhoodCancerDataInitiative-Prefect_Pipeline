import os
import sys
import requests
import subprocess

import pandas as pd
from datetime import datetime
from typing import Literal

import boto3
from botocore.exceptions import ClientError
from prefect import flow, get_run_logger
from src.utils import get_time, file_dl, folder_ul


def get_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(0)
    try:
        # doesn't even have to be reachable
        s.connect(('10.254.254.254', 1))
        ip = s.getsockname()[0]
    except Exception:
        ip = '127.0.0.1'
    finally:
        s.close()
    return ip

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
        runner_logger.error(
            f"Error reading and parsing file {f_name}: no column named 's3_url'."
        )
        sys.exit(1)

    if "patient_id" not in file_metadata.columns:
        runner_logger.error(
            f"Error reading and parsing file {f_name}: no column named 'patient_id'."
        )
        sys.exit(1)

    if "sample_id" not in file_metadata.columns:
        runner_logger.error(
            f"Error reading and parsing file {f_name}: no column named 'sample_id'."
        )
        sys.exit(1)

    if "File Name" not in file_metadata.columns:
        runner_logger.error(
            f"Error reading and parsing file {f_name}: no column named 'sample_id'."
        )
        sys.exit(1)

    if len(file_metadata) == 0:
        runner_logger.error(f"Error reading and parsing file {f_name}; empty file")
        sys.exit(1)

    return file_metadata


@flow(
    name="vcf_merge_install_bcftools",
    log_prints=True,
    flow_run_name="vcf_merge_install_bcftools_" + f"{get_time()}",
)
def bcftools_install(bucket: str, file_path: str): #TODO: add in checks for dependency installs
    """Install bcftools and dependencies on Prefect VM

    Args:
        bucket (str): bucket name
        file_path (str): path to file
    """

    runner_logger = get_run_logger()

    file_dl(bucket, file_path)

    f_name = os.path.basename(file_path)

    # check that file exists
    if not os.path.isfile(f_name):
        runner_logger.error(
            f"File {f_name} not copied over or found from URL {file_path}"
        )
        return "bcftools package not downloaded"
    else:
        pass
    
    process = subprocess.Popen(["tar", "-xf", f_name], shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,)
    
    std_out, std_err = process.communicate()

    runner_logger.info(f"Untar results: OUT: {std_out}, ERR: {std_err}")

    os.chdir(f_name.replace(".tar.bz2", ""))

    process = subprocess.Popen(["apt", "update"], shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,)
    
    std_out, std_err = process.communicate()

    runner_logger.info(f"apt update results: OUT: {std_out}, ERR: {std_err}")

    for package in ["libz-dev", "liblzma-dev", "libbz2-dev", "libcurl4-gnutls-dev"]:
        process = subprocess.Popen(["apt-get", "-y", "install", package], shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,)
        std_out, std_err = process.communicate()
        runner_logger.info(f"apt install {package} results: OUT: {std_out}, ERR: {std_err}")

    
    #runner_logger.info(subprocess.call(["apt", "install", "libbz2-dev"], shell=False))

    process = subprocess.Popen(["./configure", "--prefix=/opt/prefect/ChildhoodCancerDataInitiative-Prefect_Pipeline-CBIO-53_bcftools"], shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,)
    
    std_out, std_err = process.communicate()

    runner_logger.info(f"Configure results: OUT: {std_out}, ERR: {std_err}")

    process = subprocess.Popen(["make"], shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,)
    
    std_out, std_err = process.communicate()

    runner_logger.info(f"Make results: OUT: {std_out}, ERR: {std_err}")

    process = subprocess.Popen(["make", "install"], shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,)
    
    std_out, std_err = process.communicate()

    runner_logger.info(f"Make install results: OUT: {std_out}, ERR: {std_err}")

    #os.chdir("../bin")
    os.chdir("..")

    ###TESTING

    #runner_logger.info("Checking for ./bin/bcftools...")

    #process = subprocess.Popen(["ls", "-l"], shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,)
    
    #std_out, std_err = process.communicate()

    #runner_logger.info(f"ls -l bin/ results: OUT: {std_out}, ERR: {std_err}")

    #runner_logger.info("Testing for bin/bcftools...")

    #process = subprocess.Popen(["./bcftools", "merge"], shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,)
    
    #std_out, std_err = process.communicate()

    #runner_logger.info(f"/bin/bcftools results: OUT: {std_out}, ERR: {std_err}")

    return "successfully installed bcftools"

@flow(
    name="vcf_merge_install_htslib",
    log_prints=True,
    flow_run_name="vcf_merge_install_htslib_" + f"{get_time()}",
)
def htslib_install(bucket: str, file_path: str): #TODO: add in checks for dependency installs
    """Install htslib and dependencies on Prefect VM

    Args:
        bucket (str): bucket name
        file_path (str): path to file
    """

    runner_logger = get_run_logger()

    file_dl(bucket, file_path)

    f_name = os.path.basename(file_path)

    # check that file exists
    if not os.path.isfile(f_name):
        runner_logger.error(
            f"File {f_name} not copied over or found from URL {file_path}"
        )
        return "htslib package not downloaded"
    else:
        pass
    
    process = subprocess.Popen(["tar", "-xf", f_name], shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,)
    
    std_out, std_err = process.communicate()

    runner_logger.info(f"Untar results: OUT: {std_out}, ERR: {std_err}")

    os.chdir(f_name.replace(".tar.bz2", ""))

    """process = subprocess.Popen(["apt", "update"], shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,)
    
    std_out, std_err = process.communicate()

    runner_logger.info(f"apt update results: OUT: {std_out}, ERR: {std_err}")

    for package in ["libz-dev", "liblzma-dev", "libbz2-dev", "libcurl4-gnutls-dev"]:
        process = subprocess.Popen(["apt-get", "-y", "install", package], shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,)
        std_out, std_err = process.communicate()
        runner_logger.info(f"apt install {package} results: OUT: {std_out}, ERR: {std_err}")"""

    
    #runner_logger.info(subprocess.call(["apt", "install", "libbz2-dev"], shell=False))

    process = subprocess.Popen(["./configure", "--prefix=/opt/prefect/ChildhoodCancerDataInitiative-Prefect_Pipeline-CBIO-53_bcftools"], shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,)
    
    std_out, std_err = process.communicate()

    runner_logger.info(f"Configure results: OUT: {std_out}, ERR: {std_err}")

    process = subprocess.Popen(["make"], shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,)
    
    std_out, std_err = process.communicate()

    runner_logger.info(f"Make results: OUT: {std_out}, ERR: {std_err}")

    process = subprocess.Popen(["make", "install"], shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,)
    
    std_out, std_err = process.communicate()

    runner_logger.info(f"Make install results: OUT: {std_out}, ERR: {std_err}")

    os.chdir("../bin")

    os.mkdir("tmp")

    ###TESTING

    runner_logger.info("Checking for ./bin/bcftools...")

    process = subprocess.Popen(["ls", "-l"], shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,)
    
    std_out, std_err = process.communicate()

    runner_logger.info(f"ls -l bin/ results: OUT: {std_out}, ERR: {std_err}")

    #runner_logger.info("Testing for bin/bcftools...")

    #process = subprocess.Popen(["./bcftools", "merge"], shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,)
    
    #std_out, std_err = process.communicate()

    #runner_logger.info(f"/bin/bcftools results: OUT: {std_out}, ERR: {std_err}")

    return "successfully installed htslib"

@flow(
    name="vcf_merge_download_vcfs",
    log_prints=True,
    flow_run_name="vcf_merge_download_vcfs_" + f"{get_time()}",
)
def download_handler(df: pd.DataFrame):
    """Function to handle downloading VCF files"""

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
            temp_sample = [row['patient_id']+"_normal", row['sample_id']]
            with open("sample.txt", "w+") as w:
                w.write("\n".join(temp_sample))
            w.close()

            process = subprocess.Popen(["./bcftools", "reheader", "-s", "sample.txt", "-o", f_name, f_name], shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,)
    
            std_out, std_err = process.communicate()

            runner_logger.info(f"bcftools reheader results: OUT: {std_out}, ERR: {std_err}")

            #testing to confirm reheader
            #process = subprocess.Popen(["./bcftools", "view", f_name], shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,)
    
            #std_out, std_err = process.communicate()

            #runner_logger.info(f"bcftools reheader results in file: OUT: {std_out}, ERR: {std_err}")

            # testing index creation

            #process = subprocess.Popen(["./bgzip", "-d", f_name,], shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,)

            #std_out, std_err = process.communicate()

            #runner_logger.info(f"unzip results: OUT: {std_out}, ERR: {std_err}")

            #process = subprocess.Popen(["./bgzip",  f_name.replace(".gz", "")], shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,)

            #std_out, std_err = process.communicate()

            #runner_logger.info(f"re-compress results: OUT: {std_out}, ERR: {std_err}")

            process = subprocess.Popen(["./bcftools", "sort", "-o", f_name, "-O", "z", f_name, "-T", "/opt/prefect/ChildhoodCancerDataInitiative-Prefect_Pipeline-CBIO-53_bcftools/bin/tmp"], shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,)

            std_out, std_err = process.communicate()

            runner_logger.info(f"sort results: OUT: {std_out}, ERR: {std_err}")

            process = subprocess.Popen(["./bcftools", "index", "-t", f_name], shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,)
    
            std_out, std_err = process.communicate()

            runner_logger.info(f"./bcftools index results: OUT: {std_out}, ERR: {std_err}")

            process = subprocess.Popen(["ls", "-l",], shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,)
    
            std_out, std_err = process.communicate()

            runner_logger.info(f"bcftools reheader results in dir: OUT: {std_out}, ERR: {std_err}")

    return None


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
    name="vcf_merge_merge_vcfs",
    log_prints=True,
    flow_run_name="vcf_merge_merge_vcfs_" + f"{get_time()}",
)
def merging(df: pd.DataFrame):
    runner_logger = get_run_logger()

    vcf_to_merge = []
    not_merged = []





DropDownChoices = Literal["yes", "no"]

#main 

# copyover bcftools
# install bcftools
# copy over files in chunk
# create index files in chunk
# merge files in chunk
# delete files in the chunk

@flow(
    name="VCF Merge bcftools",
    log_prints=True,
    flow_run_name="{runner}_" + f"{get_time()}",
)
def runner(
    bucket: str,
    manifest_path: str,
    bcftools_path: str,
    htslib_path: str,
    runner: str,
    chunk_size: int,
    recursive: DropDownChoices,
):
    """CCDI Pipeline to Upload files to GDC

    Args:
        bucket (str): Bucket name of where the manifest is located in and the response output goes to
        manifest_path (str): File path of the CCDI file manifest in bucket
        bcftools_path (str): File path of location of bcftools binary package
        htslib_path (str): File path of location of htslib binary package
        runner (str): Unique runner name
        chunk_size (str): Integer for the number of files that should be merged at a time
        recursive (str): Whether to perform recursive merging of provided list of VCFs into one VCF
    """

    runner_logger = get_run_logger()

    dt = get_time()
    
    runner_logger.info(">>> Running VCF_MERGE.py ....")

    # download manifest file

    runner_logger.info(">>> Downloading manifest file ....")

    file_dl(bucket, manifest_path)

    # read in manifest file
    file_metadata = read_input(manifest_path)

    # mkdir for outputs

    os.mkdir(f"VCF_merge_{chunk_size}_{dt}")

    first_pass_vcfs = []  # record first pass merged VCFs

    runner_logger.info(">>> Installing bcftools ....")

    runner_logger.info(bcftools_install(bucket, bcftools_path))

    runner_logger.info(">>> Installing htslib ....")

    runner_logger.info(htslib_install(bucket, htslib_path))

    #for chunk in range(0, len(file_metadata), chunk_size):
    for chunk in range(0, 10, chunk_size):
        runner_logger.info(
            f"Working on chunk {round(chunk/chunk_size)+1} of {len(range(0, len(file_metadata), chunk_size))}..."
        )

        # download VCFs that need to be merged
        runner_logger.info(f"Downloading VCFs in chunk {round(chunk/chunk_size)+1}")
        download_handler(file_metadata[chunk : chunk + chunk_size])

        # merge VCFs
        """runner_logger.info(f"Merging VCFs in chunk {round(chunk/chunk_size)+1}...")
        vcf_to_merge, not_merged, merged_vcf = merging(
            file_metadata[chunk : chunk + chunk_size]
        )

        # save merged file and log files
        runner_logger.info(
            f"Saving merged VCFs and info for chunk {round(chunk/chunk_size)+1}"
        )
        with open(f"VCF_merge_{chunk_size}_{dt}/vcfs_merged_in_{chunk}.txt", "w+") as w:
            w.write("\n".join(vcf_to_merge))
        w.close()

        if len(not_merged) > 0:
            with open(
                f"VCF_merge_{chunk_size}_{dt}/vcf_not_merged_{chunk}.txt", "w+"
            ) as w:
                w.write("\n".join(not_merged))
            w.close()

        merged_vcf.to_csv(
            f"VCF_merge_{chunk_size}_{dt}/merged_file_{chunk}.vcf",
            sep="\t",
            index=False,
        )
        first_pass_vcfs.append(f"VCF_merge_{chunk_size}_{dt}/merged_file_{chunk}.vcf")

        # delete VCFs
        runner_logger.info(
            f"Removing constituent VCFs in chunk {round(chunk/chunk_size)+1}"
        )
        delete_handler(file_metadata[chunk : chunk + chunk_size])

    if recursive == "yes":
        runner_logger.info("Merging the first pass merged VCFs together...")

        if len(first_pass_vcfs) > chunk_size:

            pass
            
        else:
            vcf_to_merge_p2, not_merged_p2, merged_vcf_p2 = merging_merged(first_pass_vcfs)

        # save merged file and log files
        runner_logger.info(f"Saving doubly merged VCF and info")
        with open(
            f"VCF_merge_{chunk_size}_{dt}/vcfs_chunks_merged_together.txt", "w+"
        ) as w:
            w.write("\n".join(vcf_to_merge_p2))
        w.close()

        if len(not_merged) > 0:
            with open(
                f"VCF_merge_{chunk_size}_{dt}/vcfs_chunks_NOT_merged_together.txt", "w+"
            ) as w:
                w.write("\n".join(not_merged_p2))
            w.close()

        merged_vcf_p2.to_csv(
            f"VCF_merge_{chunk_size}_{dt}/complete_merged_file_all_chunks.vcf",
            sep="\t",
            index=False,
        )

    else:
        runner_logger.info(
            "Chunks of VCFs merged finished, but chunks not merged together."
        )"""

    # dl folder to somewhere else
    folder_ul(
        local_folder=f"VCF_merge_{chunk_size}_{dt}",
        bucket=bucket,
        destination=runner + "/",
        sub_folder="",
    )