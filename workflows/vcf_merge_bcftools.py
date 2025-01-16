import os
import sys
import requests
import subprocess
import socket

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
        s.connect(("10.254.254.254", 1))
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
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
def bcftools_install(bucket: str, file_path: str):
    """Install bcftools and dependencies on Prefect VM

    Args:
        bucket (str): bucket name
        file_path (str): path to file

    Returns:
        str: Test message confirming installation
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

    process = subprocess.Popen(
        ["tar", "-xf", f_name],
        shell=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    std_out, std_err = process.communicate()

    runner_logger.info(f"Untar results: OUT: {std_out}, ERR: {std_err}")

    os.chdir(f_name.replace(".tar.bz2", ""))

    process = subprocess.Popen(
        ["apt", "update"],
        shell=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    std_out, std_err = process.communicate()

    runner_logger.info(f"apt update results: OUT: {std_out}, ERR: {std_err}")

    for package in ["libz-dev", "liblzma-dev", "libbz2-dev", "libcurl4-gnutls-dev"]:
        process = subprocess.Popen(
            ["apt-get", "-y", "install", package],
            shell=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        std_out, std_err = process.communicate()
        runner_logger.info(
            f"apt install {package} results: OUT: {std_out}, ERR: {std_err}"
        )

    process = subprocess.Popen(
        [
            "./configure",
            "--prefix=/opt/prefect/ChildhoodCancerDataInitiative-Prefect_Pipeline-CBIO-53_bcftools",
        ],
        shell=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    std_out, std_err = process.communicate()

    runner_logger.info(f"Configure results: OUT: {std_out}, ERR: {std_err}")

    process = subprocess.Popen(
        ["make"],
        shell=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    std_out, std_err = process.communicate()

    runner_logger.info(f"Make results: OUT: {std_out}, ERR: {std_err}")

    process = subprocess.Popen(
        ["make", "install"],
        shell=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    std_out, std_err = process.communicate()

    runner_logger.info(f"Make install results: OUT: {std_out}, ERR: {std_err}")

    os.chdir("..")

    if "bcftools" in os.listdir("bin/"):  # heuristic for confirming install
        return "successfully installed bcftools"
    else:
        return "bcftools package not installed correctly"


@flow(
    name="vcf_merge_install_htslib",
    log_prints=True,
    flow_run_name="vcf_merge_install_htslib_" + f"{get_time()}",
)
def htslib_install(
    bucket: str, file_path: str
):  # TODO: add in checks for dependency installs
    """Install htslib and dependencies on Prefect VM

    Args:
        bucket (str): bucket name
        file_path (str): path to file

    Returns:
        str: Test message confirming installation
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

    process = subprocess.Popen(
        ["tar", "-xf", f_name],
        shell=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    std_out, std_err = process.communicate()

    runner_logger.info(f"Untar results: OUT: {std_out}, ERR: {std_err}")

    os.chdir(f_name.replace(".tar.bz2", ""))

    process = subprocess.Popen(
        [
            "./configure",
            "--prefix=/opt/prefect/ChildhoodCancerDataInitiative-Prefect_Pipeline-CBIO-53_bcftools",
        ],
        shell=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    std_out, std_err = process.communicate()

    runner_logger.info(f"Configure results: OUT: {std_out}, ERR: {std_err}")

    process = subprocess.Popen(
        ["make"],
        shell=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    std_out, std_err = process.communicate()

    runner_logger.info(f"Make results: OUT: {std_out}, ERR: {std_err}")

    process = subprocess.Popen(
        ["make", "install"],
        shell=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    std_out, std_err = process.communicate()

    runner_logger.info(f"Make install results: OUT: {std_out}, ERR: {std_err}")

    os.chdir("../bin")

    runner_logger.info(f"ls -l bin/ results: OUT: {std_out}, ERR: {std_err}")

    if "bgzip" in os.listdir("."):  # heuristic for confirming install

        return "successfully installed htslib"
    else:

        return "htslib package not installed correctly"


@flow(
    name="vcf_merge_download_vcfs",
    log_prints=True,
    flow_run_name="vcf_merge_download_vcfs_" + f"{get_time()}",
)
def download_handler(df: pd.DataFrame):
    """Function to handle downloading VCF files and generating index files

    Args:
        df (pd.DataFrame): dataframe of entries to file base names to remove

    Returns:
        None
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
            temp_sample = [row["patient_id"] + "_normal", row["sample_id"]]
            with open("sample.txt", "w+") as w:
                w.write("\n".join(temp_sample))
            w.close()

            process = subprocess.Popen(
                [
                    "./bcftools",
                    "reheader",
                    "-s",
                    "sample.txt",
                    "-o",
                    f_name.replace("vcf.gz", "reheader.vcf.gz"),
                    f_name,
                ],
                shell=False,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            std_out, std_err = process.communicate()

            runner_logger.info(
                f"bcftools reheader results: OUT: {std_out}, ERR: {std_err}"
            )

            os.remove("sample.txt")

            process = subprocess.Popen(
                [
                    "./bcftools",
                    "index",
                    "-t",
                    f_name.replace("vcf.gz", "reheader.vcf.gz"),
                ],
                shell=False,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            std_out, std_err = process.communicate()

            runner_logger.info(
                f"./bcftools index results: OUT: {std_out}, ERR: {std_err}"
            )

    return None


@flow(
    name="vcf_merge_remove_temp_vcfs",
    log_prints=True,
    flow_run_name="vcf_merge_remove_temp_vcfs_" + f"{get_time()}",
)
def delete_handler(df: pd.DataFrame):
    """Delete intermediate files used in merged VCF generation

    Args:
        df (pd.DataFrame): dataframe of entries to file base names to remove

    Returns:
        None
    """

    runner_logger = get_run_logger()

    for index, row in df.iterrows():
        f_name = os.path.basename(row["s3_url"])

        # delete original file
        if os.path.exists(f_name):
            os.remove(f_name)
        else:
            runner_logger.warning(f"The file {f_name} does not exist, cannot remove.")

        # delete reheader file
        if os.path.exists(f_name.replace("vcf.gz", "reheader.vcf.gz")):
            os.remove(f_name.replace("vcf.gz", "reheader.vcf.gz"))
        else:
            runner_logger.warning(
                f"The file {f_name.replace('vcf.gz', 'reheader.vcf.gz')} does not exist, cannot remove."
            )

        # delete index file
        if os.path.exists(f_name.replace("vcf.gz", "reheader.vcf.gz") + ".tbi"):
            os.remove(f_name.replace("vcf.gz", "reheader.vcf.gz") + ".tbi")
        else:
            runner_logger.warning(
                f"The file {f_name.replace('vcf.gz', 'reheader.vcf.gz')+'.tbi'} does not exist, cannot remove."
            )

    return None


@flow(
    name="vcf_merge_merge_vcfs",
    log_prints=True,
    flow_run_name="vcf_merge_merge_vcfs_" + f"{get_time()}",
)
def merging(df: pd.DataFrame, chunk: int, directory_save: str):
    """Driver function for checking for file paths and merging files

    Args:
        df (pd.DataFrame): dataframe of entries to merge
        chunk (int): chunk number for processing and recording
        directory_save (str): directory to copy files to for download

    Returns:
        list: file names merged in chunk
        list: file names not merged in chunk due to not being found
    """

    runner_logger = get_run_logger()

    vcf_to_merge = []
    not_merged = []

    # need to log which VCFs produced which merged VCF for accounting purposes
    for index, row in df.iterrows():

        f_name = os.path.basename(row["s3_url"])

        if os.path.exists(f_name.replace("vcf.gz", "reheader.vcf.gz")):
            if os.path.exists(f_name.replace("vcf.gz", "reheader.vcf.gz") + ".tbi"):
                vcf_to_merge.append(f_name.replace("vcf.gz", "reheader.vcf.gz"))
            else:
                not_merged.append(f_name.replace("vcf.gz", "reheader.vcf.gz"))
        else:
            not_merged.append(f_name.replace("vcf.gz", "reheader.vcf.gz"))

    if len(vcf_to_merge) > 1:
        runner_logger.info(f"Running merge of {len(vcf_to_merge)} VCF files....")

        with open(f"vcfs_merged_in_{chunk}.txt", "w+") as w:
            w.write("\n".join(vcf_to_merge))
        w.close()

        process = subprocess.Popen(
            [
                "./bcftools",
                "merge",
                "-l",
                f"vcfs_merged_in_{chunk}.txt",
                "-o",
                f"vcfs_merged_in_{chunk}.vcf.gz",
            ],
            shell=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        std_out, std_err = process.communicate()

        runner_logger.info(
            f"bcftools merge chunk {chunk} results: OUT: {std_out}, ERR: {std_err}"
        )

        process = subprocess.Popen(
            ["mv", f"vcfs_merged_in_{chunk}.vcf.gz", f"../{directory_save}/"],
            shell=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        std_out, std_err = process.communicate()

        process = subprocess.Popen(
            ["mv", f"vcfs_merged_in_{chunk}.txt", f"../{directory_save}/"],
            shell=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        std_out, std_err = process.communicate()

    else:
        runner_logger.error("No VCF files provided to merge.")
        merged_vcf = pd.DataFrame(
            columns=[
                "CHROM",
                "POS",
                "ID",
                "REF",
                "ALT",
                "QUAL",
                "FILTER",
                "INFO",
                "FORMAT",
            ]
        ).to_csv(f"vcfs_merged_in_{chunk}.vcf", sep="\t", index=False)

    return vcf_to_merge, not_merged


DropDownChoices = Literal["yes", "no"]


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

    runner_logger.info(f">>> IP ADDRESS IS: {get_ip()}")

    runner_logger.info(">>> Running VCF_MERGE.py ....")

    # download manifest file

    runner_logger.info(">>> Downloading manifest file ....")

    file_dl(bucket, manifest_path)

    # read in manifest file
    file_metadata = read_input(manifest_path)

    # mkdir for outputs

    os.mkdir(f"VCF_merge_{chunk_size}_{dt}")

    not_merged_total = []  # record first pass merged VCFs

    runner_logger.info(">>> Installing bcftools ....")

    runner_logger.info(bcftools_install(bucket, bcftools_path))

    runner_logger.info(">>> Installing htslib ....")

    runner_logger.info(htslib_install(bucket, htslib_path))

    # for chunk in range(0, len(file_metadata), chunk_size):
    for chunk in range(0, 200, chunk_size):
        runner_logger.info(
            f"Working on chunk {round(chunk/chunk_size)+1} of {len(range(0, len(file_metadata), chunk_size))}..."
        )

        # download VCFs that need to be merged
        runner_logger.info(f"Downloading VCFs in chunk {round(chunk/chunk_size)+1}")
        download_handler(file_metadata[chunk : chunk + chunk_size])

        # merge VCFs
        runner_logger.info(f"Merging VCFs in chunk {round(chunk/chunk_size)+1}...")
        vcf_to_merge, not_merged = merging(
            file_metadata[chunk : chunk + chunk_size],
            round(chunk / chunk_size) + 1,
            f"VCF_merge_{chunk_size}_{dt}",
        )

        not_merged_total += not_merged

        runner_logger.info(f"NOT merged: {not_merged}")

        runner_logger.info(f"merged: {vcf_to_merge}")

        # delete VCFs
        runner_logger.info(
            f"Removing constituent VCFs in chunk {round(chunk/chunk_size)+1}"
        )
        delete_handler(file_metadata[chunk : chunk + chunk_size])

    """if recursive == "yes":
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

    if len(not_merged_total) > 0:
        with open(f"VCF_merge_{chunk_size}_{dt}/vcf_not_merged_{chunk}.txt", "w+") as w:
            w.write("\n".join(not_merged))
        w.close()

    process = subprocess.Popen(
        ["ls", "-l", f"../VCF_merge_{chunk_size}_{dt}"],
        shell=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    std_out, std_err = process.communicate()

    runner_logger.info(f"check results: OUT: {std_out}, ERR: {std_err}")

    # dl folder to somewhere else
    folder_ul(
        local_folder=f"../VCF_merge_{chunk_size}_{dt}",
        bucket=bucket,
        destination=runner + "/",
        sub_folder="",
    )
