""" Script to convert VCF files to MAF files and perform annotations with VEP """

##############
#
# Env. Setup
#
##############

import json
import requests
import os
import sys
import subprocess
import pandas as pd

# prefect dependencies
import boto3
from botocore.exceptions import ClientError
from prefect import flow, get_run_logger
from src.utils import get_time, file_dl, folder_ul

@flow(
    name="vcf2maf_env_setup",
    log_prints=True,
    flow_run_name="vcf2maf_env_setup_" + f"{get_time()}",
)
def env_setup():
    """Set up utils on VM"""

    runner_logger = get_run_logger()

    for package in ["libz-dev", "liblzma-dev", "libbz2-dev", "curl"]:
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

@flow(
    name="vcf2maf_samtools_setup",
    log_prints=True,
    flow_run_name="vcf2maf_samtools_setup_" + f"{get_time()}",
)
def samtools_setup(bucket: str, samtools_path: str):
    """Download and install samtools to VM"""

    runner_logger = get_run_logger()
    
    #download samtools package
    file_dl(bucket, samtools_path)

    f_name = os.path.basename(samtools_path)

    #untar 
    process = subprocess.Popen(
        ["tar", "-xvjf", f_name],
        shell=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    std_out, std_err = process.communicate()

    runner_logger.info(
        f"tar -xvjf {f_name} results: OUT: {std_out}, ERR: {std_err}"
    )

    #config
    os.chdir(f_name.replace(".tar.bz2", ""))

    process = subprocess.Popen(
        ["./configure"],
        shell=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    std_out, std_err = process.communicate()

    runner_logger.info(
        f"tar -xvjf {f_name} results: OUT: {std_out}, ERR: {std_err}"
    )

    process = subprocess.Popen(
        [
            "./configure",
            "--prefix=/opt/prefect/ChildhoodCancerDataInitiative-Prefect_Pipeline-CBIO-61_VCF2MAF",
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

    if "samtools" in os.listdir("bin/"):  # heuristic for confirming install
        return "successfully installed samtools"
    else:
        return "samtools package not installed correctly"


# htslib >> just add path DYLD_LIBRARY_PATH=/Users/bullenca/Work/Repos/ensembl-vep/htslib etv
    # perl vcf2maf.pl --input-vcf tests/test.vcf --output-maf tests/test.vep.maf --samtools-exec ~/bin --tabix-exec ~/bin
#PERL?
# VEP
# actuial VCF2MAF package
## file_dl
## unzip
## a

@flow(
    name="GDC File Upload",
    log_prints=True,
    flow_run_name="{runner}_" + f"{get_time()}",
)
def runner(
    bucket: str,
    vcf_file_path: str,
    vcf2maf_package_path: str,
    samtools_path: str,
    runner: str,
):
    runner_logger = get_run_logger()

    runner_logger.info(">>> Running vcf2maf_conversion.py ....")

    dt = get_time()

    os.mkdir(f"vcf2maf_output_{dt}")

    #do env setup
    env_setup()

    ##download vcf file to convert package locally
    #file_dl(bucket, vcf_file_path)

    #samtools setup
    runner_logger.info(">>> Installing samtools ....")

    runner_logger.info(samtools_setup(bucket, samtools_path))
    
    #download vcf2maf package locally
    file_dl(bucket, vcf2maf_package_path)
    
    #do vcf2maf setup

    #check if perl installed

    


