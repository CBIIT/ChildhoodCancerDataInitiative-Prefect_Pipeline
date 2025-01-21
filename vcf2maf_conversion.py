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

# inputs?
    # path to VCF file
    # path to vcf2maf tool
    # path to samtools... bcftools? 
# download and install dependencies

# curl
# apt-get install liblzma-dev
# htslib >> just add path DYLD_LIBRARY_PATH=/Users/bullenca/Work/Repos/ensembl-vep/htslib etv
# samtools? 
    # copy and tar -xvjf samtools-1.21.tar.bz2 
    # cd samtools
    # ./configure
    # make
    # make install
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
    vcf2maf_path: str,
    samtools_path: str,
    runner: str,
):
    runner_logger = get_run_logger()

    runner_logger = get_run_logger("")

    env_setup()
