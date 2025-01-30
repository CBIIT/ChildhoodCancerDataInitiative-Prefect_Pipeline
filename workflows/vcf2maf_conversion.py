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
from prefect_shell import ShellOperation
from typing import Literal

# prefect dependencies
import boto3
from botocore.exceptions import ClientError
from prefect import flow, get_run_logger
from src.utils import get_time, file_dl, folder_ul


@flow(
    name="vcf2maf_dl_conda_setup",
    log_prints=True,
    flow_run_name="vcf2maf_env_setup_" + f"{get_time()}",
)
def dl_conda_setup():
    """Set up utils on VM"""

    runner_logger = get_run_logger()

    runner_logger.info(ShellOperation(commands=[
        "apt update",
        "apt-get -y install curl wget",
        "mkdir /opt/prefect/ChildhoodCancerDataInitiative-Prefect_Pipeline-CBIO-61_VCF2MAF/miniconda3",
        "wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /opt/prefect/ChildhoodCancerDataInitiative-Prefect_Pipeline-CBIO-61_VCF2MAF/miniconda3/miniconda.sh", 
        "bash /opt/prefect/ChildhoodCancerDataInitiative-Prefect_Pipeline-CBIO-61_VCF2MAF/miniconda3/miniconda.sh -b -u -p /opt/prefect/ChildhoodCancerDataInitiative-Prefect_Pipeline-CBIO-61_VCF2MAF/miniconda3", 
        "rm /opt/prefect/ChildhoodCancerDataInitiative-Prefect_Pipeline-CBIO-61_VCF2MAF/miniconda3/miniconda.sh",
    ]).run())

    return None

@flow(
    name="vcf2maf_env_setup",
    log_prints=True,
    flow_run_name="vcf2maf_env_setup_" + f"{get_time()}",
)
def env_setup():
    """Set up utils on VM"""

    runner_logger = get_run_logger()

    runner_logger.info(ShellOperation(commands=[
        "source /opt/prefect/ChildhoodCancerDataInitiative-Prefect_Pipeline-CBIO-61_VCF2MAF/miniconda3/bin/activate",
        "conda init --all",
        "conda -V",
        "conda create -n vcf2maf_38 python=3.7 --yes",
        "conda activate vcf2maf_38",
        "conda install -y  -c bioconda vcf2maf", 
        "conda install -y  -c bioconda ensembl-vep",
        "conda install -y  -c bioconda samtools",
    ]).run())

    return None

@flow(
    name="vcf2maf_env_check",
    log_prints=True,
    flow_run_name="vcf2maf_env_check_" + f"{get_time()}",
)
def env_check():
    """Check that conda packages installed correctly"""

    runner_logger = get_run_logger()

    runner_logger.info(ShellOperation(commands=[
        "source /opt/prefect/ChildhoodCancerDataInitiative-Prefect_Pipeline-CBIO-61_VCF2MAF/miniconda3/bin/activate",
        "conda init --all",
        "conda activate vcf2maf_38",
        "samtools --version",
        "vep --help"
    ]).run())

    return None

@flow(
    name="vcf2maf_vep_setup",
    log_prints=True,
    flow_run_name="vcf2maf_vep_setup_" + f"{get_time()}",
)
def vep_setup(vep_path: str):
    """Setup VEP env params and indexes"""

    runner_logger = get_run_logger()

    runner_logger.info(ShellOperation(commands=[
        "source /opt/prefect/ChildhoodCancerDataInitiative-Prefect_Pipeline-CBIO-61_VCF2MAF/miniconda3/bin/activate",
        "conda init --all",
        "conda activate vcf2maf_38",
        f"export VEP_PATH={vep_path}",
        #"export DYLD_LIBRARY_PATH=",
        "mkdir $VEP_PATH",
        "cd $VEP_PATH",
        "curl -O ftp://ftp.ensembl.org/pub/release-112/variation/indexed_vep_cache/homo_sapiens_vep_112_GRCh38.tar.gz",
        "ls -lh",
        "tar -zxvf homo_sapiens_vep_112_GRCh38.tar.gz",
        #"vep_install -a cf -s homo_sapiens -y GRCh38 -c $VEP_PATH --CONVERT --no_update", 
        #"ls -lh",
    ]).run())

    return None

##### BWA install here 
@flow(
    name="vcf2maf_bwa_setup",
    log_prints=True,
    flow_run_name="vcf2maf_bwa_setup_" + f"{get_time()}",
)
def bwa_setup(bucket, bwa_tarball):
    """Setup reference genome files needed by VEP"""
    
    runner_logger = get_run_logger()

    file_dl(bucket, bwa_tarball)

    f_name = os.path.basename(bwa_tarball)

    runner_logger.info(ShellOperation(commands=[
        f"tar -xvjf {f_name}",
        "bwa-0.7.17/bwakit/run-gen-ref hs38DH",
        "source /opt/prefect/ChildhoodCancerDataInitiative-Prefect_Pipeline-CBIO-61_VCF2MAF/miniconda3/bin/activate",
        "conda init --all",
        "conda activate vcf2maf_38",
        "samtools faidx hs38DH.fa",
    ]).run())

def conversion():
        """process = subprocess.Popen(
        [
            "perl",
            "vcf2maf.pl",
            "--input-vcf",
            "tests/test.vcf",
            "--output-maf",
            "tests/test.vep.maf",
            "--samtools-exec",
            "/opt/prefect/ChildhoodCancerDataInitiative-Prefect_Pipeline-CBIO-61_VCF2MAF/bin",
            "--tabix-exec",
            "/opt/prefect/ChildhoodCancerDataInitiative-Prefect_Pipeline-CBIO-61_VCF2MAF/bin",
        ],
        shell=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    std_out, std_err = process.communicate()

    runner_logger.info(
        f"perl vcf2maf.pl --input-vcf tests/test.vcf --output-maf tests/test.vep.maf --samtools-exec ~/bin --tabix-exec ~/bin results: OUT: {std_out}, ERR: {std_err}"
    )"""

DropDownChoices = Literal["env_setup", "convert", "env_tear_down"]

@flow(
    name="VCF2MAF Conversion",
    log_prints=True,
    flow_run_name="{runner}_" + f"{get_time()}",
)
def runner(
    bucket: str,
    runner: str,
    process_type: DropDownChoices,
    vcf_manifest_path: str,
    barcode_manifest_path: str,
    bwa_tarball_path: str,
):
    """VCF2MAF Conversion

    Args:
        bucket (str): Bucket name of where the manifest etc. is located in and the output goes to
        runner (str): Unique runner name
        process_type (str): Whether to setup env, perform vcf22maf conversion or tear down env
        vcf_manifest_path (str): Path to manifest with s3 URLs of VCF files to convert
        barcode_manifest_path (str): Path to manifest with tumor/normal sample barcodes
        bwa_tarball_path (str): Path to bwakit tarball for ref seq installation

    Raises:
        ValueError: Value Error occurs when the pipeline fails to proceed.
    """

    runner_logger = get_run_logger()

    runner_logger.info(">>> Running vcf2maf_conversion.py ....")

    dt = get_time()

    os.mkdir(f"vcf2maf_output_{dt}")

    vep_path = "/usr/local/data/vep"

    if process_type == "env_setup":
        # do env setup
        runner_logger.info(">>> Testing env setup ....")
        dl_conda_setup()
        env_setup()
        env_check()
        vep_setup(vep_path)
        bwa_setup(bucket, bwa_tarball_path)

        # check that VEP indexes installed
        runner_logger.info(ShellOperation(commands=[
            "ls -lh /usr/local/data/vep/homo_sapiens/112_GRCh38/",
            "ls -lh /usr/local/data/vep/homo_sapiens/105_GRCh38/"
        ]).run())

    elif process_type == "convert":
        pass
        # download vcf manifest

        #download barcode manifest

    elif process_type == "env_tear_down":
        pass

    # test vcf2maf


