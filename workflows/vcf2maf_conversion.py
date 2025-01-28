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
        "wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-aarch64.sh -O /opt/prefect/ChildhoodCancerDataInitiative-Prefect_Pipeline-CBIO-61_VCF2MAF/miniconda3/miniconda.sh", 
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
        "conda -V"
    ]).run())

    return None

    #file_dl(bucket, miniconda_path)




@flow(
    name="vcf2maf_samtools_setup",
    log_prints=True,
    flow_run_name="vcf2maf_samtools_setup_" + f"{get_time()}",
)
def samtools_setup(bucket: str, samtools_path: str):
    """Download and install samtools to VM"""

    runner_logger = get_run_logger()

    # download samtools package
    file_dl(bucket, samtools_path)

    f_name = os.path.basename(samtools_path)

    # check that file exists
    if not os.path.isfile(f_name):
        runner_logger.error(
            f"File {f_name} not copied over or found from URL {samtools_path}"
        )
        return "samtools package not downloaded"
    else:
        pass

    # untar
    process = subprocess.Popen(
        ["tar", "-xvjf", f_name],
        shell=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    std_out, std_err = process.communicate()

    runner_logger.info(f"tar -xvjf {f_name} results: OUT: {std_out}, ERR: {std_err}")

    # config
    os.chdir(f_name.replace(".tar.bz2", ""))

    process = subprocess.Popen(
        ["./configure"],
        shell=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    std_out, std_err = process.communicate()

    runner_logger.info(f"tar -xvjf {f_name} results: OUT: {std_out}, ERR: {std_err}")

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


@flow(
    name="vcf2maf_vc2maf_setup",
    log_prints=True,
    flow_run_name="vcf2maf_vcf2maf_setup_" + f"{get_time()}",
)
def vcf2maf_setup(bucket: str, vcf2maf_path: str):
    """Download and install vcf2maf"""

    runner_logger = get_run_logger()

    # download samtools package
    file_dl(bucket, vcf2maf_path)

    f_name = os.path.basename(vcf2maf_path)

    # check that file exists
    if not os.path.isfile(f_name):
        runner_logger.error(
            f"File {f_name} not copied over or found from URL {vcf2maf_path}"
        )
        return "vcf2maf package not downloaded"
    else:
        pass

    process = subprocess.Popen(
        ["tar", "-zxf", f_name],
        shell=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    std_out, std_err = process.communicate()

    runner_logger.info(f"Untar results: OUT: {std_out}, ERR: {std_err}")

    dir = [
        i
        for i in os.listdir(".")
        if i.startswith("mskcc-vcf2maf-") and os.path.isdir(i)
    ][0]

    runner_logger.info(os.listdir("."))

    os.chdir(dir)

    process = subprocess.Popen(
        ["perl", "vcf2maf.pl", "--man"],
        shell=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    std_out, std_err = process.communicate()

    runner_logger.info(f"perl vcf2maf.pl --man results: OUT: {std_out}, ERR: {std_err}")

    return "VCF2MAF install complete"


@flow(
    name="vcf2maf_VEP_setup",
    log_prints=True,
    flow_run_name="vcf2maf_VEP_setup_" + f"{get_time()}",
)
def vep_setup():
    """Download and install VEP"""

    runner_logger = get_run_logger()

    os.chdir(
        "/opt/prefect/ChildhoodCancerDataInitiative-Prefect_Pipeline-CBIO-61_VCF2MAF/"
    )

    process = subprocess.Popen(
        ["git", "clone", "https://github.com/Ensembl/ensembl-vep.git"],
        shell=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    std_out, std_err = process.communicate()

    runner_logger.info(f" results: OUT: {std_out}, ERR: {std_err}")

    os.chdir("ensembl-vep")

    # add htslib path
    os.environ["DYLD_LIBRARY_PATH"] = (
        "/opt/prefect/ChildhoodCancerDataInitiative-Prefect_Pipeline-CBIO-61_VCF2MAF/ensembl-vep/htslib"
    )

    runner_logger.info(os.environ["DYLD_LIBRARY_PATH"])

    for package in [
        "DBI",
        "Archive::Zip",
        "Archive::Extract",
        "DBD::mysql",
        "Module::Build",
        "List::MoreUtils",
        "LWP::Simple",
        "Bio::Root::Version"
    ]:
        process = subprocess.Popen(
            ["cpan", package],
            shell=False,
            text=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        std_out, std_err = process.communicate("y\n")

        runner_logger.info(
            f"Perl install results for {package}: OUT: {std_out}, ERR: {std_err}"
        )

    process = subprocess.Popen(
        ["perl", "INSTALL.pl"],
        shell=False,
        text=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    std_out, std_err = process.communicate("y\n")

    return f"vep install results: OUT: {std_out}, ERR: {std_err}"


@flow(
    name="VCF2MAF Conversion",
    log_prints=True,
    flow_run_name="{runner}_" + f"{get_time()}",
)
def runner(
    bucket: str,
    runner: str,
    vcf_file_path: str,
    vcf2maf_package_path: str,
    samtools_path: str,
):
    runner_logger = get_run_logger()

    runner_logger.info(">>> Running vcf2maf_conversion.py ....")

    dt = get_time()

    os.mkdir(f"vcf2maf_output_{dt}")

    # do env setup
    runner_logger.info(">>> Testing env setup ....")
    dl_conda_setup()
    env_setup()

    # download vcf file to convert package locally
    # file_dl(bucket, vcf_file_path)

    # samtools setup
    #runner_logger.info(">>> Installing samtools ....")

    # runner_logger.info(samtools_setup(bucket, samtools_path))

    # vep install
    #runner_logger.info(">>> Installing vep ....")

    #runner_logger.info(vep_setup())

    # vcf2maf setup
    #runner_logger.info(">>> Installing vcf2maf ....")

    # runner_logger.info(vcf2maf_setup(bucket, vcf2maf_package_path))

    # test vcf2maf

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
