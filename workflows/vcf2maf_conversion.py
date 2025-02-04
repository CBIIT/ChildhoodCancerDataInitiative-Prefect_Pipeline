""" Script to convert VCF files to MAF files and perform annotations with VEP 

Use conda package manager and bioconda install channel to install vcf2maf and
its dependency VEP (variant effect predictor) that performs VCF annotations

"""

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
import shutil
from prefect_shell import ShellOperation
from typing import Literal

# prefect dependencies
import boto3
from botocore.exceptions import ClientError
from prefect import flow, get_run_logger
from src.utils import get_time, file_dl, folder_ul

# cancellation handling
from prefect.states import State
import logging



@flow(
    name="vcf2maf_dl_conda_setup",
    log_prints=True,
    flow_run_name="vcf2maf_env_setup_" + f"{get_time()}",
)
def dl_conda_setup(install_path: str):
    """Set up conda on VM at install path

    Args:
        install_path (str): path on VM to install conda setup files

    Returns:
        None
    """

    runner_logger = get_run_logger()

    runner_logger.info(
        ShellOperation(
            commands=[
                "apt update",
                "apt-get -y install curl wget",
                f"mkdir -p {install_path}/miniconda3",
                f"wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O {install_path}/miniconda3/miniconda.sh",
                f"bash {install_path}/miniconda3/miniconda.sh -b -u -p {install_path}/miniconda3",
                f"rm {install_path}/miniconda3/miniconda.sh",
            ]
        ).run()
    )

    return None


@flow(
    name="vcf2maf_env_setup",
    log_prints=True,
    flow_run_name="vcf2maf_env_setup_" + f"{get_time()}",
)
def env_setup(install_path: str):
    """Set up conda env on VM

    Args:
        install_path (str): path on VM to start conda and install vcf2maf

    Returns:
        None

    """

    runner_logger = get_run_logger()

    runner_logger.info(
        ShellOperation(
            commands=[
                f"source {install_path}/miniconda3/bin/activate",
                "conda init --all",
                "conda -V",
                "conda create -n vcf2maf_38 python=3.7 --yes",
                "conda activate vcf2maf_38",
                "conda install -y  -c bioconda vcf2maf",
                "conda install -y  -c bioconda ensembl-vep",
                "conda install -y  -c bioconda samtools",
            ]
        ).run()
    )

    return None


@flow(
    name="vcf2maf_env_check",
    log_prints=True,
    flow_run_name="vcf2maf_env_check_" + f"{get_time()}",
)
def env_check(install_path: str):
    """Check that conda packages installed correctly
    
    Args:
        install_path (str): path on VM to start conda

    Returns:
        None
    
    """

    runner_logger = get_run_logger()

    runner_logger.info(
        ShellOperation(
            commands=[
                f"source {install_path}/miniconda3/bin/activate",
                "conda init --all",
                "conda activate vcf2maf_38",
                "samtools --version",
                "vep --help",
            ]
        ).run()
    )

    return None


@flow(
    name="vcf2maf_vep_setup",
    log_prints=True,
    flow_run_name="vcf2maf_vep_setup_" + f"{get_time()}",
)
def vep_setup(install_path: str):
    """Setup VEP env params and indexes
    
    Args:
        install_path (str): path on VM to start conda and install VEP cache indexes

    Returns:
        None
    
    """

    runner_logger = get_run_logger()

    runner_logger.info(
        ShellOperation(
            commands=[
                f"source {install_path}/miniconda3/bin/activate",
                "conda init --all",
                "conda activate vcf2maf_38",
                f"export VEP_PATH={install_path}/vep",
                # "export DYLD_LIBRARY_PATH=",
                "mkdir $VEP_PATH",
                "cd $VEP_PATH",
                "curl -O ftp://ftp.ensembl.org/pub/release-105/variation/indexed_vep_cache/homo_sapiens_vep_105_GRCh38.tar.gz",
                "ls -lh",
                "tar -zxvf homo_sapiens_vep_105_GRCh38.tar.gz",
                # "vep_install -a cf -s homo_sapiens -y GRCh38 -c $VEP_PATH --CONVERT --no_update",
                # "ls -lh",
            ]
        ).run()
    )

    return None


##### BWA install here
@flow(
    name="vcf2maf_bwa_setup",
    log_prints=True,
    flow_run_name="vcf2maf_bwa_setup_" + f"{get_time()}",
)
def bwa_setup(bucket, bwa_tarball, install_path):
    """Setup reference genome files needed by VEP

    Args:
        bucket (str): s3 bucket that bwa tarball is stored at
        bwa_tarball (str): path to bwa tarball used for installation
        install_path (str): path on VM to start conda

    Returns:
        None

    """

    runner_logger = get_run_logger()

    # go to installation path
    os.chdir(install_path)

    # download bwa tarball
    file_dl(bucket, bwa_tarball)

    # extract file name from file path
    f_name = os.path.basename(bwa_tarball)

    # run installation commands 
    runner_logger.info(
        ShellOperation(
            commands=[
                f"tar -xvjf {f_name}",
                f"{f_name.replace('.tar.bz2', '')}/bwakit/run-gen-ref hs38DH",
                f"source {install_path}/miniconda3/bin/activate",
                "conda init --all",
                "conda activate vcf2maf_38",
                "samtools faidx hs38DH.fa",
            ]
        ).run()
    )

    ##### BWA install here


@flow(
    name="vcf2maf_bcftools_setup",
    log_prints=True,
    flow_run_name="vcf2maf_bcftools_setup_" + f"{get_time()}",
)
def bcftools_setup(install_path):
    """Setup reference genome files needed by VEP
    
    Args:
        install_path (str): path on VM to install bcftools

    Returns:
        None
    
    """

    runner_logger = get_run_logger()

    # go to installation path
    os.chdir(install_path)

    # run installation commands
    runner_logger.info(
        ShellOperation(
            commands=[
                "apt update",
                "apt-get -y install libz-dev liblzma-dev libbz2-dev libcurl4-gnutls-dev",
                "git clone --recurse-submodules https://github.com/samtools/htslib.git",
                "git clone https://github.com/samtools/bcftools.git",
                "cd bcftools",
                "make",
            ]
        ).run()
    )

def cancellation_hook(flow, flow_run, state):
    logging.Logger(f"THE RUN WAS CANCELLED")


def crashed_hook(flow, flow_run, state):
    runner_logger = get_run_logger()
    runner_logger.info(f"THE RUN CRASHED")

def read_input(file_path: str):
    """Read in file with s3 URLs of VCFs to merge and participant IDs

    Args:
        file_path (str): path to input file that contains required cols

    Returns:
        pd.DataFrame: DataFrame with extracted necessary metadata
    """

    runner_logger = get_run_logger()

    # extract file name from file path
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

    if "tumor_sample_id" not in file_metadata.columns:
        runner_logger.error(
            f"Error reading and parsing file {f_name}: no column named 'tumor_sample_id'."
        )
        sys.exit(1)

    if "normal_sample_id" not in file_metadata.columns:
        runner_logger.error(
            f"Error reading and parsing file {f_name}: no column named 'normal_sample_id'."
        )
        sys.exit(1)

    if "File Name" not in file_metadata.columns:
        runner_logger.error(
            f"Error reading and parsing file {f_name}: no column named 'File Name'."
        )
        sys.exit(1)

    if len(file_metadata) == 0:
        runner_logger.error(f"Error reading and parsing file {f_name}; empty file")
        sys.exit(1)

    return file_metadata


@flow(
    name="vcf2maf_convert_vcf",
    log_prints=True,
    flow_run_name="vcf2maf_convert_vcf_" + f"{get_time()}",
)
def converter(
    row: pd.Series, install_path: str, output_dir: str, working_path: str
):
    """Function to handle downloading VCF files and generating index files

    Args:
        row (pd.Series): pandas Series of a row entry from manifest
        install_path (str): path to where conda libs are installed, env to activate
        output_dir (str): path to move maf files to
        working_path (str): path where work done and temp files stored

    Returns:
        patient_id, tumor_sample_id and True/False depending on success of vcf annotation and maf conversion
    """

    runner_logger = get_run_logger()

    # for index, row in df.iterrows():
    f_bucket = row["s3_url"].split("/")[2]
    f_path = "/".join(row["s3_url"].split("/")[3:])

    # make dir for this VCF file conversion
    ShellOperation(
        commands=[
            f"mkdir {row['patient_id']}",
        ]
    ).run()

    # cd into temp directory for VCF
    os.chdir(f"{row['patient_id']}")

    # download VCF file
    file_dl(f_bucket, f_path)

    # extract file name
    f_name = os.path.basename(f_path)

    # check that file exists
    if not os.path.isfile(f_name):
        runner_logger.error(
            f"File {f_name} not copied over or found from URL {row['s3_url']}"
        )
    else:
        # setup sample barcode renaming
        temp_sample = [row["normal_sample_id"], row["tumor_sample_id"]]
        with open("sample.txt", "w+") as w:
            w.write("\n".join(temp_sample))
        w.close()

        # run commands to activate conda
        # run bcftools reheader for tumor/normal samples
        # unzip file
        # and then run vcf2maf w/ vep annotation
        runner_logger.info(
            ShellOperation(
                commands=[
                    f"source {install_path}/miniconda3/bin/activate",
                    "conda init --all",
                    "conda activate vcf2maf_38",
                    f"{install_path}/bcftools/bcftools reheader -s sample.txt -o {f_name.replace('vcf.gz', 'reheader.vcf.gz')} {f_name}",
                    f"bgzip -d {f_name.replace('vcf.gz', 'reheader.vcf.gz')}",
                    f"timeout 1200 vcf2maf.pl --input-vcf {f_name.replace('vcf.gz', 'reheader.vcf')} --output-maf {f_name.replace('vcf.gz', 'reheader.vcf.vep.maf')} --ref-fasta {install_path}/hs38DH.fa --vep-path {install_path}/miniconda3/envs/vcf2maf_38/bin --vep-data {install_path}/vep --ncbi-build GRCh38 --tumor-id {row['tumor_sample_id']}  --normal-id {row['normal_sample_id']}",
                    "ls -l",  # confirm all files produced
                ]
            ).run()
        )

        if f"{f_name.replace('vcf.gz', 'reheader.vcf.vep.maf')}" in os.listdir("."):
            # rename and move file to output directory
            # and rename file from *reheader.vcf.gz.vep.maf to .vcf.vep.maf
            os.rename(
                f"{f_name.replace('vcf.gz', 'reheader.vcf.vep.maf')}",
                output_dir
                + "/"
                + f"{f_name.replace('vcf.gz', 'reheader.vcf.vep.maf')}",
            )

            # if *vep.vcf_warnings.txt produced, copy over also for log info
            if (
                f"{f_name.replace('vcf.gz', 'reheader.vep.vcf_warnings.txt')}"
                in os.listdir(".")
            ):
                os.rename(
                    f"{f_name.replace('vcf.gz', 'reheader.vep.vcf_warnings.txt')}",
                    output_dir
                    + "/"
                    + f"{f_name.replace('vcf.gz', 'reheader.vep.vcf_warnings.txt')}",
                )

            # remove temp dir and intermediate files
            os.chdir(working_path)

            ShellOperation(commands=[f"rm -r {row['patient_id']}"]).run()

            return [row["patient_id"], row["tumor_sample_id"], True]

        else:
            runner_logger.error(
                f"Something went wrong, MAF file from {f_name} not produced"
            )

            # remove temp dir and intermediate files
            os.chdir(working_path)

            ShellOperation(commands=[f"rm -r {row['patient_id']}"]).run()

            return [row["patient_id"], row["tumor_sample_id"], False]


@flow(
    name="vcf2maf_convert_handler",
    log_prints=True,
    flow_run_name="vcf2maf_convert_handler_" + f"{get_time()}",
)
def conversion_handler(
    dt: str, bucket: str, runner_path: str, manifest_path: str, install_path: str
):
    """_summary_

    Args:
        dt (str): date time of workflow run
        bucket (str): bucket to download and upload files from/to
        runner_path (str): path of upload/downloads
        manifest_path (str): path of manifest in bucket
        install_path (str): install path to activate env

    Returns: 
        None
    """

    runner_logger = get_run_logger()

    output_dir = f"/usr/local/data/vcf2maf_output_{dt}"

    if not os.path.exists(output_dir):
        ShellOperation(
            commands=[f"mkdir {output_dir}"],
        ).run()

    runner_logger.info(">>> Performing VCF annotation and conversion to MAF ....")

    conversion_recording = []

    working_path = f"/usr/local/data/vcf2maf_working_{dt}"

    if not os.path.exists(working_path):
        ShellOperation(
            commands=[f"mkdir {working_path}"],
        ).run()

    os.chdir(working_path)

    # download manifest
    file_dl(bucket, manifest_path)

    mani = os.path.basename(manifest_path)

    df = read_input(mani)

    ## TESTING
    df_test = df[33:34].reset_index()

    for index, row in df_test.iterrows():  ##TESTING
        # for index, row in df.iterrows():
        runner_logger.info(f"Attempting annotation and conversion of file {index+1} of {len(df_test)}")
        try:
            os.chdir(working_path)
            conversion_recording.append(
                converter(row, install_path, output_dir, working_path)
            )
            runner_logger.info(f"Annotation and conversion of {row['patient_id']}'s VCF file {row['File Name']} complete")
        except Exception as e:
            runner_logger.error(
                f"Error with {row['patient_id']}'s VCF file {row['File Name']}, f{e}"
            )
            conversion_recording.append(
                [row["patient_id"], row["tumor_sample_id"], False]
            )

    pd.DataFrame(
        conversion_recording,
        columns=["patient_id", "tumor_sample_id", "converted?"],
    ).to_csv(f"{output_dir}/conversion_summary.tsv", sep="\t", index=False)

    # dl folder to somewhere else
    folder_ul(
        local_folder=output_dir,
        bucket=bucket,
        destination=runner_path + "/",
        sub_folder="",
    )

    # remove working path of intermediate files to free up space
    runner_logger.info(
        ShellOperation(
            commands=[
                f"rm -r {working_path}",
            ]
        ).run()
    )

    # remove output path to free up space after download to cloud storage
    runner_logger.info(
        ShellOperation(
            commands=[
                f"rm -r {output_dir}",
            ]
        ).run()
    )

    return None


DropDownChoices = Literal["env_setup", "convert", "env_tear_down"]


@flow(
    name="VCF2MAF Conversion",
    log_prints=True,
    flow_run_name="{runner_path}_" + f"{get_time()}",
    on_cancellation=[cancellation_hook],
    on_crashed=[crashed_hook],
)
def runner(
    bucket: str,
    runner_path: str,
    process_type: DropDownChoices,
    manifest_path: str,
    bwa_tarball_path: str,
):
    """VCF2MAF Conversion

    Args:
        bucket (str): Bucket name of where the manifest etc. is located in and the output goes to
        runner_path (str): Unique runner name
        process_type (str): Whether to setup env, perform vcf22maf conversion or tear down env
        manifest_path (str): Path to tab-delimited manifest with s3 URLs of VCF files to convert and tumor/normal sample barcodes
        bwa_tarball_path (str): Path to bwakit tarball for ref seq installation

    Raises:
        ValueError: Value Error occurs when the pipeline fails to proceed.
    """

    runner_logger = get_run_logger()

    runner_logger.info(">>> Running vcf2maf_conversion.py ....")

    dt = get_time()

    # only update before installation,
    # conversion process depends on this path for locating installed software
    install_path = "/usr/local/data/vcf2maf"

    if process_type == "env_setup":

        # do env setup
        runner_logger.info(">>> Conda and env setup ....")
        dl_conda_setup(install_path)
        env_setup(install_path)
        env_check(install_path)
        vep_setup(install_path)
        bwa_setup(bucket, bwa_tarball_path, install_path)
        bcftools_setup(install_path)

        # check that VEP indexes installed
        runner_logger.info(
            ShellOperation(
                commands=[
                    f"ls -lh {install_path}/vep/homo_sapiens/105_GRCh38/",
                ]
            ).run()
        )

        # check that BWA installed
        runner_logger.info(
            ShellOperation(
                commands=[
                    f"ls -lh {install_path}",
                ]
            ).run()
        )

        # check that bcftools installed
        runner_logger.info(
            ShellOperation(
                commands=[
                    f"ls -lh {install_path}/bcftools",
                ]
            ).run()
        )

    elif (
        process_type == "convert"
    ):  # annotate and convert a list of VCF files from manifest file to MAF

        conversion_handler(dt, bucket, runner_path, manifest_path, install_path)

    elif process_type == "env_tear_down":

        runner_logger.info(">>> Tear down env setup ....")

        runner_logger.info(  # TODO: removal steps of installed data
            ShellOperation(
                commands=[
                    # f"rm -rf {install_path}",
                    # f"rm -rf {working_path}",
                    # "rm -rf OUTPUTs"
                    ## TESTING
                    f"echo {install_path}",
                    f"ls -lh {install_path}",
                    "rm -r /usr/local/data/vcf2maf_output_*",
                    "rm -r /usr/local/data/vcf2maf_working_*",
                ]
            ).run()
        )

    # confirm everything gone
    runner_logger.info(
        ShellOperation(
            commands=[
                f"ls -lh /usr/local/data/",
            ]
        ).run()
    )
