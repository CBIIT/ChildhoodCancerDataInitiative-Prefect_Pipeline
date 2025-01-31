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
import shutil
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
def dl_conda_setup(install_path: str):
    """Set up conda on VM at install path"""

    runner_logger = get_run_logger()

    runner_logger.info(ShellOperation(commands=[
        "apt update",
        "apt-get -y install curl wget",
        f"mkdir -p {install_path}/miniconda3",
        f"wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O {install_path}/miniconda3/miniconda.sh", 
        f"bash {install_path}/miniconda3/miniconda.sh -b -u -p {install_path}/miniconda3", 
        f"rm {install_path}/miniconda3/miniconda.sh",
    ]).run())

    return None

@flow(
    name="vcf2maf_env_setup",
    log_prints=True,
    flow_run_name="vcf2maf_env_setup_" + f"{get_time()}",
)
def env_setup(install_path: str):
    """Set up conda env on VM"""

    runner_logger = get_run_logger()

    runner_logger.info(ShellOperation(commands=[
        f"source {install_path}/miniconda3/bin/activate",
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
def env_check(install_path: str):
    """Check that conda packages installed correctly"""

    runner_logger = get_run_logger()

    runner_logger.info(ShellOperation(commands=[
        f"source {install_path}/miniconda3/bin/activate",
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
def vep_setup(install_path: str):
    """Setup VEP env params and indexes"""

    runner_logger = get_run_logger()

    runner_logger.info(ShellOperation(commands=[
        f"source {install_path}/miniconda3/bin/activate",
        "conda init --all",
        "conda activate vcf2maf_38",
        f"export VEP_PATH={install_path}/vep",
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
def bwa_setup(bucket, bwa_tarball, install_path):
    """Setup reference genome files needed by VEP"""
    
    runner_logger = get_run_logger()

    os.chdir(install_path)

    file_dl(bucket, bwa_tarball)

    f_name = os.path.basename(bwa_tarball)

    runner_logger.info(ShellOperation(commands=[
        f"tar -xvjf {f_name}",
        f"{f_name.replace('.tar.bz2', '')}/bwakit/run-gen-ref hs38DH",
        f"source {install_path}/miniconda3/bin/activate",
        "conda init --all",
        "conda activate vcf2maf_38",
        "samtools faidx hs38DH.fa",
    ]).run())

    ##### BWA install here 
@flow(
    name="vcf2maf_bcftools_setup",
    log_prints=True,
    flow_run_name="vcf2maf_bcftools_setup_" + f"{get_time()}",
)
def bcftools_setup(install_path):
    """Setup reference genome files needed by VEP"""
    
    runner_logger = get_run_logger()

    os.chdir(install_path)

    runner_logger.info(ShellOperation(commands=[
        "git clone --recurse-submodules https://github.com/samtools/htslib.git",
        "git clone https://github.com/samtools/bcftools.git",
        "cd bcftools",
        "make",
    ]).run())


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
def conversion_handler(row: pd.Series, install_path: str, output_dir: str):
    """Function to handle downloading VCF files and generating index files

    Args:
        row (pd.Series): pandas Series of a row entry from manifest
        install_path (str): path to where conda libs are installed, env to activate
        output_dir (str): path to move maf files to

    Returns:
        patient_id, tumor_sample_id and True/False depending on success of vcf annotation and maf conversion
    """

    runner_logger = get_run_logger()

    #for index, row in df.iterrows():
    f_bucket = row["s3_url"].split("/")[2]
    f_path = "/".join(row["s3_url"].split("/")[3:])

    #make dir for this VCF file conversion 
    ShellOperation(commands=[
        f"mkdir {row['patient_id']}",
        f"cd {row['patient_id']}"
    ]).run()

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

        runner_logger.info(ShellOperation(commands=[
            f"source {install_path}/miniconda3/bin/activate",
            "conda init --all",
            "conda activate vcf2maf_38",
            f"{install_path}/bcftools/bcftools reheader -s sample.txt -o {f_name.replace('vcf.gz', 'reheader.vcf.gz')} {f_name}",
            f"bgzip -d {f_name.replace('vcf.gz', 'reheader.vcf.gz')}",
            f"vcf2maf.pl --input-vcf {f_name.replace('vcf.gz', 'reheader.vcf.gz')} --output-maf {f_name.replace('vcf.gz', 'reheader.vcf.gz')}.vep.maf --ref-fasta {install_path}/ -vep-path {install_path}/miniconda3/bin/ --ncbi-build GRCh38 --tumor-id {row['tumor_sample_id']}  --normal-id {row['normal_sample_id']}",
            "ls -l"
        ]).run())

        if f"{f_name.replace('vcf.gz', 'reheader.vcf.gz')}.vep.maf" in os.listdir("."):
            # rename and move file to output directory 
            # rename file from *reheader.vcf.gz.vep.maf to .vcf.vep.maf
            os.rename(f"{f_name.replace('vcf.gz', 'reheader.vcf.gz')}.vep.maf", output_dir+"/"+f"{f_name.replace('.gz', '')}.vep.maf")
            os.chdir("..")
            shutil.rmtree(row['patient_id']) #remove temp folder with intermediate files
            return [row['patient_id'], row["tumor_sample_id"], True]
        else:
            runner_logger.info(f"Something went wrong, MAF file from {f_name} not produced")
            os.chdir("..")
            shutil.rmtree(row['patient_id'])
            return [row['patient_id'], row["tumor_sample_id"], False]


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
    manifest_path: str,
    bwa_tarball_path: str,
):
    """VCF2MAF Conversion

    Args:
        bucket (str): Bucket name of where the manifest etc. is located in and the output goes to
        runner (str): Unique runner name
        process_type (str): Whether to setup env, perform vcf22maf conversion or tear down env
        manifest_path (str): Path to tab-delimited manifest with s3 URLs of VCF files to convert and tumor/normal sample barcodes
        bwa_tarball_path (str): Path to bwakit tarball for ref seq installation
        bcftools_setup (str): Path to bcftools tarball

    Raises:
        ValueError: Value Error occurs when the pipeline fails to proceed.
    """

    runner_logger = get_run_logger()

    runner_logger.info(">>> Running vcf2maf_conversion.py ....")

    dt = get_time()

    output_dir = f"/usr/local/data/vcf2maf_output_{dt}"

    os.mkdir(output_dir)

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
        runner_logger.info(ShellOperation(commands=[
            f"ls -lh {install_path}/vep/homo_sapiens/112_GRCh38/",
        ]).run())

        #check that BWA installed
        runner_logger.info(ShellOperation(commands=[
            f"ls -lh {install_path}",
        ]).run())

        #check that bcftools installed
        runner_logger.info(ShellOperation(commands=[
            f"ls -lh {install_path}/bcftools",
        ]).run())

    elif process_type == "convert":

        runner_logger.info(">>> Performing VCF annotation and conversion to MAF ....")

        conversion_recording = []

        working_path = "/usr/local/data/output"

        ## TESTING
        """runner_logger.info(ShellOperation(commands=[
            f"source {install_path}/miniconda3/bin/activate",
            "conda init --all",
            "conda activate vcf2maf_38",
            "whereis bcftools",
            "whereis samtools", 
            "whereis vep"
        ]).run())"""

        if os.path.exists(working_path):
            os.chdir(working_path)
        else:
            ShellOperation(commands=[
                f"mkdir {working_path}",
                f"cd {working_path}"
            ]).run()

        # download manifest
        file_dl(bucket, manifest_path)

        mani = os.path.basename(manifest_path)

        df = read_input(mani)

        ## TESTING
        df_test = df[:2]

        for index, row in df_test.iterrows():
            conversion_recording.append(conversion_handler(row, install_path, output_dir))
        
        pd.DataFrame(conversion_recording, columns=['patient_id', 'tumor_sample_id', 'converted?']).to_csv(f"{output_dir}/conversion_summary.tsv", sep="\t", index=False)

        # dl folder to somewhere else
        folder_ul(
            local_folder=output_dir,
            bucket=bucket,
            destination=runner + "/",
            sub_folder="",
        )

    elif process_type == "env_tear_down":

        runner_logger.info(">>> Tearing env setup ....")
        
        runner_logger.info(ShellOperation(commands=[
            #f"rm -rf {install_path}", 
            #f"rm -rf {working_path}", 
            #"rm -rf OUTPUTs"
        ]).run())

    # test vcf2maf


