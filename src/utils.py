from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from dataclasses import dataclass, field
from typing import List, TypeVar, Dict, Tuple
import warnings
import os
from datetime import date
from datetime import datetime
from pytz import timezone
import logging
import pandas as pd
import boto3
import re
import requests


ExcelFile = TypeVar("ExcelFile")


@dataclass
class GithubURL:
    """DataClass"""
    ccdi_model_recent_release: str = field(default="https://api.github.com/repos/CBIIT/ccdi-model/releases/latest")
    sra_template: str = field(default="https://raw.githubusercontent.com/CBIIT/ChildhoodCancerDataInitiative-CCDI_to_SRAy/main/doc/example_inputs/phsXXXXXX.xlsx")
    ccdi_model_manifest: str =field(default="https://api.github.com/repos/CBIIT/ccdi-model/contents/metadata-manifest/")

def get_ccdi_latest_release() -> str:
    latest_url = GithubURL.ccdi_model_recent_release
    response =  requests.get(latest_url)
    tag_name  =  response.json()["tag_name"]
    return tag_name

@task
def dl_sra_template() -> None:
    sra_filename="phsXXXXXX.xlsx"
    r = requests.get(GithubURL.sra_template)
    f = open(sra_filename, "wb")
    f.write(r.content)
    return sra_filename

@task
def check_ccdi_version(ccdi_manifest: str) -> str:
    warnings.simplefilter(action="ignore", category=UserWarning)
    ccdi_dict = {}
    ccdi_excel = pd.ExcelFile(ccdi_manifest)
    ccdi_dict["instruction"] = pd.read_excel(
        ccdi_excel, sheet_name="README and INSTRUCTIONS", header=0
    )
    manifest_version =  ccdi_dict["instruction"].columns[2][1:]
    ccdi_excel.close()
    return manifest_version


@task
def dl_ccdi_template() -> None:
    manifest_page_response =  requests.get(GithubURL.ccdi_model_manifest)
    manifest_dict_list = manifest_page_response.json()
    manifest_names = [i["name"] for i in manifest_dict_list]
    latest_release = get_ccdi_latest_release()
    # There should be only one match in the list comprehension below
    manifest =  [i for i in manifest_names if latest_release in i and re.search("v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)\.xlsx$", i)]
    manifest_response =  [j for j in manifest_dict_list if j["name"]==manifest[0]]
    manifest_dl_url = requests.get(manifest_response[0]["url"]).json()["download_url"]
    manifest_dl_res = requests.get(manifest_dl_url)
    manifest_file = open(manifest[0], "wb")
    manifest_file.write(manifest_dl_res.content)
    return manifest[0]


def get_date() -> str:
    """Returns the current date"""
    date_obj = date.today()
    return date_obj.isoformat()


def get_time() -> str:
    """Returns the current time"""
    tz = timezone('EST')
    now = datetime.now(tz)
    dt_string =  now.strftime("%Y-%m-%d*%H:%M:%S")
    return dt_string

def set_s3_resource():
    """This method sets the s3_resource object to either use localstack
    for local development if the LOCALSTACK_ENDPOINT_URL variable is
    defined and returns the object
    """
    localstack_endpoint = os.environ.get("LOCALSTACK_ENDPOINT_URL")
    if localstack_endpoint != None:
        AWS_REGION = "us-east-1"
        AWS_PROFILE = "localstack"
        ENDPOINT_URL = localstack_endpoint
        boto3.setup_default_session(profile_name=AWS_PROFILE)
        s3_resource = boto3.resource(
            "s3", region_name=AWS_REGION, endpoint_url=ENDPOINT_URL
        )
    else:
        s3_resource = boto3.resource("s3")
    return s3_resource

def set_s3_session_client():
    """This method sets the s3 session client object
    to either use localstack for local development if the
    LOCALSTACK_ENDPOINT_URL variable is defined
    """
    localstack_endpoint = os.environ.get("LOCALSTACK_ENDPOINT_URL")
    if localstack_endpoint != None:
        AWS_REGION = "us-east-1"
        AWS_PROFILE = "localstack"
        ENDPOINT_URL = localstack_endpoint
        boto3.setup_default_session(profile_name=AWS_PROFILE)
        s3_client = boto3.client("s3", region_name=AWS_REGION, endpoint_url=ENDPOINT_URL)
    else:
        s3_client = boto3.client("s3")
    return s3_client

@task(name="Download file", task_run_name="download_file_{filename}")
def file_dl(bucket, filename):
    """File download using bucket name and filename"""
    # Set the s3 resource object for local or remote execution
    s3 = set_s3_resource()
    source = s3.Bucket(bucket)
    file_key = filename
    file = os.path.basename(filename)
    source.download_file(file_key, file)


@task(name="Upload file", task_run_name="upload_file_{newfile}")
def file_ul(bucket: str, output_folder: str, sub_folder: str, newfile: str):
    """File upload using bucket name, output folder name
    and filename
    """
    # Set the s3 resource object for local or remote execution
    s3 = set_s3_resource()
    source = s3.Bucket(bucket)
    # upload files outside inputs/ folder
    file_key = os.path.join(output_folder, sub_folder, newfile)
    # extra_args={'ACL': 'bucket-owner-full-control'}
    source.upload_file(newfile, file_key)  # , extra_args)


@flow(name="Upload dbgap outputs", flow_run_name="upload_dbgap_outputs_{local_folder}")
def folder_ul(local_folder: str, bucket: str, destination: str, sub_folder: str) -> None:
    """This function uploads all the files from a folder
    and preserves the original folder structure
    """
    s3 = set_s3_resource()
    source = s3.Bucket(bucket)
    folder_basename = os.path.basename(local_folder)
    for root, _, files in os.walk(local_folder):
        for filename in files:
            # construct local path
            local_path = os.path.join(root, filename)

            # construct the full dst path
            relative_path = os.path.relpath(local_path, local_folder)
            s3_path = os.path.join(destination, sub_folder, folder_basename, relative_path)

            # upload file
            # this should overwrite file if file exists in the bucket
            source.upload_file(local_path, s3_path)


@flow(name="Upload outputs", flow_run_name="upload_workflow_outputs_" + f"{get_time()}")
def outputs_ul(
    bucket: str,
    output_folder: str,
    catcherr_file: str,
    catcherr_log: str,
    validation_log: str,
    sra_file: str,
    sra_log: str,
    dbgap_folder: str,
    dbgap_log: str, 
    ) -> None:
    # upload CatchERR outputs
    file_ul(bucket, output_folder=output_folder, sub_folder="1_CatchERR_output", newfile=catcherr_file)
    file_ul(bucket, output_folder=output_folder, sub_folder="1_CatchERR_output", newfile=catcherr_log)
    # upload ValidationRy output
    file_ul(bucket, output_folder=output_folder, sub_folder="2_ValidationRy_output", newfile=validation_log)
    # upload SRA submission output
    file_ul(bucket, output_folder=output_folder, sub_folder="3_SRA_submisison_output", newfile=sra_file)
    file_ul(bucket, output_folder=output_folder, sub_folder="3_SRA_submisison_output", newfile=sra_log)
    # upload dbgap submission output
    file_ul(bucket, output_folder=output_folder, sub_folder="4_dbGaP_submisison_output", newfile=dbgap_log)
    folder_ul(local_folder=dbgap_folder, bucket=bucket, destination=output_folder, sub_folder="4_dbGaP_submisison_output")


@task
def view_all_s3_objects(source_bucket):
    """List files from source bucket"""
    # Set the s3 resource object for local or remote execution
    s3 = set_s3_resource()
    source = s3.Bucket(source_bucket)
    # Print all objects in source bucket
    source_file_list = []
    for obj in source.objects.all():
        source_file_list.append(obj.key)

    return source_file_list


@task
def markdown_task(source_bucket, source_file_list):
    """Creates markdown bucket artifacts using Prefect
    create_markdown_artifact()
    """
    markdown_report = f"""
    # S3 Viewer Run

    ## Source Bucket: {source_bucket}

    ### List of files ({len(source_file_list)}):

    {source_file_list}
    """
    create_markdown_artifact(
        key=f"bucket-check-before-workflow_{source_bucket}",
        markdown=markdown_report,
        description=f"Bucket_check_before_workflow_{source_bucket}",
    )


@task
def markdown_output_task(source_bucket, source_file_list):
    """Creates markdown bucket artifacts using Prefect 
    create_markdown_artifact()
    """
    list_wo_inputs = [i for i in source_file_list if "inputs" not in i]
    catcherr_log = [k for k in list_wo_inputs if re.search("CatchERR[0-9]{8}\.txt$", k)]
    catcherr_output = [j for j in list_wo_inputs if re.search("CatchERR[0-9]{8}\.xlsx$", j)]
    validationry_output = [l for l in list_wo_inputs if re.search("Validate[0-9]{8}\.txt$", l)]
    sra_log = [m for m in list_wo_inputs if re.search("CCDI_to_SRA_submission_[0-9]{4}-[0-9]{2}-[0-9]{2}.log$", m)]
    sra_submission = [o for o in list_wo_inputs if re.search("SRA_submission.xlsx$", o)]
    dbgap_log = [n for n in list_wo_inputs if re.search("CCDI_to_dbGaP_submission_[0-9]{4}-[0-9]{2}-[0-9]{2}.log$", n)]
    dbgap_folder =  [p for p in list_wo_inputs if re.search("dbGaP_submission_[0-9]{4}-[0-9]{2}-[0-9]{2}\/", p)]
    #dbgap_folder_str =  "\n".join(dbgap_folder)
    markdown_report = f"""
    # S3 Viewer Run

    ## Source Bucket: {source_bucket}

    ### List of outputs

    - CatchERRy output: {catcherr_output}

    - CatchERRy log: {catcherr_log}

    - ValidationRy report: {validationry_output}

    - SRA submission file: {sra_submission}

    - SRA file log: {sra_log}

    - dbGaP submission file list in folder ({len(dbgap_folder)}):
    {dbgap_folder}

    - dbGaP file log: {dbgap_log}
    """
    create_markdown_artifact(
        key=f"bucket-check-after-workflow_{source_bucket}",
        markdown=markdown_report,
        description=f"Bucket_check_after_workflow_{source_bucket}",
    )


def get_logger(loggername: str, log_level: str):
    """Returns a basic logger with a logger name using a std format

    log level can be set using one of the values in log_levels.
    """
    log_levels = {  # sorted level
        "notset": logging.NOTSET,  # 00
        "debug": logging.DEBUG,  # 10
        "info": logging.INFO,  # 20
        "warning": logging.WARNING,  # 30
        "error": logging.ERROR,  # 40
    }

    logger_filename = loggername + "_" + get_date() + ".log"
    logger = logging.getLogger(loggername)
    logger.setLevel(log_levels[log_level])

    # set the stream handler
    # stream_handler = logging.StreamHandler(sys.stdout)
    # stream_handler.setFormatter(ColorLogFormatter())
    # stream_handler.setLevel(log_levels["info"])

    # set the file handler
    file_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
    file_handler = logging.FileHandler(logger_filename, mode="w")
    file_handler.setFormatter(logging.Formatter(file_FORMAT, "%H:%M:%S"))
    file_handler.setLevel(log_levels["info"])

    # logger.addHandler(stream_handler)
    logger.addHandler(file_handler)

    return logger


def excel_sheets_to_dict(excel_file: ExcelFile, no_names: List) -> Dict:
    """Returns a list of sheet names in the excel file input"""
    warnings.simplefilter(action="ignore", category=UserWarning)
    sheetnames = excel_file.sheet_names
    sheetnames_subset = [i for i in sheetnames if i not in no_names]
    excel_dict = {}
    for i in sheetnames_subset:
        i_df = pd.read_excel(excel_file, sheet_name=i, dtype=str)
        excel_dict[i] = i_df
    excel_file.close()
    return excel_dict


@task
def ccdi_manifest_to_dict(excel_file: ExcelFile) -> Dict:
    """Reads a validated CDDI manifest excel and retruns
    a dictionary with sheetnames as keys and pandas
    dataframes as values

    The sheet will be dropped if found empty
    """
    sheets_to_avoid = ["README and INSTRUCTIONS", "Dictionary", "Terms and Value Sets"]
    ccdi_dict_raw = excel_sheets_to_dict(excel_file, no_names=sheets_to_avoid)
    ccdi_dict = {}
    for key, item_df in ccdi_dict_raw.items():
        # drop the column "type" from data frame
        item_df = item_df.drop(["type"], axis=1)
        # remove any line or column that has all na values
        item_df.dropna(axis=0, how="all", inplace=True)
        # keep empty columnsat this step
        # item_df.dropna(axis=1, how="all", inplace=True)

        # some more filtering criteria
        # test if the df is empty
        # test if all column names contain a '.', if yes, do not add it to dict
        item_df_names = item_df.columns
        if len([j for j in item_df_names if "." in j]) != len(item_df_names):
            ccdi_dict[key] = item_df
        else:
            pass
    del ccdi_dict_raw
    return ccdi_dict
