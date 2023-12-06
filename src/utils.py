from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from typing import List, TypeVar, Dict, Tuple
import warnings
import os
from datetime import date
from datetime import datetime
import logging
import pandas as pd
import boto3
import re


ExcelFile = TypeVar("ExcelFile")


def get_date() -> str:
    """Returns the current date"""
    date_obj = date.today()
    return date_obj.isoformat()


def get_time() -> str:
    """Returns the current time"""
    now = datetime.now()
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

@task
def file_dl(bucket, filename):
    """File download using bucket name and filename"""
    # Set the s3 resource object for local or remote execution
    s3 = set_s3_resource()
    source = s3.Bucket(bucket)
    file_key = filename
    file = os.path.basename(filename)
    source.download_file(file_key, file)


@task
def file_ul(bucket, output_folder, newfile):
    """File upload using bucket name, output folder name
    and filename
    """
    # Set the s3 resource object for local or remote execution
    s3 = set_s3_resource()
    source = s3.Bucket(bucket)
    # upload files outside inputs/ folder
    file_key = os.path.join(output_folder, newfile)
    # extra_args={'ACL': 'bucket-owner-full-control'}
    source.upload_file(newfile, file_key)  # , extra_args)


@flow(name="Upload outputs", flow_run_name="upload_outputs_"+f"{get_time()}")
def folder_ul(local_folder: str, bucket: str, destination: str) -> None:
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
            s3_path = os.path.join(destination, folder_basename, relative_path)

            # upload file
            # this should overwrite file if file exists in the bucket
            source.upload_file(local_path, s3_path)


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
def markdown_task(source_bucket, source_file_list, instance):
    """Creates markdown bucket artifacts using Prefect
    create_markdown_artifact()
    """
    markdown_report = f"""
    # S3 Viewer Run {instance}

    ## Source Bucket: {source_bucket}

    ### List of files ({len(source_file_list)}):

    {source_file_list}
    """
    create_markdown_artifact(
        key=f"bucket-check-{instance}",
        markdown=markdown_report,
        description=f"Bucket_check_{instance}",
    )


@task
def markdown_output_task(source_bucket, source_file_list, instance):
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
    # S3 Viewer Run {instance}

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
        key=f"bucket-check-{instance}",
        markdown=markdown_report,
        description=f"Bucket_check_{instance}",
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
