from prefect import flow, task, Task, get_run_logger
from prefect.artifacts import create_markdown_artifact
from dataclasses import dataclass, field
from typing import List, TypeVar, Dict, Tuple
import warnings
import os
import sys
from datetime import date
from datetime import datetime
from pytz import timezone
import logging
import pandas as pd
import boto3
import re
import requests
import typing
import tempfile
from urllib.request import urlopen
from io import BytesIO
from zipfile import ZipFile
from shutil import copy
import json
from botocore.exceptions import ClientError
from prefect_github import GitHubCredentials


ExcelFile = TypeVar("ExcelFile")
DataFrame = TypeVar("DataFrame")


@dataclass
class GithubAPTendpoint:
    """DataClass"""

    ccdi_model_recent_release: str = field(
        default="https://api.github.com/repos/CBIIT/ccdi-model/releases/latest"
    )
    sra_template: str = field(
        default="https://raw.githubusercontent.com/CBIIT/ChildhoodCancerDataInitiative-CCDI_to_SRAy/main/doc/example_inputs/phsXXXXXX.xlsx"
    )
    ccdi_model_manifest: str = field(
        default="https://api.github.com/repos/CBIIT/ccdi-model/contents/metadata-manifest/"
    )
    ccdi_tags: str = field(default="https://api.github.com/repos/CBIIT/ccdi-model/tags")


class CCDI_Tags(Task):
    """Class that fetches available releases, checks if a release exists,
    and download ccdi manifest of a certain release
    """

    def __init__(self) -> None:
        self.tags_api = "https://api.github.com/repos/CBIIT/ccdi-model/tags"

    def get_tags(self) -> List[Dict]:
        github_token =  get_github_token()
        headers = {"Authorization": "token " + github_token}
        api_re = requests.get(self.tags_api, headers=headers)
        tags_list = api_re.json()
        return tags_list

    def get_tags_only(self) -> List:
        tags_list = self.get_tags()
        tags = [i["name"] for i in tags_list]
        return tags

    def if_tag_exists(self, tag: str, logger):
        tags = self.get_tags_only()
        if tag in tags:
            logger.info(
                f"Version {tag} is found among the released versions of ccdi-model GitHub repo"
            )
            return True
        else:
            return False

    def get_tag_element(self, tag: str):
        tags_list = self.get_tags()
        tag_element = [i for i in tags_list if i["name"] == tag][0]
        return tag_element

    def download_tag_manifest(self, tag: str, logger) -> None:
        check_tag = self.if_tag_exists(tag=tag, logger=logger)
        if check_tag:
            tag_element = self.get_tag_element(tag=tag)
            tag_zipurl = tag_element["zipball_url"]
            http_response = urlopen(tag_zipurl)
            zipfile = ZipFile(BytesIO(http_response.read()))
            # create a temp dir to download the zipfile
            tempdirobj = tempfile.TemporaryDirectory(suffix="_github_dl")
            tempdir = tempdirobj.name
            zipfile.extractall(path=tempdir)
            # manifest folder list files
            manifests_folder_path = os.path.join(
                tempdirobj.name, os.listdir(tempdirobj.name)[0], "metadata-manifest"
            )
            try:
                manifest_file_list = os.listdir(manifests_folder_path)
                manifest_tag_match = [
                    i for i in manifest_file_list if i.endswith(tag + ".xlsx")
                ]
                if len(manifest_tag_match) == 0:
                    logger.error(
                        f"No CCDI manifest file ends with v{tag}.xlsx under matadata-manifest folder"
                    )
                    return None
                elif len(manifest_tag_match) >= 1:
                    if len(manifest_tag_match) > 1:
                        logger.warning(
                            f"More than one manifest file ends with v{tag}.xlsx.\n{*manifest_tag_match,}\nThe workflow defaults to first item {manifest_tag_match[0]}"
                        )
                    else:
                        pass
                    copy(
                        os.path.join(manifests_folder_path, manifest_tag_match[0]),
                        manifest_tag_match[0],
                    )
                    return manifest_tag_match[0]
            except FileNotFoundError as e:
                logger.error(e)
                return None
            except:
                logger.error(
                    f"Error in finding manifest .xlsx file, please download the zipfile and investigate. {tag_zipurl}"
                )
                return None
        else:
            available_tags = self.get_tags_only()
            logger.error(
                f"v{tag} is not found in released versions. Here is a list of available versions:\n{*available_tags,}"
            )
            return None


def get_ccdi_latest_release() -> str:
    latest_url = GithubAPTendpoint.ccdi_model_recent_release
    github_token =  get_github_token()
    headers = {"Authorization": "token " + github_token}
    response = requests.get(latest_url, headers=headers)
    if "tag_name" in response.json().keys():
        tag_name = response.json()["tag_name"]
    else:
        tag_name = "unknown"
    return tag_name


@task
def dl_sra_template() -> None:
    sra_filename = "phsXXXXXX.xlsx"
    github_token = get_github_token()
    headers = {"Authorization": "token " + github_token}
    r = requests.get(GithubAPTendpoint.sra_template, headers=headers)
    f = open(sra_filename, "wb")
    f.write(r.content)
    return sra_filename


@task
def dl_file_from_url(file_endpoint: str) -> str:
    filename = os.path.basename(file_endpoint)
    r = requests.get(file_endpoint)
    f = open(filename, "wb")
    f.write(r.content)
    return filename


@task
def check_ccdi_version(ccdi_manifest: str) -> str:
    warnings.simplefilter(action="ignore", category=UserWarning)
    ccdi_dict = {}
    ccdi_excel = pd.ExcelFile(ccdi_manifest)
    ccdi_dict["instruction"] = pd.read_excel(
        ccdi_excel, sheet_name="README and INSTRUCTIONS", header=0
    )
    manifest_version = ccdi_dict["instruction"].columns[2][1:]
    ccdi_excel.close()
    return manifest_version


@task(log_prints=True)
def dl_ccdi_template() -> None:
    """Downloads the latest version of CCDI manifest"""
    github_token = get_github_token()
    headers = {"Authorization": "token " + github_token}
    manifest_page_response = requests.get(GithubAPTendpoint.ccdi_model_manifest, headers=headers)
    manifest_dict_list = manifest_page_response.json()
    if not isinstance(manifest_dict_list, list):
        print("Github API return was not a list: " + str(manifest_dict_list))
        raise
    else:
        pass
    manifest_names = [i["name"] for i in manifest_dict_list]
    latest_release = get_ccdi_latest_release()
    # There should be only one match in the list comprehension below
    manifest = [
        i
        for i in manifest_names
        if latest_release in i
        and re.search(r"v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)\.xlsx$", i)
    ]
    manifest_response = [j for j in manifest_dict_list if j["name"] == manifest[0]]
    manifest_dl_url = requests.get(manifest_response[0]["url"], headers=headers).json()["download_url"]
    manifest_dl_res = requests.get(manifest_dl_url, headers=headers)
    manifest_file = open(manifest[0], "wb")
    manifest_file.write(manifest_dl_res.content)
    return manifest[0]


def get_date() -> str:
    """Returns the current date"""
    date_obj = date.today()
    return date_obj.isoformat()


def get_time() -> str:
    """Returns the current time"""
    tz = timezone("EST")
    now = datetime.now(tz)
    dt_string = now.strftime("%Y%m%d_T%H%M%S")
    return dt_string


def get_manifest_phs(manifest_path: str) -> str:
    """Return phs accession of ccdi study"""
    manifest_excel = pd.ExcelFile(manifest_path)
    warnings.simplefilter(action="ignore", category=UserWarning)
    study_sheet_df = pd.read_excel(manifest_excel, "study", dtype=str)
    phs_accession = study_sheet_df["phs_accession"].tolist()[0]
    return phs_accession


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
        s3_client = boto3.client(
            "s3", region_name=AWS_REGION, endpoint_url=ENDPOINT_URL
        )
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


def folder_ul(
    local_folder: str, bucket: str, destination: str, sub_folder: str
) -> None:
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
            s3_path = os.path.join(
                destination, sub_folder, folder_basename, relative_path
            )

            # upload file
            # this should overwrite file if file exists in the bucket
            source.upload_file(local_path, s3_path)


@task
def folder_dl(bucket: str, remote_folder: str) -> None:
    """Downloads a remote direcotry folder from s3
    bucket to local. it generates a folder that follows the
    structure in s3 bucket

    for instance, if the remote_folder is "uniq_id/test_folder",
    the local directory will create path of "uniq_id/test_folder"
    """
    s3_resouce = set_s3_resource()
    bucket_obj = s3_resouce.Bucket(bucket)
    for obj in bucket_obj.objects.filter(Prefix=remote_folder):
        if not os.path.exists(os.path.dirname(obj.key)):
            os.makedirs(os.path.dirname(obj.key))
        bucket_obj.download_file(obj.key, obj.key)
    return None


@flow(
    name="Upload ccdi workflow inputs", flow_run_name="upload_input_" + f"{get_time()}"
)
def ccdi_wf_inputs_ul(
    bucket: str,
    output_folder: str,
    ccdi_manifest: str,
    ccdi_template: str,
    sra_template: str,
):
    """Upload inputs of CCDI data curation workflow into designated bucket"""
    # upload input files
    file_ul(
        bucket,
        output_folder=output_folder,
        sub_folder="workflow_inputs",
        newfile=ccdi_manifest,
    )
    file_ul(
        bucket,
        output_folder=output_folder,
        sub_folder="workflow_inputs",
        newfile=ccdi_template,
    )
    file_ul(
        bucket,
        output_folder=output_folder,
        sub_folder="workflow_inputs",
        newfile=sra_template,
    )


@flow(
    name="Upload ccdi workflow outputs",
    flow_run_name="upload_workflow_outputs_{wf_step}",
)
def ccdi_wf_outputs_ul(
    bucket: str,
    output_folder: str,
    wf_step: str,
    sub_folder: str,
    output_path: typing.Optional[str] = None,
    output_log: typing.Optional[str] = None,
):
    if output_path is not None:
        if os.path.isdir(output_path):
            folder_ul(
                local_folder=output_path,
                bucket=bucket,
                destination=output_folder,
                sub_folder=sub_folder,
            )
        elif os.path.isfile(output_path):
            file_ul(
                bucket=bucket,
                output_folder=output_folder,
                sub_folder=sub_folder,
                newfile=output_path,
            )
        else:
            pass
    else:
        pass

    if output_log is not None:
        file_ul(
            bucket=bucket,
            output_folder=output_folder,
            sub_folder=sub_folder,
            newfile=output_log,
        )
    else:
        pass

    return None


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


def identify_data_curation_log_file(start_str: str):
    file_list = os.listdir("./")
    log_file_regx = start_str + r"[0-9]{4}-[0-9]{2}-[0-9]{2}"
    log_file_found = [
        i for i in file_list if re.search(log_file_regx, i) and ".log" in i
    ]
    if len(log_file_found) == 0:
        return None
    else:
        return log_file_found[0]


@task
def markdown_template_updater(
    source_bucket: str,
    runner: str,
    output_folder: str,
    manifest: str,
    manifest_version: str,
    template: str,
    template_version: str,
):
    """
    Creates markdown file summary of template updater flow run
    """
    source_file_list = view_all_s3_objects(source_bucket=source_bucket)
    updated_manifest = [
        i
        for i in source_file_list
        if re.search(
            r"_Updater_v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)_[0-9]{4}-[0-9]{2}-[0-9]{2}\.xlsx$",
            i,
        )
        and output_folder in i
    ]
    update_log = [
        j
        for j in source_file_list
        if re.search(
            r"Update_CCDI_manifest_[0-9]{4}-[0-9]{2}-[0-9]{2}\.log$",
            j,
        )
        and output_folder in j
    ]
    markdown_report = f"""# CCDI Template Updater Workflow Summary

### Source Bucket

{source_bucket}

### Runner

{runner}

### CCDI Manifest

- File: {os.path.basename(manifest)}

- Version: {manifest_version}

### CCDI Template

- File: {os.path.basename(template)}

- Version: {template_version}

### Outputs

- Output folder: {output_folder}

- Updated manifest: {os.path.basename(updated_manifest[0])}

- Log: {os.path.basename(update_log[0])}

"""
    create_markdown_artifact(
        key=f"{runner.lower().replace('_','-').replace(' ','-').replace('.','-').replace('/','-')}-template-updater-summary",
        markdown=markdown_report,
        description=f"{runner} template updater worklfow summary",
    )


@task
def markdown_input_task(
    source_bucket: str,
    runner: str,
    manifest: str,
    template: str,
    sra_template: str,
    sra_pre_sub: str,
    dbgap_pre_sub: str,
):
    """Creates markdown artifacts of workflow inputs using Prefect
    create_markdown_artifact()
    """
    if sra_pre_sub is None:
        sra_pre_file = ""
    else:
        sra_pre_file = sra_pre_sub

    if dbgap_pre_sub is None:
        dbgap_pre_folder = ""
    else:
        dbgap_pre_folder = dbgap_pre_sub
    markdown_report = f"""# CCDI Data Curation Workflow Input Report

### Source Bucket

{source_bucket}

### Runner

{runner}

### CCDI Manifest

{manifest}
    
### CCDI template

{template}

### SRA template

{sra_template}

### SRA previous submission if applicable

{sra_pre_file}

### dbGaP previous submission if applicable

{dbgap_pre_folder}

"""
    create_markdown_artifact(
        key=f"{runner.lower().replace('_','-').replace(' ','-').replace('.','-').replace('/','-')}-workflow-input-report",
        markdown=markdown_report,
        description=f"{runner} workflow input report",
    )


@task
def markdown_output_task(
    source_bucket: str, source_file_list: str, output_folder: str, runner: str
):
    """Creates markdown bucket artifacts using Prefect
    create_markdown_artifact()
    """
    list_wo_inputs = [i for i in source_file_list if "inputs" not in i]
    catcherr_log = [
        k
        for k in list_wo_inputs
        if re.search(r"CatchERR[0-9]{8}\.txt$", k) and output_folder in k
    ]
    catcherr_log.append("")
    catcherr_output = [
        j
        for j in list_wo_inputs
        if re.search(r"CatchERR[0-9]{8}\.xlsx$", j) and output_folder in j
    ]
    catcherr_output.append("")
    validationry_output = [
        l
        for l in list_wo_inputs
        if re.search(r"Validate[0-9]{8}\.txt$", l) and output_folder in l
    ]
    validationry_output.append("")
    sra_log = [
        m
        for m in list_wo_inputs
        if re.search(r"CCDI_to_SRA_submission_[0-9]{4}-[0-9]{2}-[0-9]{2}.log$", m)
        and output_folder in m
    ]
    sra_log.append("")
    sra_submission = [
        o
        for o in list_wo_inputs
        if re.search(r"SRA_submission.xlsx$", o) and output_folder in o
    ]
    sra_submission.append("")
    dbgap_log = [
        n
        for n in list_wo_inputs
        if re.search(r"CCDI_to_dbGaP_submission_[0-9]{4}-[0-9]{2}-[0-9]{2}.log$", n)
        and output_folder in n
    ]
    dbgap_log.append("")
    dbgap_folder = [
        p
        for p in list_wo_inputs
        if re.search(r"dbGaP_submission_[0-9]{4}-[0-9]{2}-[0-9]{2}\/", p)
        and output_folder in p
    ]
    dbgap_folder.append("")
    if len(dbgap_folder) == 1:
        dbgap_folder_len = 0
    else:
        dbgap_folder_len = len(dbgap_folder) - 1
    dbgap_folder_str = "\n\n".join([os.path.basename(i) for i in dbgap_folder])
    cds_output = [
        q
        for q in list_wo_inputs
        if re.search(r"CDS[0-9]{8}\.xlsx$", q) and output_folder in q
    ]
    cds_output.append("")
    cds_log = [
        r
        for r in list_wo_inputs
        if re.search(r"CCDI_to_CDS_submission_[0-9]{4}-[0-9]{2}-[0-9]{2}.log$", r)
        and output_folder in r
    ]
    cds_log.append("")
    index_output = [
        s
        for s in list_wo_inputs
        if re.search(r"Index[0-9]{8}\.tsv$", s) and output_folder in s
    ]
    index_output.append("")
    index_log = [
        t
        for t in list_wo_inputs
        if re.search(r"CCDI_to_Index_[0-9]{4}-[0-9]{2}-[0-9]{2}.log$", t)
        and output_folder in t
    ]
    index_log.append("")
    tabbreaker_folder = [
        u
        for u in list_wo_inputs
        if re.search(r"_[0-9]{8}_T[0-9]{6}.tsv$", u) and output_folder in u
    ]
    tabbreaker_folder.append("")
    tabbreaker_log = [
        v
        for v in list_wo_inputs
        if re.search(r"CCDI_to_TabBreakeRy_[0-9]{4}-[0-9]{2}-[0-9]{2}.log$", v)
        and output_folder in v
    ]
    tabbreaker_log.append("")
    tabbreaker_json = [
        w
        for w in list_wo_inputs
        if re.search(r"_TabBreakeRLog_[0-9]{8}_T[0-9]{6}.json$", w)
        and output_folder in w
    ]
    tabbreaker_json.append("")

    markdown_report = f"""# CCDI Data Curation Workflow Report
    
## Source Bucket

{source_bucket}

## Workflow output folder

{output_folder}

---

### CatchERRy 

* Output folder

{os.path.dirname(catcherr_log[0])}

* Excel output

{os.path.basename(catcherr_output[0])}

* CatchERRy log

{os.path.basename(catcherr_log[0])}

---

### ValidationRy

* Output folder

{os.path.dirname(validationry_output[0])}

* Report

{os.path.basename(validationry_output[0])}

---

### CCDI to SRA submission

* Output folder

{os.path.dirname(sra_log[0])}

* SRA submssion file

{os.path.basename(sra_submission[0])}

* SRA file log

{os.path.basename(sra_log[0])}

---

### CCDI to dbGaP submission

* Output folder

{os.path.dirname(dbgap_log[0])}

* Output files ({dbgap_folder_len})

{dbgap_folder_str}

* dbGaP file log

{os.path.basename(dbgap_log[0])}

---

### CCDI to CDS submission

* Output folder

{os.path.dirname(cds_log[0])}

* CDS submission file

{os.path.basename(cds_output[0])}

* CDS file log

{os.path.basename(cds_log[0])}

---

### CCDI to Index file

* Output folder

{os.path.dirname(index_log[0])}

* Index file

{os.path.basename(index_output[0])}

* Index file log

{os.path.basename(index_log[0])}

---

### CCDI to TabBreaker file

* Output TSV folder

{os.path.dirname(tabbreaker_folder[0])}

* TabBreaker log

{os.path.basename(tabbreaker_log[0])}

* TabBreaker metadata json

{os.path.basename(tabbreaker_json[0])}

"""
    create_markdown_artifact(
        key=f"{runner.lower().replace('_','-').replace(' ','-').replace('.','-').replace('/','-')}-workflow-output-report",
        markdown=markdown_report,
        description=f"{runner} workflow output report",
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

    # set the file handler
    file_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
    file_handler = logging.FileHandler(logger_filename, mode="w")
    file_handler.setFormatter(logging.Formatter(file_FORMAT, "%H:%M:%S"))
    file_handler.setLevel(log_levels["info"])

    # set the stream handler
    # stream_handler = logging.StreamHandler(sys.stdout)
    # stream_handler.setFormatter(logging.Formatter(file_FORMAT, "%H:%M:%S"))
    # stream_handler.setLevel(log_levels["info"])

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


class CheckCCDI:
    """
    A Class that takes ccdi manifest path
    and read sheet into df or extract specific
    study related information
    """

    def __init__(self, ccdi_manifest: str) -> None:
        self.ccdi_manifest = ccdi_manifest
        self.na_bank = ["NA", "na", "N/A", "n/a", ""]

    def read_sheet(self, sheetname: str) -> DataFrame:
        warnings.simplefilter(action="ignore", category=UserWarning)
        ccdi_excel = pd.ExcelFile(self.ccdi_manifest)
        sheetname_df = pd.read_excel(ccdi_excel, sheet_name=sheetname, header=0)
        ccdi_excel.close()
        return sheetname_df

    def read_sheet_na(self, sheetname: str) -> DataFrame:
        warnings.simplefilter(action="ignore", category=UserWarning)
        ccdi_excel = pd.ExcelFile(self.ccdi_manifest)
        sheetname_df = pd.read_excel(
            ccdi_excel, sheet_name=sheetname, na_values=self.na_bank, dtype="string"
        )
        ccdi_excel.close()
        return sheetname_df

    def get_version(self):
        readme_df = self.read_sheet(sheetname="README and INSTRUCTIONS")
        manifest_version = readme_df.columns[2][1:]
        return manifest_version

    def get_sheetnames(self):
        warnings.simplefilter(action="ignore", category=UserWarning)
        ccdi_excel = pd.ExcelFile(self.ccdi_manifest)
        sheet_names = ccdi_excel.sheet_names
        ccdi_excel.close()
        return sheet_names

    def get_study_id(self):
        study_df = self.read_sheet(sheetname="study")
        study_id = study_df["study_id"][0]
        return study_id

    def get_dict_df(self):
        dict_df = self.read_sheet(sheetname="Dictionary")
        # remove empty row
        dict_df.dropna(axis=0, how="all", inplace=True)
        # remove empty column
        dict_df.dropna(axis=1, how="all", inplace=True)
        return dict_df

    def get_dict_node(self):
        dict_df = self.get_dict_df()
        dict_nodes = dict_df["Node"].unique()
        return dict_nodes

    def get_terms_value_sets(self):
        terms_df = self.read_sheet(sheetname="Terms and Value Sets")
        # remove empty rows and column
        terms_df.dropna(axis=0, how="all", inplace=True)
        terms_df.dropna(axis=0, how="all", inplace=True)
        # value to terms dict
        term_dict = terms_df.groupby("Value Set Name")["Term"].apply(list).to_dict()
        if "diagnosis_classification" in term_dict.keys():
            diagnosis_terms = term_dict["diagnosis_classification"]
            diagnosis_terms_clean = [i for i in diagnosis_terms if "[-" not in i]
            term_dict["diagnosis_classification"] = diagnosis_terms_clean
            del diagnosis_terms
            del diagnosis_terms_clean
        else:
            pass
        return term_dict

    def find_file_nodes(self):
        dict_df = self.get_dict_df()
        file_node_list = dict_df[dict_df["Property"] == "file_url_in_cds"][
            "Node"
        ].tolist()
        # remove any duplcates
        file_node_list_uniq = list(set(file_node_list))
        return file_node_list_uniq


@flow(log_prints=True)
def get_github_credentials()-> None:
    runner_logger = get_run_logger()
    github_credentials_block = GitHubCredentials.load("fnlccdidatacuration")
    token_value = github_credentials_block.token.get_secret_value()
    headers = {"Authorization": "token " + token_value}
    # try to get api return
    response = requests.get(GithubAPTendpoint.ccdi_model_recent_release, headers=headers)
    runner_logger.info(json.dumps(response.json(), indent=4))
    return None

def get_github_token() -> str:
    github_credentials_block = GitHubCredentials.load("fnlccdidatacuration")
    token_value = github_credentials_block.token.get_secret_value()
    return token_value
