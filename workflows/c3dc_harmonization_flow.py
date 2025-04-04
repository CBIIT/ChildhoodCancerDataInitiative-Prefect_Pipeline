from prefect import flow, task, get_run_logger
import os
import sys
import shutil
import subprocess
from pathlib import Path
import requests
import json
from urllib.request import urlopen
from zipfile import ZipFile
from io import BytesIO
import tempfile
from shutil import copy

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)

from src.c3dc_json_summary import create_c3dc_json_summaries
from src.c3dc_json_to_tsv import process_json_files, get_datamodel, get_data_subdirectories
from src.utils import folder_dl, folder_ul, get_time, get_logger, get_date, file_ul

@task(name="Download C3DC Model Files", log_prints=True)
def download_c3dc_model(model_tag: str = "") -> tuple[str, str]:
    """Download c3dc model files from c3dc-model github repo

    Args:
        model_tag (str, optional): model tag to download. Defaults to "".
    
    Returns:
        tuple[str, str]: Returns model file names
    """
    c3dc_dict = {
        "repo": "https://github.com/CBIIT/c3dc-model",
        "model_yaml": "model-desc/c3dc-model.yml",
        "props_yaml": "model-desc/c3dc-model-props.yml",
        "tags_api": "https://api.github.com/repos/CBIIT/c3dc-model/tags",
        "master_zipball": "https://github.com/CBIIT/c3dc-model/archive/refs/heads/main.zip",
    }
    if model_tag != "":
        # get all tag names
        tag_api_link = c3dc_dict["tags_api"]
        tag_api_return = requests.get(tag_api_link)
        tag_re_list = tag_api_return.json()
        all_tags = [i["name"] for i in tag_re_list]
        if model_tag not in all_tags:
            raise ValueError(f"Model tag {model_tag} not found. Please check the model tag and try again. Here is the list of all tags available: {*all_tags,}")
        else:
            pass
        tag_item = [i for i in tag_re_list if i["name"] == model_tag][0]
        zipurl = tag_item["zipball_url"]
    else:
        zipurl = c3dc_dict["master_zipball"]
    model_file_relpath = c3dc_dict["model_yaml"]
    props_file_relpath = c3dc_dict["props_yaml"]
    # download the zip file
    http_response = urlopen(zipurl)
    zipfile = ZipFile(BytesIO(http_response.read()))
    # create a temp dir to download the zipfile
    tempdirobj = tempfile.TemporaryDirectory(suffix="_github_dl")
    tempdir = tempdirobj.name
    zipfile.extractall(path=tempdir)
    try:
        copy(
            os.path.join(tempdir, os.listdir(tempdir)[0], model_file_relpath),
            os.path.basename(model_file_relpath),
        )
        copy(
            os.path.join(tempdir, os.listdir(tempdir)[0], props_file_relpath),
            os.path.basename(props_file_relpath),
        )
    except FileNotFoundError as err:
        raise FileNotFoundError(f"File not found: {repr(err)}")
    except Exception as e:
        raise FileNotFoundError(
            f"Error occurred downloading data model files: {repr(e)}"
        )
    return (
        os.path.basename(model_file_relpath),
        os.path.basename(props_file_relpath),
    )


@task(name="Extract transformed tsv files", log_prints=True)
def extract_tsv_outputs(folderpath: str, output_folder_name: str) -> str:
    """Extract tsv files from the data folder

    Args:
        folderpath (str): data folder that contains original json files and tsv outputs

    Returns:
        str: output folder name
    """     
    # create a folder that holds the tsv outputs
    current_dir = os.getcwd()
    tsv_output_folder = os.path.join(current_dir, output_folder_name)
    os.makedirs(tsv_output_folder, exist_ok=True)

    # copy all tsv files to the output folder
    for file in os.listdir(folderpath):
        if file.endswith(".tsv"):
            file_study=file.split(" ")[0]
            # create a sunfolder for each study under output_folder_name
            study_folder = os.path.join(tsv_output_folder, file_study)
            os.makedirs(study_folder, exist_ok=True)
            # copy the tsv file to the study folder
            shutil.copy(os.path.join(folderpath, file), study_folder)
        else:
            pass

    return tsv_output_folder


@flow(name="C3DC Harmonization Flow", log_prints=True)
def c3dc_summary_transformation_flow(
    bucket: str,
    json_folder_path: str,
    runner: str,
    c3dc_model_tag: str
):
    """Prefect pipelien to run c3dc summary and transform harmonized json files to tsv files.

    Args:
        bucket (str): Bucker name for folder downloading and output uploading
        json_folder_path (str): Folder path under the bucket which contains haronized json file(s). Each study has a subfolder under this path
        runner (str): unique runner name of the flow
        c3dc_model_tag (str): c3dc model tag to be used to download the model files
    """    
    logger = get_run_logger()
    logger.info("Starting C3DC Harmonization Flow")

    # download the json data from
    folder_dl(bucket=bucket, remote_folder=json_folder_path)
    logger.info(f"Downloaded json data from bucket {bucket} folder path {json_folder_path}")
    folder_name = os.path.basename(json_folder_path)
    # rename folder name to data/
    folder_name.rename("data")

    # create a folder that holds the summary outputs
    current_dir = os.getcwd()
    json_summary_folder = os.path.join(current_dir, "json_summary")
    os.makedirs(json_summary_folder, exist_ok=True)

    # create summary for json file per study
    # json_summary_folder holds all the summary files
    logger.info(f"Creating json summaries for harmonized json files")
    create_c3dc_json_summaries(folder_path=folder_name, output_dir=json_summary_folder)
    logger.info("Uploading json summaries to s3 bucket")
    upload_folder = runner.rstrip("/") + "/" + "c3dc_transformation_outputs_" + get_time()
    folder_ul(bucket=bucket, local_folder=json_summary_folder, destination=upload_folder, sub_folder="")
    logger.info(f"Uploaded json summaries to s3 bucekt {bucket} folder path {upload_folder}")

    # download c3dc model files
    logger.info(f"Downloading c3dc model files")
    model_yaml, props_yaml = download_c3dc_model(model_tag=c3dc_model_tag)
    logger.info(f"Downloaded c3dc model files {model_yaml} and {props_yaml}")
    # get the mdf model object
    model = get_datamodel(model_mdf_path=model_yaml, props_mdf_path=props_yaml)
    dir_paths = get_data_subdirectories("./data")
    logger.info(f"Found {len(dir_paths)} subdirectories\n")
    # start transforming json files to tsv files
    logger.info(f"Transforming json files to tsv files")
    transform_logger = get_logger(logger_name="transform_logger", log_level="INFO")
    transform_logger_filename = (
        "transform_logger_" + get_date() + ".log"
    )
    process_json_files(dir_paths, model, transform_logger)

    # extract tsv files from the data folder
    logger.info(f"Extracting tsv files from the data folder")
    tsv_output_folder = extract_tsv_outputs(folderpath="./data", output_folder_name="transformed_tsv")
    logger.info(f"Extracted tsv files to {tsv_output_folder}")
    # upload the tsv files to the bucket
    logger.info(f"Uploading tsv files to bucket {bucket} folder path {tsv_output_folder}")
    folder_ul(bucket=bucket, local_folder=tsv_output_folder, destination=upload_folder, sub_folder="")
    logger.info(f"Uploaded tsv files to bucket {bucket} folder path {upload_folder}")
    # upload transform logger to the bucket
    file_ul(
        bucket=bucket,
        output_folder=os.path.join(upload_folder, "transformed_tsv"),
        sub_folder="",
        newfile=transform_logger_filename,
    )

    logger.info("C3DC Harmonization Flow completed successfully")
