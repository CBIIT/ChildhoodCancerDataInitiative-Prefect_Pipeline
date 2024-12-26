from prefect import flow, task, get_run_logger
import os
import sys
import shutil
import subprocess
parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)

from src.utils import folder_dl, folder_ul, file_ul,get_time
from src.c3dc_json_summary import create_c3dc_json_summaries

@task(name="extract_transformed_tsv", log_prints=True)
def extract_transformed_tsv(source_tsv_folder: str, output_folder_path: str) -> None:
    """It finds all transformed tsv files from the source folder, and puts them into an output folder
    tsv files from the same study will be put in the same subfolder with a folder name of study accession

    Args:
        source_tsv_folder (str): A folder path that contains all the tsv files for all studies
        output_folder_path (str): A folder path where tsv files will be copied to
    """
    file_list = [os.path.join(source_tsv_folder, tsv_file) for tsv_file in os.listdir(source_tsv_folder) if tsv_file.endswith(".tsv")]
    for file in file_list:
        file_studyname =   os.path.basename(file).split(" ")[0]
        dest_folder = os.path.join(output_folder_path, file_studyname)
        os.makedirs(dest_folder, exist_ok=True)
        shutil.copy(file, dest_folder)
    return None


@flow(name="C3DC data summary and harmonization", log_prints=True)
def c3dc_data_summary_harmonization(bucket:str, json_folder_path: str, runner: str) -> None:
    """Pipeline that generates a summery for json harmonized file per study and converts json into a set of tsv per node

    Args:
        bucket (str): Bucket name for folder downloading and ouput uploading
        json_folder_path (str): folder path which contains harmonized json file(s) under subfolder of each study
        runner (str): unique runner name of the flow
    """    
    # create a logging object
    runner_logger = get_run_logger()

    # download the json data from
    folder_dl(bucket=bucket, remote_folder=json_folder_path)
    # folder_name = os.path.basename(json_folder_path)
    folder_name = json_folder_path

    # create a folder that holds the summary outputs
    current_dir = os.getcwd()
    json_summary_folder = os.path.join(current_dir, "json_summary")
    os.makedirs(json_summary_folder, exist_ok=True)

    # create summary for json file per study
    runner_logger.info(f"Creating json summaries for harmonized json files")
    print(folder_name)
    print(json_summary_folder)
    create_c3dc_json_summaries(folder_path=folder_name, output_dir=json_summary_folder)

    # run json to tsv transformation
    # copy json folder under folder CCDI-C3DC-Dataloader, called /data
    data_foldername = os.path.join(current_dir, "CCDI-C3DC-Dataloader", "data")
    shutil.copytree(folder_name, data_foldername)
    # create a temp folder under CCDI-C3DC-Dataloader
    os.makedirs(os.path.join(current_dir, "CCDI-C3DC-Dataloader", "tmp"), exist_ok=True)
    # change directory to CCDO-C3DC-Dataloader
    os.chdir("./CCDI-C3DC-Dataloader")
    try:
        # run the transformer.py
        runner_logger.info("Running transformer.py")
        subprocess.run(["python", "transformer/transformer.py"])
    except Exception as e:
        runner_logger.error(f"Error in running transformer.py: {e}")

    # copy the transformer log file to the current_dir
    transformer_log = [
        os.path.join("./tmp", log_file)
        for log_file in os.listdir("./tmp") if log_file.endswith(".log")
    ]
    if len(transformer_log) > 0:
        shutil.copy(transformer_log[0], current_dir)
        runner_logger.info("Transformer log file copied to the current directory")
    else:
        runner_logger.warning("No transformer log file found")

    # change directory back to the original directory
    os.chdir(current_dir)
    # extract tsv files
    transformed_tsv_foldername= "json_to_tsv"
    transformed_tsv_folder = os.path.join(current_dir, transformed_tsv_foldername)
    os.makedirs(transformed_tsv_folder, exist_ok=True)
    extract_transformed_tsv(
        source_tsv_folder=data_foldername,
        output_folder_path=transformed_tsv_folder,
    )

    # folder upload
    upload_folder_name = os.path.join(runner, "c3dc_harmonization_pipeline_" + get_time())
    runner_logger.info(f"Uploading json summary folder {json_summary_folder} and transformed tsv folder {transformed_tsv_folder} to the designated bucket")
    folder_ul(
        bucket=bucket, local_folder=json_summary_folder, destination=upload_folder_name, sub_folder=""
    )
    folder_ul(bucket=bucket, local_folder=transformed_tsv_folder, destination=upload_folder_name, sub_folder="")

    # If threre is a transformer log file, upload it as well
    if len(transformer_log) > 0:
        transformer_log = [
            os.path.join(current_dir, log_file)
            for log_file in os.listdir(current_dir) if log_file.endswith(".log")
        ]
        print(transformer_log)
        file_ul(bucket=bucket, output_folder=upload_folder_name, sub_folder="", newfile= os.path.basename(transformer_log[0]))
    else:
        pass

    # workflow completion
    runner_logger.info(f"Data summary and harmonization completed")
