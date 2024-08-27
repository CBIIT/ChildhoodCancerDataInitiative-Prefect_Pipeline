from prefect import flow, get_run_logger
import sys
import os

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.utils import get_time, folder_dl, CCDI_Tags, file_ul
from src.join_tsv_to_manifest import (
    join_tsv_to_manifest_single_study,
    multi_studies_tsv_join,
    check_subfolder,
)


@flow(
    name="JoinRy tsv to manifest",
    log_prints=True,
    flow_run_name="joinry-tsv-to-manifest-{runner}-" + f"{get_time()}",
)
def join_tsv_to_manifest(
    bucket: str,
    runner: str,
    tsv_folder_path: str,
    ccdi_template_tag: str
    
) -> None:
    """Pipeline that combines a folder of tsv files into a single CCDI manifest

    Args:
        bucket (str): Bucket name of where tsv files located in and output goes to
        runner (str): Unique runner name
        tsv_folder_path (str): Folder path of tsv files in the bucket 
        ccdi_template_tag (str): Tag name of CCDI manifest
        

    Raises:
        ValueError: Value Error raised if pipeline fails to proceed
    """      
    logger = get_run_logger()
    current_time = get_time()

    # download the ccdi manifest of a given tag
    logger.info("")
    ccdi_manifest = CCDI_Tags().download_tag_manifest(
        tag=ccdi_template_tag, logger=logger
    )
    if ccdi_manifest is None:
        raise ValueError(
            "Failed to download CCDI manifest from github repo. No further execution."
        )
    else:
        pass

    # download the folder
    logger.info("Downloading folder from bucket")
    folder_dl(bucket=bucket, remote_folder=tsv_folder_path)

    # decide if the downloaded folder has multiple subfolder structure
    # subfolder structure applies to
    folder_check = check_subfolder(folder_path=tsv_folder_path, logger=logger)

    if folder_check == "single":
        logger.info(f"Folder {tsv_folder_path} contains tsv files only")
        file_list = [os.path.join(tsv_folder_path, i) for i in os.listdir(tsv_folder_path) if i.endswith("tsv")]
        manifest_output =  join_tsv_to_manifest_single_study(file_list = file_list, manifest_path=ccdi_manifest)
        manifest_output_list = []
        manifest_output_list.append(manifest_output)
        logger.info(f"CCDI manifest {manifest_output} generated ")
    elif folder_check == "multiple":
        logger.info(f"Folder {tsv_folder_path} contains subfolders of multiple studies")
        subfolders_list = [
            os.path.join(tsv_folder_path, i)
            for i in os.listdir(tsv_folder_path)
            if os.path.isdir(os.path.join(tsv_folder_path, i))
        ]
        manifest_output_list = multi_studies_tsv_join(folder_path_list = subfolders_list, manifest_path=ccdi_manifest)
        logger.info(f"List of CCDI manifests generated: {*manifest_output_list,}")
    else:
        pass

    # upload outputs to the bucket
    output_folder = os.path.join(runner, "join_tsv_to_manifest_" + current_time)
    logger.info(f"Output(s) will be uploaded to bucket {bucket} folder {output_folder}")
    for k in manifest_output_list:
        logger.info(f"Uploading {k}")
        file_ul(bucket=bucket, output_folder=output_folder, sub_folder="", newfile = k)
    logger.info("workflow finished!")

    return None
