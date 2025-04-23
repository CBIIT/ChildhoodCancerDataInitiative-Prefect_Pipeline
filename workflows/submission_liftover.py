from prefect import flow, task, get_run_logger
import os
from typing import Literal
import sys
from src.utils import folder_dl, file_dl, folder_ul, file_ul, get_date
from src.manifest_liftover import liftover_tags
from src.liftover_generic import liftover_to_tsv

sys.path.insert(0, os.path.abspath("./prefect-toolkit"))
from src.commons.datamodel import GetDataModel, ReadDataModel

AcrynomDropDown = Literal["ccdi", "cds"]

@flow(name="Submission liftover", log_prints=True)
def submission_liftover(
    bucket: str,
    submission_path: str,
    lift_from_acronym: AcrynomDropDown,
    lift_from_tag: str,
    lift_to_acronym: AcrynomDropDown,
    lift_to_tag: str,
    liftover_mapping_filepath: str,
    runner: str,
) -> None:
    """A generalized liftover pipeline that can liftover 

    Args:
        bucket (str): bucket name
        submission_path (str): path to the submission file(s) under bucket, e.g. "submissions/submission_tsv_files/"
        lift_from_acronym (AcrynomDropDown): lift from acronym
        lift_from_tag (str): tag of lift from
        lift_to_acronym (AcrynomDropDown): lift to acronym
        lift_to_tag (str): tag of lift to
        liftover_mapping_filepath (str): Mapping file path under bucket, e.g., "mapping_files/ccdi_2.1.0_to_cds_6.0.2_mapping.tsv"
        runner (str): unique runner identifier
    """     
    logger = get_run_logger()

    getmodel = GetDataModel()

    # download the lift from model and props file. Then rename them
    lift_from_model_file, list_from_props_file = getmodel.dl_model_files(commons_acronym=lift_from_acronym, tag=lift_from_tag)
    logger.info(f"downloaded lift from model file and props file: {lift_from_model_file}, {list_from_props_file}")
    os.rename(lift_from_model_file, f"{lift_from_tag}_{lift_from_model_file}")
    os.rename(list_from_props_file, f"{lift_from_tag}_{list_from_props_file}")
    logger.info(
        f"Model files have been renamed into: {lift_from_tag}_{lift_from_model_file} and {lift_from_tag}_{list_from_props_file}"
    )
    # download the lift to model and props file. Then rename them
    lift_to_model_file, lift_to_props_file = getmodel.dl_model_files(commons_acronym=lift_to_acronym, tag=lift_to_tag)
    logger.info(f"downloaded lift to model file and props file: {lift_to_model_file}, {lift_to_props_file}")
    os.rename(lift_to_model_file, f"{lift_to_tag}_{lift_to_model_file}")
    os.rename(lift_to_props_file, f"{lift_to_tag}_{lift_to_props_file}")
    logger.info(
        f"Model files have been renamed into: {lift_to_tag}_{lift_to_model_file} and {lift_to_tag}_{lift_to_props_file}"
    )
    # list all the files and directories in the current directory
    logger.info(f"all the files in current directory: {*os.listdir(),}")

    # download the set of submission files
    folder_dl(bucket=bucket, remote_folder=submission_path)
    logger.info(f"Downloaded submission files folder from bucket {bucket}: {submission_path}")

    # download mapping file
    file_dl(bucket=bucket, filename=liftover_mapping_filepath)
    mapping_file = os.path.basename(liftover_mapping_filepath)
    logger.info(f"Downloaded mapping file {mapping_file} from bucket {bucket}")

    # check if the tag version in the mapping file matches to
    mapping_lift_from_tag, mapping_lift_to_tag = liftover_tags(liftover_mapping_path=mapping_file)
    if mapping_lift_from_tag != lift_from_tag or mapping_lift_to_tag != lift_to_tag:
        logger.error(
            f"""Mapping file contains tags that do not match to what you provided:
- mapping file lift from tag {mapping_lift_from_tag}
- provided lift from tag {lift_from_tag}
- mapping file lift to tag {mapping_lift_to_tag}
- provided lift to tag {lift_to_tag}"""
        )
        raise ValueError(f"Mapping file {mapping_file} contains tags that do not match the provided tags.")
    else:
        logger.info(
            f"Tags found in mapping file {mapping_file} match the lift from tag {lift_from_tag} and lift to tag {lift_to_tag}"
        )

    liftover_output, logger_file_name = liftover_to_tsv(
        mapping_file=mapping_file,
        submission_path=submission_path,
        lift_to_model=lift_to_model_file,
        lift_to_props=lift_to_props_file,
    )
    upload_path = os.path.join(runner, "liftover_pipeline_output_" + get_date())
    folder_ul(bucket=bucket, local_folder=liftover_output, destination=upload_path, sub_folder="")
    file_ul(bucket=bucket, output_folder=upload_path, sub_folder="", newfile = logger_file_name)
    logger.info(f"Uploaded liftover output folder {liftover_output} to bucket {bucket} at {upload_path}")
    logger.info("Liftover pipeline completed successfully.")

    return None
