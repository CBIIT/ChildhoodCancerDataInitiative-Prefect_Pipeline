from prefect import flow, task, get_run_logger
import os
from typing import Literal
import sys
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

    lift_from_model_file, list_from_props_file = getmodel.dl_model_files(commons_acronym=lift_from_acronym, tag=lift_from_tag)
    logger.info(f"downloaded lift from model file and props file: {lift_from_model_file}, {list_from_props_file}")
    lift_to_model_file, lift_to_props_file = getmodel.dl_model_files(commons_acronym=lift_to_acronym, tag=lift_to_tag)
    logger.info(f"downloaded lift to model file and props file: {lift_to_model_file}, {lift_to_props_file}")

    return None
