from prefect import flow, task, get_run_logger
import os
from typing import Literal
import pandas as pd
import sys
from src.utils import folder_dl, file_dl, folder_ul, file_ul, get_time, CheckCCDI
from src.manifest_liftover import liftover_tags
from src.liftover_generic import liftover_to_tsv

sys.path.insert(0, os.path.abspath("./prefect-toolkit"))
from src.commons.datamodel import GetDataModel

FromAcrynomDropDown = Literal["ccdi", "cds", "c3dc", "icdc", "unknown"]
ToAcrynomDropDown = Literal["ccdi", "cds", "c3dc", "icdc"]


@task(name="extract lift to tag in mapping file", log_prints=True)
def lift_to_tag_in_mapping(liftover_mapping_path: str) -> str:
    """Get the lift to tag from the mapping file.

    Args:
        liftover_mapping_path (str): path to the mapping file

    Returns:
        str: lift to tag
    """
    mapping_df = pd.read_csv(liftover_mapping_path, sep="\t")
    lift_to = mapping_df["lift_to_version"].dropna().unique().tolist()
    if len(lift_to) > 1:
        print(f"More than one lift to versions were found in mapping file: {*lift_to,}")
        raise ValueError(
            f"More than one lift to versions were found in mapping file: {*lift_to,}"
        )
    else:
        pass
    return lift_to[0]


@task(name="Parse CCDI manifest to tsv sets", log_prints=True)
def parse_ccdi_manifest_to_tsv(manifest_path: str) -> str:
    """Read the CCDI metadata manifest excel file and generate a set of tsv files that are not empty

    Args:
        manifest_path (str): File path of CCDI metadata manifest

    Returns:
        str: a folder name contains a set of tsv files derived from CCDI metadata manifest
    """
    manifest_obj = CheckCCDI(manifest_path)
    sheet_names = manifest_obj.get_sheetnames()
    nodes_to_ignore = ["README and INSTRUCTIONS", "Dictionary", "Terms and Value Sets"]
    sheet_names_readable = [i for i in sheet_names if i not in nodes_to_ignore]
    output_folder = os.path.basename(manifest_path).split(".")[0] + "_tsv_files"
    os.makedirs(output_folder, exist_ok=True)
    for sheet in sheet_names_readable:
        sheet_df = manifest_obj.read_sheet_na(sheetname=sheet)
        sheet_df.drop(['type'], axis=1, inplace=True)
        sheet_df.dropna(how='all', inplace=True)
        # if the dataframe is not empty
        if not sheet_df.empty:
            sheet_filename = sheet + ".tsv"
            sheet_filepath = os.path.join(output_folder, sheet_filename)
            sheet_columns = sheet_df.columns.tolist()
            col_keep = []
            for col in sheet_columns:
                if col == "id" or col.endswith(".id"):
                    pass
                else:
                    col_keep.append(col)
            sheet_df = sheet_df[col_keep]
            # insert type column back to the df
            sheet_df.insert(0, "type", [sheet]*sheet_df.shape[0])
            sheet_df.to_csv(sheet_filepath, sep="\t", index=False)
            print(f"Saves sheet {sheet} to {sheet_filepath}")
        else:
            pass
    return output_folder

@flow(name="Submission liftover", log_prints=True)
def submission_liftover(
    bucket: str,
    submission_path: str,
    lift_from_acronym: FromAcrynomDropDown,
    lift_from_tag: str,
    lift_to_acronym: ToAcrynomDropDown,
    lift_to_tag: str,
    liftover_mapping_filepath: str,
    runner: str,
) -> None:
    """A generalized liftover pipeline that can liftover a set of submission tsv files to another version of submission tsv files.

    Args:
        bucket (str): bucket name
        submission_path (str): folder path contains a set of tsv files under bucket OR file path of CCDI metadata manifest , e.g. "submissions/submission_tsv_files/"
        lift_from_acronym (AcrynomDropDown): lift from acronym. Choose one from the dropdown list
        lift_from_tag (str): tag of lift from. This can be left empty if lift_from_acronym is UNKNOWN
        lift_to_acronym (AcrynomDropDown): lift to acronym. Choose one from the dropdown list
        lift_to_tag (str): tag of lift to
        liftover_mapping_filepath (str): Mapping file path under bucket, e.g., "mapping_files/ccdi_2.1.0_to_cds_6.0.2_mapping.tsv"
        runner (str): unique runner identifier
    """
    logger = get_run_logger()

    getmodel = GetDataModel()

    # downloadi lift to models files first
    # download the lift to model and props file. Then rename them
    lift_to_model_file, lift_to_props_file = getmodel.dl_model_files(
        commons_acronym=lift_to_acronym, tag=lift_to_tag
    )
    logger.info(
        f"downloaded lift to model file and props file: {lift_to_model_file}, {lift_to_props_file}"
    )
    os.rename(lift_to_model_file, f"{lift_to_tag}_{lift_to_model_file}")
    os.rename(lift_to_props_file, f"{lift_to_tag}_{lift_to_props_file}")
    lift_to_model_file = f"{lift_to_tag}_{lift_to_model_file}"
    lift_to_props_file = f"{lift_to_tag}_{lift_to_props_file}"
    logger.info(
        f"Model files have been renamed into: {lift_to_tag}_{lift_to_model_file} and {lift_to_tag}_{lift_to_props_file}"
    )

    if lift_from_acronym != "unknown":
        # download the lift from model and props file. Then rename them
        lift_from_model_file, list_from_props_file = getmodel.dl_model_files(
            commons_acronym=lift_from_acronym, tag=lift_from_tag
        )
        logger.info(
            f"downloaded lift from model file and props file: {lift_from_model_file}, {list_from_props_file}"
        )
        os.rename(lift_from_model_file, f"{lift_from_tag}_{lift_from_model_file}")
        os.rename(list_from_props_file, f"{lift_from_tag}_{list_from_props_file}")
        lift_from_model_file = f"{lift_from_tag}_{lift_from_model_file}"
        list_from_props_file = f"{lift_from_tag}_{list_from_props_file}"
        logger.info(
            f"Model files have been renamed into: {lift_from_tag}_{lift_from_model_file} and {lift_from_tag}_{list_from_props_file}"
        )
    else:
        logger.info(
            "You didn't provided a lift from acronym. No model or props file of lift from will be downloaded"
        )

    # list all the files and directories in the current directory
    # logger.info(f"all the files in current directory: {*os.listdir(),}")

    # if lift_from_acronym is ccdi, download the manifest file
    if lift_from_acronym == "ccdi":
        file_dl(bucket=bucket, filename=submission_path)
        ccdi_manifest = os.path.basename(submission_path)
        logger.info(f"Downloaded CCDI manifest file {ccdi_manifest} from bucket {bucket}")
        # parse ccdi manifest into a set of tsv files
        submission_path = parse_ccdi_manifest_to_tsv(manifest_path=ccdi_manifest)
    # if the lift_from_acronym is else, download the set of submission files
    else:
        # download the set of submission files
        folder_dl(bucket=bucket, remote_folder=submission_path)
        logger.info(
            f"Downloaded submission files folder from bucket {bucket}: {submission_path}"
        )

    # download mapping file
    file_dl(bucket=bucket, filename=liftover_mapping_filepath)
    mapping_file = os.path.basename(liftover_mapping_filepath)
    logger.info(f"Downloaded mapping file {mapping_file} from bucket {bucket}")

    if lift_from_acronym != "unknown":
        # check if the tag version in the mapping file matches to
        mapping_lift_from_tag, mapping_lift_to_tag = liftover_tags(
            liftover_mapping_path=mapping_file
        )
        if mapping_lift_from_tag != lift_from_tag or mapping_lift_to_tag != lift_to_tag:
            logger.error(
                f"""Mapping file contains tags that do not match to what you provided:
- mapping file lift from tag {mapping_lift_from_tag}
- provided lift from tag {lift_from_tag}
- mapping file lift to tag {mapping_lift_to_tag}
- provided lift to tag {lift_to_tag}"""
            )
            raise ValueError(
                f"Mapping file {mapping_file} contains tags that do not match the provided tags."
            )
        else:
            logger.info(
                f"Tags found in mapping file {mapping_file} match the lift from tag {lift_from_tag} and lift to tag {lift_to_tag}"
            )
    else:
        mapping_lift_to_tag = lift_to_tag_in_mapping(liftover_mapping_path=mapping_file)
        if mapping_lift_to_tag != lift_to_tag:
            logger.error(
                f"""Mapping file contains tags that do not match to what you provided:
- mapping file lift to tag {mapping_lift_to_tag}
- provided lift to tag {lift_to_tag}"""
            )
            raise ValueError(
                f"Mapping file {mapping_file} contains tags that do not match the provided tags."
            )
        else:
            logger.info(
                f"Tag found in mapping file {mapping_file} match the lift to tag {lift_to_tag}"
            )

    liftover_output, logger_file_name = liftover_to_tsv(
        mapping_file=mapping_file,
        submission_folder=submission_path,
        lift_to_model=lift_to_model_file,
        lift_to_props=lift_to_props_file,
    )
    upload_path = os.path.join(runner, "liftover_pipeline_output_" + get_time())
    folder_ul(
        bucket=bucket,
        local_folder=liftover_output,
        destination=upload_path,
        sub_folder="",
    )
    file_ul(
        bucket=bucket,
        output_folder=upload_path,
        sub_folder="",
        newfile=logger_file_name,
    )
    logger.info(
        f"Uploaded liftover output folder {liftover_output} to bucket {bucket} at {upload_path}"
    )
    logger.info("Liftover pipeline completed successfully.")

    return None
