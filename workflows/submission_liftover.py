from prefect import flow, task, get_run_logger
import os
from typing import Literal
import pandas as pd
import sys
from src.utils import folder_dl, file_dl, folder_ul, file_ul, get_time, CheckCCDI, CCDI_DCC_Tags
from src.manifest_liftover import liftover_tags
from src.liftover_generic import liftover_to_tsv

sys.path.insert(0, os.path.abspath("./prefect-toolkit"))
from src.commons.datamodel import GetDataModel

FromAcrynomDropDown = Literal["ccdi", "cds", "c3dc", "icdc", "ctdc", "ccdi_dcc", "unknown"]
ToAcrynomDropDown = Literal["ccdi", "cds", "c3dc", "icdc", "ctdc", "ccdi_dcc"]


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
        submission_path (str): folder path contains a set of tsv files under bucket, e.g. "submissions/submission_tsv_files/" OR file path of CCDI metadata manifest (The lift_from_acrynom must be CCDI in this case)
        lift_from_acronym (AcrynomDropDown): lift from acronym. Choose one from the dropdown list
        lift_from_tag (str): tag of lift from. This can be left empty if lift_from_acronym is UNKNOWN
        lift_to_acronym (AcrynomDropDown): lift to acronym. Choose one from the dropdown list
        lift_to_tag (str): tag of lift to
        liftover_mapping_filepath (str): Mapping file path under bucket, e.g., "mapping_files/ccdi_2.1.0_to_cds_6.0.2_mapping.tsv"
        runner (str): unique runner identifier
    """
    logger = get_run_logger()

    getmodel = GetDataModel()

    # define the upload path for outputs in the bucket 
    upload_path = os.path.join(runner, "liftover_pipeline_output_" + get_time())

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
        # upload the parsed tsv files to the bucket
        folder_ul(
            bucket=bucket,
            local_folder=submission_path,
            destination=upload_path,
            sub_folder="",
        )
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

    return upload_path, liftover_output


@flow(name="Generic liftover from CCDI to DCC", log_prints=True)
def submission_liftover_ccdi_to_dcc(
    bucket: str,
    runner: str,
    submission_path: str,
    liftover_mapping_filepath: str,
    lift_from_acronym: str = "ccdi",
    lift_from_tag: str = "3.1.0",
    lift_to_acronym: str = "ccdi_dcc",
    lift_to_tag: str = "1.0.0",
) -> None:
    """CCDI to DCC liftover ONLY.
    A liftover pipeline that liftover a CCDI template manifest to a DCC template manifest. 
    This pipeline is an extension of the generic liftover pipeline which handles CCDI to DCC liftover.
    Args:
        bucket (str): bucket name
        runner (str): unique runner identifier
        submission_path (str): folder path contains a set of tsv files under bucket, e.g. "submissions/submission_tsv_files/" OR file path of CCDI metadata manifest (The lift_from_acrynom must be CCDI in this case)
        liftover_mapping_filepath (str): Mapping file path under bucket, e.g., "mapping_files/ccdi_2.1.0_to_cds_6.0.2_mapping.tsv"
        lift_from_acronym (AcrynomDropDown): lift from acronym. This deployment is designed to liftover from CCDI only.
        lift_from_tag (str): tag of lift from. Default to 3.1.0
        lift_to_acronym (AcrynomDropDown): lift to acronym. This deployment is designed to liftover to CCDI-DCC only.
        lift_to_tag (str): tag of lift to. Default to 1.0.0
    """
    logger = get_run_logger()

    # run the generic liftover pipeline
    logger.info("Starting generic liftover pipeline...")
    upload_path, output_tsv_folder = submission_liftover(
        bucket=bucket,
        submission_path=submission_path,
        lift_from_acronym=lift_from_acronym,
        lift_from_tag=lift_from_tag,
        lift_to_acronym=lift_to_acronym,
        lift_to_tag=lift_to_tag,
        liftover_mapping_filepath=liftover_mapping_filepath,
        runner=runner)

    # download DCC manifest from Github repository
    logger.info("Downloading DCC manifest template...")
    dcc_manifest_template = CCDI_DCC_Tags().download_tag_manifest(tag=lift_to_tag, logger=logger)

    # read output tsv files and start copy content into DCC manifest template
    logger.info("Reading generic liftover output tsv files and populate DCC manifest template...")
    tsv_file_list = [os.path.join(output_tsv_folder, f) for f in os.listdir(output_tsv_folder) if f.endswith(".tsv")]
    for tsv in tsv_file_list:
        tsv_df = pd.read_csv(
            tsv,
            sep="\t",
            quotechar='"',
            doublequote=True,
            escapechar="\\",  # add escape char to handle special characters
            keep_default_na=False,
            na_values=[""],
        )
        # we only expect one type per tsv file
        tsv_type = tsv_df["type"].dropna().unique().tolist()[0]
        logger.info(f"Populating sheet {tsv_type} in DCC manifest template...")
        cols_not_empty = tsv_df.columns[tsv_df.notna().any()].tolist()

        # read the sheet with sheetname==type from the DCC manifest template
        # the template_tsv_df should be empty
        template_tsv_df = CheckCCDI(dcc_manifest_template).read_sheet_na(sheetname=tsv_type)
        template_tsv_df = pd.DataFrame(columns=template_tsv_df.columns) # create an emoty df using only the column names
        # filling in the non-empty columns from tsv_df to template_tsv_df
        for col in cols_not_empty:
            template_tsv_df[col] = tsv_df[col]
        logger.info(f"Sheet {tsv_type} populated with {template_tsv_df.shape[0]} rows.")

        # write template_tsv_df back to the DCC manifest template
        with pd.ExcelWriter(
            dcc_manifest_template,
            mode="a",
            engine="openpyxl",
            if_sheet_exists="overlay",
        ) as writer:
            template_tsv_df.to_excel(
                writer, sheet_name=tsv_type, index=False, header=False, startrow=1
            )
        logger.info(f"Finished populating sheet {tsv_type} in DCC manifest template.")

    # Rename the final DCC manifest tempalte file
    dcc_manifest_populated =  dcc_manifest_template.replace(".xlsx", f"_populated_{get_time()}.xlsx")
    os.rename(dcc_manifest_template, dcc_manifest_populated)
    # upload the populated DCC manifest to the bucket
    file_ul(
        bucket=bucket,
        output_folder=upload_path,
        sub_folder="",
        newfile=dcc_manifest_populated,
    )
    logger.info(
        f"Uploaded populated DCC manifest file {dcc_manifest_populated} to bucket {bucket} at {upload_path}"
    )
    logger.info("CCDI to DCC liftover pipeline completed successfully.")
    return None
