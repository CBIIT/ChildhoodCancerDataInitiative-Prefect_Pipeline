import os
import pandas as pd
from prefect import flow, task, get_run_logger
from shutil import copy
from src.utils import get_date


def check_subfolder(folder_path: str, logger) -> bool:
    subfolder_list = []
    file_list = []
    for entry in os.scandir(folder_path):
        if entry.is_dir():
            subfolder_list.append(os.path.join(folder_path, entry.name))
        elif entry.name.endswith("tsv"):
            file_list.append(os.path.join(folder_path, entry.name))
        else:
            pass
    if len(file_list) == 0 and len(subfolder_list) > 0:
        logger.info(f"Path {folder_path} contains subfolders of studies")
        logger.info(f"Subfolder names: {*subfolder_list,}")
        return "multiple"
    elif len(file_list) > 0 and len(subfolder_list) == 0:
        logger.info(f"Path {folder_path} contains tsv files without subfolder")
        logger.info(f"tsv file names: {*file_list,}")
        return "single"
    else:
        logger.error(
            "Workflow expects a bucket path that either contains tsv files ONLY or subfolders containing tsv files ONLY"
        )
        logger.info(f"Subfolder names: {*subfolder_list,}")
        logger.info(f"tsv file names: {*file_list,}")
        raise ValueError(
            "Please provide a bucket folder path that only contains tsv files or subfolders"
        )


# new function to find the phs accession
def get_study_accession(file_list: list[str]) -> str:
    """Returns the study accession number

    Args:
        file_list (list[str]): list of file paths

    Returns:
        str: dbGaP accession number
    """
    for file in file_list:
        file_df = pd.read_csv(file, sep="\t")
        if file_df.loc[0, "type"] == "study":
            dbgap_accession = file_df.loc[0, "dbgap_accession"]
        else:
            pass
    try:
        dbgap_accession
        return dbgap_accession
    except NameError as err:
        raise NameError(
            f"Missing the tsv for study node among the file list: {*file_list,}"
        )


# we need a mapping dictionary that maps id(uuid str) against key property value
def create_key_id_mapping(file_list: list[str]) -> dict:
    """Returns a dictionary of id and key property.
    Loops through every tsv, and use the id column as key and [node]_id as value

    Args:
        file_list (list[str]): a list of file paths

    Returns:
        dict: dictionary of id column value as key and [node]_id column value as value
    """
    return_dict = {}
    for file in file_list:
        file_df = pd.read_csv(file, sep="\t")
        file_type = file_df.loc[0, "type"]
        file_key_prop = file_type + "_id"
        file_mapping_dict = dict(zip(file_df["id"], file_df[file_key_prop]))
        return_dict = {**return_dict, **file_mapping_dict}
    return return_dict


def find_missing_cols(tsv_cols: list, sheet_cols: list) -> list:
    missing_cols = [i for i in sheet_cols if i not in tsv_cols]
    return missing_cols


def find_id_cols(col_list: list) -> list:
    id_cols = [i for i in col_list if i.endswith(".id")]
    return id_cols


def find_parent_id_cols(id_cols: list) -> list:
    parent_names = [i.split(".")[0] for i in id_cols]
    extended_parent_names = [i + "." + i + "_id" for i in parent_names]
    return extended_parent_names


def unpack_folder_list(folder_path_list: list[str]):
    unpacked_folder_list = []
    for i in folder_path_list:
        if os.path.isdir(i):
            i_files = os.listdir(i)
            i_files_path = [os.path.join(i, j) for j in i_files]
            unpacked_folder_list.append(i_files_path)
        else:
            pass
    return unpacked_folder_list


@task(name="Join tsv to Manifest", log_prints=True)
def join_tsv_to_manifest_single_study(file_list: list[str], manifest_path: str) -> str:
    logger = get_run_logger()
    # Remove the step of checking if only one phs is found under id column
    # Because the
    # study_accession = check_same_study_files(file_list=file_list, logger=logger)
    study_accession = get_study_accession(file_list=file_list)
    logger.info(f"Creating CCDI manifest for study {study_accession}")
    logger.info(f"tsv files will be written to manifest: {len(file_list)}")
    output_file_name = (
        os.path.basename(manifest_path)[:-5]
        + "_"
        + study_accession
        + "_JoinRy_"
        + get_date()
        + ".xlsx"
    )
    copy(manifest_path, output_file_name)

    # output_file = pd.ExcelFile(output_file_name)
    # create key prop and id(guid) mapping dict
    key_id_mapping = create_key_id_mapping(file_list=file_list)
    print(key_id_mapping)

    for tsv_file in file_list:
        logger.info(f"working on tsv file: {tsv_file}")
        tsv_df = pd.read_csv(tsv_file, sep="\t")
        if "study" in tsv_df.columns:
            tsv_df.drop(columns=["study"], inplace=True)
        else:
            pass
        # find the node type of tsv file
        node_type = tsv_df["type"].tolist()[0]
        logger.info(f"tsv file node type: {node_type}")
        # read the node sheet in ccdi manifest
        manifest_df = pd.read_excel(
            output_file_name,
            sheet_name=node_type,
            na_values=["NA", "na", "N/A", "n/a", ""],
            dtype="string",
        )

        # check if all columns in sheet can be found in tsv
        # and add the missing column in the tsv df
        missing_cols = find_missing_cols(
            tsv_cols=tsv_df.columns.tolist(), sheet_cols=manifest_df.columns.tolist()
        )
        logger.info(f"cols in sheet not found in tsv: {*missing_cols,}")
        if len(missing_cols) > 0:
            for i in missing_cols:
                tsv_df[i] = ""
        else:
            pass
        # copy [parent].id column to [parent].[parent]_id col
        # and remove content of [parent].id
        id_cols = find_id_cols(col_list=tsv_df.columns.tolist())
        logger.info(f"tsv parent id cols: {*id_cols,}")
        parent_id_cols = find_parent_id_cols(id_cols=id_cols)
        logger.info(f"sheet parent id cols: {*parent_id_cols,}")
        for i in range(len(id_cols)):
            i_col = id_cols[i] # for example participant.id
            print(i_col)
            parent_i_col = i_col.split(".")[0] + "." + i_col.split(".")[0] + "_id" # participant.participant_id
            tsv_df[parent_i_col] = [
                key_id_mapping[j] if not (pd.isna(j) or j=="") else j
                for j in tsv_df[i_col].tolist()
            ]
            # keep the i_col content
            # tsv_df[i_col] = ""
        # keep the content of col "id"
        # tsv_df["id"] = ""

        # reorder columns in tsv according to sheet
        tsv_df = tsv_df[manifest_df.columns.tolist()]
        # write tsv_df to excel sheet
        logger.info(f"writing the tsv df to sheet {node_type} in CCDI manifest")
        with pd.ExcelWriter(
            output_file_name, mode="a", engine="openpyxl", if_sheet_exists="overlay"
        ) as writer:
            tsv_df.to_excel(
                writer, sheet_name=node_type, index=False, header=False, startrow=1
            )

    return output_file_name


@flow(name="Join tsv to Manifest Concurrently")
def multi_studies_tsv_join(folder_path_list: list, manifest_path: str) -> list[str]:
    logger = get_run_logger()
    logger.info(f"Subfolder counts: {len(folder_path_list)}")

    unpacked_folder_list = unpack_folder_list(folder_path_list=folder_path_list)
    logger.info("Start creating manifest files concurrently")
    manifest_outputs = join_tsv_to_manifest_single_study.map(
        unpacked_folder_list, manifest_path
    )
    return [i.result() for i in manifest_outputs]
