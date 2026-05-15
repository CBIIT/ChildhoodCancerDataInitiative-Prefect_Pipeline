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
        file_df = pd.read_csv(file, sep="\t", dtype=str)  # force all columns to str
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


# we need a mapping dictionary that maps GUID(str) against key property value
def create_key_id_mapping(file_list: list[str]) -> dict:
    """Returns a dictionary of GUID and key property."""
    return_dict = {}
    for file in file_list:
        file_df = pd.read_csv(file, sep="\t", dtype=str)  # force all columns to str
        file_type = file_df.loc[0, "type"]
        file_key_prop = file_type + "_id"
        if file_key_prop not in file_df.columns:
            continue  # skip if key prop column doesn't exist
        file_mapping_dict = dict(zip(file_df["guid"], file_df[file_key_prop]))
        return_dict = {**return_dict, **file_mapping_dict}
    return return_dict


def find_missing_cols(tsv_cols: list, sheet_cols: list) -> list:
    missing_cols = [i for i in sheet_cols if i not in tsv_cols]
    return missing_cols


def find_guid_cols(col_list: list) -> list:
    guid_cols = [i for i in col_list if i.endswith(".guid")]
    return guid_cols


def find_parent_guid_cols(guid_cols: list) -> list:
    """Returns just 'guid' for each parent guid col since guid has no prefix in parent sheets"""
    return ["guid" for _ in guid_cols]


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


def map_ids(cell, mapping):
    if pd.isna(cell) or cell == "":
        return cell
    ids = [str(i).strip() for i in str(cell).split(";")]
    mapped = [str(mapping.get(i, i)) for i in ids if i]
    return ";".join(str(m) for m in mapped)  # triple str() safety net


# @task(name="Join tsv to Manifest", log_prints=True)
# def join_tsv_to_manifest_single_study(file_list: list[str], manifest_path: str) -> str:
#     logger = get_run_logger()
#     study_accession = get_study_accession(file_list=file_list)
#     logger.info(f"Creating CCDI manifest for study {study_accession}")
#     logger.info(f"tsv files will be written to manifest: {len(file_list)}")
#     output_file_name = (
#         os.path.basename(manifest_path)[:-5]
#         + "_"
#         + study_accession
#         + "_JoinRy_"
#         + get_date()
#         + ".xlsx"
#     )
#     copy(manifest_path, output_file_name)

#     key_id_mapping = create_key_id_mapping(file_list=file_list)
#     logger.info(f"Key ID mapping contains {len(key_id_mapping)} entries")

#     for tsv_file in file_list:
#         logger.info(f"working on tsv file: {tsv_file}")
#         tsv_df = pd.read_csv(tsv_file, sep="\t", dtype=str)

#         if "study" in tsv_df.columns:
#             tsv_df.drop(columns=["study"], inplace=True)

#         node_type = tsv_df["type"].tolist()[0]
#         logger.info(f"tsv file node type: {node_type}")

#         manifest_df = pd.read_excel(
#             output_file_name,
#             sheet_name=node_type,
#             na_values=["NA", "na", "N/A", "n/a", ""],
#             dtype="string",
#         )

#         missing_cols = find_missing_cols(
#             tsv_cols=tsv_df.columns.tolist(), sheet_cols=manifest_df.columns.tolist()
#         )
#         logger.info(f"cols in sheet not found in tsv: {*missing_cols,}")
#         for i in missing_cols:
#             tsv_df[i] = ""

#         # Find guid columns and perform mapping
#         guid_cols = find_guid_cols(col_list=tsv_df.columns.tolist())
#         logger.info(f"tsv parent guid cols: {*guid_cols,}")
#         parent_guid_cols = find_parent_guid_cols(guid_cols=guid_cols)
#         logger.info(f"sheet parent guid cols: {*parent_guid_cols,}")

#         for i in range(len(guid_cols)):
#             i_col = guid_cols[i]  # e.g. participant.guid
#             parent_node = i_col.split(".")[0]  # e.g. participant
#             parent_i_col = f"{parent_node}.{parent_node}_id"  # e.g. participant.participant_id

#             logger.info(f"Mapping column {i_col} -> {parent_i_col}")

#             mapped_values = []
#             for j in tsv_df[i_col].tolist():
#                 if j is None or (isinstance(j, float) and pd.isna(j)) or str(j).strip() in ("", "nan", "None"):
#                     mapped_values.append("")
#                     continue

#                 guids = [g.strip() for g in str(j).split(";") if g.strip()]
#                 mapped = []
#                 for guid in guids:
#                     result = key_id_mapping.get(guid)
#                     if result is None:
#                         logger.warning(f"GUID not found in mapping: '{guid}' for col={i_col} in {node_type}")
#                         mapped.append(guid)
#                     else:
#                         mapped.append(str(result))
#                 mapped_values.append(";".join(mapped))

#             tsv_df[parent_i_col] = mapped_values
#             tsv_df[i_col] = ""  # clear guid column after mapping

#         tsv_df["guid"] = ""

#         # Reorder columns in tsv according to sheet
#         tsv_df = tsv_df[manifest_df.columns.tolist()]

#         logger.info(f"writing the tsv df to sheet {node_type} in CCDI manifest")
#         with pd.ExcelWriter(
#             output_file_name, mode="a", engine="openpyxl", if_sheet_exists="overlay"
#         ) as writer:
#             tsv_df.to_excel(
#                 writer, sheet_name=node_type, index=False, header=False, startrow=1
#             )

#     return output_file_name


# REWRITE
def build_guid_to_id_mapping(file_list: list[str]) -> dict:
    """
    Builds a mapping of guid -> [node]_id from all TSV files.
    e.g. {"abc-123-guid": "participant_001", "def-456-guid": "sample_001"}
    """
    mapping = {}
    for file in file_list:
        df = pd.read_csv(file, sep="\t", dtype=str)
        node_type = df["type"].iloc[0]
        id_col = f"{node_type}_id"
        if "guid" in df.columns and id_col in df.columns:
            for _, row in df.iterrows():
                guid = row["guid"]
                node_id = row[id_col]
                if pd.notna(guid) and pd.notna(node_id):
                    mapping[str(guid).strip()] = str(node_id).strip()
    return mapping


@task(name="Join tsv to Manifest", log_prints=True)
def join_tsv_to_manifest_single_study(file_list: list[str], manifest_path: str) -> str:
    logger = get_run_logger()
    study_accession = get_study_accession(file_list=file_list)
    logger.info(f"Creating CCDI manifest for study {study_accession}")

    output_file_name = (
        os.path.basename(manifest_path)[:-5]
        + "_"
        + study_accession
        + "_JoinRy_"
        + get_date()
        + ".xlsx"
    )
    copy(manifest_path, output_file_name)

    # Build guid -> [node]_id mapping from all TSV files
    guid_to_id = build_guid_to_id_mapping(file_list=file_list)
    logger.info(f"Built guid->id mapping with {len(guid_to_id)} entries")

    for tsv_file in file_list:
        logger.info(f"Working on tsv file: {tsv_file}")
        tsv_df = pd.read_csv(tsv_file, sep="\t", dtype=str)

        if "study" in tsv_df.columns:
            tsv_df.drop(columns=["study"], inplace=True)

        node_type = tsv_df["type"].iloc[0]
        logger.info(f"Node type: {node_type}")

        manifest_df = pd.read_excel(
            output_file_name,
            sheet_name=node_type,
            na_values=["NA", "na", "N/A", "n/a", ""],
            dtype="string",
        )

        # Add any missing columns from the manifest sheet
        missing_cols = find_missing_cols(
            tsv_cols=tsv_df.columns.tolist(),
            sheet_cols=manifest_df.columns.tolist()
        )
        logger.info(f"Cols in sheet not found in tsv: {*missing_cols,}")
        for col in missing_cols:
            tsv_df[col] = ""

        # Find all [parent_node].guid columns
        parent_guid_cols = [c for c in tsv_df.columns if c.endswith(".guid")]
        logger.info(f"Parent guid cols found: {*parent_guid_cols,}")

        for parent_guid_col in parent_guid_cols:
            # e.g. parent_guid_col = "participant.guid"
            parent_node = parent_guid_col.split(".")[0]  # e.g. "participant"
            target_col = f"{parent_node}.{parent_node}_id"  # e.g. "participant.participant_id"

            logger.info(f"Mapping {parent_guid_col} -> {target_col}")

            mapped_values = []
            for cell in tsv_df[parent_guid_col].tolist():
                # Handle empty/null values
                if cell is None or str(cell).strip() in ("", "nan", "None", "NaN"):
                    mapped_values.append("")
                    continue

                # Handle semicolon-separated multiple GUIDs
                guids = [g.strip() for g in str(cell).split(";") if g.strip()]
                mapped = []
                for guid in guids:
                    result = guid_to_id.get(guid)
                    if result is None:
                        logger.warning(f"No mapping found for guid '{guid}' in col '{parent_guid_col}' for node '{node_type}'")
                        mapped.append("")
                    else:
                        mapped.append(result)
                mapped_values.append(";".join(mapped))

            # Write mapped IDs to target column, creating it if it doesn't exist
            tsv_df[target_col] = mapped_values
            logger.info(f"Wrote {len([v for v in mapped_values if v])} mapped values to {target_col}")

            # Clear the [parent_node].guid column
            tsv_df[parent_guid_col] = ""

        # Clear the current node's guid column
        if "guid" in tsv_df.columns:
            tsv_df["guid"] = ""

        # Reorder columns to match manifest sheet
        tsv_df = tsv_df[manifest_df.columns.tolist()]

        logger.info(f"Writing node '{node_type}' to manifest sheet")
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
