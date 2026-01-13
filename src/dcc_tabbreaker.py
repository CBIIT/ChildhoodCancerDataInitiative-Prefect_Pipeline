from prefect import flow, task
import sys
import os
import pandas as pd
import json
import uuid
import hashlib
from src.utils import get_logger, get_time, get_date, CheckCCDI
from src.s3_ccdi_to_tabbreakery import get_uuid


def get_dcc_namespace():
    hex_string = hashlib.md5("ccdi-dcc".encode("UTF-8")).hexdigest()
    dcc_namespace = uuid.UUID(hex=hex_string)
    return dcc_namespace


def get_dcc_uuid(x: str, study_id: str, node_name: str) -> str:
    """Generates uuid value for DCC using DCC based namespace

    Args:
        x (str): input string value, such as "PCT_001"
        study_id (str): study id value, such as "phs000123"
        node_name (str): data node label, such as "participant"

    Returns:
        str: generated DCC uuid
    """
    if pd.isna(x):
        return x
    else:
        dcc_namespace = get_dcc_namespace()
        x_str_input = study_id + "::" + node_name + "::" + x
        return get_uuid(uuid_namespace=dcc_namespace, id_str=x_str_input)


@flow(
    name="DCC_TabBreaker",
    flow_run_name="DCC_TabBreaker_" + f"{get_time()}",
)
def tabBreakeRy_dcc(manifest: str) -> tuple:
    logger = get_logger(loggername="DCC_TabBreaker", log_level="info")
    output_log = "DCC_TabBreaker_" + get_date() + ".log"

    try:
        manifest_f = pd.ExcelFile(manifest)
        logger.info(f"Reading the validated DCC manifest {manifest}")
    except FileNotFoundError as err:
        logger.error(err)
        sys.exit()
    except ValueError as err:
        logger.error(err)
        sys.exit()
    except:
        logger.error(f"Issue occurred while openning file {manifest}")
        sys.exit()

    time_now = get_time()
    output_folder = "DCC_TabBreaker_" + time_now
    output_tsv_folder = output_folder + "/tsvs"
    os.makedirs(output_tsv_folder, exist_ok=True)
    logger.info(f"Created output folder {output_folder}")

    check_dcc = CheckCCDI(ccdi_manifest=manifest)

    # Read in Dictionary page to obtain the required properties
    dict_df = check_dcc.get_dict_df()
    dict_nodes = dict_df["Node"].unique()

    # get versioin of the manifest
    github_curr_ver = "v" + check_dcc.get_version()
    logger.info(f"DCC manifest version is {github_curr_ver}")

    # get study if of the manifest
    project_id = check_dcc.get_study_id()
    logger.info(f"DCC manifest study id is {project_id}")

    # get keys of every node/sheet
    keys = dict_df[dict_df["Key"] == 1]["Property"].tolist()

    for node in dict_nodes:
        logger.info(f"Processing node {node}")
        df = check_dcc.read_sheet_na(sheetname=node)
        df["type"] = node

        # To ensure unique ids across multiple studies,
        # we are pre-appending the study_id onto all key_ids and
        # their linkages.
        logger.info("Generating uuid values for key and linkage columns")
        for column in df.columns:
            if column in keys:
                df["uuid"] = df[column].apply(
                    lambda x: get_dcc_uuid(x=x, study_id=project_id, node_name=node)
                )
            elif column[-5:] != ".guid" and "." in column:
                prev_node = column.split(".")[0]
                node_id = prev_node + ".guid"
                df[node_id] = df[column].apply(
                    lambda x: get_dcc_uuid(x=x, study_id=project_id, node_name=prev_node)
                )
            else:
                pass

        # Remove the old linking properties [node].[node]_id,
        # as that will cause issues in the data loader
        logger.info("Removing old linkage key property columns, such as study.study_id...")
        columns_to_drop = []
        for i in df.columns:
            if "." in i:
                i_list = i.split(".")
                if i_list[1] != "guid":
                    columns_to_drop.append(i)
                else:
                    pass
            else:
                pass
        logger.info("Dropping columns: " + ", ".join(columns_to_drop))
        df = df.drop(columns=columns_to_drop)

        # if the df is not empty and not all columns in the df contains "."
        # add write df into tsv file
        df_content = df.drop(columns=["type"])
        if not df_content.dropna(how="all").empty:
            # columns_wo_type = [k for k in df.columns.tolist() if k != "type"]
            columns_wo_type = df_content.columns
            if len([h for h in columns_wo_type if "." in h]) < len(columns_wo_type):
                logger.info("Writing node " + node + " into tsv file")
                df_file_path = (
                    output_tsv_folder
                    + "/"
                    + project_id
                    + "-"
                    + node
                    + "_"
                    + time_now
                    + ".tsv"
                )
                df.to_csv(df_file_path, sep="\t", header=True, index=False)
            else:
                logger.info(f"No valid content to write for node {node}, skipping...")
        else:
            logger.info(f"No valid content to write for node {node}, skipping...")
        del df
        del df_content

    logger.info("Writing a metadata json file")
    # write out a json metadata file for this job
    json_dict = {
        "template_version": github_curr_ver,
        "job_datetime": time_now,
        "submission_input_file": os.path.basename(manifest),
    }
    json_file_path = (
        output_folder + "/" + project_id + "_TabBreakeRLog_" + time_now + ".json"
    )
    with open(json_file_path, "w") as fp:
        json.dump(json_dict, fp)

    return output_folder, output_log
