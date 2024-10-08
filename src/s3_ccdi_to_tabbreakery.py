from prefect import flow, task
import sys
import os
import pandas as pd
import json
import uuid
import hashlib
from src.utils import get_logger, get_time, get_date, CheckCCDI


def get_ccdi_namespace():
    hex_string = hashlib.md5("ccdi".encode("UTF-8")).hexdigest()
    ccdi_namespace = uuid.UUID(hex=hex_string)
    return ccdi_namespace


def get_uuid(uuid_namespace, id_str: str) -> str:
    return_uuid = uuid.uuid5(uuid_namespace, id_str)
    return str(return_uuid)


def get_ccdi_id(x, study_id: str, node_name: str):
    if pd.isna(x):
        return x
    else:
        ccdi_namespace = get_ccdi_namespace()
        x_str_input = study_id + "::" + node_name + "::" + x
        return get_uuid(uuid_namespace=ccdi_namespace, id_str=x_str_input)


@flow(
    name="CCDI_to_TabBreaker",
    flow_run_name="CCDI_to_TabBreaker_" + f"{get_time()}",
)
def tabBreakeRy(manifest: str) -> tuple:
    logger = get_logger(loggername="CCDI_to_TabBreakeRy", log_level="info")
    output_log = "CCDI_to_TabBreakeRy_" + get_date() + ".log"

    try:
        manifest_f = pd.ExcelFile(manifest)
        logger.info(f"Reading the validated CCDI manifest {manifest}")
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
    output_folder = "CCDI_TabBreakeRy_" + time_now
    output_tsv_folder = output_folder + "/tsvs"
    os.makedirs(output_tsv_folder, exist_ok=True)
    logger.info(f"Created output folder {output_folder}")

    check_ccdi = CheckCCDI(ccdi_manifest=manifest)

    # Read in Dictionary page to obtain the required properties
    dict_df = check_ccdi.get_dict_df()
    dict_nodes = dict_df["Node"].unique()

    # Initiate an object of workbook list
    workbook_dict = {}

    # get versioin of the manifest
    github_curr_ver = "v" + check_ccdi.get_version()
    logger.info(f"CCDI manifest version is {github_curr_ver}")

    # get study if of the manifest
    project_id = check_ccdi.get_study_id()
    logger.info(f"CCDI manifest study id is {project_id}")

    # get keys of every node/sheet
    keys = dict_df[dict_df["Key"] == 1]["Property"].tolist()

    for node in dict_nodes:
        df = check_ccdi.read_sheet_na(sheetname=node)
        df["type"] = node

        # To ensure unique ids across multiple studies,
        # we are pre-appending the study_id onto all key_ids and
        # their linkages.
        for column in df.columns:
            if column in keys:
                df["id"] = df[column].apply(
                    lambda x: get_ccdi_id(x=x, study_id=project_id, node_name=node)
                )
            elif column[-3:] != ".id" and "." in column:
                prev_node = column.split(".")[0]
                node_id = prev_node + ".id"
                df[node_id] = df[column].apply(
                    lambda x: get_ccdi_id(x=x, study_id=project_id, node_name=prev_node)
                )
            else:
                pass

        # Remove the old linking properties [node].[node]_id,
        # as that will cause issues in the data loader
        columns_to_drop = []
        for i in df.columns:
            if "." in i:
                i_list = i.split(".")
                if i_list[0] + "_id" == i_list[1]:
                    columns_to_drop.append(i)
                else:
                    pass
            else:
                pass
        df = df.drop(columns=columns_to_drop)

        # if the df is not empty and not all columns in the df contains "."
        # add the df to the workbook list
        df_content = df.drop(columns=["type"])
        if not df_content.dropna(how="all").empty:
            # columns_wo_type = [k for k in df.columns.tolist() if k != "type"]
            columns_wo_type = df_content.columns
            if len([h for h in columns_wo_type if "." in h]) < len(columns_wo_type):
                workbook_dict[node] = df
            else:
                pass
        else:
            pass

    logger.info("Writing Excel sheet into tsv format")
    # write each tab into tsv file under /tsv subfolder
    for key, key_df in workbook_dict.items():
        key_file_path = (
            output_tsv_folder + "/" + project_id + "-" + key + "_" + time_now + ".tsv"
        )
        key_df.to_csv(key_file_path, sep="\t", header=True, index=False)

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
