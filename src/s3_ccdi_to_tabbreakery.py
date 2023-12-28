from prefect import flow, task
import sys
import os
import pandas as pd
import json
from src.utils import get_logger, get_time, get_date, CheckCCDI


@flow(
    name="CCDI_to_TabBreaker",
    flow_run_name="CCDI_to_TabBreaker_" + f"{get_time()}",
)
def tabBreakeRy(manifest: str) -> tuple:
    logger = get_logger(loggername="CCDI_to_TabBreakeRy", log_level="info")

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
                df["id"] = df[column].apply(lambda x: project_id + "::" + x)
            elif column[-3:] != ".id" and "." in column:
                prev_node = column.split(".")[0]
                node_id = prev_node + ".id"
                df[node_id] = df[column].apply(lambda x: project_id + "::" + x)
            else:
                pass

        # Remove the old linking properties [node].[node]_id,
        # as that will cause issues in the data loader
        columns_to_keep = [i for i in df.columns.tolist() if "_id" not in i]
        df = df[columns_to_keep]

        # if the df is not empty and not all columns in the df contains "."
        # add the df to the workbook list
        if not df.loc[:, df.columns != "type"].empty:
            columns_wo_type = [k for k in df.columns.tolist() if k != "type"]
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

    output_log = "CCDI_to_TabBreakeRy_" + get_date() + ".log"

    return output_folder, output_log
