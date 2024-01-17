from prefect import flow, get_run_logger
import os
import warnings
import pandas as pd
import numpy as np
import boto3
import re
from datetime import date
from src.utils import set_s3_session_client, get_time
from botocore.exceptions import ClientError
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows
import uuid
from shutil import copy

@flow(
    name="CCDI_CatchERRy",
    log_prints=True,
    flow_run_name="CCDI_CatchERRy_" + f"{get_time()}",
)
def CatchERRy(file_path: str, template_path: str):  # removed profile
    catcherr_logger = get_run_logger()

    def determine_file_name(file_path):
        file_name = os.path.splitext(os.path.split(os.path.relpath(file_path))[1])[0]
        file_ext = os.path.splitext(file_path)[1]
        file_dir_path = os.path.split(os.path.relpath(file_path))[0]

        if file_dir_path == "":
            file_dir_path = "."

        return file_name, file_ext, file_dir_path

    def refresh_date():
        today = date.today()
        today = today.strftime("%Y%m%d")
        return today

    def create_output_file(file_name, todays_date):
        return f"{file_name}_CatchERR{todays_date}"

    def read_xlsx(file_path, sheet):
        warnings.simplefilter(action="ignore", category=UserWarning)
        return pd.read_excel(file_path, sheet, dtype="string")

    def read_template(template_path):
        xlsx_model = pd.ExcelFile(template_path)
        model_dfs = {}
        for sheet_name in xlsx_model.sheet_names:
            model_dfs[sheet_name] = read_xlsx(xlsx_model, sheet_name)
        return model_dfs

    def remove_empty_tabs(meta_dfs):
        dict_nodes = set(list(meta_dfs.keys()))
        for node in dict_nodes:
            test_df = meta_dfs[node].drop("type", axis=1).dropna(how="all").dropna(how="all", axis=1)
            if test_df.empty:
                del meta_dfs[node]
        return meta_dfs

    def log_printout(output_file, file_dir_path, catcherr_out_log, dict_nodes, meta_dfs, tavs_df, dict_df):
        catcherr_out_log_path = f"{file_dir_path}/{catcherr_out_log}"
        with open(catcherr_out_log_path, "w") as outf:
            # Terms and Value sets checks
            print("The following columns have controlled vocabulary on the 'Terms and Value Sets' page of the template file. If the values present do not match, they will be noted and, in some cases, the values will be replaced:\n----------", file=outf)
            # ... (remaining log_printout code)

    def fix_url_paths(meta_dfs, dict_nodes):
        # ... (remaining fix_url_paths code)

    def assign_guids(meta_dfs, dict_nodes):
        # ... (remaining assign_guids code)

    def replace_nan(meta_dfs, dict_nodes):
        for node in dict_nodes:
            df = meta_dfs[node]
            df = df.fillna("")
            df = df.drop_duplicates()
            meta_dfs[node] = df
        return meta_dfs

    def write_out(catcherr_out_file, template_path, meta_dfs):
        catcherr_logger.info("Writing out the CatchERR using pd.ExcelWriter")
        copy(src=template_path, dst=catcherr_out_file)
        with pd.ExcelWriter(catcherr_out_file, mode="a", engine="openpyxl", if_sheet_exists="overlay") as writer:
            for sheet_name in meta_dfs.keys():
                sheet_df = meta_dfs[sheet_name]
                sheet_df.to_excel(writer, sheet_name=sheet_name, index=False, header=True)
        catcherr_logger.info(f"Process Complete. The output file can be found here: {file_dir_path}/{catcherr_out_file}")

    # Main function logic
    file_name, _, file_dir_path = determine_file_name(file_path)
    todays_date = refresh_date()
    output_file = create_output_file(file_name, todays_date)
    
    catcherr_logger.info("Reading CCDI template file")
    model_dfs = read_template(template_path)
    tavs_df = model_dfs["Terms and Value Sets"]
    dict_df = model_dfs["Dictionary"]

    catcherr_logger.info("Reading CCDI manifest file")
    xlsx_data = pd.ExcelFile(file_path)
    meta_dfs = {sheet_name: read_xlsx(xlsx_data, sheet_name) for sheet_name in xlsx_data.sheet_names}
    xlsx_data.close()

    dict_nodes = remove_empty_tabs(meta_dfs)

    log_printout(output_file, file_dir_path, f"{output_file}.txt", dict_nodes, meta_dfs, tavs_df, dict_df)
    fix_url_paths(meta_dfs, dict_nodes)
    assign_guids(meta_dfs, dict_nodes)
    meta_dfs = replace_nan(meta_dfs, dict_nodes)

    catcherr_out_file = f"{output_file}.xlsx"
    write_out(catcherr_out_file, template_path, meta_dfs)

    return (catcherr_out_file, f"{output_file}.txt")
