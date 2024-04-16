import requests
from prefect import flow, get_run_logger
import os
import time
import pandas as pd
import numpy as np
import re
import warnings
from datetime import date
from src.utils import set_s3_session_client, get_time
from botocore.exceptions import ClientError
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows
import uuid
from shutil import copy


md5sum = '2b2cc12bb6a0a1176738fbb63329496e'
size=102458

file_path="SMALL_TEST_phs002517_CHOP_CCDI_Submission_v1.7.2_X01_20240315_v2_CatchERR20240330.xlsx"

@flow(
    name="make_requests",
    log_prints=True,
    flow_run_name="make_requests_" + f"{get_time()}",
)
def make_request(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response
        else:
            print(f"Received non-200 status code: {response.status_code}. Retrying...")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}. Retrying...")
        return None

@flow(
    name="pull_guids",
    log_prints=True,
    flow_run_name="pull_guids_" + f"{get_time()}",
)
def pull_guids(df):

    # Iterate over the entries dataframe
    for index, row in df.iterrows():
        # Extract hash and size from the dataframe
        hash_value = row['md5sum']
        size = row['file_size']

        # Send API request with query parameters
        # Define the API endpoint URL
        api_url = f"https://nci-crdc.datacommons.io/index/index?hash=md5:{hash_value}&size={size}"
        response = make_request(api_url)

        time.sleep(0.25)

        # Check if the request was successful
        if response.status_code == 200:
            # Parse the JSON response
            data = response.json()
            # Extract the relevant information from the response and append to results

            if len(data['records']) > 1:
                if data['records'][0]['acl'] != None:
                    if len(data['records'])>1:
                        for pos in range(len(data['records'])):
                            if data['records'][pos]['file_name'] == None:
                                guid= data['records'][pos]['did']
                                df.at[index,'dcf_indexd_guid']=guid
                            else:
                                pass
                    else:
                        pass
                else:
                    pass
            else:
                    pass
        else:
            print(f"Error: Failed to fetch data for hash='{hash_value}' and size='{size}'")

    return df



@flow(
    name="CCDI_GUIDchecker",
    log_prints=True,
    flow_run_name="CCDI_GUIDchecker_" + f"{get_time()}",
)
def guid_checker(file_path: str):  # removed profile
    guidcheck_logger = get_run_logger()
    ##############
    #
    # File name rework
    #
    ##############
    # Determine file ext and abs path
    file_name = os.path.splitext(os.path.split(os.path.relpath(file_path))[1])[0]
    file_dir_path = os.path.split(os.path.relpath(file_path))[0]

    if file_dir_path == "":
        file_dir_path = "."

    # obtain the date
    def refresh_date():
        today = date.today()
        today = today.strftime("%Y%m%d")
        return today

    todays_date = refresh_date()

    # Output file name based on input file name and date/time stamped.
    output_file = file_name + "_GUIDcheck" + todays_date

    ##############
    #
    # Pull Dictionary Page to create node pulls
    #
    ##############

    guidcheck_logger.info("Reading CCDI template file")

    def read_xlsx(file_path: str, sheet: str):
        # Read in excel file
        warnings.simplefilter(action="ignore", category=UserWarning)
        return pd.read_excel(file_path, sheet, dtype="string")



    ##############
    #
    # Read in data
    #
    ##############

    guidcheck_logger.info("Reading CCDI manifest file")

    # create workbook
    xlsx_data = pd.ExcelFile(file_path)

    # create dictionary for dfs
    meta_dfs = {}

    # read in dfs and apply to dictionary
    for sheet_name in xlsx_data.sheet_names:
        meta_dfs[sheet_name] = read_xlsx(xlsx_data, sheet_name)
    # close xlsx_data object
    xlsx_data.close()

    # remove model tabs from the meta_dfs
    del meta_dfs["README and INSTRUCTIONS"]
    del meta_dfs["Dictionary"]
    del meta_dfs["Terms and Value Sets"]

    # create a list of present tabs
    dict_nodes = set(list(meta_dfs.keys()))

    ##############
    #
    # Go through each tab and remove completely empty tabs
    #
    ##############

    for node in dict_nodes:
        # see if the tab contain any data
        test_df = meta_dfs[node]
        test_df = test_df.drop("type", axis=1)
        test_df = test_df.dropna(how="all").dropna(how="all", axis=1)
        # if there is no data, drop the node/tab
        if test_df.empty:
            del meta_dfs[node]

    # determine nodes again
    dict_nodes = set(list(meta_dfs.keys()))

    meta_dfs['sequencing_file']['dcf_indexd_guid']

    for node in dict_nodes:
        if "file_url_in_cds" in meta_dfs[node].columns:
            meta_dfs[node]=pull_guids(meta_dfs[node])


    def reorder_dataframe(dataframe, column_list: list, sheet_name: str, logger):
        reordered_df = pd.DataFrame(columns=column_list)
        for i in column_list:
            if i in dataframe.columns:
                reordered_df[i] = dataframe[i].tolist()
            else:
                logger.warning(f"Column {i} in sheet {sheet_name} was left empty")
        return reordered_df

    guidcheck_logger.info("Writing out the CatchERR using pd.ExcelWriter")
    # save out template
    checker_out_file = f"{output_file}.xlsx"
    copy(src=file_path, dst=checker_out_file)
    with pd.ExcelWriter(
        checker_out_file, mode="a", engine="openpyxl", if_sheet_exists="overlay"
    ) as writer:
        # for each sheet df
        for sheet_name in meta_dfs.keys():
            sheet_df = meta_dfs[sheet_name]
            sheet_df_col = sheet_df.columns.tolist()
            template_sheet_df = pd.read_excel(file_path, sheet_name=sheet_name)
            template_sheet_col = template_sheet_df.columns.tolist()
            if sheet_df_col != template_sheet_col:
                sheet_df = reorder_dataframe(
                    dataframe=sheet_df,
                    column_list=template_sheet_col,
                    sheet_name=sheet_name,
                    logger=guidcheck_logger,
                )
            else:
                pass
            sheet_df.to_excel(
                writer, sheet_name=sheet_name, index=False, header=False, startrow=1
            )

    guidcheck_logger.info(
        f"Process Complete. The output file can be found here: {file_dir_path}/{checker_out_file}"
    )