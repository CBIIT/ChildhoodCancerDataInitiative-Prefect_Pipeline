import requests
from prefect import flow, task, get_run_logger
import os
import time
import pandas as pd
import warnings
from datetime import date
from openpyxl.utils.dataframe import dataframe_to_rows
from shutil import copy
from src.utils import get_time, file_dl, file_ul


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

def pull_guids(row):
    guidcheck_logger = get_run_logger()

    # Extract hash and size from the dataframe
    hash_value = row["md5sum"]
    size = row["file_size"]
    guid = row["dcf_indexd_guid"]

    #guidcheck_logger.info(f"Making API call for {hash_value} of size: {size}.")

    # Send API request with query parameters
    # Define the API endpoint URL
    api_url = (
        f"https://nci-crdc.datacommons.io/index/index?hash=md5:{hash_value}&size={size}"
    )
    response = make_request(api_url)

    time.sleep(1.5)

    # Check if the request was successful
    if response is not None and response.status_code == 200:
        with open("API_indexd_calls.log", "a") as logfile:
            logfile.write(
                f"Response {response.status_code} for {hash_value} of size: {size}.\n"
            )
        # Parse the JSON response
        data = response.json()
        # Extract the relevant information from the response and append to results

        if len(data["records"]) > 1:
            if data["records"][0]["acl"] != None:
                if len(data["records"]) > 1:
                    for pos in range(len(data["records"])):
                        if data["records"][pos]["file_name"] == None:
                            guid = data["records"][pos]["did"]
                        else:
                            pass
                else:
                    pass
            else:
                pass
        else:
            pass
    else:
        with open("API_indexd_calls.log", "a") as logfile:
            logfile.write(f"ERROR: no response for {hash_value} of size: {size}.\n")
        guidcheck_logger.error(
            f"Failed to fetch data for hash='{hash_value}' and size='{size}'"
        )

    return guid


@flow(
    name="guid_checker",
    log_prints=True,
    flow_run_name="guid_checker_" + f"{get_time()}",
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

    guidcheck_logger.info("Removing empty CCDI Manifest file tabs")

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

    guidcheck_logger.info("Calling Indexd API")

    for node in dict_nodes:
        if "file_url_in_cds" in meta_dfs[node].columns:
            guidcheck_logger.info(f"Checking {node}.")
            df = meta_dfs[node]

            total_rows = len(df)

            for index, row in df.iterrows():
                df.at[index, "dcf_indexd_guid"]=pull_guids(row)

                #guidcheck_logger.info(f"{index} / {total_rows}")

            #df["dcf_indexd_guid"] = df.apply(pull_guids, axis=1)
            meta_dfs[node] = df

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

    return checker_out_file


@flow(
    name="guid_checker_runner",
    log_prints=True,
    flow_run_name="{runner}_" + f"{get_time()}",
)
def guid_checker_runner(
    bucket: str,
    file_path: str,
    runner: str,
):

    # generate output folder name
    output_folder = runner + "/guid_checker_outputs_" + get_time()

    # download the manifest
    file_dl(bucket, file_path)

    file_path = os.path.basename(file_path)

    checker_out_file = guid_checker(file_path)

    file_ul(
        bucket=bucket,
        output_folder=output_folder,
        sub_folder="",
        newfile=checker_out_file,
    )

    file_ul(
        bucket=bucket,
        output_folder=output_folder,
        sub_folder="",
        newfile="API_indexd_calls.log",
    )


if __name__ == "__main__":
    bucket = "my-source-bucket"
    # test new version manifest and latest version template
    file_path = "inputs/test_file.xlsx"

    guid_checker_runner(bucket=bucket, file_path=file_path, runner="svb")
