import os
import time
import requests
import pandas as pd
import warnings
from shutil import copy
from datetime import date
from prefect import flow, get_run_logger
from openpyxl.utils.dataframe import dataframe_to_rows
from src.utils import get_time, file_dl, file_ul


# Utility function to get the current date in YYYYMMDD format
def get_current_date():
    return date.today().strftime("%Y%m%d")


# Function to send a GET request to a URL with retry logic
def make_request(url, retries=3, delay=1):
    """
    Sends a GET request to the provided URL and retries if an error occurs.

    Args:
        url (str): The API endpoint URL.
        retries (int): Number of retry attempts.
        delay (float): Delay (in seconds) between retries.

    Returns:
        response (requests.Response or None): Response object if successful, otherwise None.
    """
    for attempt in range(retries):
        try:
            response = requests.get(url)
            if response.status_code == 200:
                return response  # Successful response
            print(
                f"Non-200 response ({response.status_code}), retrying... ({attempt+1}/{retries})"
            )
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}, retrying... ({attempt+1}/{retries})")
        time.sleep(delay)
    return None  # Return None if all retries fail


# Function to process a row and get the corresponding GUID from the Indexd API
def pull_guids(row):
    """
    Queries the Indexd API to retrieve the GUID for the given file based on its hash and size.
    If 'dcf_indexd_guid' is already present in the row, the process is skipped.

    Args:
        row (pd.Series): A row from the DataFrame containing file information.

    Returns:
        guid (str): The GUID retrieved from the API, or the existing GUID if already present.
    """
    logger = get_run_logger()

    # Skip processing if 'dcf_indexd_guid' is already present and not empty
    if pd.notna(row["dcf_indexd_guid"]) and row["dcf_indexd_guid"].strip():
        logger.info(
            f"GUID already present for file_url: {row['file_url']}. Skipping API call."
        )
        return row["dcf_indexd_guid"]

    # Extract relevant details for the API call
    hash_value = row["md5sum"]
    size = row["file_size"]
    guid = row["dcf_indexd_guid"]
    file_url = row["file_url"]
    file_name = os.path.basename(file_url)
    file_path = os.path.dirname(file_url)

    # Build the API request URL with query parameters
    api_url = (
        f"https://nci-crdc.datacommons.io/index/index?hash=md5:{hash_value}&size={size}"
    )
    response = make_request(api_url)
    time.sleep(1.5)  # Pause to avoid overwhelming the API

    if response is not None:
        data = response.json()
        if data["records"]:  # Check if any records were returned
            for record in data["records"]:
                if (
                    os.path.basename(record["urls"][0]) == file_name
                    and os.path.dirname(record["urls"][0]) == file_path
                ):
                    return record["did"]  # Return the matching GUID
        logger.warning(f"No match found for hash='{hash_value}' and size='{size}'")
    else:
        logger.error(
            f"No response from Indexd API for hash='{hash_value}' and size='{size}'"
        )

    return row["dcf_indexd_guid"]  # Return the original GUID if no match is found


# Function to read all sheets from an Excel file into a dictionary of DataFrames
def read_excel_sheets(file_path):
    """
    Reads all sheets from an Excel file into a dictionary of DataFrames.

    Args:
        file_path (str): Path to the Excel file.

    Returns:
        dict: A dictionary where keys are sheet names and values are DataFrames.
    """
    warnings.simplefilter(
        action="ignore", category=UserWarning
    )  # Ignore warnings from openpyxl
    xlsx_data = pd.ExcelFile(file_path)
    dfs = {
        sheet: pd.read_excel(xlsx_data, sheet, dtype="string")
        for sheet in xlsx_data.sheet_names
    }
    xlsx_data.close()
    return dfs


# Function to remove completely empty tabs from a dictionary of DataFrames
def remove_empty_tabs(dfs):
    """
    Removes sheets that are completely empty from the dictionary of DataFrames.

    Args:
        dfs (dict): Dictionary of DataFrames representing the Excel sheets.

    Returns:
        dict: Dictionary with non-empty sheets only.
    """
    return {
        sheet: df
        for sheet, df in dfs.items()
        if not df.dropna(how="all").dropna(how="all", axis=1).empty
    }


@flow(name="guid_checker", log_prints=True)
def guid_checker(file_path: str):
    """
    Main flow to process an Excel file, check GUIDs using the Indexd API, and write the results to a new file.

    Args:
        file_path (str): Path to the input Excel file.

    Returns:
        str: Path to the output Excel file.
    """
    logger = get_run_logger()
    logger.info("Starting GUID checker...")

    # Prepare output file name with a date suffix
    file_name = os.path.splitext(os.path.split(os.path.relpath(file_path))[1])[0]
    output_file = f"{file_name}_GUIDcheck_{get_current_date()}.xlsx"

    # Read all sheets from the input file
    meta_dfs = read_excel_sheets(file_path)

    # Remove unnecessary sheets
    meta_dfs.pop("README and INSTRUCTIONS", None)
    meta_dfs.pop("Dictionary", None)
    meta_dfs.pop("Terms and Value Sets", None)

    # Remove sheets that are completely empty
    meta_dfs = remove_empty_tabs(meta_dfs)

    logger.info("Checking and updating GUIDs...")

    # Iterate over each sheet and process rows with 'file_url'
    for sheet_name, df in meta_dfs.items():
        if "file_url" in df.columns:
            logger.info(f"Processing sheet: {sheet_name}")
            total_rows = len(df)

            # Iterate over each row and update the 'dcf_indexd_guid' column only if it's empty
            for index, row in df.iterrows():
                df.at[index, "dcf_indexd_guid"] = pull_guids(row)
                logger.info(
                    f"Processed {index + 1} out of {total_rows} entries for {sheet_name}."
                )  # Progress counter

            # Update the dictionary with the modified DataFrame
            meta_dfs[sheet_name] = df

    # Copy the input file to the output file before updating it
    copy(file_path, output_file)

    # Write the updated DataFrames back to the Excel file
    with pd.ExcelWriter(
        output_file, mode="a", engine="openpyxl", if_sheet_exists="overlay"
    ) as writer:
        for sheet_name, df in meta_dfs.items():
            template_cols = pd.read_excel(
                file_path, sheet_name=sheet_name
            ).columns.tolist()
            if list(df.columns) != template_cols:
                df = df.reindex(columns=template_cols)
            df.to_excel(
                writer, sheet_name=sheet_name, index=False, header=False, startrow=1
            )

    logger.info(f"Process complete. Output file: {output_file}")
    return output_file


@flow(name="guid_checker_runner", log_prints=True)
def guid_checker_runner(bucket: str, file_path: str, runner: str):
    """
    Wrapper flow that handles downloading the input file, running the GUID checker, and uploading the results.

    Args:
        bucket (str): S3 bucket name.
        file_path (str): Path to the input file in the bucket.
        runner (str): Name of the runner for creating output directories.
    """
    # Prepare output folder name with a timestamp
    output_folder = f"{runner}/guid_checker_outputs_{get_time()}"

    # Download the input file
    file_dl(bucket, file_path)
    file_path = os.path.basename(file_path)

    # Run the GUID checker flow
    checker_out_file = guid_checker(file_path)

    # Upload the result and log file back to the bucket
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
    file_path = "inputs/test_file.xlsx"
    guid_checker_runner(bucket=bucket, file_path=file_path, runner="svb")
