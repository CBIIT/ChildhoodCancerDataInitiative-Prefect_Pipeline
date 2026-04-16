"""Utility functions for transforming and parsing COG and IGM JSON files"""

import os
import sys
import json
import pandas as pd
import re
import time
import openpyxl
import itertools
from collections import defaultdict
from prefect import task, flow, get_run_logger, unmapped
from src.utils import get_time, get_date, get_logger
import boto3
from botocore.exceptions import ClientError
from prefect.task_runners import ConcurrentTaskRunner
from prefect.cache_policies import NO_CACHE


def replace_en_em_dash(df: pd.DataFrame) -> pd.DataFrame:
    """Replace en and em dashes in DataFrame with hyphens"""
    for col in df.columns:
        if pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].str.replace("–", "-", regex=False)
            df[col] = df[col].str.replace("—", "-", regex=False)
            df[col] = df[col].str.replace('\u2014', '-', regex=False)
            df[col] = df[col].str.replace(" —", " -", regex=False)
            df[col] = df[col].str.replace("–", "-", regex=False)
    return df


@task(
    name="Sample mapping", 
    log_prints=True,
    retries=3,
    retry_delay_seconds=1,  
)
def sample_mapper(manifest_path: str):
    """Map samples to IGM clin reports from associated assay file's matched samples.

    Args:
        manifest_path (str): Path to the manifest file.

    Returns:
        None
    """
    seq_tab = pd.read_excel(manifest_path, sheet_name='sequencing_file')
    
    seq_tab = seq_tab[seq_tab.file_type == 'pdf']

    seq_tab = seq_tab[['sample.sample_id', 'library_strategy']]

    # rename file name column to source_pdf
    seq_tab = seq_tab.rename(columns={'library_strategy': 'assay'})

    # replace 'Archer Fusion' with archer_fusion and WXS with tumor_normal
    seq_tab['assay'] = seq_tab['assay'].replace({'Archer Fusion': 'archer_fusion', 'WXS': 'tumor_normal'})
    
    # grab meth data
    meth_tab = pd.read_excel(manifest_path, sheet_name='methylation_array_file')

    meth_tab = meth_tab[meth_tab.file_type == 'pdf'][['sample.sample_id', 'data_category']]

    # replace 'Methylation Analysis' with 'methylation'
    meth_tab['data_category'] = meth_tab['data_category'].replace({'Methylation Analysis': 'methylation'})

    # rename data_category column to assay
    meth_tab = meth_tab.rename(columns={'data_category': 'assay'})

    # concat meth_tab with seq_tab
    seq_tab = pd.concat([seq_tab, meth_tab], ignore_index=True)
    
    # drop dups from amended reports/addn normal samples
    seq_tab = seq_tab.drop_duplicates()

    sample_tab = pd.read_excel(manifest_path, sheet_name='sample')

    sample_tab = sample_tab[['participant.participant_id', 'sample_id']]

    # merge seq_tab with sample_tab
    seq_tab = seq_tab.merge(sample_tab, left_on='sample.sample_id', right_on='sample_id', how='left')

    # read in clinical_measure_file
    clin_tab = pd.read_excel(manifest_path, sheet_name='clinical_measure_file')
    
    # if all of sample.sample_id col is empty, drop the column
    if clin_tab['sample.sample_id'].isna().all():
        clin_tab = clin_tab.drop(columns=['sample.sample_id'])
        clin_tab_filled = pd.DataFrame()

    else:
        # assign clin tab with sample data already populated to clin_tab_filled
        clin_tab_filled = clin_tab[~clin_tab['sample.sample_id'].isna()]
        
        # assign clin tab rows with no sample data to clin_tab and drop sample.sample_id
        clin_tab = clin_tab[clin_tab['sample.sample_id'].isna()]
        clin_tab = clin_tab.drop(columns=['sample.sample_id'])

    for index, row in clin_tab.iterrows():
        if 'archer_fusion' in row['file_name']:
            clin_tab.at[index, 'assay'] = 'archer_fusion'
        elif 'methylation' in row['file_name']:
            clin_tab.at[index, 'assay'] = 'methylation'
        elif 'tumor_normal' in row['file_name']:
            clin_tab.at[index, 'assay'] = 'tumor_normal'
        else: # tumor normal
            clin_tab.at[index, 'assay'] = ''

    # merge clin_tab with seq_tab on participant.participant_id and sample.sample_id
    clin_tab = clin_tab.merge(seq_tab, on=['participant.participant_id', 'assay'], how='left')

    # print out num rows where data_category does not contain 'COG'
    print(f"IGM row count with no sample.sample_id: {clin_tab[~clin_tab['data_category'].str.contains('COG', na=False)].shape[0]}")

    # check for duplicates  in file_name
    if clin_tab.duplicated(subset=['file_name']).any():
        print("Duplicates found in file_name column")
    
        # print duplicates if found
        print(clin_tab[clin_tab.duplicated(subset=['file_name'], keep=False)])

        #for file_name dupes in clin_df, check if sample in sample.sample_id is in file_name; if not, set sample.sample_id to null
        for index, row in clin_tab[clin_tab.duplicated(subset=['file_name'], keep=False)].iterrows():
            if row['sample.sample_id'] not in row['file_name']:
                clin_tab.at[index, 'sample.sample_id'] = pd.NA
                clin_tab.at[index, 'sample_id'] = pd.NA

        # drop duplicates from clin_tab
        clin_tab = clin_tab.drop_duplicates().reset_index().drop(columns=['index'])


    # check for files with data category != COG and sample.sample_id is null
    for index, row in clin_tab[~clin_tab['data_category'].str.contains('COG', na=False) & clin_tab['sample.sample_id'].isna()].iterrows():

        # extract sample.sample_id from file name with regex '0[0-9A-Z]{5}'
        match = re.search(r'0[0-9A-Z]{5}', row['file_name'])
        if match:
            clin_tab.at[index, 'sample.sample_id'] = match.group(0)

    # print out number of rows where sample.sample_id is not null
    print(f"Count of rows where sample.sample_id is not null: {clin_tab[clin_tab['sample.sample_id'].notna()].shape[0]}")

    # drop assay and sample_id
    clin_tab = clin_tab.drop(columns=['assay', 'sample_id'])

    # make sample.sample_id fourth col from left
    cols = list(clin_tab.columns)
    cols.insert(3, cols.pop(cols.index('sample.sample_id')))
    clin_tab = clin_tab[cols]

    # drop duplicates
    clin_tab = clin_tab.drop_duplicates().reset_index().drop(columns=['index'])

    # concat with clin_tab_filled
    if not clin_tab_filled.empty:
        clin_tab = pd.concat([clin_tab, clin_tab_filled], ignore_index=True)

    # write to TSV
    clin_tab.to_csv("clinical_tab.tsv", sep="\t", index=False)
    
    # save to output path
    with pd.ExcelWriter(
        manifest_path, mode="a", engine="openpyxl", if_sheet_exists="overlay"
    ) as writer:
        clin_tab.to_excel(writer, sheet_name="clinical_measure_file", index=False, header=False, startrow=1)
        
    return None


@flow(
    name="Manifest Reader",
    log_prints=True,
    flow_run_name="manifest_reader_" + f"{get_time()}",
)
def manifest_reader(manifest_path: str, form_parsing: str):
    """Read in and parse manifest of JSONs to transform

    Args:
        manifest_path (str): S3 path to CCDI study manifest file

    Returns:
        pd.DataFrame: DataFrame of parsed clinical_measure_files
    """

    runner_logger = get_run_logger()

    file_name = os.path.basename(manifest_path)
    
    # perform sample mapping for clin files
    if form_parsing in ['igm_only', 'cog_and_igm']:
        sample_mapper(file_name)

    try:
        manifest_df = pd.read_excel(
            file_name, sheet_name="clinical_measure_file", engine="openpyxl"
        )
        
        if form_parsing == 'cog_only':
            # parse only COG clinical reports and return uniq file ID and s3 URL in df
            manifest_df = manifest_df[manifest_df['data_category'].str.contains('COG', na=False)][["clinical_measure_file_id", "file_name", "file_size", "file_url"]]
        elif form_parsing == 'igm_only':
            # parse only IGM clinical reports and return uniq file ID and s3 URL in df
            manifest_df = manifest_df[~manifest_df['data_category'].str.contains('COG', na=False)][["clinical_measure_file_id", "file_name", "file_size", "file_url"]]
        else: # both cog and igm
            # parse both COG and IGM clinical reports and return uniq file ID and s3 URL in df
            manifest_df = manifest_df[["clinical_measure_file_id", "file_name", "file_size", "file_url"]]
    except Exception as e:
        runner_logger.error(f"Cannot read in manifest {file_name} due to error: {e}")
        sys.exit(1)
    
    local_manifest_path = os.path.join(os.getcwd(), file_name)

    return manifest_df, local_manifest_path


def set_s3_resource():
    """This method sets the s3_resource object to either use localstack
    for local development if the LOCALSTACK_ENDPOINT_URL variable is
    defined and returns the object
    """
    localstack_endpoint = os.environ.get("LOCALSTACK_ENDPOINT_URL")
    if localstack_endpoint != None:
        AWS_REGION = "us-east-1"
        AWS_PROFILE = "localstack"
        ENDPOINT_URL = localstack_endpoint
        boto3.setup_default_session(profile_name=AWS_PROFILE)
        s3_resource = boto3.resource(
            "s3", region_name=AWS_REGION, endpoint_url=ENDPOINT_URL
        )
    else:
        s3_resource = boto3.resource("s3")
    return s3_resource

@task(
    name="Download file", 
    log_prints=True,
    tags=["json-downloader-tag"],
    retries=3,
    retry_delay_seconds=1,
    cache_policy=NO_CACHE,  
)
def file_dl(dl_parameter: dict, dups, logger, runner_logger):
    """File download using bucket name and filename
    filename is the key path in bucket
    file is the basename
    """
    # Set the s3 resource object for local or remote execution
    bucket = dl_parameter['bucket']
    file_path = dl_parameter['file_path']
    s3 = set_s3_resource()
    source = s3.Bucket(bucket)
    file_key = file_path
    row = dl_parameter['row']
    filename = os.path.basename(dl_parameter['file_path'])
    try:
        source.download_file(file_key, filename)
        # if file name is in dups list, rename to clinical_measure_file_id + JSON to be uniq
        if filename in dups:
            new_file_name = (
                row["clinical_measure_file_id"]
                if row["clinical_measure_file_id"].endswith(".json")
                else row["clinical_measure_file_id"] + ".json"
            )
            os.rename(row["file_name"], new_file_name)
            logger.info(
                f"Renamed file {row['file_name']} to {new_file_name} to be unique."
            )
            runner_logger.info(
                f"Renamed file {row['file_name']} to {new_file_name} to be unique."
            )
    except ClientError as ex:
        ex_code = ex.response["Error"]["Code"]
        ex_message = ex.response["Error"]["Message"]
        print(
            f"ClientError occurred while downloading file {filename} from bucket {bucket}:\n{ex_code}, {ex_message}"
        )
        logger.error(f"ClientError occurred while downloading file {filename} from bucket {bucket}:\n{ex_code}, {ex_message}")
        #raise




@flow(
    name="JSON Downloader",
    log_prints=True,
    flow_run_name="json_downloader_" + f"{get_time()}",
    task_runner=ConcurrentTaskRunner(), 
)
def json_downloader(manifest: pd.DataFrame, dups: list, logger):
    """Flow for downloading JSONs to VM for parsing and verifying file_name uniqueness

    Args:
        manifest (pd.DataFrame): Manifest of file_names and s3 URLs
        dups (list): List of duplicate file_names
        logger: Logger object for logging messages

    Returns:
        None
    """

    runner_logger = get_run_logger()

    # throttle submission of tasks to avoid overwhelming the system
    time.sleep(2)
    #setup with list of dicts to iterate over and then run with map
    submit_list = []

    for index, row in manifest.iterrows():
        f_bucket = row["file_url"].split("/", 3)[2]
        f_path = row["file_url"].split("/", 3)[3]
        f_name = os.path.basename(f_path)

        if f_name != row["file_name"]:
            runner_logger.error(
                f"Expected file name {row['file_name']} does not match observed file name in s3 url, {f_name}, not downloading file"
            )
            logger.error(
                f"Expected file name {row['file_name']} does not match observed file name in s3 url, {f_name}, not downloading file"
            )
        else:
            submit_list.append({"bucket" : f_bucket, "file_path" : f_path, "row": row}) 


    downloads = file_dl.map(submit_list, unmapped(dups), unmapped(logger), unmapped(runner_logger))
    
    return downloads.result()

@task(
    name="Percent Necrosis and Tumor Content Fill In", 
    log_prints=True,
)
def percent_necrosis_tumor_fill_in(manifest_path: str, decoded_tsv_path: list[str], runner_logger):
    """Function for filling in percent necrosis and tumor content data from decoded TSVs back into manifest clinical_measure_file sheet and outputting updated manifest to output path

    Args:
        manifest_path (str): Path to manifest file
        decoded_tsv_path (list[str]): List of paths to decoded TSVs to parse for percent necrosis and tumor content data
        runner_logger: Logger object for logging messages
    """
    runner_logger.info("Starting percent necrosis and tumor content fill in process.")
    # read in clin measure file sheet of manifest to df for mapping of subject_id to sample
    clin_df = pd.read_excel(manifest_path, sheet_name="clinical_measure_file", engine="openpyxl")[["participant.participant_id", "sample.sample_id", "data_category"]]
    
    # drop cols with no sample.sample_id data
    clin_df = clin_df.dropna(subset=['sample.sample_id'])
    
    # pandas one liner to replace data_category that contains the string 'Methylation' nested in a string with 'methylation' in clin_df
    clin_df.loc[clin_df['data_category'].str.contains('Methylation', na=False), 'data_category'] = 'methylation'
    clin_df.loc[clin_df['data_category'].str.contains('Gene Fusion', na=False), 'data_category'] = 'archer_fusion'
    clin_df.loc[clin_df['data_category'].str.contains('Tumor Normal', na=False), 'data_category'] = 'tumor_normal'
    
    # for each decoded TSV, parse subject_id, percent_necrosis and percent_tumor content data
    # and concat into one df then drop duplicates
    parsed_data = pd.DataFrame()
    for tsv in decoded_tsv_path:
        tsv_df = pd.read_csv(tsv, sep="\t", low_memory=False)
        tsv_df = tsv_df[["subject_id", "report_type", "percent_necrosis", "percent_tumor"]]
        parsed_data = pd.concat([parsed_data, tsv_df], ignore_index=True)
    parsed_data = parsed_data.drop_duplicates()
    
    # merge parsed_data with clin_df on subject_id and data_category to map percent necrosis and tumor content data to sample.sample_id in clin_df
    merged_df = clin_df.merge(parsed_data, left_on=['participant.participant_id', 'data_category'], right_on=['subject_id', 'report_type'], how='left')
    
    # drop subject_id, report_type, data_category cols
    merged_df = merged_df.drop(columns=['subject_id', 'report_type', 'data_category'])
    
    # drop rows with null percent_necrosis and percent_tumor data
    merged_df = merged_df.dropna(subset=['percent_necrosis', 'percent_tumor'], how='all')
    
    def refine_percent_necrosis_tumor(val):
        
        if type(val) == float or type(val) == int:
            return val
        
        # strip white space and replace any spaces with empty string
        val_fmt = val.strip().replace(" ", "")
        
        #remove chars +, ~, < and > using regex
        val_fmt = re.sub(r'[+~<>]', '', val_fmt)
        
        # if - in val, take midpoint of the two numbers on either side of the dash as the value
        if '-' in val_fmt:
            try:
                low, high = val_fmt.split('-')
                low = float(low)
                high = float(high)
                val_fmt = (low + high) / 2
            except ValueError:
                # if there is an error converting to float, return orignal value
                runner_logger.warning(f"Value {val} could not be converted to float after formatting, returning original value.")
                return val
        else:
            try:
                val_fmt = float(val_fmt)
            except ValueError:
                # if there is an error converting to float, return orignal value
                runner_logger.warning(f"Value {val} could not be converted to float after formatting, returning original value.")
                return val

        return val_fmt

    # apply refine_percent_necrosis_tumor to percent_necrosis and percent_tumor columns
    merged_df['percent_necrosis'] = merged_df['percent_necrosis'].apply(refine_percent_necrosis_tumor)
    merged_df['percent_tumor'] = merged_df['percent_tumor'].apply(refine_percent_necrosis_tumor)
    
    # read in sample sheet
    sample_df = pd.read_excel(manifest_path, sheet_name="sample", engine="openpyxl")
    
    # set sample_id as string to avoid issues with leading zeros
    sample_df['sample_id'] = sample_df['sample_id'].astype(str)
    merged_df['sample.sample_id'] = merged_df['sample.sample_id'].astype(str)
    
    # grab col order of sample_df to reset after merge
    col_order = sample_df.columns.tolist()
    
    # merge merged_df with sample_df on sample.sample_id to map percent necrosis and tumor content data to sample_id in sample sheet
    # do not overwrite existing percent necrosis and tumor content data in sample sheet, only fill in where sample sheet has null values and merged_df has data
    # using combine_first()
    sample_df = sample_df.merge(merged_df, left_on=['participant.participant_id', 'sample_id'], right_on=['participant.participant_id', 'sample.sample_id'], how='left')
    sample_df['percent_necrosis'] = sample_df['percent_necrosis_x'].combine_first(sample_df['percent_necrosis_y'])
    sample_df['percent_tumor'] = sample_df['percent_tumor_x'].combine_first(sample_df['percent_tumor_y'])
    
    # reset col order, auto drop unneeded cols from merge
    sample_df = sample_df[col_order]
    
    # save to manifest
    with pd.ExcelWriter(manifest_path, engine="openpyxl", mode="a", if_sheet_exists="overlay") as writer:
        sample_df.drop_duplicates().to_excel(writer, sheet_name="sample", index=False, header=False, startrow=1)
    runner_logger.info("Finished percent necrosis and tumor content fill in process.")