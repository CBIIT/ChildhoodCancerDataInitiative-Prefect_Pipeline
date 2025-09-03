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
from src.cog_transform_utils import cog_transformer
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
    print(f"IGM row count: {clin_tab[~clin_tab['data_category'].str.contains('COG', na=False)].shape[0]}")

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
def manifest_reader(manifest_path: str):
    """Read in and parse manifest of JSONs to transform

    Args:
        manifest_path (str): S3 path to CCDI study manifest file

    Returns:
        pd.DataFrame: DataFrame of parsed clinical_measure_files
    """

    runner_logger = get_run_logger()

    file_name = os.path.basename(manifest_path)
    
    # perform sample mapping for clin files
    sample_mapper(manifest_path)

    try:
        manifest_df = pd.read_excel(
            file_name, sheet_name="clinical_measure_file", engine="openpyxl"
        )
        # parse only COG and IGM clinical reports and return uniq file ID and s3 URL in df
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


def distinguisher(f_path: str, logger):
    """Attempt to load json and determine type

    Args:
        f_path (str): Path to JSON file

    Returns:
        str: File type (COG JSON, IGM JSON or other) or error
    """

    runner_logger = get_run_logger()

    try:
        f_begin = open(f_path, "rb").read(1000)  # read in first 1000 bytes of file

        # check for identifiers in beginning of file:
        if "upi" in str(f_begin):
            return "cog"
        elif "report_type" in str(f_begin):
            if "archer_fusion" in str(f_begin):
                return "igm.archer_fusion"
            elif "tumor_normal" in str(f_begin):
                return "igm.tumor_normal"
            elif "methylation" in str(f_begin):
                return "igm.methylation"
            else:  # not known
                logger.error(f"Error reading file at {f_path}: IGM assay type unknown.")
                runner_logger.error(
                    f"Error reading file at {f_path}: IGM assay type unknown."
                )
                return "error"
        else:
            return "other"
    except Exception as e:
        logger.error(f"Error reading file at {f_path}: {e}")
        runner_logger.error(f"Error reading file at {f_path}: {e}")
        return "error"

@flow(
    name="JSON Distinguish",
    log_prints=True,
    flow_run_name="json_distinguisher_" + f"{get_time()}",
)
def distinguish(dir_path: str, logger):
    """Function to distinguish between file types (COG JSON, IGM JSON or other)

    Args:
        dir_path (str): Inout path containing files to convert

    Returns:
        dict: Sorting of file names for files by type (COG JSON, IGM JSON, other file or error file)
    """

    runner_logger = get_run_logger()

    # initialize dict of files by type
    sorted_dict = {
        key: []
        for key in [
            "cog",
            "igm.tumor_normal",
            "igm.archer_fusion",
            "igm.methylation",
            "other",
            "error",
        ]
    }

    # get list of files in directory
    if os.path.exists(dir_path):
        # filter out those that have suffix json
        json_files = [i for i in os.listdir(dir_path) if i.endswith(".json")]
        if len(json_files) == 0:
            logger.error(f"Input path {dir_path} does not contain any JSON files.")
            runner_logger.error(
                f"Input path {dir_path} does not contain any JSON files."
            )
            sys.exit(
                f"Process exited: Input path {dir_path} does not contain any JSON files, please check and try again."
            )
        else:
            for f in json_files:
                sorted_dict[distinguisher(f"{dir_path}/{f}", logger)].append(f)
    else:
        logger.error(f"Input path {dir_path} does not exist.")
        runner_logger.error(f"Input path {dir_path} does not exist.")
        sys.exit(
            f"Process exited: Input path {dir_path} does not exist, please check and try again."
        )

    # attempt to read them in and check if they have IGM or COG key identifiers
    # segregate into dict of lists and return dict
    return sorted_dict


@flow(
    name="JSON2TSV",
    log_prints=True,
    flow_run_name="json2tsv_" + f"{get_time()}",
)
def cog_igm_json2tsv(
    manifest: pd.DataFrame, manifest_path: str, parsing: str, working_path: str, output_path: str, dt: str
):

    # get run logger
    runner_logger = get_run_logger()

    # create logger for log file
    log_filename = "COG_IGM_JSON2TSV_" + get_date() + ".log"
    logger = get_logger("COG_IGM_JSON2TSV", "info")

    logger.info(f"Logs beginning at {get_time()}")

    valid = ["cog_only", "igm_only", "cog_and_igm"]

    if parsing not in valid:
        raise ValueError(f"Parsing type {parsing} is not one of {valid}.")

    # check for duplicate file_names
    dups = manifest[manifest["file_name"].duplicated(keep=False)]["file_name"].to_list()

    if not [i for i in os.listdir(os.getcwd()) if i.endswith('.json')]: # check if dir empty, if so download JSONs

        # chunked downloading of JSON files
        chunk_size = 200

        # download JSON files
        for chunk in range(0, len(manifest), chunk_size):
            runner_logger.info(f"Downloading JSON chunk {chunk//chunk_size+1} of {len(manifest)//chunk_size+1}")
            json_downloader(manifest[chunk:chunk+chunk_size], dups, logger)
    
    # record to log file working path
    logger.info(f"Working directory: {working_path}")

    json_dir_path = working_path

    json_sorted = distinguish(json_dir_path, logger)

    # if len(json_sorted["cog"]) == 0 and len(json_sorted["igm"]) == 0:
    if (
        sum(
            [
                len(json_sorted[k])
                for k in [
                    "cog",
                    "igm.methylation",
                    "igm.archer_fusion",
                    "igm.tumor_normal",
                ]
            ]
        )
        == 0
    ):
        runner_logger.error(
            f"\n\t>>> No COG or IGM JSON files to covert in input directory, please check and try again."
        )
        logger.error(
            f"\n\t>>> No COG or IGM JSON files to covert in input directory, please check and try again."
        )
        sys.exit(1)

    # call cog_to_tsv function to read in and transform JSON files to TSV
    if len(json_sorted["cog"]) > 0:
        # make cog output dir path
        cog_op = f"{output_path}/COG"
        if not os.path.exists(cog_op):
            os.makedirs(cog_op)

        # transform COG JSONs and concatenate
        df_reshape, df_reshape_file_name, cog_success_count, cog_error_count = cog_to_tsv(
            json_dir_path, json_sorted["cog"], cog_op, dt, logger
        )

        # if -f option to parse by form, run form_parser
        if parsing in ["cog_only", "cog_and_igm"]:
            if len(df_reshape) > 0:
                cog_form_parser(df_reshape, dt, cog_op, logger)
                cog_transform_log = cog_transformer(df_reshape_file_name, cog_op) 
                print(f"COG transform log: {cog_transform_log}")
                
            else:
                logger.error(
                    "Cannot perform COG form-level parsing, no valid COG JSONs read in."
                )
                runner_logger.error(
                    "Cannot perform COG form-level parsing, no valid COG JSONs read in."
                )
        else:
            cog_transform_log = ""
    else:
        cog_success_count = 0
        cog_error_count = 0
        cog_transform_log = ""

    if (
        len(
            json_sorted["igm.archer_fusion"]
            + json_sorted["igm.tumor_normal"]
            + json_sorted["igm.methylation"]
        )
        > 0
    ):
        # init counts
        igm_success_count = 0
        igm_error_count = 0
        
        # initialize file name list storage for percent tumor necrosis
        percent_tumor_necrosis_file_names = []

        # make igm output dir path
        igm_op = f"{output_path}/IGM"
        if not os.path.exists(igm_op):
            os.mkdir(igm_op)

        # set T/F for variants parsing
        if parsing in ["igm_only", "cog_and_igm"]:
            results_parse = True
        else:
            results_parse = False

        # for each assay type, flatten JSON files and concatenate
        for assay_type in ["igm.tumor_normal", "igm.archer_fusion", "igm.methylation"]:
            if len(json_sorted[assay_type]) > 0:
                df_reshape, temp_success_count, temp_error_count, percent_tumor_necrosis_file_name = igm_to_tsv(
                    json_dir_path,
                    json_sorted[assay_type],
                    assay_type,
                    igm_op,
                    dt,
                    results_parse,
                    manifest_path,
                    logger,
                )
                
                # append percent_tumor_necrosis file name to list
                percent_tumor_necrosis_file_names.append(percent_tumor_necrosis_file_name)

                igm_success_count += temp_success_count
                igm_error_count += temp_error_count

            else:
                logger.error(
                    "Cannot perform IGM variant results-level parsing, no valid IGM JSONs read in."
                )
                runner_logger.error(
                    "Cannot perform IGM variant results-level parsing, no valid IGM JSONs read in."
                )
        
        # ------- MEGA CONCAT JSONs for reference ------------
        
        # For UChi: create concatenated mega JSON of all IGM JSON files
        # by appending the read in JSON files to one another
        # and not reading into a DataFrame
        if igm_success_count > 0:
            for assay_type in ["igm.tumor_normal", "igm.archer_fusion", "igm.methylation"]:
                mega_json = []
                for filename in json_sorted[assay_type]:
                    mega_json.append(json.loads(
                        open(f"{json_dir_path}/{filename}", "r").read()
                    ))
                igm_json_file_name = f"{igm_op}/MEGA_IGM_JSON_{assay_type}_{dt}.json"
                with open(igm_json_file_name, "w") as f:
                    json.dump(mega_json, f, indent=4)

        # ------- parse percent_tumor and percent_necrosis ------------
        if len(percent_tumor_necrosis_file_names) > 0:
            df_ptn = pd.concat(pd.read_csv(i, sep="\t") for i in percent_tumor_necrosis_file_names)[["participant.participant_id", "sample.sample_id", "percent_tumor", "percent_necrosis"]].drop_duplicates().reset_index(drop=True)
            
            # check if multiple percent tumor necrosis values for samples
            
            df_ptn_summary = df_ptn.groupby(["participant.participant_id", "sample.sample_id"]).size().reset_index(name="count")
            if df_ptn_summary["count"].max() > 1:
                logger.warning(
                    "Multiple percent tumor or necrosis values for the following samples, picking first value set:"
                )
                logger.warning("\n".join(df_ptn_summary[df_ptn_summary["count"] > 1].apply(lambda x: f"{x['participant.participant_id']} - {x['sample.sample_id']}", axis=1).tolist()))
                runner_logger.warning(
                    "Multiple percent tumor or necrosis values for the following samples, picking first value set:"
                )
                runner_logger.warning("\n".join(df_ptn_summary[df_ptn_summary["count"] > 1].apply(lambda x: f"{x['participant.participant_id']} - {x['sample.sample_id']}", axis=1).tolist()))
                df_ptn_uniq = df_ptn.drop_duplicates(subset=['participant.participant_id', 'sample.sample_id'], keep='first')[["sample.sample_id", "percent_tumor", "percent_necrosis"]].reset_index(drop=True)
                    
            else:
                df_ptn_uniq = df_ptn[["sample.sample_id", "percent_tumor", "percent_necrosis"]]
            
            # open samples sheet from manifest file and replace percent_tumor and percent_necrosis values from df_ptn_uniq
            samples_df = pd.read_excel(manifest_path, sheet_name="sample")
            # save header order
            header_sort = samples_df.columns.tolist()
            # drop empty cols
            samples_df = samples_df.drop(columns=["percent_tumor", "percent_necrosis"], errors='ignore')
            # merge percent tumor and necrosis values 
            samples_df = samples_df.merge(df_ptn_uniq, left_on="sample_id", right_on="sample.sample_id", how="left").fillna("")
            samples_df = samples_df[header_sort]
            
            # save to output path
            with pd.ExcelWriter(
                manifest_path, mode="a", engine="openpyxl", if_sheet_exists="overlay"
            ) as writer:
                samples_df.to_excel(writer, sheet_name="sample", index=False, header=False, startrow=1)

        else:
            logger.info("No percent tumor or necrosis data to parse, skipping.")
            runner_logger.info("No percent tumor or necrosis data to parse, skipping.")

    else:
        igm_success_count = 0
        igm_error_count = 0

    if len(json_sorted["other"]) > 0:
        # save list of others to output dir
        logger.info(f"Number of other/nonIGM nonCOG JSONS: {len(json_sorted['other'])}")
        runner_logger.info(
            f"Number of other/nonIGM nonCOG JSONS: {len(json_sorted['other'])}"
        )
        with open(f"{output_path}/other_jsons_{dt}.txt", "w+") as w:
            w.write("\n".join(json_sorted["other"]))
        w.close()

    if len(json_sorted["error"]) > 0:
        # save list of error JSONs that could not have type determined to output dir
        logger.info(
            f"Number of JSONS that could not be identified/opened: {len(json_sorted['error'])}"
        )
        runner_logger.info(
            f"Number of JSONS that could not be identified/opened: {len(json_sorted['error'])}"
        )
        with open(f"{output_path}/undertermined_jsons_{dt}.txt", "w+") as w:
            w.write("\n".join(json_sorted["error"]))
        w.close()

    logger.info(
        f"Conversion done: COG Success {cog_success_count}, COG error {cog_error_count}, IGM Success {igm_success_count}, IGM error {igm_error_count}"
    )
    runner_logger.info(
        f"Conversion done: COG Success {cog_success_count}, COG error {cog_error_count}, IGM Success {igm_success_count}, IGM error {igm_error_count}"
    )
    return (
        cog_success_count,
        cog_error_count,
        igm_success_count,
        igm_error_count,
        log_filename,
        cog_transform_log 
    )


def read_cog_jsons(dir_path: str, cog_jsons: list, logger):
    """Reads in COG JSON files and return concatenated DataFrame.

    Args:
        dir_path (str): The directory path containing the JSON files
            to be transformed
        cog_jsons (list): List of file names in directory path that are COG JSONs

    Returns:
        pd.DataFrame: A DataFrame object that is a concatenation of the JSON files read into DataFrames
        int: success count
        int: error count

    Raises:
        ValueError: If a given JSON file cannot be properly read and loaded in as a pandas DataFrame object

    Notes:
        The object_pairs_hook parameter allows you to intercept the
            key-value pairs of the JSON object before they are converted
            into a dictionary; aids in accounting for multiple `data` keys
    """

    runner_logger = get_run_logger()

    concatenated_df = pd.DataFrame()
    df_list = []  # List to hold DataFrames

    success_count = 0  # count of JSON files successfully processed
    error_count = 0  # count of JSON files not processed

    for filename in cog_jsons:
        file_path = os.path.join(dir_path, filename)
        try:
            with open(file_path, "r") as f:
                # Read the file as a string
                json_str = f.read()

                json_str_clean= json_str.replace('\n\r', ' ').replace('\n', ' ').replace('\r', ' ')

                # Parse the string manually to capture all `data` sections
                json_data = json.loads(json_str_clean, object_pairs_hook=custom_json_parser)

                # Normalize the JSON data into a DataFrame
                df = pd.json_normalize(json_data)

                # append to list of DataFrames
                df_list.append(df)
                success_count += 1

        except ValueError as e:
            error_count += 1
            logger.error(f" Error reading {filename}: {e}")
            runner_logger.error(f" Error reading {filename}: {e}")

    # Concatenate all the DataFrames
    if len(df_list) > 0:
        concatenated_df = pd.concat(df_list, ignore_index=True)
        return concatenated_df, success_count, error_count
    else:
        logger.error(" No valid COG JSON files found and/or failed to open.")
        runner_logger.error(" No valid COG JSON files found and/or failed to open.")
        return pd.DataFrame(), success_count, error_count


def custom_json_parser(pairs: dict):
    """Function to preserve duplicate key values.

    Args:
        pairs (dict): key, value pairs recursively fed in from json.loads()

    Returns:
        dict: A key-value pair in python dict type
    """

    # Initialize a dictionary to handle duplicated keys
    result = defaultdict(list)

    # if value of k, v pair is dict
    # append to new dict to store values
    for key, value in pairs:
        if isinstance(value, dict):
            result[key].append(custom_json_parser(value.items()))
        else:
            result[key].append(value)

    # If there's only one value for a key,
    # flatten it (i.e., don't keep it as a list)
    result = {k: (v[0] if len(v) == 1 else v) for k, v in result.items()}

    return result


def expand_cog_df(df: pd.DataFrame, logger):
    """Function to parse participant JSON and output TSV of values and column header reference

    Args:
        df (pd.DataFrame): DataFrame of concatenated, normalized JSONs

    Returns:
        pd.DataFrame: Transformed form values from JSON to pd.DataFrame with updated field names reflecting the form the field is derived from (e.g. DEMOGRAPHY.DM_BRTHDAT)
        pd.DataFrame: Column header reference (form field ID : SaS Label)

    Notes:
        To handle multiple instances of a given form (i.e. Follow-Ups),
        the parsed 'data' objects of the form type is expected as a list
        of lists of dictionaries, for example:
        [[{field : value}, {field : value}], [{field : value}, {field : value}]],
        where the sub-list is a form instance, and is itself a list of dicts.

        Each form instance will be output as a row in the TSV, i.e. multiple
        rows per participant if there are multiple instances of a form for
        the given participant.

    """

    # initialize output file lists to be converted to DataFrames
    expanded_data = []
    saslabel_data = []

    # Iterate through each row in the DataFrame
    for index, row in df.iterrows():
        expanded_rows = []  # Hold all rows for this UPI
        common_row = {
            "upi": row["upi"],
            "index_date_type": row["index_date_type"],
        }  # Store common fields

        # Process each form entry in the 'forms' column
        for form in row["forms"]:
            form_name = form["form_name"]

            # Get 'data' sections; ensure it's a list of lists of dictionaries
            data_sections = form.get("data")

            # Ensure that we handle list of lists or just a list properly
            if isinstance(data_sections, list) and all(
                isinstance(i, list) for i in data_sections
            ):
                pass  # If data_sections is already a list of lists, do nothing

            elif isinstance(data_sections, list):
                data_sections = [
                    data_sections
                ]  # If it's a list of dicts, wrap in another list
            else:
                # continue  # If data_sections is neither a list nor valid, skip this form
                upi = row["upi"]
                logger.info(
                    f" Skipping data section(s) for upi {upi} form {form_name}, not in valid format for parsing"
                )

            # Generate rows for each 'data' section (now lists of lists)
            form_rows = []
            for data_block in data_sections:
                form_row = common_row.copy()  # Start with the common data
                for field in data_block:
                    # Check if it's a valid field dictionary
                    if isinstance(field, dict):
                        form_field_id = field.get("form_field_id")
                        SASLabel = field.get("SASLabel")
                        value = field.get("value")
                        cde_id = field.get("cde_id")

                        # Ensure form_field_id exists
                        if form_field_id:
                            # Create the column name and add the value
                            column_name = f"{form_name}.{form_field_id}"
                            form_row[column_name] = value

                            # Collect SASLabel and column_name pair
                            saslabel_data.append(
                                {
                                    "column_name": column_name,
                                    "SASLabel": SASLabel.strip(),
                                    "cde_id" : str(cde_id)
                                }
                            )
                form_rows.append(form_row)

            # Append all form rows to the expanded rows for this UPI
            expanded_rows.append(form_rows)

        # Create all combinations of the rows from different forms
        if expanded_rows:  # Ensure there's at least one valid form row
            combinations = list(itertools.product(*expanded_rows))
            for combo in combinations:
                combined_row = {}
                for part in combo:
                    combined_row.update(
                        part
                    )  # Merge each part of the combo into one row
                expanded_data.append(combined_row)

    # Convert the expanded data into DataFrames
    df_expanded = pd.DataFrame(expanded_data).drop_duplicates()
    df_saslabels = pd.DataFrame(saslabel_data).drop_duplicates()

    return df_expanded, df_saslabels


def cog_to_tsv(dir_path: str, cog_jsons: list, cog_op: str, timestamp: str, logger):
    """
    Function to call the reading in and transformation of COG JSON files

    Args:
        dir_path (str): Path to directory containing COG JSON files
        cog_jsons (list): List of COG JSON filenames located in dir_path
        cog_op (str): Path to directory to output transformed COG TSV files
        timestamp (str): Date-time of when script run

    Returns:
        pd.DataFrame: dataframe of transformed and aggregated JSON files
        int: The count of JSON files successfully processed
        int: The count of JSON files unsuccessfully processed
    """

    # read in JSONs
    df_ingest, success_count, error_count = read_cog_jsons(dir_path, cog_jsons, logger)

    if success_count > 0:

        # transform JSONs and generate column name reference file
        df_reshape, df_saslabels = expand_cog_df(df_ingest, logger)

        # save data files to output COG directory
        df_reshape_file_name = f"{cog_op}/COG_JSON_table_conversion_{timestamp}.tsv"
        df_reshape.to_csv(
            df_reshape_file_name, sep="\t", index=False
        )
        df_saslabels.to_csv(
            f"{cog_op}/COG_saslabels_{timestamp}.tsv", sep="\t", index=False
        )

        return df_reshape, df_reshape_file_name, success_count, error_count

    else:
        # return empty dataframe since no files to process
        return pd.DataFrame(), "", success_count, error_count


def cog_form_parser(
    df: pd.DataFrame, timestamp: str, cog_op: str, logger
) -> pd.DataFrame:
    """Split transformed JSON data into TSVs for each form type

    Args:
        df (pd.DataFrame): transformed form values from JSON to pd.DataFrame with updated field names reflecting the form the field is derived from (e.g. DEMOGRAPHY.DM_BRTHDAT)
        timestamp (str): Date-time of when script run
        cog_op (str): Path to output directory for COG files

    Returns:
        pd.DataFrame: parsed columns by form type written to separate tsv files

    """

    if type(df) == pd.core.frame.DataFrame:

        # make directory to store split TSVs
        directory_path = f"{cog_op}/COG_form_level_TSVs_{timestamp}/"

        if not os.path.exists(directory_path):
            os.mkdir(directory_path)

        # grab indexing columns
        index_cols = list(df.columns[:2])

        # grab form names from column headers
        forms = list(set([col.split(".")[0] for col in df.columns if "." in col]))

        # split columns by form and write to file
        for form in forms:
            subset = [col for col in df.columns if form in col]
            temp_df = df[index_cols + subset]
            temp_df.to_csv(f"{directory_path}/{form}.tsv", sep="\t", index=False)

    else:
        logger.error(
            "No valid DataFrame found to \
            parse into form-level COG TSVs"
        )
        sys.exit(
            "\n\t>>> Process Exited: No valid DataFrame found to \
            parse into form-level COG TSVs"
        )

    return None


IGM_CORE_FIELDS = [
    "version",
    "subject_id",
    "report_type",
    "title",
    "service",
    "report_version",
    "disease_group",
    "percent_tumor",
    "percent_necrosis",
    "indication_for_study",
    "amendments",
]


def null_n_strip(value):
    """Format strings in IGM JSONs

    Args:
        value : Value read in from key:value pair in IGM JSON

    Returns:
        If str, formatted str; elif None, empty str; else original value argument
    """

    if value is None:
        return ""
    elif isinstance(value, str):
        return value.strip()
    else:
        return value


def flatten_igm(json_obj, parent_key="", flatten_dict=None, parse_type=None):
    """Recursive function to un-nest a nested dictionary for WXS and Archer Fusion

    Args:
        json_obj (dict): Nested JSON IGM form
        parent_key (str, optional): The inherited key from previous recursive run. Defaults to ''.
        flatten_dict (dict, optional): The inherited 'flattened' JSON from previous recursive run. Defaults to {}.
        parse_type (str, optional): When specified as 'cnv', for any key == 'disease_associated_gene_content', do not flatten value for that key

    Returns:
        dict: Un-nested dict/JSON
    """
    
    
    if flatten_dict is None:
        flatten_dict = {}

    if isinstance(json_obj, dict):
        for key, value in json_obj.items():
            new_key = f"{parent_key}.{key}" if parent_key else key

            if key == "disease_associated_gene_content" and parse_type == "cnv":
                flatten_dict[new_key] = value
                continue

            if isinstance(value, dict):
                flatten_igm(value, new_key, flatten_dict, parse_type)

            elif isinstance(value, list):
                if value:  # non-empty list
                    for i, item in enumerate(value):
                        item_key = f"{new_key}.{i}"
                        if isinstance(item, (dict, list)):
                            flatten_igm(item, item_key, flatten_dict, parse_type)
                        else:
                            flatten_dict[item_key] = null_n_strip(item)
                else:  # empty list
                    flatten_dict[new_key] = ""

            else:
                flatten_dict[new_key] = null_n_strip(value)

    elif isinstance(json_obj, list):
        if json_obj:
            for i, item in enumerate(json_obj):
                item_key = f"{parent_key}.{i}" if parent_key else str(i)
                if isinstance(item, (dict, list)):
                    flatten_igm(item, item_key, flatten_dict, parse_type)
                else:
                    flatten_dict[item_key] = null_n_strip(item)
        else:
            flatten_dict[parent_key] = ""

    return flatten_dict



def igm_full_form_convert(flatten_dict: dict, logger):
    """Convert flattened JSON to pd.DataFrame

    Args:
        flatten_dict (dict): IGM nested JSON that has been flattened to un-nested JSON

    Returns:
        pd.DataFrame: The flattened JSON converted to pd.DataFrame
    """

    runner_logger = get_run_logger()

    try:
        return pd.DataFrame([flatten_dict])
    except Exception as e:
        logger.error(f"Error converting flattened IGM JSON to pd.DataFrame: {e}")
        runner_logger.error(f"Error converting flattened IGM JSON to pd.DataFrame: {e}")
        return pd.DataFrame()


def igm_to_tsv(
    dir_path: str,
    igm_jsons: list,
    assay_type: str,
    igm_op: str,
    timestamp: str,
    results_parse: bool,
    manifest_path: str,
    logger,
):
    """Function to call the reading in and transformation of IGM JSON files

    Args:
        dir_path (str): Path to directory containing COG JSON files
        igm_jsons (list): List of COG JSON filenames located in dir_path
        assay_type (str): Molecular assay type of IGM JSONs (i.e. Archer Fusion, WXS or methylation)
        igm_op (str): Path to directory to output transformed IGM TSV files
        timestamp (str): Date-time of when script run
        results_parse (bool): If True, parse out results specific sections to separate form in long format TSV
        manifest_path (str): Path to manifest file containing sample metadata

    Returns:
        pd.DataFrame: pandas DataFrame of converted JSON data
        int: The count of JSON files successfully processed
        int: The count of JSON files unsuccessfully processed
        str: percent_tumor_necrosis_file_name for parsed values for percent_necrosis and percent_tumor
    """

    runner_logger = get_run_logger()

    valid = ["igm.tumor_normal", "igm.archer_fusion", "igm.methylation"]

    if assay_type not in valid:
        raise ValueError(f"assay_type {assay_type} is not one of {valid}.")

    df_list = []  # List to hold individual JSON DataFrames

    success_count = 0  # count of JSON files successfully processed
    error_count = 0  # count of JSON files not processed

    for filename in igm_jsons:
        file_path = os.path.join(dir_path, filename)
        try:
            file_2_flat = json.load(open(file_path))
            flatten_dict1 = flatten_igm(file_2_flat)

            flatten_dict_df = igm_full_form_convert(flatten_dict1, logger)
            
            flatten_dict_df['form_file_name'] = filename  # add file name to DataFrame
            
            df_list.append(flatten_dict_df)

            success_count += 1

        except Exception as e:
            error_count += 1
            logger.error(f" Error converting IGM JSON to TSV for file {file_path}: {e}")
            runner_logger.error(
                f" Error converting IGM JSON to TSV for file {file_path}: {e}"
            )

    if results_parse:
        # make output dir
        directory_path = f"{igm_op}/IGM_results_level_TSVs_{timestamp}"

        if not os.path.exists(directory_path):
            os.mkdir(directory_path)

        if assay_type == "igm.methylation":
            results_types = ["predicted_classification_classifier_scores", "results"]
        elif assay_type == "igm.archer_fusion":
            results_types = [
                "fusion_tier_one_or_two_result",
                "fusion_tier_three_result",
                "single_tier_one_or_two_result",
                "single_tier_three_result",
            ]
        elif assay_type == "igm.tumor_normal":
            results_types = [
                "amended_germline_results",
                "amended_somatic_cnv_results",
                "amended_somatic_results",
                "germline_cnv_results",
                "germline_results",
                "somatic_cnv_results",
                "somatic_results",
            ]

        op_dict = defaultdict(list)

        for filename in igm_jsons:
            file_path = os.path.join(dir_path, filename)
            try:
                parsed_results = igm_results_variants_parsing(
                    json.load(open(file_path)), filename, assay_type, results_types
                )

                for key in parsed_results.keys():
                    op_dict[key].append(parsed_results[key])

            except Exception as e:
                logger.error(
                    f"Could not parse results section from file {file_path}, please check and try again: {e}"
                )
        for result_type in op_dict.keys():
            replace_en_em_dash(pd.concat(op_dict[result_type])).to_csv(
                f"{directory_path}/IGM_{assay_type.replace('igm.', '')}_{result_type}_variant_data_{timestamp}.tsv",
                sep="\t",
                index=False,
            )

    # concat all processed JSONs together
    if len(df_list) > 0:
        concatenated_df = replace_en_em_dash(pd.concat(df_list, ignore_index=True))
        
        #read in file_name and sample.sample_id from manifest sheet clinical_measure_file
        clin_report_df = pd.read_excel(manifest_path, sheet_name="clinical_measure_file")
        
        # filter out instances where sample_id is NaN and keep only relevant columns
        clin_report_df = clin_report_df[~clin_report_df['sample.sample_id'].isna()][['participant.participant_id', 'sample.sample_id', 'file_name']].drop_duplicates().reset_index(drop=True)
        
        # rename columns in clin_report_df to match expected output
        clin_report_df.rename(
            columns={
                "participant.participant_id": "subject_id",
                "file_name": "form_file_name",
            },
            inplace=True
        )
        
        # merge concatenated_df with clin_report_df to add subject_id and form_file_name
        concatenated_df = concatenated_df.merge(
            clin_report_df,
            how="left",
            on=["subject_id", "form_file_name"],
        )

        #reorder columns to have subject_id, sample.sample_id and form_file_name first
        column_order = ["subject_id", "sample.sample_id", "form_file_name"] + [col for col in concatenated_df.columns if col not in ["subject_id", "sample.sample_id", "form_file_name"]]
        concatenated_df = concatenated_df[column_order]

        concatenated_df.to_csv(
            f"{igm_op}/IGM_{assay_type.replace('igm.', '')}_JSON_table_conversion_{timestamp}.tsv",
            sep="\t",
            index=False,
        )
        
        # parse pertinent_negatives_results from tumor_normal JSONs
        if assay_type == "igm.tumor_normal":
            # grab all columns that start with pertinent_negatives_results.summary
            pertinent_negatives_cols = [
                col for col in concatenated_df.columns if col.startswith("pertinent_negatives_results.summary")
            ]
            neg_df = concatenated_df[["subject_id", "sample.sample_id", "form_file_name"] + pertinent_negatives_cols].drop_duplicates().fillna("").reset_index(drop=True)


            # pivot the DataFrame to have one row per negative result
            neg_df = neg_df.melt(id_vars=["subject_id", "sample.sample_id", "form_file_name"],
                var_name="pertinent_negative_result",
                value_name="value").dropna(subset=["value"])
            
            # init gene df list
            gene_df = []
            
            #use regex to extract gene name from pertinent_negative_result
            for index, row in neg_df.iterrows():
                #try:
                genes = re.findall(r"[A-Z][A-Z0-9\-]{2,}(?: \(formerly\s[A-Z0-9\-]{3,}\))?", row['value'])
                if genes:
                    for gene in genes:
                        gene_df.append(list(row) + [gene])
                #except:
                #    gene_df.append(list(row) + [""])
            
            # filter out INDETERMINATE, CNV, LOH, NOTE, CNLOH values
            gene_df = pd.DataFrame(gene_df, columns=neg_df.columns.tolist() + ["gene_name"])
            gene_df = gene_df[~gene_df["gene_name"].isin(["INDETERMINATE", "CNV", "LOH", "NOTE", "CNLOH"])].drop_duplicates().reset_index(drop=True)
        
            # save to file
            pertinent_negatives_file_name = f"{igm_op}/IGM_{assay_type.replace('igm.', '')}_pertinent_negatives_results_{timestamp}.tsv"
            gene_df.to_csv(pertinent_negatives_file_name, sep="\t", index=False)

        # parse percent_tumor and percent_necrosis from samples metadata

        logger.info(
            f"Attempting to parse percent_tumor and percent_necrosis from samples metadata for assay type {assay_type}."
        )
        if "percent_tumor" not in concatenated_df.columns:
            concatenated_df["percent_tumor"] = ""
        if "percent_necrosis" not in concatenated_df.columns:
            concatenated_df["percent_necrosis"] = ""

        # parse relevant columns
        percent_df = concatenated_df[
            ["subject_id", "form_file_name", "percent_tumor", "percent_necrosis"]
        ].drop_duplicates().reset_index(drop=True)
        
        percent_df.columns = ['participant.participant_id', 'file_name', 'percent_tumor', 'percent_necrosis']
        
        # read in manifest to map samples to percent_tumor and percent_necrosis
        clin_report_df = pd.read_excel(manifest_path, sheet_name="clinical_measure_file")

        # filter out instances where sample_id is NaN and keep only relevant columns
        clin_report_df = clin_report_df[~clin_report_df['sample.sample_id'].isna()][['participant.participant_id', 'sample.sample_id', 'file_name']].drop_duplicates().reset_index(drop=True)

        # map values to samples
        merge_df = percent_df.merge(
            clin_report_df,
            how="left",
        )

        # formatting
        # replace any ~ with empty string
        merge_df["percent_tumor"] = merge_df["percent_tumor"].replace(
            r"~", "", regex=True
        )
        merge_df["percent_necrosis"] = merge_df["percent_necrosis"].replace(
            r"~", "", regex=True
        )
        merge_df["percent_tumor"] = merge_df["percent_tumor"].str.replace(
            "+", "",
        )
        merge_df["percent_necrosis"] = merge_df["percent_necrosis"].str.replace(
            "+", "",
        )
        merge_df["percent_tumor"] = merge_df["percent_tumor"].replace(
            "N/A", "",
        )
        merge_df["percent_necrosis"] = merge_df["percent_necrosis"].replace(
            "N/A", "",
        )
        
        # replace any < or > with empty string
        merge_df["percent_tumor"] = merge_df["percent_tumor"].replace(
            r"[<>]", "", regex=True
        )

        merge_df["percent_necrosis"] = merge_df["percent_necrosis"].replace(
            r"[<>]", "", regex=True
        )
        # for values with " - ", replace with midpoint of range
        merge_df["percent_tumor"] = pd.to_numeric(merge_df["percent_tumor"].str.replace(
            r"(\d+)\s*-\s*(\d+)",
            lambda m: str((int(m.group(1)) + int(m.group(2))) / 2),
            regex=True
        ), errors="coerce").astype("Int64")
        merge_df["percent_necrosis"] = pd.to_numeric(merge_df["percent_necrosis"].str.replace(
            r"(\d+)\s*-\s*(\d+)",
            lambda m: str((int(m.group(1)) + int(m.group(2))) / 2),
            regex=True
        ), errors="coerce").astype("Int64")

        # save merged df to output directory
        percent_tumor_necrosis_file_name = f"{igm_op}/IGM_{assay_type.replace('igm.', '')}_percent_tumor_necrosis_{timestamp}.tsv"
        
        logger.info(
            f"Saving merged percent_tumor and percent_necrosis data to {percent_tumor_necrosis_file_name}"
        )
        merge_df.to_csv(percent_tumor_necrosis_file_name, sep="\t", index=False)

        return concatenated_df, success_count, error_count, percent_tumor_necrosis_file_name
    else:
        logger.error(
            f" No valid IGM JSON files found and/or failed to open for assay_type {assay_type}."
        )
        # sys.exit("\n\t>>> Process Exited: No valid JSON files found.")
        return pd.DataFrame, success_count, error_count


def igm_results_variants_parsing(
    form: dict, form_name: str, assay_type: str, results_types: list
):
    """Results section specific parsing (long format)

    Args:
        form (dict): JSON form loaded in
        form_name (str): File name of form data is sourced from
        assay_type (str): Molecular assay type of IGM JSONs (i.e. Archer Fusion, WXS or methylation)
        results_types (list): Potential results sections that may appear in form to parse

    Raises:
        ValueError: If assay_type is not acceptable value

    Returns:
        dict: dict of dataframes of parsed and formatted results section(s)
    """

    # valid types check
    valid = ["igm.tumor_normal", "igm.archer_fusion", "igm.methylation"]

    if assay_type not in valid:
        raise ValueError(f"assay_type {assay_type} is not one of {valid}.")

    # check form type
    if not isinstance(form, dict):
        raise ValueError(f"Form is not of type dict.")

    all_output = {}  # init dict of dfs of each results section
    core_header = ["form_name"] + [field for field in IGM_CORE_FIELDS]
    core_fields = [form_name] + [form[field] for field in IGM_CORE_FIELDS]
    for results_type in results_types:
        output = []  # init list of dicts to make df of form specific results
        found = False  # if results/variants section found or not
        if results_type in form.keys():
            if assay_type == "igm.methylation":
                if len(form[results_type]) > 0:
                    found = True
                    for result in form[results_type]:
                        temp_header = list(result.keys())
                        temp_fields = [null_n_strip(i) for i in result.values()]
                        output.append(
                            dict(
                                zip(
                                    core_header + temp_header, core_fields + temp_fields
                                )
                            )
                        )

            else:  # archer fusion and wxs
                if (
                    "variants" in form[results_type].keys()
                    and len(form[results_type]["variants"]) > 0
                ):
                    found = True
                    for result in form[results_type]["variants"]:
                        if results_type in [
                            "somatic_cnv_results",
                            "amended_somatic_cnv_results",
                            "germline_cnv_results",
                        ]:
                            flatten_temp = flatten_igm(result, parse_type="cnv")
                            genes = flatten_temp["disease_associated_gene_content"]
                            flatten_temp.pop("disease_associated_gene_content")
                            for gene in genes:
                                temp_header = list(flatten_temp.keys()) + ["gene"]
                                temp_fields = [
                                    null_n_strip(i) for i in flatten_temp.values()
                                ] + [gene]
                                output.append(
                                    dict(
                                        zip(
                                            core_header + temp_header,
                                            core_fields + temp_fields,
                                        )
                                    )
                                )
                        else:
                            flatten_temp = flatten_igm(result)
                            temp_header = list(flatten_temp.keys())
                            temp_fields = [
                                null_n_strip(i) for i in flatten_temp.values()
                            ]
                            output.append(
                                dict(
                                    zip(
                                        core_header + temp_header,
                                        core_fields + temp_fields,
                                    )
                                )
                            )
        else:
            found = False
        if (
            found == False
        ):  # if never found results section, append df indicating no data for file
            output.append(dict(zip(core_header, core_fields)))

        all_output[results_type] = pd.DataFrame(output)

    return all_output
