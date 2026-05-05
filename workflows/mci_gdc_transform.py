"""Script to parse, process and transform MCI matadata template into GDC Submission Files"""

import json
import shutil
import os, sys
import pandas as pd
import argparse
from src.ccdi_gdc_mapping_functions import *
import logging
import requests
from datetime import datetime
from src.utils import get_time, folder_ul, file_dl, file_ul
from prefect import flow, task, get_run_logger
from src.cog_igm_utils import json_downloader
from prefect_shell import ShellOperation
from typing import Literal

@task(name="Extract Metadata to TSV Task: Survival Status Parser", log_prints=True)
def survival_status_parser(
    participant_df: pd.DataFrame, survival_df: pd.DataFrame
) -> pd.DataFrame:
    """Parse survival status information and add the most recent survival status to the participant DataFrame.

    Args:
        participant_df (pd.DataFrame): DataFrame containing participant information.
        survival_df (pd.DataFrame): DataFrame containing survival status information.

    Returns:
        pd.DataFrame: DataFrame with updated survival status information.
    """

    # sort and filter survival_df to get most recent survival status for each participant
    survival_df = survival_df.sort_values(
        by=["participant.participant_id", "age_at_last_known_survival_status"],
        ascending=[True, False],
    )
    most_recent_survival = survival_df.drop_duplicates(
        subset=["participant.participant_id"], keep="first"
    )[["participant.participant_id", "last_known_survival_status"]]
    # merge with participant_df to add last_known_survival_status to participant table
    participant_df = participant_df.merge(
        most_recent_survival,
        left_on="participant_id",
        right_on="participant.participant_id",
        how="left",
    )
    return participant_df

@task(name="Extract Metadata to TSV Task: Diagnosis Parser", log_prints=True)
def diagnosis_parser(
    participant_df: pd.DataFrame, diagnosis_df: pd.DataFrame
) -> pd.DataFrame:
    """Parse diagnosis information and add ICD-O3.2 diagnosis, diagnosis_category and anatomic_site to the participant DataFrame.

    Args:
        participant_df (pd.DataFrame): DataFrame containing participant information.
        diagnosis_df (pd.DataFrame): DataFrame containing diagnosis information.

    Returns:
        pd.DataFrame: DataFrame with updated diagnosis information.
        pd.DataFrame: Filtered diagnosis DataFrame with relevant columns for ICD-O3.2 diagnoses.
    """

    # filter diagnosis_df for ICD-O3.2 diagnoses and relevant columns
    diagnosis_df = diagnosis_df[
        diagnosis_df["diagnosis_classification_system"] == "ICD-O-3.2"
    ]
    diagnosis_df = diagnosis_df[
        [
            "participant.participant_id",
            "diagnosis_id",
            "diagnosis_category",
            "diagnosis",
            "anatomic_site",
            "age_at_diagnosis",
        ]
    ]
    # merge with participant_df to add diagnosis information to participant table
    participant_df = participant_df.merge(
        diagnosis_df,
        left_on="participant_id",
        right_on="participant.participant_id",
        how="left",
    )
    return participant_df, diagnosis_df

@task(name="Extract Metadata to TSV Task: FASTQ Files Parser", log_prints=True)
def fastq_parser(sequencing_file_df: pd.DataFrame) -> pd.DataFrame:
    """Parse sequencing file information and filter for FASTQ files for WXS and RNA-Seq.

    Args:
        sequencing_file_df (pd.DataFrame): DataFrame containing sequencing file information.

    Returns:
        pd.DataFrame: Filtered DataFrame containing only FASTQ files for WXS and RNA-Seq.
    """
    # filter for FASTQ files for WXS and RNA-Seq
    sequencing_file_df = sequencing_file_df[sequencing_file_df["file_type"] == "fastq"]
    sequencing_file_df = sequencing_file_df[
        sequencing_file_df["library_strategy"].isin(["WXS", "RNA-Seq"])
    ]
    return sequencing_file_df

@task(name="Extract Metadata to TSV Task: Extract Platform + Preservation Metadata to TSV", log_prints=True)
def extract_metadata_to_tsv(directory: str, output_tsv: str):
    """
    Reads all JSON files in a directory, extracts specific fields based on their type,
    and writes the data to a TSV file.

    Args:
        directory (str): Path to the directory containing JSON files.
        output_tsv (str): Path to the output TSV file.
    """
    extracted_data = []  # List to hold extracted data

    # Check if the directory exists
    if not os.path.isdir(directory):
        print(f"Directory '{directory}' does not exist.")
        return

    json_files = [f for f in os.listdir(directory) if f.endswith(".json")]
    total_files = len(json_files)

    if total_files == 0:
        print("No JSON files found in the directory.")
        return

    # Iterate through all JSON files in the directory
    for idx, filename in enumerate(json_files, start=1):
        filepath = os.path.join(directory, filename)
        try:
            # Read the JSON file
            with open(filepath, "r") as file:
                json_data = json.load(file)

            if "rawdata" in filename:
                # Handle "rawdata" files
                meta_data = json_data.get("meta_data", {})
                array_type = meta_data.get("array_type") or meta_data.get(
                    "Array type", "N/A"
                )
                material_type = meta_data.get("material_type") or meta_data.get(
                    "Material type", "N/A"
                )
                raw_id = meta_data.get("ID", "N/A")
                cleaned_id = raw_id.split("_")[1] if isinstance(raw_id, str) else "N/A"

                data_row = {
                    "file_name": filename,
                    "sample_id": cleaned_id,
                    "platform": array_type,
                    "preservation_method": material_type,
                }
                extracted_data.append(data_row)
            else:
                # Handle other JSON files
                metadata = json_data.get("metadata", {})
                sample_name = metadata.get("sample_name", "N/A")
                ffpe = metadata.get("ffpe", False)
                data_type = metadata.get("data_type", "N/A")

                cleaned_sample_name = (
                    sample_name.split("-")[-1]
                    if isinstance(sample_name, str)
                    else "N/A"
                )
                preservation_method = "FFPE" if ffpe else ""

                data_row = {
                    "file_name": filename,
                    "sample_id": cleaned_sample_name,
                    "platform": data_type,
                    "preservation_method": preservation_method,
                }
                extracted_data.append(data_row)

            # Display progress
            if (
                idx % 100 == 0 or idx == total_files
            ):  # Update every 100 files or at the end
                percent_complete = (idx / total_files) * 100
                print(
                    f"Processed {idx}/{total_files} files ({percent_complete:.2f}% complete)."
                )

        except (ValueError, json.JSONDecodeError) as e:
            print(f"Error reading {filename}: {e}")
        except Exception as e:
            print(f"Unexpected error with {filename}: {e}")

    # Convert extracted data to a DataFrame
    if extracted_data:
        df = pd.DataFrame(extracted_data)

        # Save DataFrame to a TSV file
        df.to_csv(output_tsv, sep="\t", index=False)
        print(f"Extracted data saved to {output_tsv} with {df.shape[0]} rows.")
    else:
        print("No valid JSON files were found or processed.")

@task(name="Sample Parser Task", log_prints=True)
def sample_parser(
    sample_df: pd.DataFrame, preservation_meth_platform_file: str
) -> pd.DataFrame:
    """Parse sample information and add preservation method and methylation platform from the preservation method and methylation platform file.

    Args:
        sample_df (pd.DataFrame): DataFrame containing sample information.
        preservation_meth_platform_file (str): Path to the preservation method and methylation platform file.

    Returns:
        pd.DataFrame: DataFrame with updated sample information including preservation method and methylation platform.
    """
    # read in preservation method and methylation platform file
    pres_meth_platform_df = pd.read_csv(preservation_meth_platform_file, sep="\t")

    # merge with sample_df to add preservation method and methylation platform to sample table
    sample_df = sample_df.merge(
        pres_meth_platform_df[pres_meth_platform_df.preservation_method.notnull()][
            ["sample_id", "preservation_method"]
        ].drop_duplicates(),
        left_on="sample_id",
        right_on="sample_id",
        how="left",
    )
    # if there are samples with missing preservation method, fill with "Not Reported"
    sample_df["preservation_method"] = sample_df["preservation_method"].fillna(
        "Not Reported"
    )
    return sample_df

@task(name="Extract Metadata to TSV Task: Methylation Array Parser", log_prints=True)
def methylation_parser(
    methylation_array_file_df: pd.DataFrame, preservation_meth_platform_file: str
) -> pd.DataFrame:
    """Parse methylation array file information and add methylation platform and preservation method from the preservation method and methylation platform file.

    Args:
        methylation_array_file_df (pd.DataFrame): DataFrame containing methylation array file information.
        preservation_meth_platform_file (str): Path to the preservation method and methylation platform file.

    Returns:
        pd.DataFrame: DataFrame with updated methylation array file information including methylation platform and preservation method.
    """
    # read in preservation method and methylation platform file
    pres_meth_platform_df = pd.read_csv(preservation_meth_platform_file, sep="\t")

    # merge with methylation_array_file_df to add methylation platform and preservation method to methylation array file table
    methylation_array_file_df = methylation_array_file_df.merge(
        pres_meth_platform_df[
            (pres_meth_platform_df.platform.notnull())
            & (pres_meth_platform_df.platform != "WES")
            & (pres_meth_platform_df.platform != "")
        ][["sample_id", "platform"]].drop_duplicates(),
        left_on="sample.sample_id",
        right_on="sample_id",
        how="left",
    )

    # map platform values to GDC dictionary API values
    platform_mapping = {
        "IlluminaHumanMethylationEPIC": "Illumina Methylation Epic",
        "IlluminaHumanMethylationEPICv2": "Illumina Methylation Epic v2",
    }

    methylation_array_file_df["platform"] = (
        methylation_array_file_df["platform"]
        .map(platform_mapping)
        .fillna(methylation_array_file_df["platform"])
    )

    # if there are samples with missing platform or preservation method, fill with "Unknown"
    methylation_array_file_df["platform"] = methylation_array_file_df[
        "platform"
    ].fillna("Unknown")

    # filter out only IDAT files for methylation_array_file_df
    methylation_array_file_df = methylation_array_file_df[
        methylation_array_file_df["file_type"] == "idat"
    ]
    return methylation_array_file_df

@flow(name="Preservation Method and Methylation Platform Parser Flow", log_prints=True)
def preservation_method_n_meth_platform_parser(
    manifest_file: str,
    working_dir: str,
    dt: str,
    bucket: str,
    runner: str,
    logger: logging.Logger,
) -> str:
    """Parse preservation method and methylation platform from node data.

    Args:
        manifest_file (str): Path to the MCI manifest file.
        working_dir (str): Path to the working directory for intermediate files.
        dt (str): Datetime string for file naming.
        bucket (str): Name of the S3 bucket where input files are stored and output files will be saved.
        runner (str): Name of the runner.
        logger (logging.Logger): Logger object for logging progress and results.
    Returns:
        pd.DataFrame: DataFrame with an additional column for preservation method.
    """
    

    # get cwd for later use
    cwd = os.getcwd()
    
    ## make dir
    pres_files_dir = f"{working_dir}/preservation_method_platform_files"
    os.makedirs(pres_files_dir, exist_ok=True)

    ## read in manifest file sheets to get files to download
    pres_seq_df = pd.read_excel(manifest_file, sheet_name="sequencing_file")
    pres_meth_df = pd.read_excel(
        manifest_file, sheet_name="methylation_array_file")
    
    # chdir to pres_files_dir to download JSON files there
    os.chdir(pres_files_dir)
    
    ## download json files from node
    pres_seq_df = pres_seq_df[pres_seq_df.file_type == 'json'][['file_name', 'file_url']].drop_duplicates()
    pres_meth_df = pres_meth_df[pres_meth_df.file_type == 'json'][['file_name', 'file_url']].drop_duplicates()
    concat_dl_df = pd.concat([pres_seq_df, pres_meth_df], ignore_index=True).drop_duplicates()
    if not [i for i in os.listdir(os.getcwd()) if i.endswith('.json')]: # check if dir empty, if so download JSONs

        # chunked downloading of JSON files
        chunk_size = 200

        # download JSON files
        for chunk in range(0, len(concat_dl_df), chunk_size):
            print(f"Downloading JSON chunk {chunk//chunk_size+1} of {len(concat_dl_df)//chunk_size+1}")
            json_downloader(concat_dl_df[chunk:chunk+chunk_size], [], logger)
    
    ## run preservation method and methylation platform parser to add preservation method and methylation platform to
    output_file = f"{pres_files_dir}/preservation_method_platform_output_{dt}.tsv"
    extract_metadata_to_tsv(pres_files_dir, output_file)
    
    # after processing, upload to output dir in S3
    file_ul(bucket, runner, f"mci_gdc_transform_{dt}_outputs", os.path.basename(output_file))
    
    # chdir back to running dir
    os.chdir(cwd)

    return output_file

@task(name="Setup Transform Task", log_prints=True)
def setup_transform(
    working_dir: str,
    manifest_file: str,
    dt: str,
    preservation_meth_platform_file: str = None,
    bucket: str = None,
    runner: str = None,
    logger: logging.Logger = None,
) -> str:
    """Set up the transformation process by reading input files and preparing data.

    Args:
        working_dir (str): Path to the working directory for intermediate files.
        manifest_file (str): Path to the MCI manifest file.
        dt (str): Datetime string for file naming.
        preservation_meth_platform_file (str, optional): Path to the preservation method and methylation platform file. Defaults to None.
        bucket (str, optional): Name of the S3 bucket where input files are stored and output files will be saved.
        runner (str, optional): Name of the runner.
        logger (logging.Logger, optional): Logger object for logging progress and results.

    Returns:
        str: Path to the prepped manifest file ready for transformation.
    """

    processed_manifest_file = f"{working_dir}/processed_manifest_{dt}.xlsx"

    if preservation_meth_platform_file is None or not os.path.isfile(preservation_meth_platform_file):
        preservation_meth_platform_file = preservation_method_n_meth_platform_parser(
            manifest_file, working_dir, dt, bucket, runner, logger
        )

    # transforms
    # read in manifest file sheets into dataframes
    participant = pd.read_excel(manifest_file, sheet_name="participant")
    diagnosis = pd.read_excel(manifest_file, sheet_name="diagnosis")
    survival = pd.read_excel(manifest_file, sheet_name="survival")
    sequencing_file_df = pd.read_excel(manifest_file, sheet_name="sequencing_file")
    sample_df = pd.read_excel(manifest_file, sheet_name="sample")
    methylation_array_file_df = pd.read_excel(
        manifest_file, sheet_name="methylation_array_file"
    )
    sample_df = pd.read_excel(manifest_file, sheet_name="sample")

    # 1: filter most recent surival status and add surival.last_known_survival_status to participant table
    participant_parse = survival_status_parser(participant, survival)
    # 2: filter ICD-O3.2 diagnosis, diagnosis_category and anatomic_site to participant table and add to participant table
    participant_parse, diagnosis_parse = diagnosis_parser(participant_parse, diagnosis)
    # 3: filter for FASTQ for WXS and RNA-Seq for sequencing_file
    sequencing_file_parse = fastq_parser(sequencing_file_df)
    # 4: add preservation method from preservation_meth_platform_file to sample_df
    sample_df_parse = sample_parser(sample_df, preservation_meth_platform_file)
    # 5: add methyation platform and filter idat files for methlyation_array_file
    methylation_parse = methylation_parser(
        methylation_array_file_df, preservation_meth_platform_file
    )

    # save prepped manifest file for transformations
    with pd.ExcelWriter(processed_manifest_file) as writer:
        participant_parse.to_excel(writer, sheet_name="participant", index=False)
        diagnosis_parse.to_excel(writer, sheet_name="diagnosis", index=False)
        sequencing_file_parse.to_excel(
            writer, sheet_name="sequencing_file", index=False
        )
        methylation_parse.to_excel(
            writer, sheet_name="methylation_array_file", index=False
        )
        sample_df_parse.to_excel(writer, sheet_name="sample", index=False)

    return processed_manifest_file

@task(name="Validate Graph Task", log_prints=True)
def validate_graph(output_dfs: dict, logger: logging.Logger) -> None:
    """Validate that the transformed dataframes for each node have complete data for cases with sequencing and methylation files.

    Args:
        output_dfs (dict): Dictionary containing the transformed dataframes for each node.
        logger (logging.Logger): Logger object for logging validation results.

    Returns:
        None
    """

    # mapping dict for required nodes for cases with sequencing and methylation files
    required_nodes = {
        "submitted_unaligned_reads": "read_groups.submitter_id",
        "raw_methylation_array": "aliquots.submitter_id",
        "read_group": "aliquots.submitter_id",
        "aliquot": "samples.submitter_id",
        "sample": "cases.submitter_id",
        "demographic": "cases.submitter_id",
        "diagnosis": "cases.submitter_id",
    }

    for node in output_dfs.keys():
        if node in required_nodes.keys():
            id_col = required_nodes[node]
            if id_col not in output_dfs[node].columns:
                logger.error(
                    f"Validation error: {id_col} column missing from {node} node dataframe."
                )
            parent_node = id_col.split(".")[0].rstrip("s")
            missing_ids = set(output_dfs[node][id_col]) - set(
                output_dfs[parent_node]["submitter_id"]
            )
            if missing_ids:
                logger.error(
                    f"Validation error: Missing parent cases in {parent_node} node dataframe that are present in {node} node dataframe: {missing_ids}"
                )


@task(name="Validate PVs Task", log_prints=True)
def validate_pvs(output_dfs: dict, logger: logging.Logger) -> None:
    """Validate that the transformed dataframes' PVs match expected values from GDC dictionary API.

    Args:
        output_dfs (dict): Dictionary containing the transformed dataframes for each node.
        logger (logging.Logger): Logger object for logging validation results.

    Returns:
        None
    """

    for node in output_dfs.keys():
        print(f"Validating PVs for node {node}...")
        prop_pv = requests.get(
            f"https://api.gdc.cancer.gov/v0/submission/_dictionary/{node}"
        ).json()
        for col in output_dfs[node].columns:
            if col in prop_pv["properties"]:
                if "enum" in prop_pv["properties"][col]:
                    valid_pvs = set(prop_pv["properties"][col]["enum"])
                    invalid_pvs = (
                        set(output_dfs[node][col].dropna().unique()) - valid_pvs
                    )
                    if invalid_pvs:
                        logger.error(
                            f"Validation error: Invalid PVs in column {col} of node {node} dataframe: {invalid_pvs}"
                        )
                        print(
                            f"Validation error: Invalid PVs in column {col} of node {node} dataframe: {invalid_pvs}"
                        )

DataCleanup = Literal["yes", "no"]

@flow(
    name="MCI GDC Transform Main Flow",
    log_prints=True,
    flow_run_name="mci_gdc_transform-{runner}_" + f"{get_time()}",
)
def mci_gdc_transform(
    bucket: str,
    manifest_file: str,
    runner: str,
    data_cleanup: DataCleanup = "no",
    preservation_meth_platform_file: str = None,
) -> None:
    """Main function to transform MCI manifest file into GDC submission files.

    Args:
        bucket: str: Name of the S3 bucket where input files are stored and output files will be saved.
        manifest_file (str): Path to the MCI manifest file.
        runner (str): Name of the runner.
        data_cleanup (DataCleanup, optional): Whether to perform data cleanup of working directories. Defaults to "no".
        preservation_meth_platform_file (str, optional): Path to the preservation method and methylation platform file. Defaults to None.
    Returns:
        None
    """
    
    if data_cleanup == "yes":
        runner_logger = get_run_logger()
        runner_logger.info(">>> Performing data cleanup ...")
        ShellOperation(
            commands=[
                "ls -l /usr/local/data/",
                "rm -r /usr/local/data/mci_gdc_transform_*",
                "ls -l /usr/local/data/",  # confirm removal of mci_gdc_transform working dirs
            ]
        ).run()
        runner_logger.info(">>> Data clean up completed, exiting workflow ....")
        return None
    
    start_time = datetime.now()
    dt = get_time()

    # set up logger
    log_filename = f"DCC_GDC_transform_{dt}.log"
    file_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format=file_FORMAT,
        filemode="w"
    )
    logger = logging.getLogger("DCC_GDC_transform")

    logger.info(f"Logs beginning at {dt}")

    # download manifest file
    file_dl(bucket, manifest_file)

    # set up working directory path
    working_dir = f"/usr/local/data/mci_gdc_transform_{dt}"

    # check if downloaded
    if not os.path.isfile(os.path.basename(manifest_file)):
        print(f"Error: Manifest file '{manifest_file}' not found.")
        sys.exit(1)
    
    manifest_file = os.path.basename(manifest_file)

    # do prior work to transform the manifest file into GDC submission files
    # i.e. filter out most recent surival status, filter out already submitted files and patients, etc.
    prepped_manifest = setup_transform(
        working_dir, manifest_file, dt, preservation_meth_platform_file, bucket, runner, logger
    )

    # read in rules file for transformations
    rules_df = pd.read_csv("docs/MCI_CCDI_GDC_Mappings.txt", sep="\t")

    # strip whitespace from rules_df
    rules_df = rules_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # get source and target node mappings
    rules_df_node_map = (
        rules_df[["source_node", "target_node"]]
        .dropna()
        .drop_duplicates()
        .to_dict(orient="records")
    )

    output_dfs = {}  # dict to hold output dataframes for each node
    outputs_dir = f"{working_dir}/mci_gdc_transform_{dt}_outputs"
    os.makedirs(outputs_dir, exist_ok=True)

    # actual transformations to create GDC submission files
    # iterate over each node and apply transformations based on rules file
    for mapping in rules_df_node_map:
        try:
            engine = TransformerEngine(
                rules_df[rules_df.target_node == mapping["target_node"]]
            )
            input_df = pd.read_excel(
                prepped_manifest, sheet_name=mapping["source_node"]
            )
            output_df = engine.transform(
                mapping["target_node"], input_df
            )  # input_data is the data for the source node, need to read in from manifest file or from previous transformations
            output_dfs[mapping["target_node"]] = output_df
        except Exception as e:
            print(f"Error during transformation: {e}")
            logger.error(f"Error during transformation: {e}")
            raise e

    for node, df in output_dfs.items():
        print(f"Output for node {node}:")
        print(df.head())
        # save to file
        df.to_csv(f"{outputs_dir}/{node}_output.tsv", sep="\t", index=False)

    # validate that seq/meth files thru case have complete set of nodes' data
    validate_graph(output_dfs, logger)

    # validate transformed files' PVs against GDC dictionary API
    validate_pvs(output_dfs, logger)

    end_time = datetime.now()
    time_diff = end_time - start_time
    print(f"\n\t>>> Time to Completion: {time_diff}")
    logger.info(f"Time to Completion: {time_diff}")

    # log files to output directory
    logging.shutdown()
    
    # move log file to outputs dir
    shutil.move(log_filename, f"{outputs_dir}/{log_filename}")
    
    # upload outputs to S3
    folder_ul(
        local_folder=f"{outputs_dir}",
        bucket=bucket,
        destination=runner + "/",
        sub_folder="",
    )

