import shutil

import pandas as pd
import sys, os
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from datetime import datetime
from pytz import timezone
from src.cog_rules_functions import *
from src.utils import get_time, get_logger
from prefect import flow, get_run_logger


@flow(
    name="COG IGM to DCC Mapping Transform",
    log_prints=True,
    flow_run_name="json2tsv_dcc_mapper_flow-" + f"{get_time()}",
)
def cog_igm_dcc_mapping_transform(rule_source: str, rules_file: str, input_file: str, manifest_path: str, output_path: str):
    """Map COG or IGM parsed/flattened data to DCC model manifest using rules XLSX to guide mapping

    Args:
        rule_source (str): Rules source to use for mapping; must be either "COG" or "IGM"
        rules_file (str): Path to the Excel file containing the mapping rules
        input_file (str): Path to the input COG or IGM file to be transformed
        manifest_path (str): Path to the DCC model manifest XLSX file
        output_path (str): Directory where the transformed files and updated manifest will be saved

    Raises:
        ValueError: If the rule source is invalid
    """
    runner_logger = get_run_logger()
    dt = get_time()
    
    log_filename = f"{output_path}/COG_IGM_JSON2TSV_DCC_mapping_transform_{dt}"
    logger = get_logger(log_filename, "info")
    
    logger.info(f"Starting COG data transformation process for {rule_source}.")
    runner_logger.info(f"Starting COG data transformation process for {rule_source}.")
    logger.info(f"Input file: {input_file}")
    runner_logger.info(f"Input file: {input_file}")
    logger.info(f"Output directory: {output_path}")
    runner_logger.info(f"Output directory: {output_path}")
    
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    
    # Load mapping rules
    # check that rules source is valid
    valid_rule_sources = ["COG", "IGM"]
    
    if rule_source not in valid_rule_sources:
        logger.error(f"Invalid rule source: {rule_source}. Valid options are: {valid_rule_sources}")
        runner_logger.error(f"Invalid rule source: {rule_source}. Valid options are: {valid_rule_sources}")
        raise ValueError(f"Invalid rule source: {rule_source}. Valid options are: {valid_rule_sources}")
    
    #rules_df = pd.read_csv(sys.argv[1], sep="\t")
    if rule_source == "COG":
        rules_df = pd.read_excel(rules_file, sheet_name="cog_rules", engine="openpyxl")
    elif rule_source == "IGM":
        rules_df = pd.read_excel(rules_file, sheet_name="igm_rules", engine="openpyxl")
    
    # filter rules source
    rules_df = rules_df[rules_df["source"] == rule_source]
    
    # strip all cells in dataframe of whitespace
    rules_df = rules_df.map(lambda x: x.strip() if isinstance(x, str) else x)

    # Example input dataframe
    input_df = pd.read_csv(input_file, sep="\t", low_memory=False)
    
    # filter out rows where upi isna()
    input_df = input_df[~input_df["upi"].isna()]

    try:
        engine = TransformerEngine(rules_df)
        node_outputs = engine.transform(input_df)
    except Exception as e:
        logger.error(f"Error during transformation: {e}")
        runner_logger.error(f"Error during transformation: {e}")
        raise e
    
    #### postprocessing and upload ####
    
    # for node diagnosis, if any submitted_dianosis == 'Neoplasm, uncertain whether benign or malignant#Neoplasm, NOS', 
    # replace with 'Neoplasm, NOS'
    if "diagnosis" in node_outputs:
        diagnosis_df = node_outputs["diagnosis"]
        diagnosis_df.loc[diagnosis_df["submitted_diagnosis"] == "Neoplasm, uncertain whether benign or malignant#Neoplasm, NOS", "submitted_diagnosis"] = "Neoplasm, NOS"
        node_outputs["diagnosis"] = diagnosis_df
        
    if "treatment_response" in node_outputs:
        # filter out rows where response is 'Not Reported' and response is not null
        treatment_response_df = node_outputs["treatment_response"]
        treatment_response_df = treatment_response_df[~((treatment_response_df["response"] == "Not Reported") & (treatment_response_df["response"].notna()))]
        node_outputs["treatment_response"] = treatment_response_df
        
    if "treatment_surgery" in node_outputs:
        # filter out rows where surgery_type is Not Applicable
        treatment_surgery_df = node_outputs["treatment_surgery"]
        treatment_surgery_df = treatment_surgery_df[~((treatment_surgery_df["surgery_type"] == "Not Applicable") & (treatment_surgery_df["surgery_type"].notna()))]
        node_outputs["treatment_surgery"] = treatment_surgery_df
    
    if "treatment_chemotherapy" in node_outputs:
        # drop rows where chemotherapy_type is empty
        treatment_chemotherapy_df = node_outputs["treatment_chemotherapy"]
        print(treatment_chemotherapy_df.columns)
        treatment_chemotherapy_df = treatment_chemotherapy_df[~(treatment_chemotherapy_df["chemotherapy_type"].isna()) | (treatment_chemotherapy_df["chemotherapy_type"] != "")]
        
        # for rows that have the same treatment_chemotherapy_id, combine chemotherapy_agent values into single row and deduope all other rows
        cols_order = treatment_chemotherapy_df.columns.tolist()
        
        treatment_chemotherapy_df = treatment_chemotherapy_df.groupby("treatment_chemotherapy_id").agg({
            "type": "first",
            "participant.participant_id": "first",
            "treatment_type": "first",
            "chemotherapy_type": "first",
            "age_at_treatment_start": "first",
            "age_at_treatment_end": "first",
            "chemotherapy_agent": lambda x: ";".join(set([item for sublist in x.str.split(";") for item in sublist if item])),
            "chemotherapy_class" : "first",
            "chemotherapy_subclass" : "first",
            "dose" : "first",
            "dose_unit" : "first",
            "dose_route" : "first",
            "dose_frequency" : "first",
            "protocol" : "first",
        }).reset_index()
        node_outputs["treatment_chemotherapy"] = treatment_chemotherapy_df[cols_order]
        
    if "treatment_other" in node_outputs:
        # filter out rows where treatment_other_type is Not Applicable
        treatment_other_df = node_outputs["treatment_other"]
        cols_order = treatment_other_df.columns.tolist()
        
        treatment_other_df = treatment_other_df.groupby("treatment_other_id").agg({
            "type": "first",
            "participant.participant_id": "first",
            "treatment_type": "first",
            "age_at_treatment_start": "first",
            "age_at_treatment_end": "first",
            "other_treatment_type": lambda x: ";".join(set([item for sublist in x.str.split(";") for item in sublist if item])),
        }).reset_index()
        node_outputs["treatment_other"] = treatment_other_df[cols_order]
        
    # save transformed datarames to tsv
    os.makedirs(f"{output_path}/COG/DCC_Mapping/", exist_ok=True)
    
    for node, df in node_outputs.items():
        df.drop_duplicates().to_csv(f"{output_path}/COG/DCC_Mapping/{node}_{dt}_transformed.tsv", sep="\t", index=False)
    
    # for each node type in node_outouts, find corresponding sheet in manifest
    # then load in sheet and append data in node_outputs to manifest sheet and update sheet in the manifest; use columns headers from manifest
    for node, df in node_outputs.items():
        manifest_df = pd.read_excel(manifest_path, sheet_name=node, engine="openpyxl")
        if not manifest_df.empty:
            # make an empty dataframe with same columns as manifest sheet and append data from node_outputs to it, then concat with manifest sheet
            empty_df = pd.DataFrame(columns=manifest_df.columns)
            # strip whitespace from column names in node_outputs df
            df.columns = df.columns.str.strip()
            # reorder df cols to match manifest sheet cols, 
            df = df.reindex(columns=manifest_df.columns)
            # strip whitespace from column names in manifest_df
            manifest_df.columns = manifest_df.columns.str.strip()
            # concat data from node_outputs to empty_df to ensure same columns
            df = pd.concat([empty_df, df], ignore_index=True)
            # append data in node_outputs to manifest sheet and update sheet in the manifest
            with pd.ExcelWriter(manifest_path, engine="openpyxl", mode="a", if_sheet_exists="overlay") as writer:
                df.drop_duplicates().to_excel(writer, sheet_name=node, index=False, header=False, startrow=1)
    

if __name__ == "__main__":
    cog_igm_dcc_mapping_transform(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])