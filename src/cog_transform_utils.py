"""Further transforms of COG data and perform mappings back to SAS Labels and CCDI data model"""

import os
import pandas as pd
import numpy as np
import sys
from datetime import datetime
from src.utils import get_time, get_date, get_logger
from prefect import flow, get_run_logger
from prefect_shell import ShellOperation
import logging  # Add this import

##for testing
def get_time() -> str:
    """Returns the current time"""
    now = datetime.now()
    dt_string = now.strftime("%Y%m%d_T%H%M%S")
    return dt_string

def clean_column_semicolon_concat(
    df: pd.DataFrame, new_col_name: str, col_name1: str, col_name2: str
):
    """Concatenate values from col_name1 and col_name2 with ';' between them, handling NaNs

    Args:
        df (pd.DataFrame): DataFrame to perform changes
        new_col_name (str): New column name for dataframe for 2 concatenated columns
        col_name1 (str): first column to be concatenated
        col_name2 (str): second column to be concatenated

    Returns:
        pd.DataFrame: modified dataframe
    """
    # Concatenate values from col_name1 and col_name2 with ';' between them, handling NaNs
    df[new_col_name] = np.where(
        df[col_name1].notna() & df[col_name2].notna(),
        df[col_name1].fillna("").astype(str)
        + ";"
        + df[col_name2].fillna("").astype(str),
        df[col_name1].fillna("").astype(str) + df[col_name2].fillna("").astype(str),
    )

    # Remove trailing ";" from the concatenated string
    df[new_col_name] = df[new_col_name].str.rstrip(";")

    return df


def clean_column_underscore_concat(
    df: pd.DataFrame, new_col_name: str, col_name1: str, col_name2: str
):
    """Concatenate values from col_name1 and col_name2 with '_' between them, handling NaNs

    Args:
        df (pd.DataFrame): DataFrame to perform changes
        new_col_name (str): New column name for dataframe for 2 concatenated columns
        col_name1 (str): first column to be concatenated
        col_name2 (str): second column to be concatenated

    Returns:
        pd.DataFrame: modified dataframe
    """
    # Concatenate values from col_name1 and col_name2 with '_' between them, handling NaNs
    df[new_col_name] = np.where(
        df[col_name1].notna() & df[col_name2].notna(),
        df[col_name1].fillna("").astype(str)
        + "_"
        + df[col_name2].fillna("").astype(str).str.replace(".0", ""),
        df[col_name1].fillna("").astype(str) + df[col_name2].fillna("").astype(str),
    )

    # Remove trailing "_" from the concatenated string
    df[new_col_name] = df[new_col_name].str.rstrip("_")

    return df

def clean_column_underscore_simple_concat(
    df: pd.DataFrame, new_col_name: str, col_name1: str, col_name2: str, str_val: str
):
    """Concatenate values from col_name1 and a STRING value with '_' between them, handling NaNs

    Args:
        df (pd.DataFrame): DataFrame to perform changes
        new_col_name (str): New column name for dataframe for 2 concatenated columns
        col_name1 (str): first column to be concatenated
        col_name2 (str): second column to be check if empty/NaN
        str_val (str): STRING value to be concatenated

    Returns:
        pd.DataFrame: modified dataframe
    """
    # Concatenate values from col_name1 and col_name2 with '_' between them, handling NaNs
    mask = (
    df[col_name1].notna() & (df[col_name1] != "") &
    df[col_name2].notna() & (df[col_name2] != "")
)

    df[new_col_name] = np.where(
        mask,
        df[col_name1].astype(str) + f"_{str_val}",
        ""
    )

    # Remove trailing "_" from the concatenated string
    df[new_col_name] = df[new_col_name].str.rstrip("_")

    return df


def clean_column_space_colon_concat(
    df: pd.DataFrame, new_col_name: str, col_name1: str, col_name2: str
):
    """Concatenate values from col_name1 and col_name2 with ' : ' between them, handling NaNs

    Args:
        df (pd.DataFrame): DataFrame to perform changes
        new_col_name (str): New column name for dataframe for 2 concatenated columns
        col_name1 (str): first column to be concatenated
        col_name2 (str): second column to be concatenated

    Returns:
        pd.DataFrame: modified dataframe
    """
    # Concatenate values from col_name1 and col_name2 with ' : ' between them, handling NaNs
    df[new_col_name] = np.where(
        df[col_name1].notna() & df[col_name2].notna(),
        df[col_name1].fillna("").astype(str)
        + " : "
        + df[col_name2].fillna("").astype(str),
        df[col_name1].fillna("").astype(str) + df[col_name2].fillna("").astype(str),
    )

    # Remove trailing " : " from the concatenated string
    df[new_col_name] = df[new_col_name].str.rstrip(" : ")

    return df

def summarizer(df: pd.DataFrame, logger: logging.Logger):
    """Summarizes the DataFrame by counting unique values in each column and logging the results. 
    Also counts the number of instances of each unique value and the number of null values in each column.
    Returns a long dataframe structure with the summary of unique values and their counts for each column.

    Args:
        df (pd.DataFrame): The DataFrame to summarize.
        logger (logging.Logger): Logger object for logging messages.

    Returns:
        A dictionary with the summary of unique values and their counts for each column.
    """
    logger.info("Starting summarization of COG CCDI transformation.")

    # Create a long DataFrame structure with the summary
    summary = []

    # Count unique values in each column
    unique_counts = df.nunique()
    logger.info("Unique value counts:")
    for column, count in unique_counts.items():
        logger.info(f"  {column}: {count} unique values")
    logger.info("Counting instances of each unique value in each column:")
    for column in df.columns:
        value_counts = df[column].value_counts(dropna=False)
        logger.info(f"  {column} value counts:")
        for value, count in value_counts.items():
            logger.info(f"    {value}: {count} instances")
            summary.append({
                "column": column,
                "unique_value": value,
                "count": count
            })
    
    df_summary = pd.DataFrame(summary)

    #replace all blank strings with value <BLANK>
    df_summary['unique_value'] = df_summary['unique_value'].replace('', '<BLANK>')

    return df_summary


@flow(
    name="COG Transformer",
    log_prints=True,
    flow_run_name="cog-transformer-" + f"{get_time()}",
)
def cog_transformer(df_reshape_file_name: str, output_dir: str):  # Remove logger parameter
    """
    Transforms and reshapes COG data to map back to SAS Labels and CCDI data model.

    This function performs various transformations on the input DataFrame, including:
    - Selecting specific columns
    - Renaming columns
    - Concatenating columns with specific delimiters
    - Calculating new columns based on existing data
    - Applying conditional logic to create new columns
    - Combining multiple columns into a single column
    - Dropping unnecessary columns
    - Removing duplicates

    Args:
        df_reshape_file_name (str): Path to the input TSV file containing the reshaped data.
        output_dir (str): Directory where the output TSV file will be saved.
        logger (Logger): Logger object for logging messages during the transformation process.

    Returns:
        log_filename (str): Name of the log file created during the transformation process.
    """
    
        # Set up logging
    """log_filename = f"COG_IGM_JSON2TSV_cog_transformations_{get_time()}.log"
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger("COG_Transformer")"""
    log_filename = f"{output_dir}/COG_IGM_JSON2TSV_COG_Transform"
    logger = get_logger(log_filename, "info")
    
    dt = get_time()

    # Data Reshape/mutate

    runner_logger = get_run_logger()

    #log_filename = f"COG_IGM_JSON2TSV_cog_transformations" + get_date() + ".log"
    #logger = get_logger("COG_IGM_JSON2TSV_cog_transformations", "info")

    # Log the start of the transformation
    logger.info("Starting COG data transformation process.")
    logger.info(f"Input file: {df_reshape_file_name}")
    logger.info(f"Output directory: {output_dir}")

    # Load the data
    df_reshape = pd.read_csv(df_reshape_file_name, sep="\t", low_memory=False)
    
    logger.info("Starting row count: %d", len(df_reshape))

    df_reshape = df_reshape.applymap(lambda x: x.replace('\n\r', ' ').replace('\n', ' ').replace('\r', ' ') if isinstance(x, str) else x)

    df_reshape.replace('.', np.nan, inplace=True) #fix issue where should be blank/null but only a period supplied

    df_reshape.replace('', np.nan, inplace=True) #fix issue where should be blank/null but only a blank str supplied

    df_reshape.replace(' ', np.nan, inplace=True) #fix issue where should be blank/null but only a space supplied

    # the specific columns we want in our mutation df
    direct_columns = [
        "upi",
        "DEMOGRAPHY.DM_ETHNIC",
        "DEMOGRAPHY.DM_SEX",
        "DEMOGRAPHY.DM_CRACE",
        "COG_UPR_DX.PTDT_IDP",
        "COG_UPR_DX.DATE_DIA",
        "DEMOGRAPHY.DM_BRTHDAT",
        "COG_UPR_DX.TOPO_TEXT",
        "COG_UPR_DX.MORPHO_TEXT",
        "COG_UPR_DX.MORPHO_ICDO",
        "COG_UPR_DX.TOPO_ICDO",
        "FINAL_DIAGNOSIS.PRIMDXDSCAT",
        "COG_UPR_DX.REG_STAGE_CODE_TEXT",
        "CNS_DIAGNOSIS_DETAIL.MH_MHCAT_CNSDXCAT",
        "FOLLOW_UP.REP_EVAL_PD_TP",
        "FOLLOW_UP.PT_FU_END_DT",
        "FOLLOW_UP.PT_VST",
        "FOLLOW_UP.COMP_RESP_CONF_IND_3",
        "FOLLOW_UP.DZ_EXM_REP_IND_2",
        "FOLLOW_UP.PT_INF_CU_FU_COL_IND",
    ]

    # columns we want in our mutation df that require certain patterns
    pattern_columns_followup = [
        col
        for col in df_reshape.columns
        if col.startswith("FOLLOW_UP.FSTLNTXINIDXADMCAT_A")
    ]
    pattern_columns_agent = [
        col for col in df_reshape.columns if col.__contains__("AGT_ADM_NM_A")
    ]
    pattern_columns_diagdetail = [
        col
        for col in df_reshape.columns
        if col.startswith("CNS_DIAGNOSIS_DETAIL.MH_MHSCAT_CNSDXINTGRT")
    ]

    selected_columns = (
        direct_columns
        + pattern_columns_agent
        + pattern_columns_diagdetail
        + pattern_columns_followup
    )
    selected_columns = list(set(selected_columns))

    # Log selected columns
    logger.info(f"Selected columns for transformation: {selected_columns}")

    # Apply the selected columns to the new mutation df
    df_mutation = df_reshape[selected_columns]

    logger.info("Row count after selecting columns: %d", len(df_mutation))

    # Replace items in "FOLLOW_UP" columns if the corresponding item in "FOLLOW_UP.PT_INF_CU_FU_COL_IND" is 'No'
    logger.info("Replacing items in FOLLOW_UP columns where FOLLOW_UP.PT_INF_CU_FU_COL_IND is 'No'")
    follow_up_columns = [col for col in df_mutation.columns if col.startswith("FOLLOW_UP")]
    df_mutation.loc[df_mutation["FOLLOW_UP.PT_INF_CU_FU_COL_IND"] == "No", follow_up_columns] = ""

    # Check for follow-ups not in sequential order
    logger.info("Checking for sequential order of follow-ups")
    for upi, group in df_mutation.groupby("upi"):
        group = group.sort_values(by="FOLLOW_UP.REP_EVAL_PD_TP", ascending=True)
        previous_date = None
        for index, row in group.iterrows():
            try:
                current_date = row["FOLLOW_UP.PT_FU_END_DT"]
                if previous_date is not None and int(current_date) < int(previous_date):
                    logger.error(
                        f"Logic error for participant_id {upi}: FOLLOW_UP.PT_FU_END_DT {current_date} is less than the preceding FOLLOW_UP.PT_FU_END_DT {previous_date}."
                    )
                previous_date = current_date
            except Exception as e:
                logger.error(
                    f"Error processing participant_id {upi}: {e}"
                )

    logger.info("Row count after check for follow-ups not in sequential order: %d", len(df_mutation))

    # Rename columns that do not have value changes
    df_mutation = df_mutation.rename(
        columns={
            "upi": "participant_id",
            "DEMOGRAPHY.DM_SEX": "sex_at_birth",
            "COG_UPR_DX.MORPHO_TEXT" : "diagnosis",
            "COG_UPR_DX.MORPHO_ICDO" : "icd_o_code",
            "FINAL_DIAGNOSIS.PRIMDXDSCAT": "primary_diagnosis_disease_group",
            "COG_UPR_DX.REG_STAGE_CODE_TEXT": "registry_stage_code",
            "FOLLOW_UP.PT_VST": "vital_status",
            "CNS_DIAGNOSIS_DETAIL.MH_MHCAT_CNSDXCAT": "CNS_category",
            "FOLLOW_UP.FSTLNTXINIDXADMCAT_A1": "Chemotherapy;Immunotherapy",
            "FOLLOW_UP.FSTLNTXINIDXADMCAT_A2": "Radiation Therapy, NOS",
            "FOLLOW_UP.FSTLNTXINIDXADMCAT_A3": "Stem Cell Transplant",
            "FOLLOW_UP.FSTLNTXINIDXADMCAT_A4": "Surgical Procedure",
            "FOLLOW_UP.FSTLNTXINIDXADMCAT_A5": "Cellular Therapy",
            "FOLLOW_UP.FSTLNTXINIDXADMCAT_A6": "Other",
        }
    )

    # Log renamed columns
    logger.info("Renamed columns:")
    for old_col, new_col in {
        "upi": "participant_id",
        "DEMOGRAPHY.DM_SEX": "sex_at_birth",
        "COG_UPR_DX.MORPHO_TEXT": "diagnosis",
        "COG_UPR_DX.MORPHO_ICDO": "icd_o_code",
        "FINAL_DIAGNOSIS.PRIMDXDSCAT": "primary_diagnosis_disease_group",
        "COG_UPR_DX.REG_STAGE_CODE_TEXT": "registry_stage_code",
        "FOLLOW_UP.PT_VST": "vital_status",
        "CNS_DIAGNOSIS_DETAIL.MH_MHCAT_CNSDXCAT": "CNS_category",
    }.items():
        logger.info(f"  {old_col} -> {new_col}")

    # Create new columns that have value changes

    # Log concatenation operations
    logger.info("Performing concatenation operations:")
    logger.info("  Creating 'race' by concatenating DEMOGRAPHY.DM_CRACE and DEMOGRAPHY.DM_ETHNIC with ';'")
    logger.info("  Creating 'diagnosis_id' by concatenating participant_id and COG_UPR_DX.PTDT_IDP with '_'")
    logger.info("  Creating 'follow_up_id' by concatenating participant_id and FOLLOW_UP.REP_EVAL_PD_TP with '_'")
    logger.info("  Creating 'primary_site' by concatenating COG_UPR_DX.TOPO_ICDO and COG_UPR_DX.TOPO_TEXT with ' : '")

    # CONCATENATIONS
    df_mutation = clean_column_semicolon_concat(
        df_mutation, "race", "DEMOGRAPHY.DM_CRACE", "DEMOGRAPHY.DM_ETHNIC"
    )
    df_mutation = clean_column_underscore_concat(
        df_mutation, "diagnosis_id", "participant_id", "COG_UPR_DX.PTDT_IDP"
    )
    df_mutation = clean_column_underscore_concat(
        df_mutation, "follow_up_id", "participant_id", "FOLLOW_UP.REP_EVAL_PD_TP"
    )
    df_mutation = clean_column_space_colon_concat(
        df_mutation, "primary_site", "COG_UPR_DX.TOPO_ICDO", "COG_UPR_DX.TOPO_TEXT"
    )

    logger.info("Row count after concatenation operations: %d", len(df_mutation))

    # Log calculation operations
    logger.info("Performing calculation operations:")
    logger.info("  Calculating 'age_at_diagnosis' as the sum of DEMOGRAPHY.DM_BRTHDAT and COG_UPR_DX.DATE_DIA")
    logger.info("  Calculating 'age_at_follow_up' as the sum of DEMOGRAPHY.DM_BRTHDAT and FOLLOW_UP.PT_FU_END_DT")

    # EQUATIONS

    df_mutation['DEMOGRAPHY.DM_BRTHDAT'] = pd.to_numeric(df_mutation['DEMOGRAPHY.DM_BRTHDAT'], errors='coerce')
    df_mutation['COG_UPR_DX.DATE_DIA'] = pd.to_numeric(df_mutation['COG_UPR_DX.DATE_DIA'], errors='coerce')
    df_mutation['FOLLOW_UP.PT_FU_END_DT'] = pd.to_numeric(df_mutation['FOLLOW_UP.PT_FU_END_DT'], errors='coerce')

    df_mutation["age_at_diagnosis"] = abs(df_mutation["DEMOGRAPHY.DM_BRTHDAT"]) + abs(df_mutation["COG_UPR_DX.DATE_DIA"])
    df_mutation["age_at_follow_up"] = abs(df_mutation["DEMOGRAPHY.DM_BRTHDAT"]) + abs(df_mutation["FOLLOW_UP.PT_FU_END_DT"])

    logger.info("Row count after calculation operations: %d", len(df_mutation))

    # Log conditional operations
    logger.info("Performing conditional operations:")
    logger.info("  Creating 'response' based on FOLLOW_UP.COMP_RESP_CONF_IND_3 and FOLLOW_UP.DZ_EXM_REP_IND_2")
    logger.info("  Creating 'CNS_diagnosis' by combining CNS_DIAGNOSIS_DETAIL columns with ';'")

    # CONDITIONAL

    # Create Response
    # Define the conditions
    conditions_response = [
        (df_mutation["FOLLOW_UP.COMP_RESP_CONF_IND_3"] == "Yes"),
        (df_mutation["FOLLOW_UP.COMP_RESP_CONF_IND_3"] == "No")
        & (df_mutation["FOLLOW_UP.DZ_EXM_REP_IND_2"] == "Yes"),
        (df_mutation["FOLLOW_UP.COMP_RESP_CONF_IND_3"] == "No")
        & (df_mutation["FOLLOW_UP.DZ_EXM_REP_IND_2"] == "No"),
        (df_mutation["FOLLOW_UP.COMP_RESP_CONF_IND_3"] == "Unknown"),
    ]
    # Define the corresponding choices for each condition
    choices_response = ["Complete Remission", "Unknown", "Not Reported", "Unknown"]
    # Apply the conditions and choices to create the 'response' column
    df_mutation["response"] = np.select(conditions_response, choices_response, default="")

    logger.info("Row count after conditional operations: %d", len(df_mutation))
    # Create CNS diagnosis
    # Select columns that start with 'CNS_DIAGNOSIS_DETAIL'
    cns_diagnosis_columns = df_mutation.filter(like="CNS_DIAGNOSIS_DETAIL.").columns

    # Combine the selected columns into one, separated by `;`, with "" for NaN
    df_mutation["CNS_diagnosis"] = (
        df_mutation[cns_diagnosis_columns].fillna("").astype(str).agg(";".join, axis=1)
    )

    # Optional: Remove any trailing or leading `;`
    df_mutation["CNS_diagnosis"] = df_mutation["CNS_diagnosis"].str.strip(";")

    # Remove the old CNS diagnosis columns
    df_mutation = df_mutation.drop(columns=cns_diagnosis_columns)

    # Create Treatment
    treatment_cols = [
        "Chemotherapy;Immunotherapy",
        "Radiation Therapy, NOS",
        "Stem Cell Transplant",
        "Surgical Procedure",
        "Cellular Therapy",
        "Other",
    ]

    # Replace "checked" with the column name in the specified columns
    for col in treatment_cols:
        df_mutation[col] = df_mutation[col].apply(
            lambda x: col if x == "checked" else np.nan
        )

    # Combine the specified columns into one, separated by `;`
    df_mutation["treatment"] = df_mutation[treatment_cols].fillna("").agg(";".join, axis=1)

    # Clean up by removing any trailing/leading `;` and ensuring only non-empty values are concatenated
    df_mutation["treatment"] = (
        df_mutation["treatment"].str.replace(r";+", ";", regex=True).str.strip(";")
    )

    logger.info("Row count after treatment operations: %d", len(df_mutation))

    #format race enum cases
    for index, row in df_mutation.iterrows():
        race = row['race'].split(";")[0]
        eth = row['race'].split(";")[1]
        case_fix = " ".join([word[0].upper() + word[1:].lower() for word in race.split(" ")]).replace("Or", "or").replace("Other", "other")
        df_mutation.loc[index, "race"] = case_fix + ";" + eth

    # Remove the old treatment columns
    df_mutation = df_mutation.drop(columns=treatment_cols)


    # Create Agent
    # Step 1: Identify all columns that have the 'AGT_ADM_NM_A' suffix pattern
    suffix_pattern = "AGT_ADM_NM_A"
    agent_columns_to_combine = [col for col in df_mutation.columns if suffix_pattern in col]

    # Step 2: Group columns by their suffix
    combined_columns = {}
    for col in agent_columns_to_combine:
        # Extract the suffix (e.g., 'AGT_ADM_NM_A01' from 'TX_CHEMO_CNS.AGT_ADM_NM_A01')
        suffix = col.split(".")[-1]

        if suffix not in combined_columns:
            combined_columns[suffix] = []
        combined_columns[suffix].append(col)

    # Step 3: Combine the values from columns with the same suffix
    for suffix, cols in combined_columns.items():
        # Combine columns by collecting unique values for each row
        df_mutation[suffix] = df_mutation[cols].apply(
            lambda row: ";".join(pd.unique(row.dropna())), axis=1
        )

    # Step 4: Remove the old prefixed columns
    df_mutation = df_mutation.drop(columns=agent_columns_to_combine)

    # Step 5: Rename suffix pattern to agent name
    chemo_agent_rename = {
        "AGT_ADM_NM_A01": "13-cis- retinoic acid (13cRA, Isotretinoin, Accutane)",
        "AGT_ADM_NM_A02": "Bevacizumab (Avastin)",
        "AGT_ADM_NM_A03": "Bleomycin (Blenoxane, BLEO)",
        "AGT_ADM_NM_A04": "Busulfan (Myleran)",
        "AGT_ADM_NM_A05": "Carboplatin (CBDCA)",
        "AGT_ADM_NM_A06": "Carmustine (BiCNU, BCNU)",
        "AGT_ADM_NM_A07": "Cetuximab (Erbitux)",
        "AGT_ADM_NM_A08": "Cisplatin (Platinol, CDDP)",
        "AGT_ADM_NM_A09": "Crizotinib (Xalkori)",
        "AGT_ADM_NM_A10": "Cyclophosphamide (Cytoxan, CTX)",
        "AGT_ADM_NM_A11": "Cytarabine (Ara-C, Cytosine arabinoside, Cytosar)",
        "AGT_ADM_NM_A12": "Dacarbazine (DTIC)",
        "AGT_ADM_NM_A13": "Dactinomycin (Cosmegen, ACT-D, actinomycin-D)",
        "AGT_ADM_NM_A14": "Dexamethasone (Decadron, DEX)",
        "AGT_ADM_NM_A15": "Dinutuximab (Unituxin, ch 14.18)",
        "AGT_ADM_NM_A16": "Docetaxel (Taxotere)",
        "AGT_ADM_NM_A17": "Doxorubicin (Adriamycin, ADR)",
        "AGT_ADM_NM_A18": "Eribulin (Halaven)",
        "AGT_ADM_NM_A19": "Erlotinib (Tarceva)",
        "AGT_ADM_NM_A20": "Etoposide (VePesid, VP-16)",
        "AGT_ADM_NM_A21": "Fluorouracil (5-FU)",
        "AGT_ADM_NM_A22": "Ganitumab (AMG 479)",
        "AGT_ADM_NM_A23": "Gefitinib (Iressa)",
        "AGT_ADM_NM_A24": "Gemcitabine (Gemzar, dFdC)",
        "AGT_ADM_NM_A25": "Ifosfamide (IFOS, IFEX)",
        "AGT_ADM_NM_A26": "Interleukin 2 (IL-2, Proleukin, Aldesleukin)",
        "AGT_ADM_NM_A27": "Irinotecan (CPT-11, Camptosar)",
        "AGT_ADM_NM_A28": "Lapatinib (Tykerb, Tyverb)",
        "AGT_ADM_NM_A29": "Lenalidomide (Revlimid)",
        "AGT_ADM_NM_A30": "Lomustine (CeeNU, CCNU)",
        "AGT_ADM_NM_A31": "Melphalan (Alkeran, l-PAM)",
        "AGT_ADM_NM_A32": "Methotrexate (MTX)",
        "AGT_ADM_NM_A33": "MIBG (Iobenguane, metaiodobenzylguanidine)",
        "AGT_ADM_NM_A34": "Mitomycin C (Mutamycin, MTC)",
        "AGT_ADM_NM_A35": "Oxaliplatin (Eloxatin)",
        "AGT_ADM_NM_A36": "Paclitaxel (Taxol)",
        "AGT_ADM_NM_A37": "Pazopanib (Votrient)",
        "AGT_ADM_NM_A38": "Prednisone (Deltasone, PRED)",
        "AGT_ADM_NM_A39": "Sirolimus (Rapamycin, Rapamune)",
        "AGT_ADM_NM_A40": "Sorafenib (Nexavar)",
        "AGT_ADM_NM_A41": "Sunitinib (Sutent)",
        "AGT_ADM_NM_A42": "Temozolomide (TMZ, Temodar)",
        "AGT_ADM_NM_A43": "Temsirolimus (Torisel)",
        "AGT_ADM_NM_A44": "Topotecan (Hycamptin)",
        "AGT_ADM_NM_A45": "Vandetanib (Caprelsa)",
        "AGT_ADM_NM_A46": "Vinblastine (Velban, VLB)",
        "AGT_ADM_NM_A47": "Vincristine (Oncovin, VCR)",
        "AGT_ADM_NM_A48": "Vinorelbine (Navelbine)",
        "AGT_ADM_NM_A49": "Vorinostat (SAHA)",
        "AGT_ADM_NM_A50": "Other"
    }
    
    df_mutation = df_mutation.rename(columns=chemo_agent_rename)

    logger.info("Renamed chemo agent columns:")
    for old_col, new_col in chemo_agent_rename.items():
        logger.info(f"  {old_col} -> {new_col}")

    # Step 6, replace 'checked' of agent cols with the agent
    agent_cols = [val for key, val in chemo_agent_rename.items()]

    # Replace "checked" with the column name in the specified columns
    for col in agent_cols:
        df_mutation[col] = df_mutation[col].apply(
            lambda x: col if x == "checked" else np.nan
        )

    # Step 7, Combine the specified columns into one, separated by `;`
    df_mutation["agent"] = df_mutation[agent_cols].fillna("").agg(";".join, axis=1)
    # Clean up by removing any trailing/leading `;` and ensuring only non-empty values are concatenated
    df_mutation["agent"] = (
        df_mutation["agent"].str.replace(r";+", ";", regex=True).str.strip(";")
    )

    logger.info("Row count after agent operations: %d", len(df_mutation))

    # Step 8, temove the old agent columns
    df_mutation = df_mutation.drop(columns=agent_cols)


    # Clean ups

    # set age values to int types
    df_mutation["age_at_diagnosis"] = df_mutation["age_at_diagnosis"].fillna("").astype(str).str.replace(".0", "")
    df_mutation["age_at_follow_up"] = df_mutation["age_at_follow_up"].fillna("").astype(str).str.replace(".0", "")

    # Use regex to remove (C##.#) from diagnosis
    df_mutation["diagnosis"] = df_mutation["diagnosis"].str.replace(
        r" \([A-Z0-9._]+\)", "", regex=True
    )

    logger.info("Formatting follow_up_id, treatment_id and treatment_response_id columns")
    logger.info("  Remove 'follow_up_ids' for rows that don't actually have follow-up data")
    logger.info("  Reformat follow_up_ids by replacing spaces with '_' and removing parentheses")
    logger.info("  Generate treatment_id by combining follow_up_id with '_treatment' for rows with treatment data")
    logger.info("  Generate treatment_response_id by combining follow_up_id with '_response' for rows with response data")

    # remove "follow_up_ids" for row that don't actually have follow-up data
    df_mutation["follow_up_id"] = np.where(
        df_mutation["follow_up_id"].str.contains("Follow-up", case=False, na=False),
        df_mutation["follow_up_id"],
        "",
    )

    #format follow_up_ids
    df_mutation["follow_up_id"] = df_mutation["follow_up_id"].str.replace(" ", "_").str.replace("(", "").str.replace(")", "")

    #generate treatment_id
    df_mutation = clean_column_underscore_simple_concat(
        df_mutation, "treatment_id", "follow_up_id", "treatment", "treatment"
    )
    
    #generate treatment_response_id
    df_mutation = clean_column_underscore_simple_concat(
        df_mutation, "treatment_response_id", "follow_up_id", "response", "response"
    )

    # Check and update diagnosis_id column
    logger.info("  Validating and updating 'diagnosis_id' column")
    df_mutation["diagnosis_id"] = df_mutation["diagnosis_id"].apply(
        lambda x: f"{x}_diagnosis" if pd.notna(x) and x != "" and not x.split("_")[-1].isdigit() else x
    )

    logger.info("Row count after clean up operations: %d", len(df_mutation))

    # Delete old columns that are no longer needed
    df_mutation = df_mutation.drop(
        columns=[
            "DEMOGRAPHY.DM_CRACE",
            "DEMOGRAPHY.DM_ETHNIC",
            "COG_UPR_DX.PTDT_IDP",
            "COG_UPR_DX.TOPO_ICDO",
            "COG_UPR_DX.TOPO_TEXT",
            "DEMOGRAPHY.DM_BRTHDAT",
            "COG_UPR_DX.DATE_DIA",
            "FOLLOW_UP.COMP_RESP_CONF_IND_3",
            "FOLLOW_UP.DZ_EXM_REP_IND_2",
            "FOLLOW_UP.REP_EVAL_PD_TP",
            "FOLLOW_UP.PT_FU_END_DT",
            "FOLLOW_UP.PT_INF_CU_FU_COL_IND",
        ]
    )

    # Log dropped columns
    logger.info("Dropped columns:")
    logger.info("  DEMOGRAPHY.DM_CRACE, DEMOGRAPHY.DM_ETHNIC, COG_UPR_DX.PTDT_IDP, COG_UPR_DX.TOPO_ICDO, COG_UPR_DX.TOPO_TEXT, DEMOGRAPHY.DM_BRTHDAT, COG_UPR_DX.DATE_DIA, FOLLOW_UP.COMP_RESP_CONF_IND_3, FOLLOW_UP.DZ_EXM_REP_IND_2, FOLLOW_UP.REP_EVAL_PD_TP, FOLLOW_UP.PT_FU_END_DT")

    df_mutation = df_mutation.drop_duplicates()

    logger.info("Row count after dropping duplicates: %d", len(df_mutation))

    output_order = [
        "participant_id",
        "race",
        "sex_at_birth",
        "diagnosis_id",
        "diagnosis",
        "icd_o_code",
        "CNS_category",
        "CNS_diagnosis",
        "age_at_diagnosis",
        "primary_site",
        "primary_diagnosis_disease_group",
        "follow_up_id",
        "age_at_follow_up",
        "vital_status",
        "treatment_response_id",
        "response",
        "treatment_id",
        "treatment",
        "agent",
        "registry_stage_code",
    ]

    # Identify any additional columns that are not in output_order
    additional_columns = [col for col in df_mutation.columns if col not in output_order]

    # Create the final column order by adding the extra columns to the end
    final_order = output_order + additional_columns

    # Log final column order
    logger.info(f"Final column order: {final_order}")

    # save to file
    df_mutation[final_order].to_csv(f"{output_dir}/COG_CCDI_submission_{dt}.tsv", sep="\t", index=False)

    # perform summary of the transformation and save to file
    summary_cog = summarizer(df_mutation, logger)
    summary_cog.to_csv(f"{output_dir}/COG_CCDI_summary_{dt}.tsv", sep="\t", index=False)

    # Log the end of the transformation
    logger.info("COG data transformation process completed successfully.")

    runner_logger.info(
        ShellOperation(
            commands=[
                "pwd",
                "ls -l ."
                "ls -l /usr/local/data/",
                f"ls -l {output_dir}"
            ]
        ).run()
    )

    return log_filename  # Return the log file name for reference

if __name__ == "__main__":
    # Testing
    log_file = cog_transformer(sys.argv[1], ".")
    print(f"Logs written to: {log_file}")

