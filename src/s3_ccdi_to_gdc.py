import pandas as pd
import numpy as np
import os
import warnings
import json
import re
from prefect import flow, get_run_logger
from src.utils import get_time


#################
#
# Functions
#
################


# function to process the data frame for use in the json file
def preprocess_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocesses the DataFrame by converting numeric strings to numbers,
    "null" strings to JSON null values, and "true"/"false" strings to booleans.

    Args:
        df (pd.DataFrame): The DataFrame to preprocess.

    Returns:
        pd.DataFrame: The preprocessed DataFrame.
    """

    def preprocess_value(value):
        # If the value is already a float (e.g. NaN), return it as-is
        if isinstance(value, float) and np.isnan(value):
            return value

        if isinstance(value, str):
            # Handle integer string (e.g., "1", "2", "3")
            if value.isdigit():
                return int(value)
            # Handle float-like strings (e.g., "1.0", "3.14")
            elif re.match(r"^-?\d+\.\d+$", value):
                return float(value)
            # Handle special strings "null", "true", "false"
            elif value.lower() == "null":
                return None
            elif value.lower() == "true":
                return True
            elif value.lower() == "false":
                return False

        return value

    # Apply the preprocessing function to each element in the DataFrame
    return df.applymap(preprocess_value)


# Function to take 'node.property' column headers and convert to nested dictionary
def flatten_to_nested(row):
    """
    Converts a single row of a DataFrame into a nested dictionary
    based on column names with dots, excluding keys with NaN values.

    Args:
        row (pd.Series): A single row from a DataFrame.

    Returns:
        dict: Nested dictionary for the row.
    """
    nested_dict = {}
    for key, value in row.items():
        if pd.isna(value):  # Skip NaN values
            continue
        if value == "":  # Skip "" values
            continue
        keys = key.split(".")
        current = nested_dict
        for k in keys[:-1]:
            current = current.setdefault(k, {})
        current[keys[-1]] = value
    return nested_dict


# Function to take a DataFrame and convert to nested JSON
def save_dataframe_as_nested_json(df: pd.DataFrame, output_file: str):
    """
    Converts a DataFrame to JSON with nested keys and saves it to a file,
    excluding keys with NaN values.

    Args:
        df (pd.DataFrame): The DataFrame to convert to JSON.
        output_file (str): The path to the output JSON file.
    """

    #set up logger for prefect
    logger = get_run_logger()

    try:
        # Preprocess the DataFrame
        df = preprocess_dataframe(df)

        # Convert each row to a nested dictionary
        nested_data = [flatten_to_nested(row) for _, row in df.iterrows()]

        # Write the nested JSON data to a file
        with open(output_file, "w") as file:
            json.dump(nested_data, file, indent=4)

        logger.info(f"Nested JSON written to {output_file}.")
    except Exception as e:
        logger.info(f"ERROR writing JSON: {e}")


def read_xlsx(file_path: str, sheet: str):
    # Read in excel file
    warnings.simplefilter(action="ignore", category=UserWarning)
    df = pd.read_excel(
        file_path,
        sheet,
        dtype="string",
        keep_default_na=False,
        na_values=[
            "",
            "#N/A",
            "#N/A N/A",
            "#NA",
            "-1.#IND",
            "-1.#QNAN",
            "-NaN",
            "-nan",
            "1.#IND",
            "1.#QNAN",
            "<NA>",
            "N/A",
            "NA",
            "NULL",
            "NaN",
            # "None",
            "n/a",
            "nan",
            "null",
        ],
    )

    # Remove leading and trailing whitespace from all cells
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    return df


# Create a dictionary for faster lookup
def create_translation_dict(df, property: str):
    df = df[df["GDC_property"] == property]
    dict = pd.Series(
        df["GDC"].values,
        index=df["CCDI"],
    ).to_dict()

    return dict


# Function to extract the desired string for read_group
def extract_section_skip_first_three(filename):
    # Pattern to match the part of the string after skipping the first three underscore-separated parts
    pattern = r"^(?:[^_]+_){3}([^_]+(?:_[^_]+)*)_R\d+_\d+"
    match = re.search(pattern, filename)
    if match:
        return match.group(1)
    return None


# Function to update the submitter_id column for read_groups
def update_submitter_id(row):
    extracted_value = extract_section_skip_first_three(row["file_name"])
    if extracted_value:
        return f"{row['read_group_fix']}_{extracted_value}"
    return row["read_group_fix"]


# special fix for experiment_name to make sure typographical variance doesn't create false duplicate lines
def reconcile_experiment_names(df):
    """
    Reconcile 'experiment_name' values for rows with the same 'submitter_id'.

    Args:
        df (pd.DataFrame): The input DataFrame with 'submitter_id' and 'experiment_name'.

    Returns:
        pd.DataFrame: DataFrame with reconciled 'experiment_name' values and unique rows.
    """

    def resolve_experiment_name(group):
        # Find the longest string in 'experiment_name' with a tie-breaker for the first one
        reconciled_name = max(
            group["experiment_name"],
            key=lambda x: (len(x), group["experiment_name"].tolist().index(x)),
        )
        group["experiment_name"] = reconciled_name
        return group

    # Group by 'submitter_id' and reconcile 'experiment_name'
    df = df.groupby("submitter_id", group_keys=False).apply(resolve_experiment_name)

    # Drop duplicates based on all columns
    df = df.drop_duplicates()

    return df


@flow(
    name="CCDI_to_GDC",
    log_prints=True,
    flow_run_name="CCDI_to_GDC_" + f"{get_time()}",
)
def ccdi_to_gdc(
    file_path: str, CCDI_GDC_translation_file: str, platform_preservation_file: str
):
    
    logger = get_run_logger()


    ################
    #
    # File setups
    #
    ################

    # if conv file are not present
    if CCDI_GDC_translation_file == "":
        CCDI_GDC_translation_file = None

    if platform_preservation_file == "":
        platform_preservation_file = None

    # Determine file ext and abs path

    # file_name = os.path.splitext(os.path.split(os.path.relpath(file_path))[1])[0]
    # file_ext = os.path.splitext(file_path)[1]
    file_dir_path = os.path.split(os.path.relpath(file_path))[0]

    if file_dir_path == "":
        file_dir_path = "."

    todays_date = get_time()

    # Output file name based on input file name and date/time stamped.
    output_dir = f"CCDI_GDC_conversion_{todays_date}"

    os.makedirs(output_dir, exist_ok=True)

    ##############
    #
    # Pull Dictionary Page to create node pulls
    #
    ##############

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

    ##############
    #
    # Read in Conv files
    #
    ##############

    if CCDI_GDC_translation_file:
        # anatomic_site
        CCDI_GDC_conv = pd.read_csv(CCDI_GDC_translation_file, sep="\t")
        # clean up whitespace
        CCDI_GDC_conv = CCDI_GDC_conv.applymap(
            lambda x: x.strip() if isinstance(x, str) else x
        )

    if platform_preservation_file:
        # platform and preservation
        platform_preservation_conv = pd.read_csv(platform_preservation_file, sep="\t")

    #####################
    #####################
    ##
    ## SETUP FOR EACH NODE
    ##
    #####################
    #####################

    # Create GDC Data frames for nodes
    # df_ = pd.DataFrame({
    #     "type": pd.Series(dtype='str'),
    #     "submitter_id": pd.Series(dtype='str'),
    #     "": pd.Series(dtype='str'),
    #     "": pd.Series(dtype='str'),
    #     "": pd.Series(dtype='str'),
    #     "": pd.Series(dtype='str'),
    #     "": pd.Series(dtype='str'),
    #     "": pd.Series(dtype='str'),
    #     "": pd.Series(dtype='str'),
    #     "": pd.Series(dtype='str'),
    #     "": pd.Series(dtype='str'),
    #     "": pd.Series(dtype='str'),
    #     "": pd.Series(dtype='str'),
    #     "": pd.Series(dtype='str'),
    # })

    ###################################
    # aligned_reads_index
    ###################################

    df_aligned_reads_index = pd.DataFrame(
        {
            "type": pd.Series(dtype="str"),
            "submitter_id": pd.Series(dtype="str"),
            "aligned_reads_files.submitter_id": pd.Series(dtype="str"),
            "data_category": pd.Series(dtype="str"),
            "data_format": pd.Series(dtype="str"),
            "data_type": pd.Series(dtype="str"),
            "file_name": pd.Series(dtype="str"),
            "file_size": pd.Series(dtype="str"),
            "md5sum": pd.Series(dtype="str"),
            "project_id": pd.Series(dtype="str"),
        }
    )

    data = meta_dfs["sequencing_file"]
    data = data[data["file_type"] == "bai"]

    df_aligned_reads_index["submitter_id"] = data["sequencing_file_id"]
    df_aligned_reads_index["aligned_reads_files.submitter_id"] = data[
        "sequencing_file_id"
    ].str.replace(".bam.bai", ".bam", regex=True)
    df_aligned_reads_index["data_category"] = "Sequencing Data"
    df_aligned_reads_index["data_format"] = "BAI"
    df_aligned_reads_index["data_type"] = "Aligned Reads Index"
    df_aligned_reads_index["file_name"] = data["file_name"]
    df_aligned_reads_index["file_size"] = data["file_size"]
    df_aligned_reads_index["md5sum"] = data["md5sum"]
    df_aligned_reads_index["project_id"] = "CCDI-MCI"
    df_aligned_reads_index["type"] = "aligned_reads_index"

    df_aligned_reads_index = df_aligned_reads_index.drop_duplicates()

    ###################################
    # aliquot
    ###################################

    df_aliquot = pd.DataFrame(
        {
            "type": pd.Series(dtype="str"),
            "submitter_id": pd.Series(dtype="str"),
            "samples.submitter_id": pd.Series(dtype="str"),
            "project_id": pd.Series(dtype="str"),
        }
    )

    data = meta_dfs["sample"]

    df_aliquot["submitter_id"] = data["sample_id"] + "_aliquot"
    df_aliquot["samples.submitter_id"] = data["sample_id"]
    df_aliquot["project_id"] = "CCDI-MCI"
    df_aliquot["type"] = "aliquot"

    df_aliquot = df_aliquot.drop_duplicates()

    ###################################
    # case
    ###################################

    df_case = pd.DataFrame(
        {
            "type": pd.Series(dtype="str"),
            "submitter_id": pd.Series(dtype="str"),
            "projects.code": pd.Series(dtype="str"),
            "disease_type": pd.Series(dtype="str"),
            "primary_site": pd.Series(dtype="str"),
            "project_id": pd.Series(dtype="str"),
        }
    )

    data = meta_dfs["participant"]

    df_case["submitter_id"] = data["participant_id"]
    df_case["projects.code"] = "MCI"
    df_case["project_id"] = "CCDI-MCI"
    df_case["type"] = "case"

    # fix disease_type and primary_site
    data = meta_dfs["diagnosis"]
    data = data[data["participant.participant_id"].notna()]
    data = data[data["diagnosis_classification_system"] == "ICD-O-3.2"]

    df_case["primary_site"] = "Not Applicable"
    df_case["disease_type"] = "Neoplasms, NOS"

    CCDI_GDC_translation_dict_primary_site = create_translation_dict(
        CCDI_GDC_conv, "primary_site"
    )
    CCDI_GDC_translation_dict_disease_type = create_translation_dict(
        CCDI_GDC_conv, "disease_type"
    )

    for index, row in df_case.iterrows():
        case = df_case["submitter_id"][index]
        data_case = data[data["participant.participant_id"] == case.strip("_demo")]
        if not data_case.empty:
            if CCDI_GDC_translation_dict_primary_site:
                if data_case["anatomic_site"].notna().all():
                    primary_site = data_case["anatomic_site"].dropna().unique()[0]
                    primary_site = primary_site.split(" : ")[1]
                    df_case["primary_site"][index] = primary_site
            if CCDI_GDC_translation_dict_disease_type:
                if data_case["diagnosis"].notna().all():
                    diagnosis = data_case["diagnosis"].dropna().unique()[0]
                    diagnosis = diagnosis.split(" : ")[1]
                    df_case["disease_type"][index] = diagnosis

    if CCDI_GDC_translation_dict_primary_site:
        df_case["primary_site"] = (
            df_case["primary_site"]
            .map(CCDI_GDC_translation_dict_primary_site)
            .fillna(df_case["primary_site"])
        )

    if CCDI_GDC_translation_dict_disease_type:
        df_case["disease_type"] = (
            df_case["disease_type"]
            .map(CCDI_GDC_translation_dict_disease_type)
            .fillna(df_case["disease_type"])
        )

    df_case = df_case.drop_duplicates()

    ###################################
    # demographic
    ###################################

    df_demographic = pd.DataFrame(
        {
            "type": pd.Series(dtype="str"),
            "submitter_id": pd.Series(dtype="str"),
            "cases.submitter_id": pd.Series(dtype="str"),
            "ethnicity": pd.Series(dtype="str"),
            "gender": pd.Series(dtype="str"),
            "race": pd.Series(dtype="str"),
            "vital_status": pd.Series(dtype="str"),
            "project_id": pd.Series(dtype="str"),
        }
    )

    data = meta_dfs["participant"]

    df_demographic["submitter_id"] = data["participant_id"] + "_demo"
    df_demographic["cases.submitter_id"] = data["participant_id"]
    df_demographic["gender"] = data["sex_at_birth"].str.lower()
    df_demographic["project_id"] = "CCDI-MCI"
    df_demographic["type"] = "demographic"

    # fix ethnicity
    df_demographic["ethnicity"] = data["race"]
    df_demographic["ethnicity"] = df_demographic["ethnicity"] = df_demographic[
        "ethnicity"
    ].apply(
        lambda x: (
            "hispanic or latino"
            if "hispanic or latino" in str(x).lower()
            else "not reported"
        )
    )
    # fix race
    df_demographic["race"] = "unknown"
    df_demographic["race"] = data["race"].str.lower()
    df_demographic["race"] = (
        df_demographic["race"]
        .str.replace("Hispanic or Latino", "", case=False, regex=False)
        .str.strip()
    )

    # Replace double or more `;;` with a single `;`
    df_demographic["race"] = df_demographic["race"].str.replace(
        r";{2,}", ";", regex=True
    )
    # Remove leading or trailing `;`
    df_demographic["race"] = df_demographic["race"].astype(str).str.split(";").str[0]
    # fix empty values
    df_demographic["race"] = (
        df_demographic["race"].replace("", "unknown").fillna("unknown")
    )

    # fix vital_status
    data = meta_dfs["survival"]
    for index, row in df_demographic.iterrows():
        case = df_demographic["submitter_id"][index]
        data_case = data[data["participant.participant_id"] == case.strip("_demo")]
        if not data_case.empty:
            if data_case["last_known_survival_status"].notna().all():
                if (
                    data_case["last_known_survival_status"]
                    .str.contains("Dead", na=False)
                    .any()
                ):
                    df_demographic["vital_status"][index] = "Dead"
                else:
                    df_demographic["vital_status"][index] = "Alive"
            else:
                df_demographic["vital_status"][index] = "Not Reported"
        else:
            df_demographic["vital_status"][index] = "Not Reported"

    df_demographic = df_demographic.drop_duplicates()

    ###################################
    # diagnosis
    ###################################

    df_diagnosis = pd.DataFrame(
        {
            "type": pd.Series(dtype="str"),
            "submitter_id": pd.Series(dtype="str"),
            "cases.submitter_id": pd.Series(dtype="str"),
            "age_at_diagnosis": pd.Series(dtype="int"),
            "diagnosis_is_primary_disease": pd.Series(dtype="str"),
            "morphology": pd.Series(dtype="str"),
            "primary_diagnosis": pd.Series(dtype="str"),
            "site_of_resection_or_biopsy": pd.Series(dtype="str"),
            "tissue_or_organ_of_origin": pd.Series(dtype="str"),
            "project_id": pd.Series(dtype="str"),
        }
    )

    data = meta_dfs["diagnosis"]
    data = data[data["participant.participant_id"].notna()]
    data = data[data["diagnosis_classification_system"] == "ICD-O-3.2"]

    # Fix the 999 : Uknown to Unknown : Unknown, so that is comes out matching GDC model
    data["diagnosis"] = data["diagnosis"].replace(
        "999 : Unknown, to be completed later", "Unknown : Unknown"
    )

    df_diagnosis["submitter_id"] = data["diagnosis_id"]
    df_diagnosis["cases.submitter_id"] = data["participant.participant_id"]
    df_diagnosis["diagnosis_is_primary_disease"] = "true"
    df_diagnosis["morphology"] = data["diagnosis"].str.split(" : ").str[0]
    df_diagnosis["primary_diagnosis"] = data["diagnosis"].str.split(" : ").str[1]
    df_diagnosis["site_of_resection_or_biopsy"] = "Not Reported"
    df_diagnosis["tissue_or_organ_of_origin"] = (
        data["anatomic_site"].str.split(" : ").str[1]
    )
    df_diagnosis["project_id"] = "CCDI-MCI"
    df_diagnosis["type"] = "diagnosis"

    # fix age_at_diagnosis
    df_diagnosis["age_at_diagnosis"] = data["age_at_diagnosis"]
    df_diagnosis["age_at_diagnosis"] = df_diagnosis["age_at_diagnosis"].replace(
        "-999", "null"
    )

    CCDI_GDC_translation_dict_diagnosis = create_translation_dict(
        CCDI_GDC_conv, "primary_diagnosis"
    )
    CCDI_GDC_translation_dict_morpho = create_translation_dict(
        CCDI_GDC_conv, "morphology"
    )
    CCDI_GDC_translation_dict_tissue = create_translation_dict(
        CCDI_GDC_conv, "tissue_or_organ_of_origin"
    )

    # fix diagnosis, morpho and site

    if CCDI_GDC_translation_dict_diagnosis:
        df_diagnosis["primary_diagnosis"] = (
            df_diagnosis["primary_diagnosis"]
            .map(CCDI_GDC_translation_dict_diagnosis)
            .fillna(df_diagnosis["primary_diagnosis"])
        )

    if CCDI_GDC_translation_dict_morpho:
        df_diagnosis["morphology"] = (
            df_diagnosis["morphology"]
            .map(CCDI_GDC_translation_dict_morpho)
            .fillna(df_diagnosis["morphology"])
        )

    if CCDI_GDC_translation_dict_tissue:
        df_diagnosis["tissue_or_organ_of_origin"] = (
            df_diagnosis["tissue_or_organ_of_origin"]
            .map(CCDI_GDC_translation_dict_tissue)
            .fillna(df_diagnosis["tissue_or_organ_of_origin"])
        )

    df_diagnosis = df_diagnosis.drop_duplicates()

    ###################################
    # raw_methylation_array
    ###################################

    df_raw_methylation_array = pd.DataFrame(
        {
            "type": pd.Series(dtype="str"),
            "submitter_id": pd.Series(dtype="str"),
            "aliquots.submitter_id": pd.Series(dtype="str"),
            "channel": pd.Series(dtype="str"),
            "data_category": pd.Series(dtype="str"),
            "data_format": pd.Series(dtype="str"),
            "data_type": pd.Series(dtype="str"),
            "file_name": pd.Series(dtype="str"),
            "file_size": pd.Series(dtype="str"),
            "md5sum": pd.Series(dtype="str"),
            "experimental_strategy": pd.Series(dtype="str"),
            "platform": pd.Series(dtype="str"),
            "project_id": pd.Series(dtype="str"),
        }
    )

    data = meta_dfs["methylation_array_file"]
    data = data[data["file_type"] == "idat"]

    df_raw_methylation_array["submitter_id"] = data["methylation_array_file_id"]
    df_raw_methylation_array["aliquots.submitter_id"] = (
        data["sample.sample_id"] + "_aliquot"
    )
    df_raw_methylation_array["data_category"] = "DNA Methylation"
    df_raw_methylation_array["data_format"] = "IDAT"
    df_raw_methylation_array["data_type"] = "Raw Intensities"
    df_raw_methylation_array["file_name"] = data["file_name"]
    df_raw_methylation_array["file_size"] = data["file_size"]
    df_raw_methylation_array["md5sum"] = data["md5sum"]
    df_raw_methylation_array["experimental_strategy"] = "Methylation Array"
    df_raw_methylation_array["platform"] = "Illumina Methylation Epic"
    df_raw_methylation_array["project_id"] = "CCDI-MCI"
    df_raw_methylation_array["type"] = "raw_methylation_array"

    # fix channel
    df_raw_methylation_array["channel"] = data["file_name"]
    df_raw_methylation_array["channel"] = df_raw_methylation_array["channel"].apply(
        lambda x: "Red" if "Red" in x else "Green" if "Grn" in x else "ERROR"
    )

    # fix platform
    platform_conv = platform_preservation_conv[["sample_id", "platform"]]
    # Remove rows where platform is "WES" or NA (null values)
    platform_conv = platform_conv[
        ~platform_conv["platform"].isin(["WES"]) & platform_conv["platform"].notna()
    ]
    platform_conv.loc[:, "sample_id"] = platform_conv.loc[:, "sample_id"] + "_aliquot"
    df_raw_methylation_array = df_raw_methylation_array.drop(columns="platform")

    # Merge the two DataFrames on `submitter_id` and `sample_id`
    merged_df = df_raw_methylation_array.merge(
        platform_conv, how="left", left_on="aliquots.submitter_id", right_on="sample_id"
    )

    df_raw_methylation_array = merged_df.drop(columns="sample_id")

    # Replace NA values in the 'platform' column with 'Illumina Methylation Epic'
    df_raw_methylation_array["platform"] = df_raw_methylation_array["platform"].fillna(
        "Illumina Methylation Epic"
    )

    # Define a dictionary mapping old values to new values
    platform_replacements = {
        "IlluminaHumanMethylationEPIC": "Illumina Methylation Epic",
        "IlluminaHumanMethylationEPICv2": "Illumina Methylation Epic v2",
    }

    # Replace the values in the 'platform' column
    df_raw_methylation_array["platform"] = df_raw_methylation_array["platform"].replace(
        platform_replacements
    )

    df_raw_methylation_array = df_raw_methylation_array.drop_duplicates()

    ###################################
    # read_group
    ###################################

    df_read_group = pd.DataFrame(
        {
            "type": pd.Series(dtype="str"),
            "submitter_id": pd.Series(dtype="str"),
            "aliquots.submitter_id": pd.Series(dtype="str"),
            "experiment_name": pd.Series(dtype="str"),
            "is_paired_end": pd.Series(dtype="str"),
            "library_name": pd.Series(dtype="str"),
            "library_strategy": pd.Series(dtype="str"),
            "library_selection": pd.Series(dtype="str"),
            "platform": pd.Series(dtype="str"),
            "read_group_name": pd.Series(dtype="str"),
            "read_length": pd.Series(dtype="str"),
            "sequencing_center": pd.Series(dtype="str"),
            "target_capture_kit": pd.Series(dtype="str"),
            "lane_number": pd.Series(dtype="int"),
            "project_id": pd.Series(dtype="str"),
        }
    )

    data = meta_dfs["sequencing_file"]
    data = data[data["library_id"].notna()]
    data = data[data["file_type"].isin(["fastq", "bam"])]
    data["read_group_fix"] = data["library_id"] + "_rg"

    df_read_group["aliquots.submitter_id"] = data["sample.sample_id"] + "_aliquot"
    df_read_group["experiment_name"] = data["design_description"]
    df_read_group["library_name"] = data["library_id"]
    df_read_group["library_strategy"] = data["library_strategy"]
    df_read_group["library_selection"] = data["library_selection"]
    df_read_group["platform"] = data["platform"]
    df_read_group["read_length"] = "150"
    df_read_group["sequencing_center"] = (
        "The Institute for Genomic Medicine at Nationwide Children's Hospital"
    )
    df_read_group["target_capture_kit"] = "Unknown"
    df_read_group["project_id"] = "CCDI-MCI"
    df_read_group["type"] = "read_group"
    df_read_group["lane_number"] = data["file_name"].str.extract(r"_L00(\d)_R\d_")

    # Apply the function to update the submitter_id
    data["read_group_fix"] = data.apply(update_submitter_id, axis=1)
    df_read_group["submitter_id"] = data["read_group_fix"]
    df_read_group["read_group_name"] = df_read_group["submitter_id"]

    # fix is_paired_end
    df_read_group["is_paired_end"] = data["library_layout"]
    df_read_group["is_paired_end"] = df_read_group["is_paired_end"].apply(
        lambda x: (
            "True" if "Paired end" in x else "False" if "Single end" in x else "ERROR"
        )
    )

    # fix library_strategy
    df_read_group["library_strategy"] = df_read_group["library_strategy"].replace(
        "Archer Fusion", "RNA-Seq"
    )

    df_read_group = reconcile_experiment_names(df_read_group)

    df_read_group = df_read_group.drop_duplicates()

    ###################################
    # sample
    ###################################

    df_sample = pd.DataFrame(
        {
            "type": pd.Series(dtype="str"),
            "submitter_id": pd.Series(dtype="str"),
            "cases.submitter_id": pd.Series(dtype="str"),
            "preservation_method": pd.Series(dtype="str"),
            "specimen_type": pd.Series(dtype="str"),
            "tissue_type": pd.Series(dtype="str"),
            "tumor_descriptor": pd.Series(dtype="str"),
            "project_id": pd.Series(dtype="str"),
        }
    )

    data = meta_dfs["sample"]

    df_sample["submitter_id"] = data["sample_id"]
    df_sample["cases.submitter_id"] = data["participant.participant_id"]
    df_sample["tissue_type"] = data["sample_tumor_status"]
    df_sample["tumor_descriptor"] = data["tumor_classification"]
    df_sample["project_id"] = "CCDI-MCI"
    df_sample["type"] = "sample"

    # fix specimen_type
    df_sample["specimen_type"] = data["anatomic_site"]
    df_sample["specimen_type"] = df_sample["specimen_type"].apply(
        lambda x: "Peripheral Whole Blood" if "C42.0 : Blood" in x else "Solid Tissue"
    )

    # fix preservation
    df_sample = df_sample.drop(columns="preservation_method")
    preservation_conv = platform_preservation_conv[["sample_id", "preservation_method"]]
    preservation_conv = preservation_conv.drop_duplicates()

    # Identify rows with multiple `sample_id` and drop rows where `preservation_method` is NA
    preservation_conv = preservation_conv.groupby(
        "sample_id", group_keys=False
    ).apply(  # Group by `sample_id`
        lambda group: (
            group.dropna(subset=["preservation_method"])
            if group["preservation_method"].isna().any()
            else group
        )
    )
    preservation_conv = preservation_conv.drop_duplicates()

    df_sample = pd.merge(
        df_sample,
        preservation_conv,
        left_on="submitter_id",
        right_on="sample_id",
        how="left",
    )

    # Replace missing `preservation_method` values with "Not Reported"
    df_sample["preservation_method"] = df_sample["preservation_method"].fillna(
        "Not Reported"
    )

    df_sample = df_sample.drop_duplicates()

    # This fix is to catch samples that have multiple preservation methods.
    # IGM has confirmed that in this case, we should ignore the 'Frozen' samples and
    # should go with the FFPE versions.

    # Identify duplicates based on 'sample_id'
    duplicates = df_sample[df_sample.duplicated(subset="sample_id", keep=False)]

    # Keep rows where preservation method is not "Frozen" if duplicates exist
    filtered_duplicates = duplicates[duplicates["preservation_method"] != "Frozen"]

    # Combine with non-duplicate rows
    non_duplicates = df_sample[~df_sample.duplicated(subset="sample_id", keep=False)]
    df_sample = pd.concat([non_duplicates, filtered_duplicates], ignore_index=True)

    df_sample = df_sample.drop(columns="sample_id")

    df_sample = df_sample.drop_duplicates()

    ###################################
    # submitted_aligned_reads_Archer_Fusion
    ###################################

    df_submitted_aligned_reads_Archer_Fusion = pd.DataFrame(
        {
            "type": pd.Series(dtype="str"),
            "submitter_id": pd.Series(dtype="str"),
            "read_groups.submitter_id": pd.Series(dtype="str"),
            "data_category": pd.Series(dtype="str"),
            "data_format": pd.Series(dtype="str"),
            "data_type": pd.Series(dtype="str"),
            "file_name": pd.Series(dtype="str"),
            "file_size": pd.Series(dtype="str"),
            "md5sum": pd.Series(dtype="str"),
            "experimental_strategy": pd.Series(dtype="str"),
            "project_id": pd.Series(dtype="str"),
        }
    )

    data = meta_dfs["sequencing_file"]
    data = data[data["file_type"] == "bam"]
    data = data[data["library_strategy"] == "Archer Fusion"]

    # Filter rows where 'file_name' contains the string "merged_bam"
    # we can only have one SAR file per read_group, so we will only submit the merged as that
    # is what is now always available every month.
    data = data[data["file_name"].str.contains("merged_bam", case=False, na=False)]

    df_submitted_aligned_reads_Archer_Fusion["submitter_id"] = data[
        "sequencing_file_id"
    ]
    df_submitted_aligned_reads_Archer_Fusion["read_groups.submitter_id"] = (
        data["library_id"] + "_rg"
    )
    df_submitted_aligned_reads_Archer_Fusion["data_category"] = "Sequencing Reads"
    df_submitted_aligned_reads_Archer_Fusion["data_format"] = "BAM"
    df_submitted_aligned_reads_Archer_Fusion["data_type"] = "Aligned Reads"
    df_submitted_aligned_reads_Archer_Fusion["file_name"] = data["file_name"]
    df_submitted_aligned_reads_Archer_Fusion["file_size"] = data["file_size"]
    df_submitted_aligned_reads_Archer_Fusion["md5sum"] = data["md5sum"]
    df_submitted_aligned_reads_Archer_Fusion["experimental_strategy"] = "RNA-Seq"
    df_submitted_aligned_reads_Archer_Fusion["project_id"] = "CCDI-MCI"
    df_submitted_aligned_reads_Archer_Fusion["type"] = "submitted_aligned_reads"

    df_submitted_aligned_reads_Archer_Fusion = (
        df_submitted_aligned_reads_Archer_Fusion.drop_duplicates()
    )

    ###################################
    # submitted_aligned_reads_WXS
    ###################################

    df_submitted_aligned_reads_WXS = pd.DataFrame(
        {
            "type": pd.Series(dtype="str"),
            "submitter_id": pd.Series(dtype="str"),
            "read_groups.submitter_id": pd.Series(dtype="str"),
            "data_category": pd.Series(dtype="str"),
            "data_format": pd.Series(dtype="str"),
            "data_type": pd.Series(dtype="str"),
            "file_name": pd.Series(dtype="str"),
            "file_size": pd.Series(dtype="str"),
            "md5sum": pd.Series(dtype="str"),
            "experimental_strategy": pd.Series(dtype="str"),
            "project_id": pd.Series(dtype="str"),
        }
    )

    data = meta_dfs["sequencing_file"]
    data = data[data["file_type"] == "bam"]
    data = data[data["library_strategy"] == "WXS"]

    # CURRENTLY EMPTY, ALL WXS IS CRAM

    ###################################
    # submitted_unaligned_reads_Archer_Fusion
    ###################################

    df_submitted_unaligned_reads_Archer_Fusion = pd.DataFrame(
        {
            "type": pd.Series(dtype="str"),
            "submitter_id": pd.Series(dtype="str"),
            "read_groups.submitter_id": pd.Series(dtype="str"),
            "data_category": pd.Series(dtype="str"),
            "data_format": pd.Series(dtype="str"),
            "data_type": pd.Series(dtype="str"),
            "file_name": pd.Series(dtype="str"),
            "file_size": pd.Series(dtype="str"),
            "md5sum": pd.Series(dtype="str"),
            "experimental_strategy": pd.Series(dtype="str"),
            "read_pair_number": pd.Series(dtype="str"),
            "project_id": pd.Series(dtype="str"),
        }
    )

    # submitted_unaligned_reads_Archer_Fusion
    data = meta_dfs["sequencing_file"]
    data = data[data["file_type"] == "fastq"]
    data = data[data["library_strategy"] == "Archer Fusion"]

    # CURRENTLY EMPTY, ALL ARCHER FUSION IS BAM ONLY

    ###################################
    # submitted_unaligned_reads_WXS
    ###################################

    df_submitted_unaligned_reads_WXS = pd.DataFrame(
        {
            "type": pd.Series(dtype="str"),
            "submitter_id": pd.Series(dtype="str"),
            "read_groups.submitter_id": pd.Series(dtype="str"),
            "data_category": pd.Series(dtype="str"),
            "data_format": pd.Series(dtype="str"),
            "data_type": pd.Series(dtype="str"),
            "file_name": pd.Series(dtype="str"),
            "file_size": pd.Series(dtype="str"),
            "md5sum": pd.Series(dtype="str"),
            "experimental_strategy": pd.Series(dtype="str"),
            "read_pair_number": pd.Series(dtype="str"),
            "project_id": pd.Series(dtype="str"),
        }
    )

    data = meta_dfs["sequencing_file"]
    data = data[data["file_type"] == "fastq"]
    data = data[data["library_strategy"] == "WXS"]
    data["read_group_fix"] = data["library_id"] + "_rg"

    df_submitted_unaligned_reads_WXS["submitter_id"] = data["sequencing_file_id"]
    df_submitted_unaligned_reads_WXS["read_groups.submitter_id"] = (
        data["library_id"] + "_rg"
    )
    df_submitted_unaligned_reads_WXS["data_category"] = "Sequencing Reads"
    df_submitted_unaligned_reads_WXS["data_format"] = "FASTQ"
    df_submitted_unaligned_reads_WXS["data_type"] = "Unaligned Reads"
    df_submitted_unaligned_reads_WXS["file_name"] = data["file_name"]
    df_submitted_unaligned_reads_WXS["file_size"] = data["file_size"]
    df_submitted_unaligned_reads_WXS["md5sum"] = data["md5sum"]
    df_submitted_unaligned_reads_WXS["experimental_strategy"] = data["library_strategy"]
    df_submitted_unaligned_reads_WXS["project_id"] = "CCDI-MCI"
    df_submitted_unaligned_reads_WXS["type"] = "submitted_unaligned_reads"

    # read pair number fix
    df_submitted_unaligned_reads_WXS["read_pair_number"] = data["file_name"]
    df_submitted_unaligned_reads_WXS["read_pair_number"] = (
        df_submitted_unaligned_reads_WXS["read_pair_number"].str.extract(r"_(R\d+)_")
    )

    # fix read_groups.submitter_id based on lane_number
    data["read_group_fix"] = data.apply(update_submitter_id, axis=1)
    df_submitted_unaligned_reads_WXS["read_groups.submitter_id"] = data[
        "read_group_fix"
    ]

    df_submitted_unaligned_reads_WXS = (
        df_submitted_unaligned_reads_WXS.drop_duplicates()
    )

    logger.info("DF WRITE OUT")
    ###################################
    # DF file write out
    ###################################
    # Identify all DataFrames in the current environment
    dataframes = {
        name: obj for name, obj in globals().items() if isinstance(obj, pd.DataFrame)
    }

    # Save each DataFrame as a TSV file
    for name, df in dataframes.items():
        logger.info(name)
        if isinstance(df, pd.DataFrame) and name.startswith("df_"):
            if not df.empty:  # Skip if the DataFrame is empty
                output_name = name[3:]  # Remove the "df_" prefix
                df.to_csv(f"{output_dir}/{output_name}.tsv", sep="\t", index=False)
                save_dataframe_as_nested_json(
                    df=df, output_file=f"{output_dir}/{output_name}.json"
                )
                logger.info(f"TSV written {output_dir}/{output_name}.tsv")
            else:
                logger.info(f"Skipped {name} (empty DataFrame).")


    return output_dir
