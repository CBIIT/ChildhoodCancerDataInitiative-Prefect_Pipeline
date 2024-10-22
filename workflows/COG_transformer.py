import pandas as pd
import numpy as np
import os
import json
from collections import defaultdict
import itertools
from prefect import flow, get_run_logger
from src.utils import get_time, file_ul, file_dl


def read_cog_jsons(dir_path: str):
    df_list = []  # List to hold DataFrames

    for filename in os.listdir(dir_path):
        if filename.endswith(".json"):
            file_path = os.path.join(dir_path, filename)
            try:
                with open(file_path, "r") as f:
                    # Read the file as a string
                    json_str = f.read()

                    # Parse the string manually to capture all `data` sections
                    json_data = json.loads(
                        json_str, object_pairs_hook=custom_json_parser
                    )

                    # Normalize the JSON data into a DataFrame
                    df = pd.json_normalize(json_data)
                    df_list.append(df)
            except ValueError as e:
                print(f"Error reading {filename}: {e}")

    # Concatenate all the DataFrames
    if df_list:
        concatenated_df = pd.concat(df_list, ignore_index=True)
    else:
        print("No valid JSON files found.")

    return concatenated_df


def custom_json_parser(pairs):
    # Initialize a dictionary to handle duplicated keys
    result = defaultdict(list)

    for key, value in pairs:
        if isinstance(value, dict):
            result[key].append(custom_json_parser(value.items()))
        else:
            result[key].append(value)

    # If there's only one value for a key, flatten it (i.e., don't keep it as a list)
    result = {k: (v[0] if len(v) == 1 else v) for k, v in result.items()}

    return result


def expand_cog_df(df: pd.DataFrame):
    expanded_data = []
    saslabel_data = []

    # Iterate through each row in the DataFrame
    for index, row in df.iterrows():
        expanded_rows = []  # Hold all rows for this UPI
        common_row = {"upi": row["upi"]}  # Store common fields

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
                continue  # If data_sections is neither a list nor valid, skip this form

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

                        # Ensure form_field_id exists
                        if form_field_id:
                            # Create the column name and add the value
                            column_name = f"{form_name}.{form_field_id}"
                            form_row[column_name] = value

                            # Collect SASLabel and column_name pair
                            saslabel_data.append(
                                {"column_name": column_name, "SASLabel": SASLabel}
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


def cog_to_tsv(dir_path: str):
    df_ingest = read_cog_jsons(dir_path)

    df_reshape, df_saslabels = expand_cog_df(df_ingest)

    df_reshape.to_csv(f"JSON_table_conversion_{get_time}.tsv", sep="\t", index=False)
    df_saslabels.to_csv(f"saslabels_{get_time}.tsv", sep="\t", index=False)

    return df_reshape, df_saslabels


# Functions for reshaping the data:


def clean_column_semicolon_concat(
    df: pd.DataFrame, new_col_name: str, col_name1: str, col_name2: str
):
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
    # Concatenate values from col_name1 and col_name2 with '_' between them, handling NaNs
    df[new_col_name] = np.where(
        df[col_name1].notna() & df[col_name2].notna(),
        df[col_name1].fillna("").astype(str)
        + "_"
        + df[col_name2].fillna("").astype(str),
        df[col_name1].fillna("").astype(str) + df[col_name2].fillna("").astype(str),
    )

    # Remove trailing "_" from the concatenated string
    df[new_col_name] = df[new_col_name].str.rstrip("_")

    return df


def clean_column_space_colon_concat(
    df: pd.DataFrame, new_col_name: str, col_name1: str, col_name2: str
):
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


# Data Reshape/mutate

df_reshape, df_saslabels = cog_to_tsv("MCI_COG_clinical_JSON_v4")

# the specific columns we want in our mutation df
direct_columns = [
    "upi",
    "DEMOGRAPHY.DM_ETHNIC",
    "DEMOGRAPHY.DM_SEX",
    "DEMOGRAPHY.DM_CRACE",
    "COG_UPR_DX.ADM_DX_CD_SEQ",
    "COG_UPR_DX.DX_DT",
    "DEMOGRAPHY.DM_BRTHDAT",
    "COG_UPR_DX.TOPO_TEXT",
    "COG_UPR_DX.MORPHO_TEXT",
    "COG_UPR_DX.MORPHO_ICDO",
    "COG_UPR_DX.TOPO_ICDO",
    "FINAL_DIAGNOSIS.PRIMDXDSCAT",
    "COG_UPR_DX.REG_STAGE_CODE_TEXT",
    "ON_STUDY_DX_CNS.TUMOR_GP_ST",
    "ON_STUDY_DX_CNS.CNSTMRMSTG",
    "CNS_DIAGNOSIS_DETAIL.MH_MHCAT_CNSDXCAT",
    "CNS_DIAGNOSIS_DETAIL.SUPPTU_QVAL_TUTUDX_OTHS",
    "FOLLOW_UP.REP_EVAL_PD_TP",
    "FOLLOW_UP.PT_FU_END_DT",
    "FOLLOW_UP.PT_VST",
    "FOLLOW_UP.COMP_RESP_CONF_IND_3",
    "FOLLOW_UP.DZ_EXM_REP_IND_2",
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

# Apply the selected columns to the new mutation df
df_mutation = df_reshape[selected_columns]

# Rename columns that do not have value changes
df_mutation = df_mutation.rename(
    columns={
        "upi": "participant_id",
        "DEMOGRAPHY.DM_SEX": "sex_at_birth",
        "FINAL_DIAGNOSIS.PRIMDXDSCAT": "primary_diagnosis_disease_group",
        "COG_UPR_DX.REG_STAGE_CODE_TEXT": "registry_stage_code",
        "ON_STUDY_DX_CNS.TUMOR_GP_ST": "tumor_grade",
        "ON_STUDY_DX_CNS.CNSTMRMSTG": "tumor_m_stage",
        "FOLLOW_UP.PT_VST": "vital_status",
        "CNS_DIAGNOSIS_DETAIL.MH_MHCAT_CNSDXCAT": "CNS_category",
        "CNS_DIAGNOSIS_DETAIL.SUPPTU_QVAL_TUTUDX_OTHS": "CNS_category_other",
        "FOLLOW_UP.FSTLNTXINIDXADMCAT_A1": "Chemotherapy;Immunotherapy",
        "FOLLOW_UP.FSTLNTXINIDXADMCAT_A2": "Radiation Therapy, NOS",
        "FOLLOW_UP.FSTLNTXINIDXADMCAT_A3": "Stem Cell Transplant",
        "FOLLOW_UP.FSTLNTXINIDXADMCAT_A4": "Surgical Procedure",
        "FOLLOW_UP.FSTLNTXINIDXADMCAT_A5": "Cellular Therapy",
        "FOLLOW_UP.FSTLNTXINIDXADMCAT_A6": "Other",
    }
)


# Create new columns that have value changes

# CONCATENATIONS
df_mutation = clean_column_semicolon_concat(
    df_mutation, "race", "DEMOGRAPHY.DM_CRACE", "DEMOGRAPHY.DM_ETHNIC"
)
df_mutation = clean_column_underscore_concat(
    df_mutation, "diagnosis_id", "participant_id", "COG_UPR_DX.ADM_DX_CD_SEQ"
)
df_mutation = clean_column_underscore_concat(
    df_mutation, "follow_up_id", "participant_id", "FOLLOW_UP.REP_EVAL_PD_TP"
)
df_mutation = clean_column_space_colon_concat(
    df_mutation, "primary_site", "COG_UPR_DX.TOPO_ICDO", "COG_UPR_DX.TOPO_TEXT"
)
df_mutation = clean_column_space_colon_concat(
    df_mutation, "diagnosis", "COG_UPR_DX.MORPHO_ICDO", "COG_UPR_DX.MORPHO_TEXT"
)
df_mutation = clean_column_semicolon_concat(
    df_mutation, "CNS_category", "CNS_category", "CNS_category_other"
)

# EQUATIONS
df_mutation["age_at_diagnosis"] = abs(
    df_mutation["DEMOGRAPHY.DM_BRTHDAT"].astype(float)
) + abs(df_mutation["COG_UPR_DX.DX_DT"].astype(float))
df_mutation["age_at_follow_up"] = abs(
    df_mutation["DEMOGRAPHY.DM_BRTHDAT"].astype(float)
) + abs(df_mutation["FOLLOW_UP.PT_FU_END_DT"].astype(float))


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
df_mutation = df_mutation.rename(
    columns={
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
        "AGT_ADM_NM_A50": "Other",
    }
)

# Step 6, replace 'checked' of agent cols with the agent
agent_cols = [
    "13-cis- retinoic acid (13cRA, Isotretinoin, Accutane)",
    "Bevacizumab (Avastin)",
    "Bleomycin (Blenoxane, BLEO)",
    "Busulfan (Myleran)",
    "Carboplatin (CBDCA)",
    "Carmustine (BiCNU, BCNU)",
    "Cetuximab (Erbitux)",
    "Cisplatin (Platinol, CDDP)",
    "Crizotinib (Xalkori)",
    "Cyclophosphamide (Cytoxan, CTX)",
    "Cytarabine (Ara-C, Cytosine arabinoside, Cytosar)",
    "Dacarbazine (DTIC)",
    "Dactinomycin (Cosmegen, ACT-D, actinomycin-D)",
    "Dexamethasone (Decadron, DEX)",
    "Dinutuximab (Unituxin, ch 14.18)",
    "Docetaxel (Taxotere)",
    "Doxorubicin (Adriamycin, ADR)",
    "Eribulin (Halaven)",
    "Erlotinib (Tarceva)",
    "Etoposide (VePesid, VP-16)",
    "Fluorouracil (5-FU)",
    "Ganitumab (AMG 479)",
    "Gefitinib (Iressa)",
    "Gemcitabine (Gemzar, dFdC)",
    "Ifosfamide (IFOS, IFEX)",
    "Interleukin 2 (IL-2, Proleukin, Aldesleukin)",
    "Irinotecan (CPT-11, Camptosar)",
    "Lapatinib (Tykerb, Tyverb)",
    "Lenalidomide (Revlimid)",
    "Lomustine (CeeNU, CCNU)",
    "Melphalan (Alkeran, l-PAM)",
    "Methotrexate (MTX)",
    "MIBG (Iobenguane, metaiodobenzylguanidine)",
    "Mitomycin C (Mutamycin, MTC)",
    "Oxaliplatin (Eloxatin)",
    "Paclitaxel (Taxol)",
    "Pazopanib (Votrient)",
    "Prednisone (Deltasone, PRED)",
    "Sirolimus (Rapamycin, Rapamune)",
    "Sorafenib (Nexavar)",
    "Sunitinib (Sutent)",
    "Temozolomide (TMZ, Temodar)",
    "Temsirolimus (Torisel)",
    "Topotecan (Hycamptin)",
    "Vandetanib (Caprelsa)",
    "Vinblastine (Velban, VLB)",
    "Vincristine (Oncovin, VCR)",
    "Vinorelbine (Navelbine)",
    "Vorinostat (SAHA)",
    "Other",
]

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

# Step 8, temove the old agent columns
df_mutation = df_mutation.drop(columns=agent_cols)


# Clean ups
# Use regex to remove (C##.#) from diagnosis
df_mutation["diagnosis"] = df_mutation["diagnosis"].str.replace(
    r" \([A-Z0-9._]+\)", "", regex=True
)

# remove "follow_up_ids" for row that don't actually have follow-up data
df_mutation["follow_up_id"] = np.where(
    df_mutation["follow_up_id"].str.contains("Follow-up", case=False, na=False),
    df_mutation["follow_up_id"],
    "",
)

# Delete old columns that are no longer needed
df_mutation = df_mutation.drop(
    columns=[
        "DEMOGRAPHY.DM_CRACE",
        "DEMOGRAPHY.DM_ETHNIC",
        "COG_UPR_DX.ADM_DX_CD_SEQ",
        "COG_UPR_DX.TOPO_ICDO",
        "COG_UPR_DX.TOPO_TEXT",
        "COG_UPR_DX.MORPHO_ICDO",
        "COG_UPR_DX.MORPHO_TEXT",
        "CNS_category_other",
        "DEMOGRAPHY.DM_BRTHDAT",
        "COG_UPR_DX.DX_DT",
        "FOLLOW_UP.COMP_RESP_CONF_IND_3",
        "FOLLOW_UP.DZ_EXM_REP_IND_2",
        "FOLLOW_UP.REP_EVAL_PD_TP",
        "FOLLOW_UP.PT_FU_END_DT",
    ]
)

df_mutation = df_mutation.drop_duplicates()

output_order = [
    "participant_id",
    "race",
    "sex_at_birth",
    "diagnosis_id",
    "diagnosis",
    "CNS_category",
    "CNS_diagnosis",
    "age_at_diagnosis",
    "primary_site",
    "primary_diagnosis_disease_group",
    "follow_up_id",
    "age_at_follow_up",
    "vital_status",
    "response",
    "treatment",
    "agent",
    "tumor_m_stage",
    "registry_stage_code",
    "tumor_grade",
]

# Identify any additional columns that are not in output_order
additional_columns = [col for col in df_mutation.columns if col not in output_order]

# Create the final column order by adding the extra columns to the end
final_order = output_order + additional_columns

df_mutation[final_order].to_csv(f"COG_CCDI_submission_{get_time}.tsv", sep="\t", index=False)
