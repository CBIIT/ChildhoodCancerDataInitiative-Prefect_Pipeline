from prefect import flow, task, get_run_logger
import os
import pandas as pd
import sys
import re
from src.utils import folder_dl, file_dl, folder_ul, file_ul, get_time, CheckCCDI, CCDI_DCC_Tags

# to load TSV files from a specified folder
@task(name="Load TSVs from Folder", log_prints=True)
def load_tsvs_from_folder(folder_path):
    files_loaded = 0
    sheet_dfs = {}
    for file in os.listdir(folder_path):
        if file.endswith(".tsv"):
            file_path = os.path.join(folder_path, file)
            file_name = re.sub(r'_\d{4}-\d{2}-\d{2}', '', file).replace(".tsv", "")
            df = pd.read_csv(file_path, sep="\t").astype(str).replace('nan', pd.NA)
            df = df.apply(lambda col: col.str.replace(r'\.0$', '', regex=True))
            df = df.drop_duplicates()
            sheet_dfs[file_name] = df
            files_loaded += 1
        else:
            print(f"Skipping {file} — not a TSV file")

    # report input metrics
    if files_loaded > 0:
        print(f"Loaded {files_loaded} file(s) from {folder_path}")
    else:
        raise ValueError(
            "ALERT: No files were found in the specified folder. "
            "Please check the input folder for valid TSV files."
        )
    
    return sheet_dfs

# to save the dataframes to TSV files in the specified output folder
@task(name= "Save TSVs to Folder", log_prints=True)
def save_tsvs_to_folder(sheet_dfs, output_path):
    """Saves the given sheet dataframes to TSV files in the specified output folder."""
    os.makedirs(output_path, exist_ok=True)
    files_saved = 0

    for name, df in sheet_dfs.items():
        # collect actual data (non-type/non-key) columns and check if empty
        empty_cols = [col for col in df.columns if '.' not in col and col != 'type']
        if df[empty_cols].dropna(how='all').empty:
            print(f"Skipping {name}.tsv — no data in non-key columns")
            continue

        # drop empty rows with no actual data (non-type/non-key) columns
        key_cols = {'type', 'study.study_id'}
        data_cols = [col for col in df.columns if col not in key_cols]
        df = df[df[data_cols].notna().any(axis=1)]

        # drop duplicate rows
        df = df.drop_duplicates()

        # save as TSV files
        file_path = os.path.join(output_path, f"{name}.tsv")
        df.to_csv(file_path, sep="\t", index=False)
        files_saved += 1

    # report output metrics
    if files_saved > 0:
        print(f"Saved {files_saved} file(s) to {output_path}")
    else:
        raise ValueError(
        "ALERT: No valid data was found across all input sheets. "
        "Please check the input files for empty rows or missing data."
    )

# transform the genetic analysis node
@task(name="Genetic Analysis Transformation Task", log_prints=True)
def transform_genetic_analysis(sheet_dfs: dict) -> dict:
    """Splits fusion genetic_analysis rows into separate partner rows.

    The 2.0.0 schema update removed fusion properties and instead places each partner fusion 
    gene in its own row, expanding each fusion row into two rows. In order to retain fusion 
    data for this transformation, mapping temporarily stores fusion data in incorrect placeholder
    columns. This task transfers the fusion data from these temporary columns into their own row.

    Args:
        sheet_dfs (dict): dict of node name -> DataFrame, as produced by
            load_tsvs_from_folder

    Returns:
        dict: same dict, with 'genetic_analysis' replaced by the
            fusion-split version (if present)
    """

    # check required sheets for fusion processing
    if 'genetic_analysis' not in sheet_dfs:
        print('No genetic_analysis node provided, skipping fusion post-processing')
        return sheet_dfs

    df = sheet_dfs['genetic_analysis'].copy()

    # check required columns for fusion processing
    required_cols = {'alteration_region', 'alteration_location_role', 'external_references', 'gene_symbol'}
    if not required_cols.issubset(df.columns):
        missing = required_cols - set(df.columns)
        print(f"Missing required column(s) {missing} in genetic_analysis, skipping fusion post-processing")
        return sheet_dfs

    # fusion data is dropped during liftover, so its carried over in placeholder columns
    # move placeholder columns carrying fusion data to new, correct columns
    fusion_cols = {
        "alteration_region": "fusion_partner_exon",
        "alteration_location_role": "fusion_partner_gene",
        "external_references": "fusion_partner_transcript",
    }

    for placeholder_col, correct_col in fusion_cols.items():
        df[correct_col] = df[placeholder_col]  
        df[placeholder_col] = pd.NA   

    # process fusion rows
    new_rows = []
    fusion_row_count = 0
    for index, row in df.iterrows():
        base = row.to_dict()

        if pd.notna(base["reported_significance"]) and ";" in str(base["reported_significance"]):
            tier, system = base["reported_significance"].split(";")
            base["reported_significance"] = f"{system}: {tier}"
        else:
            reported_significance = base.get("reported_significance")
            print(f"Row {index}: reported_significance missing or unexpected format ({reported_significance!r}), leaving as-is")

        # if row contains gene fusion data, split into two rows
        if pd.notna(row["fusion_partner_gene"]):
            fusion_row_count += 1

            # hardcode default values for fusion rows
            base["platform"] = "Archer Fusion"
            base["analysis_type"] = "Gene Fusion Analysis"
            base["alteration_region"] = "exon"
            base["alteration_type"] = "Not Reported"
            base["alteration_effect"] = "Gene Fusion"

            # construct the primary row (the first gene data)
            primary_row = base.copy()
            primary_row["alteration_location_role"] = "Five Prime Partner"
            new_rows.append(primary_row)

            # construct partner row (fusion partner gene data)
            partner_row = base.copy()
            partner_row["gene_symbol"] = base["fusion_partner_gene"]
            partner_row["transcript"] = base["fusion_partner_transcript"]
            partner_row["alteration_region_number"] = base["fusion_partner_exon"]
            partner_row["alteration_location_role"] = "Three Prime Partner"
            new_rows.append(partner_row)

        # if not a fusion record, carry the row through unchanged
        else:
            new_rows.append(base)

    # create new dataframe and report results
    new_df = pd.DataFrame(new_rows)
    # drop vestigial 1.0.0 columns 
    new_df = new_df.drop(["fusion_partner_exon","fusion_partner_gene","fusion_partner_transcript"], axis=1)
    sheet_dfs['genetic_analysis'] = new_df

    print(f"Checked {len(df)} genetic_analysis row(s); split {fusion_row_count} fusion record(s) into {fusion_row_count * 2} rows")
    print(f"genetic_analysis now has {len(new_df)} row(s)")

    return sheet_dfs


# orchestrate downloading, fixing fusion records, and uploading TSVs
@flow(name="Genetic Analysis Transformation Flow", log_prints=True)
def genetic_analysis_transform_flow(bucket: str, submission_path: str, runner: str) -> None:
    """
    Main flow that loads TSVs, splits fusion genetic_analysis rows, and saves the fixed TSVs

    Args:
        bucket (str): bucket name on aws where the submission folder is located
        submission_path (str): folder path containing tsv files under bucket
        runner (str): folder path where output files from this flow will be uploaded
    """

    logger = get_run_logger()

    # download: bring the folder from S3 to the local Prefect runner
    # submission path in S3 becomes the folder name locally
    logger.info(f"Downloading {submission_path} from bucket {bucket}")
    folder_dl(bucket=bucket, remote_folder=submission_path)

    # load: convert local TSVs --> dataframes
    logger.info("Loading local TSVs...")
    sheet_dfs = load_tsvs_from_folder(submission_path)
    
    # process: fix the IDs in the dataframes
    logger.info("Splitting fusion genes in genetic_analysis sheet...")
    fixed_dfs = transform_genetic_analysis(sheet_dfs)

    # save: convert fixed dataframes --> local TSVs
    output_folder = f"output_id_fixed_{get_time()}" # create output folder
    logger.info(f"Saving fixed TSVs to local folder: {output_folder}")
    save_tsvs_to_folder(fixed_dfs, output_folder) # populate folder with fixed TSVs

    # upload: send the local fixed folder back to the S3 runner location
    logger.info(f"Uploading fixed folder to S3 at: {runner}")
    folder_ul(bucket=bucket, local_folder=output_folder, destination=runner, sub_folder="")

    logger.info("DCC Liftover Post-Processing completed successfully.")