from prefect import flow, task, get_run_logger
import os
import pandas as pd
import sys
from src.utils import folder_dl, file_dl, folder_ul, file_ul, get_time, CheckCCDI, CCDI_DCC_Tags

@task (name="Load TSVs from Folder", log_prints=True)
def load_tsvs_from_folder(folder_path):
    sheet_dfs = {}
    for file in os.listdir(folder_path):
        if file.endswith(".tsv"):
            file_path = os.path.join(folder_path, file)
            file_name = file.replace(".tsv","")
            sheet_dfs[file_name] = pd.read_csv(file_path, sep="\t")
    return sheet_dfs

@task(name= "Save TSVs to Folder", log_prints=True)
def save_tsvs_to_folder(sheet_dfs, output_path):
    """Saves the given sheet dataframes to TSV files in the specified output folder."""
    os.makedirs(output_path, exist_ok=True)
    
    for name, df in sheet_dfs.items():
        file_path = os.path.join(output_path, f"{name}.tsv")
        df.to_csv(file_path, sep="\t", index=False)

def move_id_to_front(df, id_column_name):
    """Moves the specified ID column to the first position."""
    if id_column_name in df.columns:
        cols = [id_column_name] + [col for col in df.columns if col != id_column_name]
        return df[cols]
    return df

@task(
    name="Generate IDs for GC Liftover",
    log_prints=True,
    task_run_name="Generate IDs for GC Liftover",
)

def generate_ids_task(sheet_dfs):
    # --- STUDY ID Generation ---
    if 'study' in sheet_dfs:
        sheet_dfs['study']['study_id'] = sheet_dfs['study']['dbgap_accession'] + "_" + sheet_dfs['study']['study_acronym']
        gc_study_id = sheet_dfs['study']['study_id'].iloc[0]
    else:
        print('No study node provided, skipping liftover ID generation')
        return sheet_dfs
    
    # --- INVESTIGATOR ID Generation ---
    if 'study_personnel' in sheet_dfs:
        df = sheet_dfs['study_personnel']
        df['investigator_id'] = gc_study_id + "_" + df['email_address']
        sheet_dfs['study_personnel'] = move_id_to_front(df, 'investigator_id')
        
    # --- PARTICIPANT ID Generation ---
    if 'participant' in sheet_dfs:
        df = sheet_dfs['participant']
        df['study_participant_id'] = gc_study_id + "_" + df['participant_id']
        sheet_dfs['participant'] = move_id_to_front(df, 'study_participant_id')
        
    # --- DIAGNOSIS ID Generation ---
    if 'diagnosis' in sheet_dfs:
        if sheet_dfs['diagnosis']['participant.participant_id'].notnull().any():
            df = sheet_dfs['diagnosis'].copy()
            df['participant.study_participant_id'] = gc_study_id + "_" + df['participant.participant_id']
            df['study_diagnosis_id'] = df['participant.study_participant_id'] + "_" + df['diagnosis_id']
            sheet_dfs['diagnosis'] = move_id_to_front(df, 'study_diagnosis_id')
            if sheet_dfs['diagnosis']['participant.participant_id'].isnull().any():
                print("Some liftover diagnosis IDs skipped because some participant IDs were missing.")
        elif sheet_dfs['diagnosis']['participant.participant_id'].isnull().all():
            print("Missing participant IDs, skipping liftover diagnosis ID generation")
            
    # --- TREATMENT ID Generation ---
    if 'treatment' in sheet_dfs:
        if sheet_dfs['treatment']['treatment_agent'].notnull().any():
            df = sheet_dfs['treatment']
            df['treatment_id'] = gc_study_id + "_" + df['participant_id'] + df['treatment_agent']
            sheet_dfs['treatment'] = move_id_to_front(df, 'treatment_id')
            if sheet_dfs['treatment']['treatment_agent'].isnull().any():
                print("Some liftover treatment IDs skipped because some treatment agents were missing.")
        else:
            print('Missing treatment agents, skipping treatment ID generation')
            
    return sheet_dfs



@flow(name="GC ID Post-Processing Flow", log_prints=True)
def generate_ids_flow(bucket: str, runner: str) -> None:
    
    """
    Main flow that calls tasks to load TSVs, generate GC IDs and save the fixed TSVs
    Args:
        bucket (str): bucket name
        submission_path (str): folder path contains a set of tsv files under bucket, e.g. "submissions/submission_tsv_files/"
    """
    logger = get_run_logger()

    # download: bring the folder from S3 to the local Prefect runner
    # The runner path in S3 becomes the folder name locally
    logger.info(f"Downloading {runner} from bucket {bucket}")
    folder_dl(bucket=bucket, remote_folder=runner)

    # load: convert local TSVs --> dataframes
    logger.info("Loading local TSVs...")
    sheet_dfs = load_tsvs_from_folder(runner)
    
    # process: fix the IDs in the dataframes
    logger.info("Generating GC IDs...")
    fixed_dfs = generate_ids_task(sheet_dfs)

    # save: convert fixed dataframes --> local TSVs
    output_folder = f"output_id_fixed_{get_time()}" # create output folder
    logger.info(f"Saving fixed TSVs to local folder: {output_folder}")
    save_tsvs_to_folder(fixed_dfs, output_folder) # populate folder with fixed TSVs

    # upload: send the local fixed folder back to the S3 runner location
    logger.info(f"Uploading fixed folder to S3 at: {runner}")
    folder_ul(bucket=bucket, local_folder=output_folder, destination=runner, sub_folder="")

    logger.info("CCDI GC ID Post-Processing completed successfully.")






