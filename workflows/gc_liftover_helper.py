from prefect import flow, task, get_run_logger
import os
import pandas as pd
import sys
import re
from src.utils import folder_dl, file_dl, folder_ul, file_ul, get_time, CheckCCDI, CCDI_DCC_Tags

@task (name="Load TSVs from Folder", log_prints=True)
def load_tsvs_from_folder(folder_path):
    sheet_dfs = {}
    for file in os.listdir(folder_path):
        if file.endswith(".tsv"):
            file_path = os.path.join(folder_path, file)
            file_name = re.sub(r'_\d{4}-\d{2}-\d{2}', '', file).replace(".tsv", "")
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

        if ('phs_accession' not in sheet_dfs['study'].columns or 
            'study_acronym' not in sheet_dfs['study'].columns or
            sheet_dfs['study']['phs_accession'].isnull().all() or
            sheet_dfs['study']['study_acronym'].isnull().all()):
            print('Missing required study fields (phs_accession or study_acronym), skipping liftover ID generation')
            return sheet_dfs
            
        sheet_dfs['study']['study_id'] = sheet_dfs['study']['phs_accession'] + "_" + sheet_dfs['study']['study_acronym']
        gc_study_id = str(sheet_dfs['study']['study_id'].iloc[0])
        print(f"Successfully generated study_id: {gc_study_id}")
        
    else:
        print('No study node provided, skipping liftover ID generation')
        return sheet_dfs
        

    # --- INVESTIGATOR ID Generation ---
    if 'investigator' in sheet_dfs:
        df = sheet_dfs['investigator'].copy()

        if ('primary_investigator_email' not in df.columns or
            df['primary_investigator_email'].isnull().all()):
            print('Missing required investigator field (primary_investigator_email), skipping investigator ID generation')
       
        else:
            df['investigator_id'] = gc_study_id + "_" + df['primary_investigator_email']
            sheet_dfs['investigator'] = move_id_to_front(df, 'investigator_id')
            print(f"Sample Investigator ID created: {df['investigator_id'].iloc[0]}")
        

    # --- PARTICIPANT ID Generation ---
    if 'participant' in sheet_dfs:
        df = sheet_dfs['participant'].copy()

        if ('participant_id' not in df.columns or
            df['participant_id'].isnull().all()):
            print('Missing required participant field (participant_id), skipping participant ID generation')
        
        else:
            df['study_participant_id'] = gc_study_id + "_" + df['participant_id']
            sheet_dfs['participant'] = move_id_to_front(df, 'study_participant_id')
            print(f"Generated {len(df)} study_participant_id values (e.g., {df['study_participant_id'].iloc[0]})")
        

    # --- DIAGNOSIS ID Generation ---
    if 'diagnosis' in sheet_dfs:
        if sheet_dfs['diagnosis']['participant.study_participant_id'].notnull().any():
            df = sheet_dfs['diagnosis'].copy()

            df['participant.study_participant_id'] = gc_study_id + "_" + df['participant.study_participant_id']
            df['study_diagnosis_id'] = df['participant.study_participant_id'] + "_" + df['diagnosis_id']
            sheet_dfs['diagnosis'] = move_id_to_front(df, 'study_diagnosis_id')

            if sheet_dfs['diagnosis']['participant.study_participant_id'].isnull().any():
                print("Some liftover diagnosis IDs skipped because some participant IDs were missing.")
            print(f"Generated {len(df)} study_diagnosis_id values (e.g., {df['study_diagnosis_id'].iloc[0]})")
        elif sheet_dfs['diagnosis']['participant.study_participant_id'].isnull().all():
            print("Missing participant IDs, skipping liftover diagnosis ID generation")


     # --- TREATMENT ID Generation ---
    if 'treatment' in sheet_dfs:
        if (sheet_dfs['treatment']['therapeutic_agents'].notnull().any() and
            'participant.study_participant_id' in sheet_dfs['treatment'].columns and
            sheet_dfs['treatment']['participant.study_participant_id'].notnull().any()):
            df = sheet_dfs['treatment'].copy()

            df['treatment_id'] = gc_study_id + "_" + df['participant.study_participant_id'] + "_" + df['therapeutic_agents']
            sheet_dfs['treatment'] = move_id_to_front(df, 'treatment_id')

            if sheet_dfs['treatment']['therapeutic_agents'].isnull().any():
                print("Some liftover treatment IDs skipped because some treatment agents were missing.")
            print(f"Generated {len(df)} treatment_id values (e.g., {df['treatment_id'].iloc[0]})")
        else:
            print('Missing treatment fields (therapeutic_agents or participant.study_participant_id), skipping treatment ID generation')


    # --- GENOMIC INFO ID Generation --
    if 'genomic_info' in sheet_dfs:
        if sheet_dfs['genomic_info']['file.file_id'].notnull().any():
            df = sheet_dfs['genomic_info'].copy()
            
            def build_genomic_id(row):
                file_val = str(row.get('file.file_id', "")).strip()
                lib = row.get('library_id', "")
                lib_val = str(lib).strip()
                
                if lib_val and pd.notnull(lib):
                    return f"{file_val}_{lib_val}"
                else:
                    return file_val
            df['genomic_info_id'] = df.apply(build_genomic_id, axis=1)
            sheet_dfs['genomic_info'] = move_id_to_front(df, 'genomic_info_id')

            if sheet_dfs['genomic_info']['file.file_id'].isnull().any():
                print("Some liftover genomic_info IDs skipped because some file IDs were missing.")
            print(f"Generated {len(df)} genomic_info_id values (e.g., {df['genomic_info_id'].iloc[0]})")
        else:
            print('Missing genomic_info field (file.file_id), skipping genomic_info ID generation')
        
    return sheet_dfs


@flow(name="GC ID Post-Processing Flow", log_prints=True)
def generate_ids_flow(bucket: str, submission_path: str, runner: str) -> None:
    
    """
    Main flow that calls tasks to load TSVs, generate GC IDs and save the fixed TSVs
    Args:
        bucket (str): bucket name on aws where the submission folder is located, e.g. "ccdi-validation"
        submission_path (str): folder path that contains a set of tsv files under bucket, e.g. "your_name/JIRA_tix/submission_tsv_files/"
        runner (str): folder path where the output files from this flow will be uploaded, e.g. "your_name/JIRA_tix/"
    """
    logger = get_run_logger()

    # download: bring the folder from S3 to the local Prefect runner
    # The submission path in S3 becomes the folder name locally
    logger.info(f"Downloading {submission_path} from bucket {bucket}")
    folder_dl(bucket=bucket, remote_folder=submission_path)

    # load: convert local TSVs --> dataframes
    logger.info("Loading local TSVs...")
    sheet_dfs = load_tsvs_from_folder(submission_path)
    
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






