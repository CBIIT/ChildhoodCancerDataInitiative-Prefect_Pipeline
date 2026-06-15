from prefect import flow, task, get_run_logger
import os
import pandas as pd
import sys
import re
from src.utils import folder_dl, file_dl, folder_ul, file_ul, get_time, CheckCCDI, CCDI_DCC_Tags

@task(name="Load TSVs from Folder", log_prints=True)
def load_tsvs_from_folder(folder_path):
    sheet_dfs = {}
    for file in os.listdir(folder_path):
        if file.endswith(".tsv"):
            file_path = os.path.join(folder_path, file)
            file_name = re.sub(r'_\d{4}-\d{2}-\d{2}', '', file).replace(".tsv", "")
            df = pd.read_csv(file_path, sep="\t").astype(str).replace('nan', pd.NA)
            df = df.drop_duplicates()
            sheet_dfs[file_name] = df
    return sheet_dfs

@task(name= "Save TSVs to Folder", log_prints=True)
def save_tsvs_to_folder(sheet_dfs, output_path):
    """Saves the given sheet dataframes to TSV files in the specified output folder."""
    os.makedirs(output_path, exist_ok=True)
    
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

        # save as TSV files
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
        df = sheet_dfs['study'].copy()
        df = df.groupby('type', as_index=False).first()

        if ('phs_accession' not in df.columns or df['phs_accession'].isnull().all() or
            'study_acronym' not in df.columns or df['study_acronym'].isnull().all()):
            print('Missing required study fields (phs_accession or study_acronym), skipping liftover ID generation')
            return sheet_dfs
        
        else:
            df['study_id'] = df['phs_accession'] + "_" + df['study_acronym']
            sheet_dfs['study'] = df
            gc_study_id = str(df['study_id'].iloc[0])
            print(f"Successfully generated study_id: {gc_study_id}")

        sheets = ['consent_group', 'investigator', 'participant', 'sample', 'file']
        for sheet in sheets:
            if sheet in sheet_dfs:
                df = sheet_dfs[sheet].copy()
                df['study.study_id'] = gc_study_id
                sheet_dfs[sheet] = df
        
    else:
        print('No study node provided, skipping liftover ID generation')
        return sheet_dfs
    
    # --- CONSENT Reformatting ---
    if 'consent_group' in sheet_dfs:
        df = sheet_dfs['consent_group'].copy()

        if ('consent_group_number' not in df.columns or df['consent_group_number'].isnull().all()):
            print('Missing required consent group field (consent_group_number), skipping consent group number reformatting')
        
        else:
            df['study_consent_number'] = df['consent_group_number'].astype(str).str.strip().str.replace('c', '', regex=False)
            print(f"Reformatted consent group number(s): e.g. {df['study_consent_number'].iloc[0]}")
            sheet_dfs['consent_group'] = df

    # --- INVESTIGATOR ID Generation ---
    if 'investigator' in sheet_dfs:
        df = sheet_dfs['investigator'].copy()

        if ('email' not in df.columns or
            df['email'].isnull().all()):
            print('Missing required investigator field (email), skipping investigator ID generation')
       
        else:
            df['investigator_id'] = gc_study_id + "_" + df['email']
            sheet_dfs['investigator'] = move_id_to_front(df, 'investigator_id')
            print(f"Sample Investigator ID created: {df['investigator_id'].iloc[0]}")

    # --- INVESTIGATOR NAME Generation ---
        def parse_name(full_name):
            title, first, middle, last, suffix = None, None, None, None, None
            if pd.notna(full_name) and str(full_name).strip():

                clean_name = str(full_name).replace(',', '')
                parsed_name = str(clean_name).strip().split()

                prefixes = {'Dr.', 'Dr', 'Mr.', 'Mr', 'Mrs.', 'Mrs', 'Ms.', 
                            'Ms', 'Miss', 'Sir', 'Dame', 'Lord', 'Lady'}
                suffixes = {'Jr.', 'Jr', 'Sr.', 'Sr', 'II', 'III', 'IV', 
                            'MD', 'M.D.', 'PhD', 'Ph.D.', 'DO', 'D.O.'}

                if parsed_name and parsed_name[0] in prefixes:
                    title = parsed_name.pop(0)

                if parsed_name and parsed_name[-1] in suffixes:
                    suffix = parsed_name.pop(-1)

                if len(parsed_name) > 2:
                    first, middle, last = parsed_name[0], parsed_name[1], " ".join(parsed_name[2:])
                elif len(parsed_name) == 2:
                    first, last = parsed_name[0], parsed_name[1]
                elif len(parsed_name) == 1:
                    last = parsed_name[0]

            return title, first, middle, last, suffix

        # where primary_investigator_name is not null, parse/populate the name fields
        if 'primary_investigator_name' in df.columns:
            name_cols = ['title', 'first_name', 'middle_name', 'last_name', 'suffix']
            mask = df['primary_investigator_name'].notna()
            parsed = df.loc[mask, 'primary_investigator_name'].apply(parse_name)
            df.loc[mask, name_cols] = pd.DataFrame(parsed.tolist(), index=parsed.index, columns=name_cols)
            sheet_dfs['investigator'] = df
            

    # --- PARTICIPANT ID Generation ---
    if 'participant' in sheet_dfs:
        df = sheet_dfs['participant'].copy()

        if ('participant_id' not in df.columns or
            df['participant_id'].isnull().all()):
            print('Missing required participant field (participant_id), skipping participant ID generation in participant sheet')
        
        else:
            df['study_participant_id'] = gc_study_id + "_" + df['participant_id']
            sheet_dfs['participant'] = move_id_to_front(df, 'study_participant_id')
            print(f"Generated {len(df)} study_participant_id values (e.g., {df['study_participant_id'].iloc[0]}) in participant sheet")
        
    if 'sample' in sheet_dfs:
        df = sheet_dfs['sample'].copy()

        if ('participant.study_participant_id' not in df.columns or
            df['participant.study_participant_id'].isnull().all()):
            print('Missing required participant field (participant.study_participant_id), skipping participant ID generation in sample sheet')
        
        else:
            df['participant.study_participant_id'] = gc_study_id + "_" + df['participant.study_participant_id']
            sheet_dfs['sample'] = df
            print(f"Generated {len(df)} study_participant_id values (e.g., {df['participant.study_participant_id'].iloc[0]}) in sample sheet")

    # --- DIAGNOSIS ID Generation ---
    if 'diagnosis' in sheet_dfs:
        df = sheet_dfs['diagnosis'].copy()
        if ('participant.study_participant_id' in df.columns and
            'diagnosis_id' in df.columns and
            df['participant.study_participant_id'].notnull().any()):

            df['participant.study_participant_id'] = gc_study_id + "_" + df['participant.study_participant_id']
            df['study_diagnosis_id'] = df['participant.study_participant_id'] + "_" + df['diagnosis_id']
            sheet_dfs['diagnosis'] = move_id_to_front(df, 'study_diagnosis_id')

            if sheet_dfs['diagnosis']['participant.study_participant_id'].isnull().any():
                print("Some liftover diagnosis IDs skipped because some participant IDs were missing.")
            print(f"Generated {len(df)} study_diagnosis_id values (e.g., {df['study_diagnosis_id'].iloc[0]})")
        elif ('participant.study_participant_id' in df.columns and
              df['participant.study_participant_id'].isnull().all()):
            print("Missing participant IDs, skipping liftover diagnosis ID generation")
        else:
            print("Missing diagnosis fields (participant.study_participant_id or diagnosis_id), skipping liftover diagnosis ID generation")


     # --- TREATMENT ID Generation ---
    if 'treatment' in sheet_dfs:
        if ('therapeutic_agents' in sheet_dfs['treatment'].columns and sheet_dfs['treatment']['therapeutic_agents'].notnull().any() and
            'participant.study_participant_id' in sheet_dfs['treatment'].columns and sheet_dfs['treatment']['participant.study_participant_id'].notnull().any()):
            df = sheet_dfs['treatment'].copy()

            df['participant.study_participant_id'] = gc_study_id + "_" + df['participant.study_participant_id']
            df['treatment_id'] = df['participant.study_participant_id'] + "_" + df['therapeutic_agents']
            sheet_dfs['treatment'] = move_id_to_front(df, 'treatment_id')

            if sheet_dfs['treatment']['therapeutic_agents'].isnull().any():
                print("Some liftover treatment IDs skipped because some treatment agents were missing.")
            print(f"Generated {len(df)} treatment_id values (e.g., {df['treatment_id'].iloc[0]})")
        else:
            print('Missing treatment fields (therapeutic_agents or participant.study_participant_id), skipping treatment ID generation')


    # --- GENOMIC INFO ID Generation --
    if 'genomic_info' in sheet_dfs:
        if ('file.file_id' in sheet_dfs['genomic_info'].columns and
            sheet_dfs['genomic_info']['file.file_id'].notnull().any()):
            df = sheet_dfs['genomic_info'].copy()
            
            def build_genomic_id(row):
                file_val = str(row.get('file.file_id', "")).strip()
                lib = row.get('library_id', "")
                lib_val = str(lib).strip()
                
                if lib_val and pd.notnull(lib) and lib_val.lower() != 'not available':
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


    # --- IMAGE NOT REPORTED Generation --
    if 'image' in sheet_dfs:
        df = sheet_dfs['image'].copy()
        unreported_cols = ['organ_or_tissue', 'imaging_equipment_manufacturer', 'citation_or_DOI']
        for col in unreported_cols:
            df[col] = df[col].astype('string').fillna('Not Reported') if col in df.columns else 'Not Reported'
        sheet_dfs['image'] = df
        
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