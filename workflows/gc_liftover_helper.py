from prefect import flow, task, get_run_logger
import os
import pandas as pd
import sys
import re
from src.utils import folder_dl, file_dl, folder_ul, file_ul, get_time, CheckCCDI, CCDI_DCC_Tags

@task(name="Load TSVs from Folder", log_prints=True)
def load_tsvs_from_folder(folder_path):
    files_loaded = 0
    sheet_dfs = {}

    for file in os.listdir(folder_path):
        if file.endswith(".tsv"):
            file_path = os.path.join(folder_path, file)
            file_name = re.sub(r'_\d{4}-\d{2}-\d{2}', '', file).replace(".tsv", "")
            df = pd.read_csv(file_path, sep="\t").astype(str).replace('nan', pd.NA)
            df = df.replace(';', '|', regex=True)
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

                clean_name = str(full_name).replace(',', '').replace('.', '')
                parsed_name = str(clean_name).strip().split()

                prefixes = {'Dr', 'Mr', 'Mrs', 'Ms', 'Miss', 'Sir', 'Dame', 'Lord', 'Lady'}
                suffixes = {'Jr', 'Sr', 'II', 'III', 'IV', 'MD', 'PhD', 'DO'}

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
                print("Some liftover diagnosis IDs may be skipped because some participant IDs were missing.")
            print(f"Generated {len(df)} study_diagnosis_id values (e.g., {df['study_diagnosis_id'].iloc[0]})")

        elif ('participant.study_participant_id' in df.columns and
              df['participant.study_participant_id'].isnull().all()):
            print("Missing participant IDs, skipping liftover diagnosis ID generation")
        else:
            print("Missing diagnosis fields (participant.study_participant_id or diagnosis_id), skipping liftover diagnosis ID generation")

        # if there is a sample.sample_id but no participant.study_participant_id
        df = sheet_dfs['diagnosis'].copy()
        if ('diagnosis_id' in df.columns and
            'sample.sample_id' in df.columns and 
            df['sample.sample_id'].notnull().any()):
            
            mask = df['participant.study_participant_id'].isnull() & df['sample.sample_id'].notnull()
            df.loc[mask, 'study_diagnosis_id'] = df.loc[mask, 'sample.sample_id'] + "_" + df.loc[mask, 'diagnosis_id']
            sheet_dfs['diagnosis'] = df

            if mask.sum() > 0:
                example_id = df.loc[mask, 'study_diagnosis_id'].iloc[0]
                print(f"Generated {mask.sum()} study_diagnosis_id values from sample_ids (e.g., {example_id})")

    #--- DIAGNOSIS ROW Consolidation ---

        # load the data
        df = sheet_dfs['diagnosis'].copy()

        # select only rows with survival info    
        # squash if multiple survival rows per id (any dead = dead, all alive = alive)
        survival_df = (
            df[df['vital_status'].notna()]
            .groupby('participant.study_participant_id')['vital_status']
            .apply(lambda x: 'Dead' if (x == 'Dead').any() else 'Alive')
            .reset_index()
        )

        # select only rows with resection/biopsy site (if multiple sites, just keep the first)
        site_df = (
            df[df['site_of_resection_or_biopsy'].notna()]
            .drop_duplicates(subset=['participant.study_participant_id'])
            [['participant.study_participant_id', 'site_of_resection_or_biopsy']]
        )

        # drop hanging survival/site rows and empty columns for a cleaner merge
        df = df[df['vital_status'].isna() & df['site_of_resection_or_biopsy'].isna()].copy()
        df = df.drop(columns=['vital_status', 'site_of_resection_or_biopsy'])

        # weave survival and site info in-line into the df
        df = df.merge(survival_df, on='participant.study_participant_id', how='left')
        df = df.merge(site_df, on='participant.study_participant_id', how='left')
        sheet_dfs['diagnosis'] = df


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

    # --- SAMPLE TYPE Generation --
    if 'sample' in sheet_dfs and 'sample_type' in sheet_dfs['sample'].columns:
        df = sheet_dfs['sample'].copy()

        df['sample_type'] = df['sample_type'].apply(
            lambda x: 'blood' if pd.notna(x) and x.lower() == 'blood' else 'analyte'
        )
        
        sheet_dfs['sample'] = df

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