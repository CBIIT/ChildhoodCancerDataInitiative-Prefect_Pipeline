import pandas as pd
import sys, os, time
import boto3
from prefect import flow, task, get_run_logger
from botocore.exceptions import ClientError
from src.utils import get_time, file_dl, file_ul

def title_case_except_and_or(s):
    if pd.isna(s):
        return s

    if isinstance(s, str) and s.isupper() and s not in ['WGS', 'WXS']:
        # Convert to title case, but handle special words
        return ' '.join([
            word.lower() if word.upper() in ['AND', 'OR'] 
            else word if word.upper() == 'NOS'
            else word.title()
            for word in s.split()
        ])
    else:
        return s
@task(name=f"parsing_{get_time()}")
def general_parser(workbook: str, sheet: str, prop_name: str, prop_encoding: str) -> pd.DataFrame:
    """Parses a specific sheet in the workbook and returns a summary dataframe.

    Args:
        workbook (str): Path to the excel workbook.
        sheet (str): Name of the sheet to parse.
        prop_name (str): Name of the property to summarize.
        prop_encoding (str): Encoding for the property.

    Returns:
        pd.DataFrame: Summary dataframe.
    """
    df = pd.read_excel(workbook, sheet_name=sheet)
    summary = df.groupby(prop_name).size().reset_index(name='count')
    summary['Data Element'] = prop_encoding
    summary['Statistic Type'] = 'Count'
    summary = summary[['Data Element', prop_name, 'Statistic Type', 'count']]
    summary = summary.rename(columns={prop_name: 'Data Element Value', 'count': 'Statistic Value'})
    return summary

@task(name=f"age_parsing_{get_time()}")
def age_at_diagnosis_parser(workbook: str, sheet: str) -> pd.DataFrame:
    """Parses the age at diagnosis data from the specified sheet.

    Args:
        workbook (str): _description_
        sheet (str): _description_

    Returns:
        pd.DataFrame: _description_
    """
    
    diagnosis = pd.read_excel(workbook, sheet_name=sheet)
    
    diagnosis['age_at_diagnosis'] = pd.to_numeric(diagnosis['age_at_diagnosis'], errors='coerce').fillna(-999).astype(int)
    
    diagnosis_age = diagnosis[diagnosis.age_at_diagnosis != -999]

    diagnosis_age.loc[:, 'age_year'] = diagnosis_age.loc[:, 'age_at_diagnosis'] / 365

    bins = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80]

    diagnosis_age['age_binned'] = pd.cut(diagnosis_age.age_year,bins=bins, right=False)

    age_df = diagnosis_age.groupby('age_binned', observed=False).size().reset_index(name='count')

    #update labels such that [0, 5) becomes 0 to 4 years, [5, 10) becomes 5 to 9 years, etc
    age_df['age_binned'] = age_df['age_binned'].astype(str).str.replace(r'\[(\d+),\s*(\d+)\)', lambda m: f"{m.group(1)} to {int(m.group(2))-1} years", regex=True)

    new_row = {'age_binned' : 'Not Reported', 'count' : len(diagnosis[diagnosis.age_at_diagnosis == -999])}

    age_df.loc[len(age_df)] = new_row

    age_df['Data Element'] = 'Case Age at Diagnosis'
    age_df['Statistic Type'] = 'Count'

    # reorder cols
    age_df = age_df[['Data Element', 'age_binned', 'Statistic Type', 'count']]
    age_df = age_df.rename(columns={'age_binned': 'Data Element Value', 'count': 'Statistic Value'})

    return age_df

@task(name=f"total_counts_{get_time()}")
def total_counts(workbook: str, sheet: str, prop_encoding: str) -> pd.DataFrame:
    """Parses a specific sheet in the workbook and returns a total counts dataframe.

    Args:
        workbook (str): Path to the excel workbook.
        sheet (str): Name of the sheet to parse.
        prop_name (str): Name of the property to summarize.
        prop_encoding (str): Encoding for the property.

    Returns:
        pd.DataFrame: Total counts dataframe.
    """
    df = pd.read_excel(workbook, sheet_name=sheet)
    total = len(df)
    total_df = pd.DataFrame({'Data Element Value': 'Any', 'Statistic Value': [total]})
    total_df['Data Element'] = prop_encoding
    total_df['Statistic Type'] = 'Count'
    
    #rearrange columns
    total_df = total_df[['Data Element', 'Data Element Value', 'Statistic Type', 'Statistic Value']]
    return total_df

@task(name=f"sample_level_counts_{get_time()}")
def sample_level_counts(workbook: str, sheet: str, prop_name: str, prop_encoding: str) -> pd.DataFrame:
    """Parses a specific sheet in the workbook and returns a sample level counts dataframe.

    Args:
        workbook (str): Path to the excel workbook.
        sheet (str): Name of the sheet to parse.
        prop_name (str): Name of the property to summarize.
        prop_encoding (str): Encoding for the property.

    Returns:
        pd.DataFrame: Sample level counts dataframe.
    """
    
    df = pd.read_excel(workbook, sheet_name=sheet)
    
    if sheet == 'methylation_array_file':
        df[prop_name] = 'Methylation Array'
    elif sheet == 'cytogenomic_file':
        df[prop_name] = 'Cytogenomic Array'

    summary = df.groupby(['sample.sample_id', prop_name]).size().reset_index().groupby(prop_name).size().reset_index(name='count')
    summary['Data Element'] = prop_encoding
    summary['Statistic Type'] = 'Count'
    summary = summary[['Data Element', prop_name, 'Statistic Type', 'count']]
    summary = summary.rename(columns={prop_name: 'Data Element Value', 'count': 'Statistic Value'})
    return summary

@task(name=f"file_counts_{get_time()}")
def file_counts(workbook: str, file_sheets: list) -> pd.DataFrame:
    """Parses file sheets in the workbook and returns a file counts dataframe.

    Args:
        workbook (str): Path to the excel workbook.
        file_sheets (list): List of file sheet names.

    Returns:
        pd.DataFrame: File counts dataframe.
    """
    file_counts =[]

    for sheet in file_sheets:
        temp = pd.read_excel(workbook, sheet_name=sheet)
        temp2 = temp.groupby('file_type').size().reset_index(name='counts')
        file_counts.append(temp2)

    file_counts2 = pd.concat(file_counts).groupby('file_type').sum().reset_index()
    file_counts_df = file_counts2.rename(columns={'file_type': 'Data Element Value', 'counts': 'Statistic Value'})
    file_counts_df['Data Element'] = 'Available File Types'
    file_counts_df['Statistic Type'] = 'Count'
    file_counts_df = file_counts_df[['Data Element', 'Data Element Value', 'Statistic Type', 'Statistic Value']]

    # calculate total files from dataframe
    total_files = file_counts_df['Statistic Value'].sum()
    total_df = pd.DataFrame({'Data Element': ['Total File Count'], 'Data Element Value': [pd.NA], 'Statistic Type': ['Count'], 'Statistic Value': [total_files]})
    file_counts_df = pd.concat([file_counts_df, total_df], ignore_index=True)

    return file_counts_df

@task(name=f"total_file_size_{get_time()}")
def total_file_size(workbook: str, file_sheets: list) -> pd.DataFrame:
    """Parses file sheets in the workbook and returns a total file size in terabytes dataframe.

    Args:
        workbook (str): Path to the excel workbook.
        file_sheets (list): List of file sheet names.

    Returns:
        pd.DataFrame: Total file size dataframe.
    """
    total_size = 0

    for sheet in file_sheets:
        temp = pd.read_excel(workbook, sheet_name=sheet)
        if 'file_size' in temp.columns:
            total_size += temp['file_size'].sum()

    size_df = pd.DataFrame({'Data Element': ['Total File Size (Tb)', 'Total File Size (Gb)'], 'Data Element Value': [pd.NA, pd.NA], 'Statistic Type': ['Exact Value', 'Exact Value'], 'Statistic Value': [total_size / (1000 ** 4), total_size / (1000 ** 3)]})

    return size_df

@flow(name=f"data_catalog_stats_flow", log_prints=True, flow_run_name="data_catalog_counts_" + f"{get_time()}",)
def data_catalog_stats(bucket: str, workbook_path: str, phs: str, upload_path: str):
    """Generates summary statistics from a data catalog excel workbook.
    
    Args:
        bucket (str): Bucket name of where the manifest is located in and the output goes to, i.e. ccdi-validation.
        workbook_path (str): Path to excel workbook of study to generate counts for, e.g. bullenca/test_data_catalog.xlsx.
        phs (str): dbGaP phs identifier to use in output file names, e.g. phs000178.
        upload_path (str): Path in bucket to upload output file to, e.g. bullenca/data_catalog_counts.
    
    """

    start_time = time.time()

    # create a logging object
    runner_logger = get_run_logger()

    runner_logger.info(">>> Running data_catalog_stats.py ....")

    # download the file
    file_dl(bucket, workbook_path)
    
    workbook = os.path.basename(workbook_path)

    # find all file sheets
    file_sheets = [i for i in pd.ExcelFile(workbook).sheet_names if '_file' in i]
    
    header = ['Data Element', 'Data Element Value', 'Statistic Type', 'Statistic Value']
    
    # init list of dataframes
    summary_dfs = []
    
    # init dataframe with phs ID
    phs_df = pd.DataFrame({'Data Element': ['dbGaP Study Identifier'], 'Data Element Value': [phs], 'Statistic Type': ['Exact Value'], 'Statistic Value': pd.NA})
    summary_dfs.append(phs_df)

    # list of summaries to generate as instructions to general_parser
    general_summaries = [
        ['participant', 'sex_at_birth', 'Case Sex'], 
        ['participant', 'race', 'Case Race'],
        ['diagnosis', 'diagnosis', 'Case Disease Diagnosis'],
        ['sample', 'anatomic_site', 'Sample Tumor Site'],
        ['sample', 'tumor_classification', 'Sample Tumor Classification'],
        ]

    # list of summaries to generate as instructions to total_counts
    total_summaries = [
        ['participant', 'Case ID'],
        ['sample', 'Sample ID'],
    ]
    
    # list of summaries to generate as instructions to sample_level_counts
    sample_level_counts_summaries = [
        ['sequencing_file', 'library_strategy', 'Sample Assay Method'],
        ['methylation_array_file', 'methylation_platform', 'Sample Assay Method'],
        ['cytogenomic_file', 'cytogenomic_platform', 'Sample Assay Method'],
    ]

    for sheet, prop in total_summaries:
        runner_logger.info(f">>> Processing total counts for sheet: {sheet}, property: {prop}")
        summary_dfs.append(total_counts(workbook, sheet, prop))
    
    for sheet, prop, encoding in general_summaries:
        runner_logger.info(f">>> Processing general summary for sheet: {sheet}, property: {prop}, encoding: {encoding}")
        summary_dfs.append(general_parser(workbook, sheet, prop, encoding))
        
    # append age at diagnosis summary
    runner_logger.info(f">>> Processing age at diagnosis summary")
    summary_dfs.append(age_at_diagnosis_parser(workbook, 'diagnosis'))

    for sheet, prop, encoding in sample_level_counts_summaries:
        runner_logger.info(f">>> Processing sample level counts for sheet: {sheet}, property: {prop}, encoding: {encoding}")
        summary_dfs.append(sample_level_counts(workbook, sheet, prop, encoding))
        
    # get file counts
    runner_logger.info(f">>> Processing file counts for sheets: {file_sheets}")
    file_counts_df = file_counts(workbook, file_sheets)
    summary_dfs.append(file_counts_df)
    
    # get total file size
    runner_logger.info(f">>> Processing total file size for sheets: {file_sheets}")
    total_size_df = total_file_size(workbook, file_sheets)
    summary_dfs.append(total_size_df)

    # concatenate all dataframes
    df = pd.concat(summary_dfs)
    
    runner_logger.info(f">>> Final dataframe shape: {df.shape}")
    
    runner_logger.info(f">>> Cleaning up Data Element Value column")
    
    # replace anatomic site codes
    df['Data Element Value'] = df['Data Element Value'].str.replace(r'C\d{2,4}.*\d*\s:\s', '', regex=True)
    
    # replace ICD codes
    df['Data Element Value'] = df['Data Element Value'].str.replace(r'\d{4}/\d\s:\s', '', regex=True)

    df.columns = header

    # Convert all values to numeric first
    df['Statistic Value'] = pd.to_numeric(df['Statistic Value'], errors='coerce')
    
    df['Statistic Value'] = df['Statistic Value'].astype('object')

    # convert any Data Element Value hat is in all caps to title case except for words AND or OR should be lower case
    df['Data Element Value'] = df['Data Element Value'].apply(title_case_except_and_or)


    # Convert to int where Statistic Type is 'Count'
    df.loc[df['Statistic Type'] == 'Count', 'Statistic Value'] = \
        df.loc[df['Statistic Type'] == 'Count', 'Statistic Value'].astype('Int64')
    
    custom_order = ["dbGaP Study Identifier",
        "Case ID",
        "Case Sex",
        "Case Race",
        "Case Age at Diagnosis",
        "Case Disease Diagnosis",
        "Sample ID",
        "Sample Tumor Site",
        "Sample Tumor Classification",
        "Sample Assay Method",
        "Available File Types",
        "Total File Count",
        "Total File Size (Tb)",
        "Total File Size (Gb)"
    ]

    df['Data Element'] = pd.Categorical(df['Data Element'], categories=custom_order, ordered=True)  
    df = df.sort_values(['Data Element']).reset_index(drop=True)

    runner_logger.info(f">>> Saving to excel and uploading to bucket {bucket} at path {upload_path}")

    # save to excel file
    output_file_name = phs+f"_data_catalog_summary_{get_time()}.xlsx"
    df.to_excel(output_file_name, index=False)
    
    # upload file to bucket
    file_ul(bucket=bucket, output_folder=upload_path, sub_folder='', newfile=output_file_name)
    
    end_time = time.time()
    elapsed_time = end_time - start_time

    runner_logger.info(f">>> Upload complete for {output_file_name} to bucket {bucket} at path {upload_path}")
    runner_logger.info(f">>> Total time elapsed: {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    """inputs: excel workbook, phs
    """
    data_catalog_stats(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])


