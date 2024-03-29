from prefect import flow, task
from src.utils import CheckCCDI, get_date
import pandas as pd
from shutil import copy


def if_version_match(xlsx_list: list[str], template_version: str) -> tuple[list]:
    if_match = []
    if_not_match = []
    for i in xlsx_list:
        i_version = CheckCCDI(ccdi_manifest=i).get_version()
        if i_version == template_version:
            if_match.append(i)
        else:
            if_not_match.append((i, i_version))
    return if_match, if_not_match

@task
def append_one_submission(submission_file: str, append_to_file: str):
    submission_obj = CheckCCDI(ccdi_manifest=submission_file)
    append_to_obj = CheckCCDI(ccdi_manifest=append_to_file)
    skip_sheetnames =  ["README and INSTRUCTIONS","Dictionary","Terms and Value Sets"]
    sheetnames = submission_obj.get_sheetnames()
    sheetnames = [i for i in sheetnames if i not in skip_sheetnames]
    for j in sheetnames:
        j_df = submission_obj.read_sheet_na(sheetname=j)
        if j_df.empty:
            pass
        else:
            j_append_to_df =  append_to_obj.read_sheet_na(sheetname=j)
            j_append_to_df = pd.concat([j_append_to_df, j_df], ignore_index=True)
            # drop any duplicated lines
            j_append_to_df.drop_duplicates(inplace=True, ignore_index=True)
            with pd.ExcelWriter(
                append_to_file, mode="a", engine="openpyxl", if_sheet_exists="overlay"
            ) as writer:
                j_append_to_df.to_excel(writer, sheet_name=j, index=False, header=False, startrow=1)

    return None


@flow
def concatenate_submissions(xlsx_list: list[str], template_file: str, logger) -> str:
    """Merge several submission files into one"""
    # check if submisison files' version matches to template's
    tempalte_version = CheckCCDI(ccdi_manifest=template_file).get_version()
    logger.info(f"CCDI template version: {tempalte_version}")
    xlsx_list, not_matched_list = if_version_match(xlsx_list=xlsx_list, template_version=tempalte_version)
    if len(not_matched_list) != 0:
        logger.error(f"Found {len(not_matched_list)} submission files has version different from template:  {*not_matched_list,}")
    else:
        logger.info("Submission files version matches to template's")

    # create an output name
    output_name = "CCDI_MetaMerge_v" + tempalte_version + "_" + get_date + ".xlsx"
    copy(template_file, output_name)

    # concatinate info of submission files
    completed = 0
    for h in xlsx_list:
        logger.info(f"Appending info from file {h}")
        append_one_submission(submission_file=h, append_to_file=output_name)
        logger.info(f"Progress: {completed}/{len(xlsx_list)}")
        completed += 1
    return output_name


