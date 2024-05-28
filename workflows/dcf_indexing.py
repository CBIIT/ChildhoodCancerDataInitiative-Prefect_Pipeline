from prefect import flow, get_run_logger
import os
import sys
import pandas as pd
from shutil import copy2
import hashlib


parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.utils import (
    CheckCCDI,
    file_dl,
    file_ul,
    extract_dcf_index,
    combine_dcf_dicts,
    get_time,
    get_date
)


@flow(
    name="Create DCF Index Manifest",
    flow_run_name="create-dcf-index-manifest-{runner}-" + f"{get_time()}",
)
def dcf_index_manifest(
    manifest_path: str, runner: str, bucket: str = "ccdi-validation"
) -> None:
    logger = get_run_logger()

    # get time
    current_time = get_time()
    # define output folder name in bucket for uploading
    output_folder = os.path.join(runner, "dcf_index_output_" + current_time)

    logger.info(f"Downloading manifest {manifest_path} from bucket {bucket}")
    file_dl(bucket=bucket, filename=manifest_path)
    manifest_file = os.path.basename(manifest_path)
    manifest_obj = CheckCCDI(ccdi_manifest=manifest_file)

    # copy the manifest and rename for potential new guids assigned
    modified_manifest_file = manifest_file.rsplit(".", 1)[0] + "_GUIDadded" + get_date() + ".xlsx"
    copy2(src=manifest_file, dst=modified_manifest_file)
    hashlib.md5(open(manifest_file, "rb").read()).hexdigest()
    hashlib.md5(open(modified_manifest_file, "rb").read()).hexdigest()

    # extract study accession of the manifest
    study_accession = manifest_obj.get_study_id()
    acl = f"['{study_accession}']"
    authz = f"['/programs/{study_accession}']"
    logger.info(f"Study accesion: {study_accession}")

    # find the data file sheets/nodes
    obj_sheets = manifest_obj.find_file_nodes()
    logger.info(f"Sheets of data files: {*obj_sheets,}")

    # extract columns related to dcf indexing
    logger.info("Extracting columns for DCF indexing")
    list_dicts = extract_dcf_index(
        CCDI_manifest=manifest_obj, sheetname_list=obj_sheets, modified_manifest=modified_manifest_file
    )
    # combined these dicts into a single dict
    combined_dict = combine_dcf_dicts(list_dicts=list_dicts)
    del list_dicts

    # Convert combined_dict into a dataframe
    # add acl and authz
    combined_df = pd.DataFrame(combined_dict)
    del combined_dict
    logger.info(f"Number of objects in total before duplcates removed: {combined_df.shape[0]}")
    if sum(combined_df["if_guid_missing"])>0:
        # upload modified_manifest_file only if missing guid objects were found
        counts_of_missing_guid = sum(combined_df["if_guid_missing"])
        missing_guid_summary_df = combined_df[combined_df["if_guid_missing"]].groupby(["node"]).size().reset_index(name="counts")
        missing_guid_summary_str = missing_guid_summary_df.to_markdown(tablefmt="pipe", index=False)
        logger.warning(f"Number of objects with missing guid were assigned with new one: {counts_of_missing_guid}\n{missing_guid_summary_str}")
        file_ul(
            bucket=bucket,
            output_folder=output_folder,
            sub_folder="",
            newfile=modified_manifest_file
        )
        logger.info(f"Uploaded modified manifest {modified_manifest_file} with new guids to bucket {bucket} folder path {output_folder}")
        del missing_guid_summary_df
    else:
        pass
    combined_df.drop_duplicates(inplace=True, ignore_index=True)
    logger.info(f"Number of objects in total after removing duplicates: {combined_df.shape[0]}")

    # finish up with the dcf index df
    combined_df.drop(columns=["node","if_guid_missing"], inplace=True)
    combined_df["acl"] = acl
    combined_df["authz"] = authz
    col_order = ["GUID", "md5", "size", "acl", "authz", "urls"]
    combined_df = combined_df[col_order]

    # save df to tsv and upload to bucket
    output_filename = "dcf_index_" + study_accession + "_" + current_time + ".tsv"
    combined_df.to_csv(output_filename, sep="\t", index=False)
    del combined_df
    # upload file to the bucket
    file_ul(
        bucket=bucket,
        output_folder=output_folder,
        sub_folder="",
        newfile=output_filename,
    )
    logger.info(
        f"Uploaded output {output_filename} to bucket {bucket} folder path {output_folder}"
    )
    logger.info("Workflow finished!")

    return None
