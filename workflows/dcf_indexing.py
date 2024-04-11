from prefect import flow, get_run_logger
import os
import sys
import pandas as pd


parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.utils import (
    CheckCCDI,
    file_dl,
    file_ul,
    extract_dcf_index,
    combine_dcf_dicts,
    get_time,
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

    logger.info(f"Downloading manifest {manifest_path} from bucket {bucket}")
    file_dl(bucket=bucket, filename=manifest_path)
    manifest_file = os.path.basename(manifest_path)
    manifest_obj = CheckCCDI(ccdi_manifest=manifest_file)

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
        CCDI_manifest=manifest_obj, sheetname_list=obj_sheets
    )
    # combined these dicts into a single dict
    combined_dict = combine_dcf_dicts(list_dicts=list_dicts)
    del list_dicts

    # Convert combined_dict into a dataframe
    # add acl and authz
    combined_df = pd.DataFrame(combined_dict)
    combined_df["acl"] = acl
    combined_df["authz"] = authz
    col_order = ["GUID", "md5", "size", "acl", "authz", "urls"]
    combined_df = combined_df[col_order]
    logger.info(f"Number of objects in total: {combined_df.shape[0]}")

    # save df to tsv and upload to bucket
    output_filename = "dcf_index_" + study_accession + "_" + current_time + ".tsv"
    combined_df.to_csv(output_filename, sep="\t", index=False)
    del combined_df
    # upload file to the bucket
    output_folder = os.path.join(runner, "dcf_index_output_" + current_time)
    file_ul(
        bucket=bucket,
        output_folder=output_folder,
        sub_folder="",
        newfile=output_filename,
    )
    logger.info(
        f"Uploaded output {output_filename} to the bucket {bucket} folder {output_folder}"
    )
    logger.info("Workflow finished!")

    return None
