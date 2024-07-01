from prefect import flow, get_run_logger
import os
import sys

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.utils import file_dl, folder_dl, file_ul, get_time, folder_ul
from src.manifest_liftover import liftover_tags, tags_validation
from src.ccdi_liftover_cds import validate_mapping_cds, liftover_to_cds_template


@flow(name="CCDI to CDS liftover", log_prints=True)
def ccdi_to_cds_liftover(
    bucket: str,
    mapping_filepath: str,
    lift_from_filepath: str,
    lift_to_folderpath: str,
    lift_to_version: str,
    runner: str,
) -> None:
    logger = get_run_logger()

    output_folder = os.path.join(runner, f"liftover_ccdi_template_{get_time()}")

    # download mapping file
    file_dl(bucket=bucket, filename=mapping_filepath)
    mapping_file = os.path.basename(mapping_filepath)
    logger.info(f"Downloaded mapping file: {mapping_file}")

    # download CCDI manifest
    file_dl(bucket=bucket, filename=lift_from_filepath)
    ccdi_manifest = os.path.basename(lift_from_filepath)
    logger.info(f"Downloaded CCDI manifest: {ccdi_manifest}")

    # download CDS template tsv files
    folder_dl(bucket=bucket, remote_folder=lift_to_folderpath)
    tsv_templates = lift_to_folderpath
    logger.info(f"Downloaded cds template files: {tsv_templates}")

    # validate if the manifest version matches to lift_from_version, and template version matches to lift_to_version
    lift_from_tag, lift_to_tag = liftover_tags(liftover_mapping_path=mapping_file)
    lift_from_validation = tags_validation(
        manifest_path=ccdi_manifest, tag=lift_from_tag, logger=logger
    )
    lift_to_validation = lift_to_version == lift_to_tag
    if lift_from_validation == True and lift_to_validation == True:
        pass
    else:
        logger.error(
            f"At least one of tag validations between manifest and mapping file failed:\n- manifest tag = lift from tag: {lift_from_validation}\n- template tag = lift to tag: {lift_to_validation}"
        )
        raise ValueError(
            "Version incompatibility found between manifest/template and mapping file"
        )

    # validate mapping file
    validate_report = validate_mapping_cds(
        manifest_path=ccdi_manifest,
        template_version=lift_to_version,
        template_folderpath=tsv_templates,
        mapping_path=mapping_file,
    )
    logger.info(f"Created a report for mapping file evaluation: {validate_report}")
    file_ul(
        bucket=bucket,
        output_folder=output_folder,
        sub_folder="",
        newfile=validate_report,
    )
    logger.info(
        f"Uploaded mapping file evaluation report to the bucket {bucket} folder {output_folder}"
    )

    # liftover values to cds templates
    tsv_output_folder, logfile_name = liftover_to_cds_template(
        mapping_file=mapping_file,
        manifest_file=ccdi_manifest,
        template_folderpath=tsv_templates,
        template_version=lift_to_version,
    )

    # upload tsv template folder to bucket
    file_ul(
        bucket=bucket, output_folder=output_folder, sub_folder="", newfile=logfile_name
    )
    logger.info(f"Uploaded liftover log file {logfile_name} to bucket {bucket} folder {output_folder}")
    folder_ul(
        local_folder=tsv_output_folder,
        bucket=bucket,
        destination=output_folder,
        sub_folder=""
    )
    logger.info(f"Uploaded liftover cds tsv folder {tsv_output_folder} to bucket {bucket} folder {output_folder}")
    
    logger.info("CCDI to CDS liftover pipeline finished!")

    return None
