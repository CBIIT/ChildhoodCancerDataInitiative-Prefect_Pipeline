from prefect import flow, get_run_logger
import os
import sys

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.utils import file_dl, dl_ccdi_template, CCDI_Tags, get_time, file_ul
from src.manifest_liftover import (
    tags_validation,
    liftover_tags,
    validate_mapping,
    liftover_to_template,
)


@flow(name="CCDI manifest liftover")
def manifest_liftover(
    bucket: str,
    lift_from_filepath: str,
    lift_to_tag: str,
    liftover_mapping_filepath: str,
    runner: str,
) -> None:
    logger = get_run_logger()

    output_folder = os.path.join(runner, f"liftover_ccdi_template_{get_time()}")

    # download manifest
    file_dl(bucket=bucket, filename=lift_from_filepath)
    ccdi_manifest = os.path.basename(lift_from_filepath)
    logger.info(f"Downloaded file {lift_from_filepath} from bucket {bucket}")

    # download template
    ccdi_template = CCDI_Tags().download_tag_manifest(tag=lift_to_tag, logger=logger)
    logger.info(f"Downloaded CCDI template with tag {lift_to_tag}")

    # download mapping file
    file_dl(bucket=bucket, filename=liftover_mapping_filepath)
    mapping_file = os.path.basename(liftover_mapping_filepath)
    logger.info(
        f"Downlaoded mapping file {liftover_mapping_filepath} from bucket {bucket}"
    )

    # validate if the manifest version matches to lift_from_version, and template version matches to lift_to_version
    lift_from_tag, lift_to_tag = liftover_tags(liftover_mapping_path=mapping_file)
    logger.info(
        f"Extracted tags from mapping file:\n\t- lift from version: {lift_from_tag}\n\t- lift to version: {lift_to_tag}"
    )

    lift_from_validation = tags_validation(
        manifest_path=ccdi_manifest, tag=lift_from_tag, logger=logger
    )
    lift_to_validation = tags_validation(
        manifest_path=ccdi_template, tag=lift_to_tag, logger=logger
    )
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
    validate_report = validate_mapping(
        manifest_path=ccdi_manifest,
        template_path=ccdi_template,
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

    logger.info("Start lifting values")
    output_file, liftover_log = liftover_to_template(
        mapping_file=mapping_file,
        manifest_file=ccdi_manifest,
        template_file=ccdi_template,
    )
    file_ul(
        bucket=bucket, output_folder=output_folder, sub_folder="", newfile=output_file
    )
    logger.info(f"Uploaded output {output_file} to the bucket {bucket} folder {output_folder}")
    file_ul(
        bucket=bucket, output_folder=output_folder, sub_folder="", newfile=liftover_log
    )
    logger.info(
        f"Uploaded output {liftover_log} to the bucket {bucket} folder {output_folder}"
    )

    return None
