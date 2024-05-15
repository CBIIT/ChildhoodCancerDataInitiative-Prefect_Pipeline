from prefect import flow, get_run_logger
import sys
import os

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.s3_validationry_refactored import ValidationRy_new
from src.utils import file_dl, dl_ccdi_template, get_time, file_ul


@flow(name="Validate CCDI Manifest", flow_run_name="{runner}_" + f"{get_time()}")
def validation_run(ccdi_file_path: str, bucket: str, runner: str)-> None:
    logger = get_run_logger()

    output_folder = runner + "/validation_refactored_" + get_time()

    # download manifest
    file_dl(filename=ccdi_file_path, bucket=bucket)
    ccdi_file = os.path.basename(ccdi_file_path)

    # download ccdi template
    logger.info("Downloading cccdi template")
    manifest_file =  dl_ccdi_template()

    # generate validation report output
    logger.info(f"Validating ccdi manifest: {manifest_file}")
    output_file = ValidationRy_new(file_path=ccdi_file, template_path=manifest_file)

    # upload output_file to bucket
    logger.info(f"Uploading validation report {output_file} to the bucket {bucket} folder {output_folder}")
    file_ul(bucket=bucket, output_folder=output_folder, sub_folder="", newfile=output_file)

    logger.info("Workflow finished!")
