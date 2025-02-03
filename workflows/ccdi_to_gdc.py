from prefect import flow, get_run_logger
import os
from src.s3_ccdi_to_gdc import ccdi_to_gdc
from src.utils import file_dl, get_time, folder_ul


@flow(name="CCDI to GDC", flow_run_name="{runner}_" + f"{get_time()}")
def ccdi_to_gdc_run(ccdi_file_path: str,
                    CCDI_GDC_translation_file : str, 
                    platform_preservation_file : str,
                    bucket: str, 
                    runner: str)-> None:
    
    logger = get_run_logger()

    # download manifest
    file_dl(filename=ccdi_file_path, bucket=bucket)
    ccdi_file = os.path.basename(ccdi_file_path)

    # download translation file
    file_dl(filename=CCDI_GDC_translation_file, bucket=bucket)
    CCDI_GDC_translation_file = os.path.basename(CCDI_GDC_translation_file)

    # download platform_preservation file 
    file_dl(filename=platform_preservation_file, bucket=bucket)
    platform_preservation_file = os.path.basename(platform_preservation_file)

    # generate ccdi_to_gdc output
    logger.info(f"Converting ccdi manifest to gdc manifest: {ccdi_file}")
    output_folder = ccdi_to_gdc(file_path=ccdi_file, CCDI_GDC_translation_file=CCDI_GDC_translation_file, platform_preservation_file=platform_preservation_file )

    # upload output_folder to bucket
    logger.info(f"Uploading coversion folder {output_folder} to the bucket {bucket}")
    folder_ul(local_folder=output_folder, bucket=bucket, destination= runner, sub_folder="", )

    logger.info("Workflow finished!")