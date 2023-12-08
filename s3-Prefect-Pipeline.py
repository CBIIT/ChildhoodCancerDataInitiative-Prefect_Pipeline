"""
This prefect pipeline works flow combines four tools
that catches error and validate the CCDI manifest before it
generates files for dbGaP and SRA submission.

Authors: Sean Burke <sean.burke2@nih.gov>
         Qiong Liu <qiong.liu@nih.gov>
"""
from prefect import flow
import os
from src.s3_ccdi_to_sra import CCDI_to_SRA
from src.s3_ccdi_to_dbgap import CCDI_to_dbGaP
from src.s3_catcherry import CatchERRy
from src.s3_validationry import ValidationRy
from src.utils import (get_time, outputs_ul, file_dl, 
                       view_all_s3_objects, markdown_task, 
                       markdown_output_task, check_ccdi_version, 
                       dl_ccdi_template, dl_sra_template, 
                       get_ccdi_latest_release)


@flow(
    name="S3 Prefect Pipeline", 
    log_prints=True, flow_run_name="{runner}_"+f"{get_time()}"
)
def runner(
    bucket: str, file_path: str, runner: str, template_path: str = None, 
    sra_template_path: str = None, output_folder: str ="outputs"
):  
    
    # if not profile:
    profile = "default"
    source_file_list = view_all_s3_objects(bucket)
    markdown_task(bucket, source_file_list)

    # download the manifest
    file_dl(bucket, file_path)

    # check the manifest version before the workflow starts
    manifest_version =  check_ccdi_version(os.path.basename(file_path))
    latest_manifest_version =  get_ccdi_latest_release()
    if manifest_version != latest_manifest_version:
        raise ValueError(f"The CCDI manifest is not in the lates version of {latest_manifest_version}")
    else:
        pass

    # download CCDI template if not provided
    if template_path != None:
        file_dl(bucket, template_path)
        input_template = os.path.basename(template_path)
    else:
        input_template = dl_ccdi_template()

    # download SRA template if not provided
    if sra_template_path != None:
        file_dl(bucket, sra_template_path)
        input_sra_template = os.path.basename(sra_template_path)
    else:
        input_sra_template = dl_sra_template()

    input_file = os.path.basename(file_path)

    # run CatchERR
    (catcherr_out_file, catcherr_out_log) = CatchERRy(
        input_file, input_template
    )
    # run ValidationRy
    validation_out_file = ValidationRy(catcherr_out_file, input_template)
    # run CCDI to SRA
    (sra_out_file, sra_out_log) = CCDI_to_SRA(
        manifest=catcherr_out_file, template=input_sra_template
    )
    # run CCDI to dbGaP
    (dbgap_output_folder, dbgap_out_log)= CCDI_to_dbGaP(manifest=catcherr_out_file)

    # upload all outputs to the source bucket
    outputs_ul(
        bucket=bucket,
        output_folder=output_folder,
        catcherr_file=catcherr_out_file,
        catcherr_log=catcherr_out_log,
        validation_log=validation_out_file,
        sra_file=sra_out_file,
        sra_log=sra_out_log,
        dbgap_folder=dbgap_output_folder,
        dbgap_log=dbgap_out_log
    )

    source_file_list = view_all_s3_objects(bucket)
    markdown_output_task(bucket, source_file_list)


if __name__ == "__main__":
    bucket = "my-source-bucket"
    file_path = "inputs/CCDI_Submission_Template_v1.7.1_40ExampleR20231207.xlsx"
    output_folder = "test_out"

    runner(
        bucket, file_path, 
        runner="QL", output_folder=output_folder
    ) 
