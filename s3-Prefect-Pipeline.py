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
from src.utils import get_time, file_ul, file_dl, view_all_s3_objects, markdown_task, markdown_output_task, folder_ul


@flow(
    name="S3 Prefect Pipeline test", log_prints=True, flow_run_name="{runner}_test_{get_time}"
)
def runner(
    bucket: str, file_path: str, template_path: str, sra_template_path: str, runner:str
):  
    # if not profile:
    profile = "default"
    instance = 0
    source_file_list = view_all_s3_objects(bucket)
    markdown_task(bucket, source_file_list, instance)

    file_dl(bucket, file_path)
    file_dl(bucket, template_path)
    file_dl(bucket, sra_template_path)

    input_file = os.path.basename(file_path)
    input_template = os.path.basename(template_path)
    input_sra_template = os.path.basename(sra_template_path)

    (catcherr_out_file, catcherr_out_log) = CatchERRy(
        input_file, input_template, time=get_time()
    )

    file_ul(bucket, file_path, catcherr_out_file)
    file_ul(bucket, file_path, catcherr_out_log)

    validation_out_file = ValidationRy(catcherr_out_file, input_template, time=get_time())

    file_ul(bucket, file_path, validation_out_file)

    (sra_out_file, sra_out_log) = CCDI_to_SRA(
        manifest=catcherr_out_file, template=input_sra_template, time=get_time()
    )

    file_ul(bucket, file_path, sra_out_file)
    file_ul(bucket, file_path, sra_out_log)

    (dbgap_output_folder, dbgap_out_log)= CCDI_to_dbGaP(manifest=catcherr_out_file, time=get_time())
    folder_ul(
        local_folder=dbgap_output_folder,
        bucket=bucket,
        destination=dbgap_output_folder,
        time=get_time(),
    )
    file_ul(bucket, file_path, dbgap_out_log)

    instance = 1
    source_file_list = view_all_s3_objects(bucket)
    markdown_output_task(bucket, source_file_list, instance)


if __name__ == "__main__":
    bucket = "my-source-bucket"
    file_path = "inputs/CCDI_Submission_Template_v1.7.1_40ExampleR20231205.xlsx"
    template_path = "inputs/CCDI_Submission_Template_v1.7.1.xlsx"
    sra_template_path = "inputs/phsXXXXXX.xlsx"

    runner(
        bucket, file_path, template_path, sra_template_path=sra_template_path, runner="QL"
    ) 
