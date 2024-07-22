from prefect import flow, task, get_run_logger
import os
import sys


parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.s3_ccdi_to_sra import CCDI_to_SRA
from src.utils import file_ul, file_dl, get_time, identify_data_curation_log_file


@flow(name="CCDI to SRA submission", log_prints=True)
def run_ccdi_to_sra(
    bucket: str,
    manifest_path: str,
    template_path: str,
    runner: str,
    pre_submission=None,
) -> None:

    # downlaod manifest
    file_dl(bucket=bucket, filename=manifest_path)
    manifest_name = os.path.basename(manifest_path)
    print(f"Downloaded manifest {manifest_name}")

    # download the sra template
    file_dl(bucket=bucket, filename=template_path)
    template_name = os.path.basename(template_path)
    print(f"Downloaded sra template {template_name}")

    # output folder for outputs
    output_folder = os.path.join(runner, "ccdi_to_sra_outputs_" + get_time())

    print("Start generating SRA submission file")
    try:
        (sra_out_file, sra_out_log) = CCDI_to_SRA(
            manifest=manifest_name,
            template=template_name,
            pre_submission=None,
        )
    except:
        sra_out_file = None
        sra_out_log = identify_data_curation_log_file(
            start_str="CCDI_to_SRA_submission_"
        )
    print(f"Two outputs:\n - {sra_out_file}\n - {sra_out_log}")

    # upload outputs
    file_ul(
        bucket=bucket, output_folder=output_folder, sub_folder="", newfile=sra_out_file
    )
    file_ul(
        bucket=bucket, output_folder=output_folder, sub_folder="", newfile=sra_out_log
    )
    print(f"Uploaded outputs to the bucket {bucket} folder {output_folder}")

    return None
