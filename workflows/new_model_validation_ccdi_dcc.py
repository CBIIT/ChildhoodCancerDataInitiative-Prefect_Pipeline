import os
import sys
import traceback

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from model_to_submission_ccdi_dcc import create_submission_manifest
from src.template_exampler_ccdi_dcc import make_template_example
from src.s3_validationry_refactored import ValidationRy_new
from src.utils import get_date, get_time, file_ul
from prefect import flow, get_run_logger
from requests.exceptions import ConnectionError


@flow(
    name="New Model Validation",
    log_prints=True,
    flow_run_name="new-model-validation-{runner}-" + f"{get_time()}",
)
def validate_new_model(
    bucket: str,
    runner: str,
    release_title: str
) -> None:
    """Pipeline that creates a manifest using model files in GitHub repo main branch, generates a manifest with simulated data, and performs validation using the current validation pipeline

    Args:
        bucket (str): Bucket name that output goes to
        runner (str): Unique runner name
        release_title (str): Release title to use in new model
    """    
    # create a logging object
    runner_logger = get_run_logger()

    # get date and time
    todaydate = get_date()
    currenttime = get_time()

    new_model_validation_out = os.path.join(runner, "new_model_validation_"+currenttime)

    # generate new submission manifest file using model files
    # downloaded from Github
    try:
        create_submission_manifest(bucket=bucket, runner=new_model_validation_out, release_title=release_title)
        runner_logger.info(f"New model submission file has been created and uploaded")
    except:
        runner_logger.error("Creating submission manifest file using new model FAILED")

    filelist = os.listdir("./")
    manifest_file = [i for i in filelist if "CCDI-DCC_Submission_Template" in i]
    if len(manifest_file) >= 0:
        manifest_file = manifest_file[0]
    else:
        runner_logger.error("Failed to create a submission template using model files")
        return None
        
    # generate template exampler file with 30 entries
    template_exampler_output_folder = os.path.join(
        new_model_validation_out, "template_exampler_outputs_" + currenttime
    )
    try:
        output_exampler, exampler_logger = make_template_example(manifest_path=manifest_file, entry_num=30)
        file_ul(
            bucket=bucket,
            output_folder=template_exampler_output_folder,
            sub_folder="",
            newfile=output_exampler,
        )
        file_ul(
            bucket=bucket,
            output_folder=template_exampler_output_folder,
            sub_folder="",
            newfile=exampler_logger,
        )
        runner_logger.info(
            f"Template exampler file has been created. The exampler file and log file have been uploadded to bucket {bucket} at {template_exampler_output_folder}"
        )

    except:
        output_exampler = None
        exampler_logger = "template_exampler_" + todaydate + ".log"
        file_ul(
            bucket=bucket,
            output_folder=template_exampler_output_folder,
            sub_folder="",
            newfile=exampler_logger,
        )
        runner_logger.error(
            f"Creating manifest exampler file FAILED. Log file was uploaded to bucket {bucket} at {template_exampler_output_folder}"
        )
        return None

    exampler_file = [i for i in os.listdir("./") if "_30Exampler.xlsx" in i][0]

    # run validationRy workflow
    validation_output_folder =  os.path.join(
        new_model_validation_out, "validation_output_" + currenttime
    )
    try:
        validation_out_file = ValidationRy_new(file_path=exampler_file, template_path=manifest_file)
        file_ul(
            bucket=bucket,
            output_folder=validation_output_folder,
            sub_folder="",
            newfile=validation_out_file,
        )
        runner_logger.info(f"Validation report was generated and uploaded to bucket {bucket} at {validation_output_folder}")
    except:
        runner_logger.error(
            f"Validation step FAILED"
        )

    runner_logger.info(
        f"Workflow new model validation has FINISHED. All outputs can be found in bucket {bucket} at {new_model_validation_out}"
    )
    return None


if __name__=="__main__":
    bucket="my-source-bucket"
    runner="QL"
    release_title="my new model release"

    validate_new_model(bucket=bucket, runner=runner, release_title=release_title)
