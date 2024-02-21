from prefect import flow, get_run_logger
import os
import sys

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from botocore.exceptions import ClientError
import json
import boto3

@flow
def get_s3_parameter(parameter_name: str):
    # get logger
    logger = get_run_logger()

    # get s3_client object
    ssm_client = boto3.client("ssm")

    try:
        parameter_response =  ssm_client.get_parameter(Name=parameter_name)
        logger.info(f"Parameter info:\n{json.dumps(parameter_response, indent=4)}")
    except ClientError as err:
        ex_code = err.response["Error"]["Code"]
        ex_message = err.response["Error"]["Message"]
        logger.error(ex_code + ":" + ex_message)
    except Exception as error:
        logger.error(f"Get s3 parameter {parameter_name} FAILED")
        logger.error("General exception noted.", exc_info=True)

    return None
