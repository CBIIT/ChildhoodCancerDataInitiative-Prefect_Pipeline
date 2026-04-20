from src.utils import get_time
from prefect import flow, task, get_run_logger
import os
import sys

from workflows.pull_db_data import pull_db_data
from workflows.db_tsv_to_manifest import join_tsv_to_manifest


@flow(
    name="Pull DB data and join to manifest for single study",
    log_prints=True,
    flow_run_name="pull-db-join-tsvs-{runner}-" + f"{get_time()}",
)
def pull_n_join_manifest_single_study(
    bucket: str,
    runner: str,
    database_account_id: str,
    database_secret_path: str,
    database_secret_key_ip: str,
    database_secret_key_username: str,
    database_secret_key_password: str,
    study_id: str,
    dcc_template_tag: str
):
    """Pipeline that pulls ingested study from a DB database for study phs ID provided.

    Args:
        bucket (str): The S3 bucket to pull data from.
        runner (str): The runner to use for the workflow.
        database_account_id (str): AWS account ID for accessing secrets
        database_secret_path (str): Path to the secret in AWS Secrets Manager
        database_secret_key_ip (str): Key name for the URI in the secret
        database_secret_key_username (str): Key name for the username in the secret
        database_secret_key_password (str): Key name for the password in the secret
        study_id (str): The study ID to pull data for.
        dcc_template_tag (str): Tag name of the DCC template
    """
    
    logger = get_run_logger()
    logger.info(f"Pulling joined DB for study {study_id} from bucket {bucket} using runner {runner}")

    op_folder = pull_db_data(
        bucket=bucket,
        runner=runner,
        database_account_id=database_account_id,
        database_secret_path=database_secret_path,
        database_secret_key_ip=database_secret_key_ip,
        database_secret_key_username=database_secret_key_username,
        database_secret_key_password=database_secret_key_password,
        study_id_list=[study_id],
    )
    
    op_folder_path = f"{op_folder}/{study_id}".replace("s3://"+bucket+"/", "").replace("/./", "/")

    logger.info(f"Pulled data stored at {op_folder_path}")
    
    join_tsv_to_manifest(
        bucket=bucket,
        runner=runner,
        tsv_folder_path=op_folder_path,
        dcc_template_tag=dcc_template_tag
    )
    
    return None