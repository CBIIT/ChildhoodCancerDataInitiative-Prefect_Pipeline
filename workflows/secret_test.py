from prefect import flow, task
import boto3
from botocore.exceptions import ClientError
import json
from src.utils import get_secret_centralized_worker

@flow(name="TEST Secret Retrieval Pipeline", log_prints=True)
def secret_pipeline(secret_name_path: str, secret_key_name: str, account: str) -> None:
    """
    Prefect pipeline that retrieves and prints a secret.
    
    Args:
        secret_name_path: Path to the secret to retrieve
        secret_key_name: Name of the key within the secret to retrieve
        account: AWS account identifier
    """
    secret_value = get_secret_centralized_worker(secret_name_path, secret_key_name, account)
    # test if secret value is a string and has length greater than 0
    if isinstance(secret_value, str) and len(secret_value) > 0:
        print("$$$$$$$$$ We successfully retrieved the secret value! $$$$$$$$$")
    else:
        print("Failed to retrieve a valid secret value.")

