from prefect import flow, task
from src.utils import get_secret
import boto3
from botocore.exceptions import ClientError


@flow(name="TEST Secret Retrieval Pipeline", log_prints=True)
def secret_pipeline(secret_name_path: str, secret_key_name: str) -> None:
    """
    Prefect pipeline that retrieves and prints a secret.
    
    Args:
        secret_name_path: Path to the secret to retrieve
        secret_key_name: Name of the key within the secret to retrieve
    """
    secret_value = get_secret(secret_name_path, secret_key_name)
    print(f"###########Secret value: {secret_value}############")

