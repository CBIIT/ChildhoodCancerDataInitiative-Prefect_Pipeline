from prefect import flow, task
from src.utils import get_secret
import boto3
from botocore.exceptions import ClientError


@flow(name="TEST Secret Retrieval Pipeline")
def secret_pipeline(secret_name: str, region_name: str) -> None:
    """
    Prefect pipeline that retrieves and prints a secret.
    
    Args:
        secret_name: Name of the secret to retrieve
        region_name: AWS region where the secret is stored
    """
    secret_value = get_secret(secret_name=secret_name, region_name=region_name)
    print(f"Secret value: {secret_value}")

