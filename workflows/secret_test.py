from prefect import flow, task
import boto3
from botocore.exceptions import ClientError
import json

@task(name="ccdi get secret from secrets manager", log_prints=True)
def get_secret(secret_name_path: str, secret_key_name: str, account: str):
    """Retrieve a secret hash from AWS Secrets Manager

    Args:
        secret_name_path (str): Secrets name path, i.e. ccdi/storage/inventory/token
        secret_key_name (str): Secret key name associated with hash/token
        account (str): AWS account identifier
    Returns:
        str: Secret hash/token
    """
    region_name = "us-east-1"
    secret_name_path = f"arn:aws:secretsmanager:{region_name}:{account}:secret:{secret_name_path}"
    print(f"Retrieving secret from path: {secret_name_path} with key: {secret_key_name}")
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name_path)
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    return json.loads(get_secret_value_response["SecretString"])[secret_key_name]


@flow(name="TEST Secret Retrieval Pipeline", log_prints=True)
def secret_pipeline(secret_name_path: str, secret_key_name: str, account: str) -> None:
    """
    Prefect pipeline that retrieves and prints a secret.
    
    Args:
        secret_name_path: Path to the secret to retrieve
        secret_key_name: Name of the key within the secret to retrieve
        account: AWS account identifier
    """
    secret_value = get_secret(secret_name_path, secret_key_name, account)
    print(f"###########Secret value: {secret_value}############")

