from prefect import flow, task
from utils import get_secret


@task
def retrieve_secret(secret_name: str, region_name: str) -> str:
    """Retrieve a secret using the get_secret function."""
    secret_value = get_secret(secret_name, region_name=region_name)
    return secret_value


@task
def print_secret(secret_value: str) -> None:
    """Print the secret value."""
    print(f"Secret value: {secret_value}")


@flow(name="TEST Secret Retrieval Pipeline")
def secret_pipeline(secret_name: str, region_name: str) -> None:
    """
    Prefect pipeline that retrieves and prints a secret.
    
    Args:
        secret_name: Name of the secret to retrieve
        region_name: AWS region where the secret is stored
    """
    secret_value = retrieve_secret(secret_name, region_name=region_name)
    print_secret(secret_value)


if __name__ == "__main__":
    # Example usage
    secret_pipeline(secret_name="test_secret", region_name="us-east-1")