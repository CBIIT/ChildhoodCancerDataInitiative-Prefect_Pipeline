from typing import Any

import yaml

from kf_prefect.src.utils.models import Config


def source_config_local(config_path: str) -> Config:
    """
    Loads the configuration file from the specified path and returns it as a dictionary.
    The configuration file is expected to be in YAML format.
    """
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return Config(**config)


def source_config_s3(bucket: str, key: str, client: Any) -> Config:
    """
    Loads the configuration file from S3 and returns it as a dictionary.
    The configuration file is expected to be in YAML format.
    """
    response = client.get_object(Bucket=bucket, Key=key)
    config = yaml.safe_load(response["Body"].read().decode("utf-8"))
    return Config(**config)
