"""
This module defines Pydantic models for configuring S3 access, manifest processing,
and status mapping. These models are used to validate and structure configuration
data for working with S3 manifests and their associated metadata.

Classes:
    ConfigManifest:
            - bucket (Optional[str]): Name of the S3 Bucket that stores the input manifest file.
            - key (str): Key (or path) to the input manifest file in the S3 Bucket.
            - url_column (str): Name of the column in the manifest file that contains the S3 URLs.
            - status_column (str): Column in the manifest that contains the registration and release
                status.

    ConfigStatusMap:
        A Pydantic model representing the registration and release status values based on a status
        map key.
            - kf_registered (bool): Whether the S3 object is registered in the system.
            - kf_released (bool): Whether the S3 object is released in the system.

    ConfigBoto:
        A Pydantic model representing the configuration for Boto3 S3 access. This configuration
        is primarily used for local testing and development.
            - profile_name (Optional[str]): Name of the AWS profile to use for S3 access.

    Config:
            - manifest (ConfigManifest): Configuration for the S3 manifest file.
            - status_map (List[Dict[str, ConfigStatusMap]]): Mapping of S3 status to the
                registration and release status.
            - boto (ConfigBoto): Configuration for Boto3 S3 access.
"""

import datetime
from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, computed_field

# create a type that is Enum to indicate the file type, whether that is a raw manifest, enriched manifest, or report


class FileType(str, Enum):
    """
    FileType
    An Enum representing the type of file being processed.

    Attributes:
        RAW_MANIFEST (str): Indicates a raw manifest file.
        ENRICHED_MANIFEST (str): Indicates an enriched manifest file.
        REPORT (str): Indicates a report file.
    """

    RAW_MANIFEST = "input"
    ENRICHED_MANIFEST = "enriched_manifest"
    REPORT = "report"
    INVENTORY = "inventory"


class ConfigManifest(BaseModel):
    """
    ConfigManifest
    A Pydantic model representing the configuration for an S3 manifest file.

    Attributes:
        bucket (Optional[str]): Name of the S3 Bucket that stores the input manifest file.
        key (str): Key (or path) to the input manifest file in the S3 Bucket.
        url_column (str): Name of the column in the manifest file that contains the S3 URLs.
        status_column (str): Column in the manifest that contains the registration and release
            status.

    Example:
    ```python
        manifest_config = ConfigManifest(
            bucket="my-bucket",
            key="path/to/manifest.csv",
            url_column="s3_url",
            status_column="status"
        )
    ```
    """

    bucket: Optional[str] = Field(
        alias="bucket",
        title="S3 Bucket",
        description="Name of the S3 Bucket that stores the input manifest file.",
        default="ccdi-kidsfirst-transfer-manifests",
    )

    key: str = Field(
        alias="key",
        title="S3 Key",
        description="Key (or path) to the input manifest file in the S3 Bucket.",
    )

    url_column: Optional[str] = Field(
        alias="url_column",
        title="Fully-Qualified S3 URL Column",
        description="Name of the column in the manifest file that contains the S3 URLs.",
        default="urls",
    )

    status_column: Optional[str] = Field(
        alias="status_column",
        title="S3 Status Column",
        description="Column in the manifest that contains the registration and release status.",
        default="registration_status",
    )

    @computed_field
    @property
    def timestamp(self) -> str:
        """
        Returns the current timestamp in the format YYYYMMDDTHHMMSS.
        """
        return datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    @computed_field
    @property
    def enriched_manifest_key(self) -> str:
        """
        Returns the key for the enriched manifest file in the S3 Bucket.
        The key is constructed by replacing the "/input/" part of the original key with
        "/enriched_manifest/{timestamp}/".
        """
        return str(self.key).replace("/input/", f"/enriched_manifest/{self.timestamp}/")

    @computed_field
    @property
    def report_key(self) -> str:
        """
        Returns the key for the report file in the S3 Bucket.
        The key is constructed by replacing the "/input/" part of the original key with
        "/report/{timestamp}/".
        """
        return str(self.key).replace("/input/", f"/report/{self.timestamp}/")

    @computed_field
    @property
    def inventory_key(self) -> str:
        """
        Returns the key for the inventory file in the S3 Bucket.
        The key is constructed by replacing the "/input/" part of the original key with
        "/inventory/{timestamp}/".
        """
        return str(self.key).replace("/input/", f"/inventory/{self.timestamp}/")


class ConfigDataFiles(BaseModel):
    bucket: Optional[str] = Field(
        alias="bucket",
        title="S3 Bucket",
        description="Name of the S3 Bucket where the data files are stored.",
        default=None,
    )

    suffix: Optional[str] = Field(
        alias="suffix",
        title="S3 Suffix",
        description="The suffix appended to the name of the NCI S3 Bucket.",
        default="nci",
    )


class ConfigStatusMap(BaseModel):
    """
    ConfigStatusMap
    A Pydantic model representing the registration and release status values based on a status map
    key.

    Attributes:
        kf_registered (bool): Whether the S3 object is registered in the system.
        kf_released (bool): Whether the S3 object is released in the system.

    Example:
    ```python
        status_map = ConfigStatusMap(
            kf_registered=True,
            kf_released=False
        )
    ```
    """

    kf_registered: bool = Field(
        alias="kf_registered",
        title="Registered",
        description="Whether the S3 object is registered in the system.",
    )

    kf_released: bool = Field(
        alias="kf_released",
        title="Released",
        description="Whether the S3 object is released in the system.",
    )


class ConfigBoto(BaseModel):
    """
    ConfigBoto
    A Pydantic model representing the configuration for Boto3 S3 access. This
    configuration is primarily used for local testing and development. The
    profile_name does not need to be set when running in the AWS environment.

    Attributes:
        profile_name (Optional[str]): Name of the AWS profile to use for S3 access.

    Example:
    ```python
        boto_config = ConfigBoto(
            profile_name="my-aws-profile"
        )
    ```
    """

    profile_name: Optional[str] = Field(
        alias="profile_name",
        title="AWS Profile Name",
        description="Name of the AWS profile to use for S3 access.",
        default=None,
    )


class Config(BaseModel):
    """
    Config
    A Pydantic model representing the configuration for S3 access and manifest processing.
    Attributes:
        manifest (ConfigManifest): Configuration for the S3 manifest file.
        status_map (List[Dict[str, ConfigStatusMap]]): Mapping of S3 status to the registration
            and release status.
        boto (ConfigBoto): Configuration for Boto3 S3 access.
    Example:

    ```python
        config_dict = {
            "manifest": {
                "bucket": "my-bucket",
                "key": "path/to/manifest.csv",
                "url_column": "s3_url",
                "status_column": "status"
            },
            "status_map": [
                "registered and released": {
                    "kf_registered": True,
                    "kf_released": True
                    },
                "registered and not released": {
                    "kf_registered": True,
                    "kf_released": False
                },
            ],
            "boto": {
                "profile_name": "my-aws-profile"
            }
        }
        config = Config(**config_dict)
    ```
    """

    manifest: ConfigManifest = Field(
        alias="manifest",
        title="S3 Manifest",
        description="Configuration for the S3 manifest file.",
    )

    data_files: ConfigDataFiles = Field(
        alias="data_files",
        title="S3 Data Files",
        description="Configuration for the S3 data files to be tagged.",
        default=ConfigDataFiles(),
    )

    status_map: List[Dict[str, ConfigStatusMap]] = Field(
        alias="status_map",
        title="S3 Status Map",
        description="Mapping of S3 status to the registration and release status.",
        default=[
            {
                "registered and released": ConfigStatusMap(
                    kf_registered=True, kf_released=True
                )
            },
            {
                "registered and not released": ConfigStatusMap(
                    kf_registered=True, kf_released=False
                )
            },
            {
                "not registered and not releeased": ConfigStatusMap(
                    kf_registered=False, kf_released=False
                )
            },
            {
                "not registered and not released": ConfigStatusMap(
                    kf_registered=False, kf_released=False
                )
            },
            {
                "not registered and released": ConfigStatusMap(
                    kf_registered=False, kf_released=True
                )
            },
        ],
    )

    boto: ConfigBoto = Field(
        alias="boto",
        title="Boto Configuration",
        description="Configuration for Boto3 S3 access.",
    )
