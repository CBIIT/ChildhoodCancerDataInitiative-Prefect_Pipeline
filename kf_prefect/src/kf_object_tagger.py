"""
KFObjectTagger is a class designed to handle the tagging of objects in an S3
bucket based on a manifest file. It is initialized with a validated
configuration object that provides the necessary parameters for retrieving
and procesesing the manifest file.

The manifest file, expected to be in CSV format, contains information about
the objects to be tagged, including their S3 bucket and key, as well as their
registration and release status. The class provides methods to load the manifest
file from S3, enrich it with additional information, and apply tags to the
objects in the S3 bucket based on the enriched manifest.

Attributes:
    config (Config): A configuration object containing parameters for S3 interactions and
        manifest processing.
    logger (Logger): A logger instance for logging messages and errors.
    client (S3Client): An S3 client instance for interacting with AWS S3.

Methods:
    __init__(config: Config):
        Initializes the KFObjectTagger instance with a configuration object, logger, and S3 client.
    get_input_manifest() -> List[Dict[str, Any]]:
        Loads a manifest file from an S3 bucket and parses its contents into a list of dictionaries.
    get_enriched_manifest(manifest: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        Enriches a given manifest by processing it with additional information.
    tag_objects(enriched_manifest: List[Dict[str, Any]]):
        Tags objects in an S3 bucket based on the enriched manifest and returns a manifest
        report that includes the tagging status of each object.
"""

from typing import Any, Dict, List, Optional

from botocore.exceptions import ClientError
from mypy_boto3_s3.type_defs import PutObjectOutputTypeDef

from src.utils.boto_helper import (
    get_client,
    inventory_object_tags,
    load_manifest,
    tag_objects,
    upload_object,
)
from kf_prefect.src.utils.logger import get_logger
from kf_prefect.src.utils.models import Config, FileType
from kf_prefect.src.utils.transforms import enrich_manifest


class KFObjectTagger:
    """
    KFObjectTagger is a class designed to handle the tagging of objects in an S3
    bucket based on a manifest file. It is initialized with a validated
    configuration object that provides the necessary parameters for retrieving
    and procesesing the manifest file.

    The manifest file, expected to be in CSV format, contains information about
    the objects to be tagged, including their S3 bucket and key, as well as their
    registration and release status. The class provides methods to load the manifest
    file from S3, enrich it with additional information, and apply tags to the
    objects in the S3 bucket based on the enriched manifest.

    Attributes:
        config (Config): A configuration object containing parameters for S3 interactions and
            manifest processing.
        logger (Logger): A logger instance for logging messages and errors.
        client (S3Client): An S3 client instance for interacting with AWS S3.

    Methods:
        __init__(config: Config):
            Initializes the KFObjectTagger instance with a configuration object, logger, and S3
            client.
        get_input_manifest() -> List[Dict[str, Any]]:
            Loads a manifest file from an S3 bucket and parses its contents into a list of
            dictionaries.
        get_enriched_manifest(manifest: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            Enriches a given manifest by processing it with additional information.
        tag_objects(enriched_manifest: List[Dict[str, Any]]):
            Tags objects in an S3 bucket based on the enriched manifest and returns a manifest
            report that includes the tagging status of each object.
    """

    def __init__(self, config: Config):
        self.logger = get_logger()
        self.config = config
        self.client = get_client(self.config)

    def get_input_manifest(self) -> List[Dict[str, Any]]:
        """
        Loads a manifest file from an S3 bucket and parses its contents into a list of dictionaries.
        The method retrieves the manifest file specified by the bucket and key in the configuration,
        reads its content, and parses it as a CSV file. Each row in the CSV is converted into a
        dictionary, where the keys are the column headers.

        Returns:
            list[dict]: A list of dictionaries representing the rows in the manifest file.

        Raises:
            ClientError: If there is an error retrieving the manifest file from S3.
        """
        self.logger.info("Loading manifest from S3")
        try:
            response = load_manifest(self.client, self.config)
            self.logger.info("Manifest loaded successfully")
        except ClientError as err:
            self.logger.error("Failed to load manifest: %s", err)
            raise err
        return response

    def get_enriched_manifest(
        self, manifest: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Enriches a given manifest by processing it with additional information.

        Args:
            manifest (List[Dict[str, Any]]): A list of dictionaries representing the
                manifest to be enriched.

        Returns:
            response (List[Dict[str, Any]]): A list of dictionaries representing the enriched manifest.

        Raises:
            Exception: If an error occurs during the enrichment process.
        """
        if not self.config.manifest.url_column:
            self.logger.error("URL column not specified in the configuration")
            raise ValueError("URL column not specified in the configuration")

        if not self.config.manifest.status_column:
            self.logger.error("Status column not specified in the configuration")
            raise ValueError("Status column not specified in the configuration")

        try:
            response = enrich_manifest(
                manifest=manifest,
                url_column=self.config.manifest.url_column,
                suffix=self.config.data_files.suffix or "nci",
                status_column=self.config.manifest.status_column,
                status_map=self.config.status_map,
                logger=self.logger,
            )
            self.logger.info("Manifest enriched successfully")
        except Exception as err:
            self.logger.error("Failed to enrich manifest: %s", err)
            raise err
        return response

    def tag_objects(self, enriched_manifest: List[Dict[str, Any]]):
        """
        This method iterates over a list of records (`enriched_manifest`) and tags the
        objects specified in each record with indicators of release and registration
        status. The tags applied are `"kf_registered"` and `"kf_released"` with their
        respective values from the record in the enriched manifest.

        The enriched manifest is expected to contain the following keys:
        - `nci_bucket` (str): The S3 bucket where the object is stored.
        - `nci_key` (str): The key of the object in the S3 bucket.
        - `kf_registered` (bool): A boolean indicating if the object is registered.
        - `kf_released` (bool): A boolean indicating if the object is released.

        The method returns a list of dictionaries similar to the input `enriched_manifest`,
        with an additional key `tagged` indicating whether the tagging operation was
        successful for each record.

        Args:
            enriched_manifest (List[Dict[str, Any]]): A list of dictionaries representing
                the enriched manifest with S3 object information and registration/release
                status.
            client (S3Client): The S3 client instance used to interact with AWS S3.

        Returns:
            response (List[Dict[str, Any]]): A list of dictionaries representing the enriched
            manifest with an additional key `tagged` indicating the success of the tagging
            operation.

        """

        response = tag_objects(
            enriched_manifest=enriched_manifest,
            client=self.client,
        )
        return response

    def upload_enriched_manifest(
        self, manifest: List[Dict[str, Any]]
    ) -> PutObjectOutputTypeDef:
        """
        Uploads the tagging report to S3.

        Args:
            report (List[Dict[str, Any]]): The tagging report to be uploaded.

        Returns:
            response (PutObjectOutputTypeDef): The response from the S3 upload operation.

        Raises:
            ClientError: If there is an error during the upload process.
        """
        self.logger.info("Uploading enriched manifest to S3")
        try:
            response = upload_object(
                data=manifest,
                ftype=FileType.ENRICHED_MANIFEST,
                config=self.config,
                client=self.client,
            )
            self.logger.info("Enriched manifest uploaded successfully")
            return response
        except ClientError as err:
            self.logger.error("Failed to upload report: %s", err)
            raise err

    def upload_report(self, report: List[Dict[str, Any]]) -> PutObjectOutputTypeDef:
        """
        Uploads the tagging report to S3. The final destination of the report
        is determined by the configuration settings. The report is uploaded
        to the same S3 Bucket and top-level directory where the manifest was sourced
        from, but in a subdirectory named "reports". The report is saved
        as a CSV with a filename that includes the current date and time, ensuring
        uniqueness and traceability.

        Args:
            report (List[Dict[str, Any]]): The tagging report to be uploaded.

        Returns:
            response (PutObjectOutputTypeDef): The response from the S3 upload operation.

        Raises:
            ClientError: If there is an error during the upload process.
        """
        self.logger.info("Uploading final report to S3")
        try:
            response = upload_object(
                data=report,
                ftype=FileType.REPORT,
                config=self.config,
                client=self.client,
            )
            self.logger.info("Report uploaded successfully")
            return response
        except ClientError as err:
            self.logger.error("Failed to upload report: %s", err)
            raise err

    def get_object_tag_inventory(
        self, bucket: str, prefix: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Generates an inventory of all objects in the S3 bucket with their tags.
        The inventory is generated by calling the `inventory_object_tags` function,
        which retrieves the tags for each object in the specified bucket and prefix.

        Args:
            bucket (str): The name of the S3 bucket to generate the inventory for.
            prefix (Optional[str]): The prefix (or path) within the S3 bucket to filter
                the objects. If not specified, all objects in the bucket will be included.

        Returns:
            response (List[Dict[str, Any]]): A list of dictionaries representing the
                inventory of objects and their tags.

        Raises:
            ClientError: If there is an error generating the inventory or retrieving
                the object tags.
        """

        self.logger.info("Generating object tag inventory")
        try:
            response = inventory_object_tags(
                client=self.client,
                bucket=bucket,
                prefix=prefix,
            )
            self.logger.info("Object tag inventory generated successfully")
        except ClientError as err:
            self.logger.error("Failed to generate object tag inventory: %s", err)
            raise err
        return response

    def upload_inventory(
        self, inventory: List[Dict[str, Any]]
    ) -> PutObjectOutputTypeDef:
        """
        Uploads an inventory of all objects in the S3 bucket to S3 in a CSV format.
        The inventory is saved in the same S3 Bucket and top-level directory where the
        manifest was sourced from, but in a subdirectory named "inventory". The inventory
        is saved as a CSV with a filename that includes the current date and time,
        ensuring uniqueness and traceability.

        Args:
            inventory (List[Dict[str, Any]]): The inventory to be uploaded.

        Returns:
            response (PutObjectOutputTypeDef): The response from the S3 upload operation.

        Raises:
            ClientError: If there is an error during the upload process.

        """
        self.logger.info("Uploading object tag inventory to S3")
        try:
            response = upload_object(
                data=inventory,
                ftype=FileType.INVENTORY,
                config=self.config,
                client=self.client,
            )
            self.logger.info("Inventory uploaded successfully")
            return response
        except ClientError as err:
            self.logger.error("Failed to upload inventory: %s", err)
            raise err
