import csv
import datetime
from io import StringIO
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from pydantic import BaseModel, Field

session = boto3.Session()
client = session.client("s3")


class Config(BaseModel):
    """Custom type for the configuration of the workflow."""

    manifest_bucket: str = Field(
        title="Manifest Bucket Name",
        description="The name of the S3 bucket where the manifest is stored",
        default="ccdi-kidsfirst-transfer-manifests",
    )

    manifest_key: str = Field(
        title="Manifest Key",
        description="The S3 key for the manifest file",
    )

    manifest_url_column: str = Field(
        title="Manifest URL Column",
        description="The name of the column in the manifest that contains the S3 URLs",
        default="urls",
    )

    manifest_status_column: str = Field(
        title="Manifest Status Column",
        description="The name of the column in the manifest that contains the object status",
        default="registration_status",
    )

    nci_bucket: str = Field(
        title="NCI Data Bucket Name",
        description="The name of the S3 Bucket where CHOP data was transferred to",
    )

    nci_bucket_suffix: Optional[str] = Field(
        title="NCI Data Bucket Suffix",
        description="The suffix of the S3 bucket where CHOP data was transferred to",
        default=None,
    )

    status_map: List[Dict[str, Dict[str, bool]]] = Field(
        title="Release and Registration Status Map Lookup",
        description="Mapping of status values to Kids First registered and released status",
        default=[
            {"registered and released": {"kf_registered": True, "kf_released": True}},
            {"released and registered": {"kf_registered": True, "kf_released": True}},
            {
                "registered and not released": {
                    "kf_registered": True,
                    "kf_released": False,
                }
            },
            {
                "not registered and not released": {
                    "kf_registered": False,
                    "kf_released": False,
                }
            },
            {
                "not registered and released": {
                    "kf_registered": False,
                    "kf_released": True,
                }
            },
            {
                "released and not registered": {
                    "kf_registered": False,
                    "kf_released": True,
                }
            },
            {
                "not registered and not releeased": {
                    "kf_registered": False,
                    "kf_released": False,
                }
            },
        ],
    )


@task(name="Load Manifest from S3", cache_policy=NO_CACHE)
def load_manifest(s3_client: Any, bucket: str, key: str) -> List[Dict[str, Any]]:
    """Load the manifest from S3 and return it as a list of dictionaries."""
    try:
        result = s3_client.get_object(Bucket=bucket, Key=key)
        manifest = result["Body"].read().decode("utf-8").splitlines()
        reader = csv.DictReader(manifest)
        return [row for row in reader]
    except ClientError as err:
        raise err


@task(name="Parse Manifest URL Column")
def parse_manifest_url(
    manifest: List[Dict[str, Any]], manifest_url_column: str
) -> List[Dict[str, Any]]:
    """Parse the manifest URL column to extract bucket and key information."""
    response: List[Dict[str, Any]] = []

    for row in manifest:
        if not row[manifest_url_column]:
            row["chop_bucket"] = None
            row["chop_key"] = None
            row["valid_url"] = False
            row["invalid_url_reason"] = "No file URL detected"
            response.append(row)

        if not row[manifest_url_column].startswith("s3://"):
            row["chop_bucket"] = None
            row["chop_key"] = None
            row["valid_url"] = False
            row["invalid_url_reason"] = "File URL does not start with s3://"
            response.append(row)

        chop_url = row[manifest_url_column].split("s3://")[1]
        chop_bucket, chop_key = chop_url.split("/", 1)

        if not chop_bucket or not chop_key:
            row["chop_bucket"] = None
            row["chop_key"] = None
            row["valid_url"] = False
            row["invalid_url_reason"] = "Malformed URL"
            response.append(row)
        else:
            row["chop_bucket"] = chop_bucket
            row["chop_key"] = chop_key
            row["valid_url"] = True
            row["invalid_url_reason"] = None
            response.append(row)
    return response


@task(name="Validate Manifest Bucket Name")
def validate_manifest_bucket_name(
    manifest: List[Dict[str, Any]], nci_data_bucket: str, nci_data_bucket_suffix: Optional[str]
) -> List[Dict[str, Any]]:
    """Validate the bucket name parsed from the URL column matches the expected NCI data bucket."""
    response: List[Dict[str, Any]] = []

    if not nci_data_bucket_suffix:
        for row in manifest: 
            if row["chop_bucket"] == nci_data_bucket:
                row["manifest_bucket_matches_expected"] = True
            else:
                row["manifest_bucket_matches_expected"] = False
            response.append(row)
        return response
    else:
        for row in manifest:
            if f'{row["chop_bucket"]}-{nci_data_bucket_suffix}' == nci_data_bucket:
                row["manifest_bucket_matches_expected"] = True
            else:
                row["manifest_bucket_matches_expected"] = False
            response.append(row)
        return response


@task(name="Parse Object Status")
def parse_object_status(
    manifest: List[Dict[str, Any]],
    manifest_status_column: str,
    status_map: List[Dict[str, Dict[str, bool]]],
) -> List[Dict[str, Any]]:
    """Parse the manifest status column to determine Kids First registration and release status."""
    logger = get_run_logger()
    response: List[Dict[str, Any]] = []

    for row in manifest:
        row["kf_registered"] = None
        row["kf_released"] = None
        row["kf_status_valid"] = False
        row["invalid_status_reason"] = "Status value does not match config status map"

        if not row[manifest_status_column]:
            logger.warning("No status column detected for object %s, ", row["chop_key"])
            row["invalid_status_reason"] = "No status column detected"

        for status in status_map:
            if row[manifest_status_column] in status:
                row["kf_registered"] = status[row[manifest_status_column]][
                    "kf_registered"
                ]
                row["kf_released"] = status[row[manifest_status_column]]["kf_released"]
                row["kf_status_valid"] = True
                row["invalid_status_reason"] = None
                break
        response.append(row)
    return response


@task(name="Upload Object", cache_policy=NO_CACHE)
def upload_object(
    s3_client: Any, bucket: str, key: str, manifest: List[Dict[str, Any]]
) -> None:
    """Upload an object to S3 as a CSV file."""
    logger = get_run_logger()
    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=manifest[0].keys())
    writer.writeheader()
    writer.writerows(manifest)
    response = s3_client.put_object(
        Bucket=bucket, Key=key, Body=buffer.getvalue(), ContentType="text/csv"
    )
    if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        logger.info("... Object uploaded successfully to %s/%s", bucket, key)
    else:
        logger.warning(
            "... Failed to upload object to %s/%s with status code %s",
            bucket,
            key,
            response["ResponseMetadata"]["HTTPStatusCode"],
        )


@task(name="Tag Objects", cache_policy=NO_CACHE)
def tag_objects(
    s3_client: Any, manifest: List[Dict[str, Any]], nci_data_bucket: str
) -> List[Dict[str, Any]]:
    """Tag objects in the NCI data bucket based on the manifest."""
    logger = get_run_logger()

    response: List[Dict[str, Any]] = []
    for row in manifest:
        cond1 = row["valid_url"]
        cond2 = row["manifest_bucket_matches_expected"]
        cond3 = row["kf_status_valid"]

        if not cond1:
            logger.warning("Skipping object %s due to invalid URL", row["chop_key"])
            row["tagged"] = False
            response.append(row)
            continue

        if not cond2:
            logger.warning(
                "Skipping object %s due to mismatched bucket name", row["chop_key"]
            )
            row["tagged"] = False
            response.append(row)
            continue

        if not cond3:
            logger.warning(
                "Skipping object %s due to invalid Kids First status", row["chop_key"]
            )
            row["tagged"] = False
            response.append(row)
            continue

        try:
            result = s3_client.put_object_tagging(
                Bucket=nci_data_bucket,
                Key=row["chop_key"],
                Tagging={
                    "TagSet": [
                        {
                            "Key": "kf_registered",
                            "Value": str(row["kf_registered"]),
                        },
                        {"Key": "kf_released", "Value": str(row["kf_released"])},
                    ]
                },
            )
            if result["ResponseMetadata"]["HTTPStatusCode"] == 200:
                row["tagged"] = True
            else:
                row["tagged"] = False
        except ClientError as e:
            logger.error("Error tagging object %s: %s", row["chop_key"], e)
            row["tagged"] = False

        response.append(row)

    return response


@flow(name="Kids First Object Tagger")
def kf_main_runner(config: Config):
    """Main workflow to tag Kids First objects in the NCI data bucket."""
    logger = get_run_logger()

    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    logger.info("*** STARTING KIDS FIRST OBJECT TAGGER WORKFLOW ***")

    logger.info("*** LOADING MANIFEST FROM S3 ***")
    manifest1 = load_manifest(client, config.manifest_bucket, config.manifest_key)

    logger.info("*** PARSING MANIFEST URL COLUMN ***")
    manifest2 = parse_manifest_url(manifest1, config.manifest_url_column)

    logger.info("*** VALIDATING BUCKET NAME PARSED FROM THE URL COLUMN ***")
    manifest3 = validate_manifest_bucket_name(
        manifest2, config.nci_bucket, config.nci_bucket_suffix
    )

    logger.info("*** PARSING COLUMN TO DETERMINE REGISTRATION/RELEASE STATUS ***")
    manifest4 = parse_object_status(
        manifest3, config.manifest_status_column, config.status_map
    )

    logger.info("*** UPLOADING ENRICHED MANIFEST TO S3 ***")
    upload_object(
        client,
        config.manifest_bucket,
        config.manifest_key.replace("/input/", f"/enriched_manifest/{timestamp}/"),
        manifest4,
    )

    logger.info("*** TAGGING OBJECTS IN NCI DATA BUCKET ***")
    tagged_objects = tag_objects(client, manifest4, config.nci_bucket)

    logger.info("*** UPLOADING TAGGING REPORT TO S3 ***")
    upload_object(
        client,
        config.manifest_bucket,
        config.manifest_key.replace("/input/", f"/tagging_report/{timestamp}/"),
        tagged_objects,
    )

    logger.info("*** KIDS FIRST OBJECT TAGGER WORKFLOW COMPLETED ***")
    return None
