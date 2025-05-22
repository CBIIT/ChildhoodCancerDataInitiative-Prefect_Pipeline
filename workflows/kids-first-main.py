import csv
from io import StringIO
from typing import Any, Dict, List

import boto3
from botocore.exceptions import ClientError
from prefect import flow, get_run_logger, task
from pydantic import BaseModel, Field

logger = get_run_logger()
session = boto3.Session()
client = session.client("s3")


class Config(BaseModel):
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

    nci_bucket_suffix: str = Field(
        title="NCI Data Bucket Suffix",
        description="The suffix of the S3 bucket where CHOP data was transferred to",
        default="nci",
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

@flow
def load_manifest(s3_client: Any, bucket: str, key: str) -> List[Dict[str, Any]]:
    try:
        result = s3_client.get_object(Bucket=bucket, Key=key)
        manifest = result["Body"].read().decode("utf-8").splitlines()
        reader = csv.DictReader(manifest)
        return [row for row in reader]
    except ClientError as err:
        raise err

@flow
def parse_manifest_url(
    manifest: List[Dict[str, Any]], manifest_url_column: str
) -> List[Dict[str, Any]]:
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

@flow
def validate_manifest_bucket_name(
    manifest: List[Dict[str, Any]], nci_data_bucket: str, nci_data_bucket_suffix: str
) -> List[Dict[str, Any]]:
    response: List[Dict[str, Any]] = []
    for row in manifest:
        if f'{row["chop_bucket"]}-{nci_data_bucket_suffix}' == nci_data_bucket:
            row["manifest_bucket_matches_expected"] = True
        else:
            row["manifest_bucket_matches_expected"] = False
        response.append(row)
    return response

@flow
def parse_object_status(
    manifest: List[Dict[str, Any]],
    manifest_status_column: str,
    status_map: List[Dict[str, Dict[str, bool]]],
) -> List[Dict[str, Any]]:
    response: List[Dict[str, Any]] = []
    for row in manifest:
        if not row[manifest_status_column]:
            row["kf_registered"] = None
            row["kf_released"] = None
            row["kf_status_valid"] = False
            row["invalid_status_reason"] = "No status column detected"

        if row[manifest_status_column] not in status_map:
            row["kf_registered"] = None
            row["kf_released"] = None
            row["kf_status_valid"] = False
            row["invalid_status_reason"] = "Invalid status value"

        else:
            for status in status_map:
                if row[manifest_status_column] in status:
                    row["kf_registered"] = status[row[manifest_status_column]][
                        "kf_registered"
                    ]
                    row["kf_released"] = status[row[manifest_status_column]][
                        "kf_released"
                    ]
                    row["kf_status_valid"] = True
                    row["invalid_status_reason"] = None
        response.append(row)
    return response

@flow
def upload_object(
    s3_client: Any, bucket: str, key: str, manifest: List[Dict[str, Any]]
) -> bool:
    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=manifest[0].keys())
    writer.writeheader()
    writer.writerows(manifest)
    response = s3_client.put_object(
        Bucket=bucket, Key=key, Body=buffer.getvalue(), ContentType="text/csv"
    )
    if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        return True
    else:
        return False

@flow
def tag_objects(
    s3_client: Any, manifest: List[Dict[str, Any]], nci_data_bucket: str
) -> List[Dict[str, Any]]:
    response: List[Dict[str, Any]] = []
    for row in manifest:
        cond1 = row["valid_url"]
        cond2 = row["manifest_bucket_matches_expected"]
        cond3 = row["kf_status_valid"]

        if cond1 and cond2 and cond3:
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
        else:
            row["tagged"] = False

        response.append(row)
    return response


@flow(name="Kids First Object Tagger")
def kf_main_runner(config: Config):

    logger.info("Starting Kids First Object Tagger flow")
    logger.info("Loading manifest from S3")
    manifest1 = load_manifest(client, config.manifest_bucket, config.manifest_key)

    logger.info("Parsing manifest URL column")
    manifest2 = parse_manifest_url(manifest1, config.manifest_url_column)

    logger.info("Validating bucket name parsed from the URL column")
    manifest3 = validate_manifest_bucket_name(
        manifest2, config.nci_bucket, config.nci_bucket_suffix
    )

    logger.info("Parsing object status column")
    manifest4 = parse_object_status(
        manifest3, config.manifest_status_column, config.status_map
    )

    logger.info("Uploading manifest to S3")
    upload_enriched_manifest = upload_object(
        client,
        config.manifest_bucket,
        config.manifest_key.replace("/input/", "/enriched_manifest/"),
        manifest4,
    )

    if upload_enriched_manifest:
        logger.info("Manifest uploaded successfully")
    else:
        logger.error("Manifest upload failed")

    logger.info("Tagging objects in S3")
    tagged_objects = tag_objects(client, manifest4, config.nci_bucket)

    logger.info("Upload Tagging Report to S3")
    upload_tagging_report = upload_object(
        client,
        config.manifest_bucket,
        config.manifest_key.replace("/input/", "/tagging_report/"),
        tagged_objects,
    )

    if upload_tagging_report:
        logger.info("Tagging report uploaded successfully")
    else:
        logger.error("Tagging report upload failed")

    logger.info("Kids First Object Tagger flow completed")
    return tagged_objects
