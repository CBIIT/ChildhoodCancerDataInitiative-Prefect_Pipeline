"""
A Python program that support object tagging for the Kids First project.
Leverages the KidsFirstObjectTagger class to obtain and enrich a manifest file
with descriptive metadata and facilitate object tagging based on the enriched
manifest content.
"""

from typing import Optional
import boto3
from src.kf_object_tagger import KFObjectTagger
from src.utils.config import source_config_s3
from prefect import flow, task, get_run_logger


def run_tagging_process(kf_object_tagger: KFObjectTagger):
    """
    A function to run the tagging process using the KFObjectTagger class, which
    facilitates the following steps:

    1. Retrieve the input manifest from S3.
    2. Enrich the manifest with metadata used for tagging.
    3. Uploads the enriched manifest to S3 for optional review.
    4. Tags data file objects in the S3 bucket based on the enriched manifest.
    5. Generates a tag report and uploads it to S3 for optional review.

    The manifest, the enriched manifest, and the tag report are all expected to be
    in CSV format.
    Args:
        kf_object_tagger (KFObjectTagger): An instance of the KFObjectTagger class
            that handles the object tagging process.

    Returns:
        None: This function does not return any value. It performs the tagging process
        and uploads the enriched manifest and tag report to S3.
    """

    raw = kf_object_tagger.get_input_manifest()

    enriched = kf_object_tagger.get_enriched_manifest(raw)
    enriched_upload = kf_object_tagger.upload_enriched_manifest(enriched)
    if enriched_upload["ResponseMetadata"]["HTTPStatusCode"] == 200:
        print("Enriched manifest is now available.")

    tag_report = kf_object_tagger.tag_objects(enriched)
    tag_report_upload = kf_object_tagger.upload_report(tag_report)
    if tag_report_upload["ResponseMetadata"]["HTTPStatusCode"] == 200:
        print("Tag report is now available.")

    return None


def run_tag_inventory_process(
    kf_object_tagger: KFObjectTagger, bucket: str, prefix: Optional[str] = None
) -> None:
    """
    A function to run the inventory process using the KFObjectTagger class, which
    will list all objects in the specified S3 Bucket. If a prefix is provided,
    it will only list objects that start with that prefix. The function will then
    upload a CSV inventory report that itemizes the objects in the bucket and the
    values of the tags that have the `kf_released` and `kf_registered` keys.

    The purpose of this report is to provide insights into objects that may be present
    in the S3 Bucket but were not specified in the input manifest. It may also be used
    to identify objects that did not receive expected tag values due to data quality
    issues in the input manifest.

    Args:
        kf_object_tagger (KFObjectTagger): An instance of the KFObjectTagger class
            that handles the object tagging process.
        bucket (str): The name of the S3 Bucket to list objects from.
        prefix (Optional[str]): An optional prefix to filter the objects in the S3 Bucket.

    Returns:
        None: This function does not return any value. It performs the inventory process
        and uploads the inventory report to S3.

    """
    inventory = kf_object_tagger.get_object_tag_inventory(bucket, prefix)
    inventory_upload = kf_object_tagger.upload_inventory(inventory)
    if inventory_upload["ResponseMetadata"]["HTTPStatusCode"] == 200:
        print("Inventory is now available.")

    return None

@flow(
    name="Kids First Object Tagger",
    log_prints=True,
    flow_run_name="Kids First Object Tagger+{config_file}",
)
def main(
    config_bucket: str, config_key: str, bucket: Optional[str] = None, prefix: Optional[str] = None
) -> None:
    """
    Main function.
    """

    session = boto3.Session()
    client = session.client("s3")

    config = source_config_s3(bucket= config_bucket, key = config_key, client=client)
    kf = KFObjectTagger(config)

    run_tagging_process(kf)

    if bucket:
        run_tag_inventory_process(kf, config_bucket, prefix)

    return None
