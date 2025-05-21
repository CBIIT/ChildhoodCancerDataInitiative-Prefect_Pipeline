"""
This module provides utility functions for processing and enriching manifest records.
It includes functions for parsing S3 URLs, determining object statuses, and enriching
manifests with additional metadata.

Functions:
    parse_s3_url(record: Dict[str, Any], url_column: str, logger: logging.Logger) -> Dict[str, Any]:
        Parses an S3 URL from a specified column in a record, extracts the bucket and key,
        and appends metadata indicating whether the URL is valid.

    parse_object_status(
        Parses the registration status from the manifest and modifies the input record
        by adding registered/released flags based on a provided status map.

    enrich_manifest(
        Processes a list of manifest records, enriching each record by parsing its S3 URL
        and determining its status based on a provided mapping.
"""

import logging
from typing import Any, Dict, List

from kf_prefect.src.utils.models import ConfigStatusMap


def parse_s3_url(
    record: Dict[str, Any], url_column: str, suffix: str, logger: logging.Logger
) -> Dict[str, Any]:
    """
    Parses an S3 URL from a specified column in a record and extracts the bucket and key.
    This function validates the S3 URL format, extracts the bucket and key, and appends
    them to the record with additional metadata indicating whether the URL is valid.
    If the URL is malformed or missing, the corresponding fields are set to `None` and
    a warning is logged.
    Args:
        record (Dict[str, Any]): The input record containing the S3 URL.
        url_column (str): The key in the record that contains the S3 URL.
        suffix (str): The suffix to append to the bucket name.
        logger (logging.Logger): Logger instance for logging warnings.
    Returns:
        Dict[str, Any]: The updated record with the following additional fields:
            - "nci_bucket" (str or None): The extracted bucket name with the suffix
                specified in the config, or `None` if the URL is invalid.
            - "nci_key" (str or None): The extracted key from the S3 URL, or `None` if
              the URL is invalid.
            - "valid_url" (bool): `True` if the URL is valid, otherwise `False`.
    Raises:
        None: This function handles all exceptions internally and logs warnings for
        malformed URLs.
    """

    if not record[url_column]:
        record["nci_bucket"] = None
        record["nci_key"] = None
        record["valid_url"] = False
        return record

    if not record[url_column].startswith("s3://"):
        record["nci_bucket"] = None
        record["nci_key"] = None
        record["valid_url"] = False

    try:
        url = record[url_column].split("s3://")[1]
        bucket, key = url.split("/", 1)
        record["nci_bucket"] = f"{bucket}-{suffix}"
        record["nci_key"] = key
        record["valid_url"] = True

    except (IndexError, ValueError) as e:
        record["nci_bucket"] = None
        record["nci_key"] = None
        record["valid_url"] = False
        logger.warning("Malformed S3 URL: %s", record[url_column])
        logger.warning("Error: %s", e)

    return record


def parse_object_status(
    record: Dict[str, Any],
    status_key: str,
    status_map: List[Dict[str, ConfigStatusMap]],
    logger: logging.Logger,
) -> Dict[str, Any]:
    """
    Parses the registration status from the manifest and adds registered/released flags to the
    Parses the registration status from the manifest and modifies the input `record` in place by
    adding registered/released flags to the record. The function expects the status to be one
    of the keys in the status_map dictionary, which maps status strings to tuples of
    (registered, released). If the status is not found in the map, it sets both flags to None
    and logs a warning.
    if not record[status_key]:
        logger.warning("Empty registration status found in record: %s", record)
        record["kf_registered"] = None
        record["kf_released"] = None
        return record
    """

    #  example status_map: [{'registered and released': {'kf_registered': True, 'kf_released': True}}, {'registered and not released': {'kf_registered': True, 'kf_released': False}}, {'not registered and not released': {'kf_registered': False, 'kf_released': False}}, {'not registered and not releeased': {'kf_registered': False, 'kf_released': False}}, {'not registered and not releeased': {'kf_registered': False, 'kf_released': False}}]
    #  example status map entry: {'registered and released': {'kf_registered': True, 'kf_released': True}}

    status = record.get(status_key)
    status_keys = [key for entry in status_map for key in entry.keys()]

    if not status:
        logger.warning("Column %s was not found or is empty", status_key)
        record["valid_status"] = False
        record["kf_registered"] = None
        record["kf_released"] = None

    if status not in status_keys:
        logger.warning("Status %s not found in status map", status)
        record["kf_registered"] = None
        record["kf_released"] = None
        record["valid_status"] = False

    record["valid_status"] = True
    for entry in status_map:
        if status in entry:
            config_status = entry[status]
            record["kf_registered"] = config_status.kf_registered
            record["kf_released"] = config_status.kf_released
            break
    return record


def enrich_manifest(
    manifest: List[Dict[str, Any]],
    url_column: str,
    suffix: str,
    status_column: str,
    status_map: List[Dict[str, ConfigStatusMap]],
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    """
    Enriches a manifest by parsing S3 URLs and object statuses.
    This function processes a list of manifest records, enriching each record by
    parsing its S3 URL and determining its status based on a provided mapping.
    The enriched records are returned as a new list.
    Args:
        manifest (List[Dict[str, Any]]): A list of dictionaries representing the manifest records.
        url_column (str): The key in the manifest dictionaries that contains the S3 URL.
        suffix (str): The suffix to append to the bucket name.
        status_column (str): The key in the manifest dictionaries that contains the status
            information.
        status_map (List[Dict[str, ConfigStatusMap]]): A mapping of status values to their
            corresponding configurations.
        logger (logging.Logger): A logger instance for logging messages during processing.
    Returns:
        enriched_manifest (List[Dict[str, Any]]): A list of enriched manifest records.
    """
    enriched_manifest: List[Dict[str, Any]] = []
    for rec in manifest:
        record = parse_s3_url(rec, url_column, suffix, logger)
        enriched = parse_object_status(record, status_column, status_map, logger)
        enriched_manifest.append(enriched)
    return enriched_manifest
