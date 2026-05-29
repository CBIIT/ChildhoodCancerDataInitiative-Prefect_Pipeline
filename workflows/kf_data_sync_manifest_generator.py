import pandas as pd
from pathlib import Path
from src.utils import file_dl, folder_ul, file_ul, get_time
from prefect import flow, get_run_logger
import os
import sys


def split_s3(url: str):
    """
    Splits s3://bucket/path/to/file into:
    ('s3://bucket', 'path/to/file')
    """
    url = url.replace("s3://", "", 1)
    parts = url.split("/", 1)

    bucket = f"s3://{parts[0]}"
    path = parts[1] if len(parts) > 1 else ""

    return bucket, path


def process_file(input_tsv: str, output_dir: str):
    logger = get_run_logger()
    
    # Read TSV (no headers)
    logger.info(f"Reading input TSV file: {input_tsv}")
    df = pd.read_csv(input_tsv, sep="\t", header=None)
    df.columns = ["source", "dest"]

    # Split columns
    df[["source_bucket", "source_path"]] = df["source"].apply(
        lambda x: pd.Series(split_s3(x))
    )

    df[["dest_bucket", "dest_path"]] = df["dest"].apply(
        lambda x: pd.Series(split_s3(x))
    )

    # Group by source/dest bucket combinations
    grouped = df.groupby(["source_bucket", "dest_bucket"])

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    for (src_bucket, dst_bucket), group in grouped:
        logger.info(f"Processing group: {src_bucket} to {dst_bucket}")
        # Clean bucket names for filename
        src_name = src_bucket.replace("s3://", "")
        dst_name = dst_bucket.replace("s3://", "")


        output_file = Path(output_dir) / f"{src_name}_TRANSFER_{dst_name}.csv"

        # Keep only source_path
        out_df = group[["source_path"]]

        out_df.to_csv(output_file, index=False, header=False)

        logger.info(f"Wrote: {output_file} ({len(out_df)} rows)")


@flow(
    name="KF Data Sync Manifest Generator",
    log_prints=True,
    flow_run_name="kf-data-sync-manifest-{runner}-" + f"{get_time()}",
)
def kf_data_sync_manifest_generator(bucket: str, file_path: str, runner: str, kf_data_sync_bucket: str) -> None:
    """Pipeline that takes a KF File Manifest and generates new manifests for syncing files from source buckets to destination buckets based on the file paths in the original manifest. The output manifests are grouped by source and destination bucket combinations.

    Args:
        bucket (str): Bucket name of where the manifest located at and output goes to
        file_path (str): File path of KF Data Sync manifest, two column tsv with no header, source and destination paths
        runner (str): Unique runner name
        kf_data_sync_bucket (str): Bucket name of where the manifest located at and output goes to
    """
    logger = get_run_logger()

    # Download the manifest file from S3
    logger.info(f"Downloading manifest from s3://{bucket}/{file_path}")
    file_dl(bucket, file_path)
    logger.info(f"Downloaded manifest from s3://{bucket}/{file_path}")

    logger.info(f"Processing manifest file: {file_path}")
    output_dir = f"outputs_{get_time()}"
    file_name = os.path.basename(file_path)
    process_file(input_tsv=file_name, output_dir=output_dir)

    logger.info(f"Uploading generated manifests to s3://{bucket}/{output_dir}/")
    folder_ul(
        local_folder=output_dir,
        bucket=bucket,
        destination=runner + "/",
        sub_folder="",
    )

    # For each generated manifest in the output directory, upload to S3 for the kf_data_sync_bucket
    for manifest_file in os.listdir(output_dir):
        if manifest_file.endswith(".csv"):
            local_path = os.path.join(output_dir, manifest_file)
            file_ul(local_path, kf_data_sync_bucket, ".")
            logger.info(f"Uploaded {local_path} to s3://{kf_data_sync_bucket}/.")