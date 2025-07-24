import os
import io
import datetime
from pytz import timezone
import boto3
import pandas as pd
from prefect import task, flow, get_run_logger
from botocore.exceptions import ClientError

client = boto3.client("s3")


DATESTAMP = datetime.datetime.now(timezone("EST")).strftime("%Y%m%d_%H%M%S")
OUTPUT_DIR = f"file_compare_{DATESTAMP}"


def get_separator(file_path: str) -> str:
    """Determine the separator based on the file extension."""
    if file_path.lower().endswith(".csv"):
        return ","
    elif file_path.lower().endswith(".tsv"):
        return "\t"
    else:
        raise ValueError("File must end with .csv or .tsv")


@task
def get_object(bucket: str, key: str) -> pd.DataFrame:
    """Loads and returns a DataFrame from a specified S3 Object (bucket, key)"""
    separator = get_separator(key)

    try:
        response = client.get_object(Bucket=bucket, Key=key)
        body = response["Body"].read().decode("utf-8")
        df = pd.read_csv(io.StringIO(body), sep=separator)
        return df
    except ClientError as e:
        raise Exception(f"Failed to download DataFrame from S3: {e}")


@task
def compare_dataframes(
    df1: pd.DataFrame, df2: pd.DataFrame, join_column1: str, join_column2: str
):
    """Compare two DataFrames based on specified join columns."""
    # Perform the merge to find common rows
    merged_df = pd.merge(
        df1,
        df2,
        left_on=join_column1,
        right_on=join_column2,
        how="outer",
        indicator=True,
    )
    both_present = merged_df[merged_df["_merge"] == "both"]
    only_in_df1 = merged_df[merged_df["_merge"] == "left_only"]
    only_in_df2 = merged_df[merged_df["_merge"] == "right_only"]

    return both_present, only_in_df1, only_in_df2


@task
def upload_dataframe(df: pd.DataFrame, bucket: str, key: str) -> str:
    """Uploads a provided DataFrame to an S3 bucket as a TSV file."""
    buffer = df.to_csv(sep="\t", index=False)
    try:
        response = client.put_object(
            Bucket=bucket, Key=key, Body=buffer.encode("utf-8")
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            return f"s3://{bucket}/{key}"
    except ClientError as e:
        raise Exception(f"Failed to upload DataFrame to S3: {e}")


@flow(
    name="S3 DataFrame Comparison Flow",
    log_prints=True,
    flow_run_name=f"{runner}_{DATESTAMP}",
)
def compare_dataframes_runner(
    bucket: str,
    file_path_1: str,
    file_path_2: str,
    runner: str,
    join_column_1: str,
    join_column_2: str,
):
    """Runner function to compare two dataframes from files in a specified S3 bucket."""

    logger = get_run_logger()
    logger.info(f"Retrieving file {file_path_1} from the {bucket} bucket.")
    df1 = get_object(bucket, file_path_1)

    logger.info(f"Retrieving file {file_path_2} from the {bucket} bucket.")
    df2 = get_object(bucket, file_path_2)

    logger.info("Comparing dataframes...")
    both_present, only_in_df1, only_in_df2 = compare_dataframes(
        df1, df2, join_column_1, join_column_2
    )

    logger.info("Uploading comparison results to S3...")
    base_path = f"{runner}/{OUTPUT_DIR}"
    both_present_path = f"{base_path}/both_present_{DATESTAMP}.tsv"
    only_in_df1_path = f"{base_path}/only_in_df1_{DATESTAMP}.tsv"
    only_in_df2_path = f"{base_path}/only_in_df2_{DATESTAMP}.tsv"

    upload_both_present = upload_dataframe(both_present, bucket, both_present_path)
    logger.info(f"Both present DataFrame uploaded to: {upload_both_present}")

    upload_only_in_df1 = upload_dataframe(only_in_df1, bucket, only_in_df1_path)
    logger.info(f"Only in DF1 DataFrame uploaded to: {upload_only_in_df1}")

    upload_only_in_df2 = upload_dataframe(only_in_df2, bucket, only_in_df2_path)
    logger.info(f"Only in DF2 DataFrame uploaded to: {upload_only_in_df2}")

    return {
        "both_present": upload_both_present,
        "only_in_df1": upload_only_in_df1,
        "only_in_df2": upload_only_in_df2,
    }
