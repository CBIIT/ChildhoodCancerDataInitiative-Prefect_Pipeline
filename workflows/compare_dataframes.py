import pandas as pd
from prefect import task, flow, get_run_logger
from datetime import datetime
import os
from src.utils import get_time, file_dl, folder_ul

date_stamp = get_time()

output_dir = f"file_compare_{date_stamp}"

if not os.path.exists(output_dir):
    os.makedirs(output_dir)


@task
def read_dataframe(file_path: str) -> pd.DataFrame:
    """Read a TSV or CSV file into a pandas DataFrame."""
    if file_path.endswith(".tsv"):
        return pd.read_csv(file_path, sep="\t")
    elif file_path.endswith(".csv"):
        return pd.read_csv(file_path)
    else:
        raise ValueError("File must be a TSV or CSV format.")


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

    # Rows present in both DataFrames
    both_present = merged_df[merged_df["_merge"] == "both"]

    # Rows only in df1
    only_in_df1 = merged_df[merged_df["_merge"] == "left_only"]

    # Rows only in df2
    only_in_df2 = merged_df[merged_df["_merge"] == "right_only"]

    return both_present, only_in_df1, only_in_df2


@task
def save_dataframes(
    both_present: pd.DataFrame, only_in_df1: pd.DataFrame, only_in_df2: pd.DataFrame
) -> None:
    """Save the comparison results to TSV files."""

    both_present_file = f"both_present_{date_stamp}.tsv"
    only_in_df1_file = f"only_in_df1_{date_stamp}.tsv"
    only_in_df2_file = f"only_in_df2_{date_stamp}.tsv"

    both_present.to_csv(
        os.path.join(output_dir, both_present_file), sep="\t", index=False
    )
    only_in_df1.to_csv(
        os.path.join(output_dir, only_in_df1_file), sep="\t", index=False
    )
    only_in_df2.to_csv(
        os.path.join(output_dir, only_in_df2_file), sep="\t", index=False
    )


@flow(
    name="S3 DataFrame Comparison Flow",
    log_prints=True,
    flow_run_name="{runner}_" + f"{get_time()}",
)
def compare_dataframes_runner(
    bucket: str,
    file_path_1: str,
    file_path_2: str,
    runner: str,
    join_column_1: str,
    join_column_2: str,
):
    """Flow to compare two dataframes from files in a specified S3 bucket.

    Args:
        bucket (str): Bucket name of where the manifest is located in and the output goes to
        file_path_1 (str): File path of the first file to compare
        file_path_2 (str): File path of the second file to compare
        runner (str): Unique runner name (also becomes directory output path)
        join_column_1 (str): Column name in the first DataFrame to join on
        join_column_2 (str): Column name in the second DataFrame to join on
    """

    # create a logging object
    runner_logger = get_run_logger()

    runner_logger.info(f"Obtaining files from bucket: {bucket}")
    runner_logger.info(f"File 1 path: {file_path_1}")
    # download file_1
    file_dl(bucket, file_path_1)

    runner_logger.info(f"File 2 path: {file_path_2}")
    # download file_2
    file_dl(bucket, file_path_2)

    file_path_1 = os.path.basename(file_path_1)
    file_path_2 = os.path.basename(file_path_2)

    runner_logger.info(f"Reading dataframes from files: {file_path_1}, {file_path_2}")
    df1 = read_dataframe(file_path_1)
    df2 = read_dataframe(file_path_2)

    runner_logger.info("Comparing dataframes...")
    both_present, only_in_df1, only_in_df2 = compare_dataframes(
        df1, df2, join_column_1, join_column_2
    )

    runner_logger.info("Saving comparison results to TSV files...")
    save_dataframes(both_present, only_in_df1, only_in_df2)

    runner_logger.info(f"Files uploaded to bucket: {bucket}/{runner}/{output_dir}")
    # Upload the output files to S3
    folder_ul(local_folder=output_dir, bucket=bucket, destination=runner, sub_folder="")
