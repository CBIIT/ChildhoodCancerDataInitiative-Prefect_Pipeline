import os
import csv
import math
from datetime import datetime
import prefect
from prefect import flow, task, get_run_logger
from src.utils import file_dl, get_time, folder_ul


timestamp = get_time()

@task
def detect_delimiter(filepath: str) -> str:
    ext = os.path.splitext(filepath)[1].lower()
    if ext == ".tsv":
        return "\t"
    elif ext == ".csv":
        return ","
    else:
        raise ValueError(f"Unsupported file type: '{ext}'. Only .csv and .tsv are supported.")

@task
def read_file(filepath: str, delimiter: str) -> tuple[list[str], list[list[str]]]:
    with open(filepath, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=delimiter)
        headers = next(reader)
        rows = list(reader)
    prefect.get_run_logger().info(f"Read {len(rows)} data rows from '{filepath}'")
    return headers, rows

@task
def create_output_folder(filepath: str) -> str:
    base_name = os.path.splitext(os.path.basename(filepath))[0]
    folder_name = f"{base_name}_{timestamp}"
    os.makedirs(folder_name, exist_ok=True)
    prefect.get_run_logger().info(f"Created output folder: '{folder_name}'")
    return folder_name

@task
def split_and_write_files(
    headers: list[str],
    rows: list[list[str]],
    rows_per_file: int,
    output_folder: str,
    delimiter: str,
    original_filepath: str,
) -> int:
    logger = prefect.get_run_logger()
    ext = os.path.splitext(original_filepath)[1].lower()
    base_name = os.path.splitext(os.path.basename(original_filepath))[0]

    # Subtract 1 from rows_per_file to account for the header row
    data_rows_per_file = rows_per_file - 1
    if data_rows_per_file < 1:
        raise ValueError("rows_per_file must be at least 2 (1 header + 1 data row).")

    total_files = math.ceil(len(rows) / data_rows_per_file)

    for i in range(total_files):
        chunk = rows[i * data_rows_per_file : (i + 1) * data_rows_per_file]
        file_number = str(i + 1).zfill(len(str(total_files)))  # zero-pad file numbers
        output_filename = f"{base_name}_part{file_number}{ext}"
        output_path = os.path.join(output_folder, output_filename)

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter=delimiter)
            writer.writerow(headers)
            writer.writerows(chunk)

        logger.info(f"Written: '{output_path}' ({len(chunk)} data rows)")

    return total_files

@flow(name="Split CSV/TSV File")
def split_file(filepath: str, rows_per_file: int):
    logger = prefect.get_run_logger()

    delimiter = detect_delimiter(filepath)
    headers, rows = read_file(filepath, delimiter)
    output_folder = create_output_folder(filepath)
    total_files = split_and_write_files(
        headers=headers,
        rows=rows,
        rows_per_file=rows_per_file,
        output_folder=output_folder,
        delimiter=delimiter,
        original_filepath=filepath,
    )

    logger.info(f"Done. Split {len(rows)} rows into {total_files} files in '{output_folder}'")

    return output_folder


@flow(
    name="Table Snipper Flow",
    log_prints=True,
    flow_run_name="{runner}_" + f"{get_time()}",
)
def submission_cruncher(
    bucket: str,
    file_path: str,
    runner: str,
    row_numbers: int = 100000,
) -> None:
    """Pipeline that splits a large CSV or TSV file into smaller chunks based on the specified number of rows per output file.

    Args:
        bucket (str): Bucket name of where the manifests located in and the output goes to
        file_path (str): Path to the manifest file within the bucket
        runner (str): Unique runner name
        row_numbers (int, optional): Number of rows per output file. Defaults to 100000.
    """    
    logger = prefect.get_run_logger()

    # dl file_path
    logger.info(
        f"Downloading {file_path} from bucket {bucket}"
    )

    # download the manifest
    file_dl(bucket, file_path)

    file = os.path.basename(file_path)

    output_folder = split_file(filepath=file, rows_per_file=row_numbers)

    logger.info(
        f"Uploading output folder {output_folder} to {bucket} in {runner}"
    )

    folder_ul(bucket=bucket, local_folder=output_folder, destination=runner, sub_folder="")