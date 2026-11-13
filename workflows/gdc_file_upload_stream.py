"""Script to upload files to the GDC via API stream"""

##############
#
# Env. Setup
#
##############

import requests
import os
import pandas as pd

# prefect dependencies
from prefect import flow, task, get_run_logger, unmapped
from prefect.task_runners import ConcurrentTaskRunner
from prefect.cache_policies import NO_CACHE
from src.utils import get_time, file_dl, file_ul, get_secret, set_s3_resource


@task(name="read_input_manifest_{file_path}")
def read_input(file_path: str):
    """Read in TSV file and extract file_name, md5sum and file_size columns

    Args:
        file_path (str): path to input file that contains required cols

    Returns:
        pd.DataFrame: DataFrame with extracted necessary metadata
    """

    file_metadata = pd.read_csv(file_path)

    required_cols = ["file_url", "file_name", "id"]

    # check if required columns exist in the dataframe
    for col in required_cols:
        if col not in file_metadata.columns:
            raise ValueError(f"Missing required column: {col}")

    for col in required_cols:
        if file_metadata[col].isnull().any():
            raise ValueError(f"Missing values in required column: {col}")

    return file_metadata

class SizedStreamWrapper:
    def __init__(self, stream, size):
        self.stream = stream
        self.size = size

    def __len__(self):
        return self.size

    def read(self, chunk_size=-1):
        # boto3 StreamingBody.read() expects amt=None instead of -1 for "read all"
        if chunk_size == -1:
            return self.stream.read()
        return self.stream.read(chunk_size)

@task(
    name="upload_to_gdc", 
    task_run_name=f"upload_to_gdc_{get_time()}",
    tags=["gdc-upload-tag"],
    retry_delay_seconds=1,
    retries=3,
    cache_policy=NO_CACHE,
)
def upload_to_gdc(
    ul_parameter: dict,
    gdc_url: str,
    gdc_token: str
):
    s3 = set_s3_resource()
    s3_client = s3.meta.client

    bucket = ul_parameter["bucket"]
    key = ul_parameter["key"]
    file_uuid = ul_parameter["file_uuid"]

    metadata = s3_client.head_object(Bucket=bucket, Key=key)
    file_size = metadata["ContentLength"]

    upload_url = f"{gdc_url}/files/{file_uuid}"

    # Remove manual Content-Length and Connection headers; requests will set them accurately
    headers = {
        "X-Auth-Token": gdc_token,
        "Content-Type": "application/octet-stream",
    }

    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    wrapped_body = SizedStreamWrapper(body, file_size)

    try:
        with requests.Session() as session:
            result = session.put(
                upload_url,
                headers=headers,
                data=wrapped_body,
                timeout=(30, 300), # 30s connect, 5m read stall timeout
                verify=True
            )

            if not result.ok:
                raise RuntimeError(
                    f"GDC upload failed: HTTP {result.status_code}: {result.text}"
                )

            return result.status_code

    except requests.exceptions.SSLError as exc:
        raise RuntimeError(f"SSL error during GDC upload: {exc}") from None
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(f"HTTP error during GDC upload: {exc}") from None
    finally:
        body.close()


@flow(
    name="file_upload_handler",
    log_prints=True,
    flow_run_name="file_upload_handler_" + f"{get_time()}",
    task_runner=ConcurrentTaskRunner(),
)
def uploader_handler(
    sub_manifest: pd.DataFrame,
    project_id: str,
    secret_name_path: str,
    secret_key_name: str,
):
    """Handles upload of chunk of files to GDC

    Args:
        sub_manifest (pd.DataFrame): DataFrame of metadata for files to upload
        project_id (str): GDC Project ID to submit to (e.g. CCDI-MCI, TARGET-AML)
        secret_name_path (str): Path to AWS secrets manager where token hash stored
        secret_key_name (str): Authentication token string secret key name for file upload to GDC

    Returns:
        pd.DataFrame: Updated sub-manifest with file upload results
    """

    runner_logger = get_run_logger()
    
    token = get_secret(secret_name_path, secret_key_name).strip()

    program = project_id.split("-")[0]
    project = "-".join(project_id.split("-")[1:])

    url_prefix = f"https://api.gdc.cancer.gov/v0/submission/{program}/{project}"

    upload_list = []

    for index, row in sub_manifest.iterrows():
        bucket = row["file_url"].split("/")[2]
        key = "/".join(row["file_url"].split("/")[3:])
        file_id = row["id"]

        upload_list.append({"bucket" : bucket, "key" : key, "file_uuid": file_id})

    uploads = upload_to_gdc.map(upload_list, unmapped(url_prefix), unmapped(token))
        
    """response_status = upload_to_gdc(
        bucket=bucket,
        key=key,
        gdc_url=url_prefix,
        file_uuid=file_id,
        gdc_token=token
    )
    if response_status != 200:
        runner_logger.warning(f"Failed to upload file {file_id}: {response_status}")

    sub_manifest.at[index, "upload_status"] = response_status"""

    return uploads.result()

@flow(
    name="GDC File Stream Upload",
    log_prints=True,
    flow_run_name="{runner}_" + f"{get_time()}",
)
def runner(
    bucket: str,
    project_id: str,
    manifest_path: str,
    secret_name_path: str,
    secret_key_name: str,
    runner: str,
):
    """CCDI Pipeline to Upload files to GDC

    Args:
        bucket (str): Bucket name of where the manifest is located in and the response output goes to
        project_id (str): GDC Project ID to submit to (e.g. CCDI-MCI, TARGET-AML)
        manifest_path (str): File path of the CSV file upload manifest in bucket. Manifest must contain cols file_url, file_name, and id (GDC file UUID).
        secret_name_path (str): Path to AWS secrets manager where token hash stored
        secret_key_name (str): Authentication token string secret key name for file upload to GDC
        runner (str): Unique identifier for the current workflow run
    """

    # runner_logger setup

    runner_logger = get_run_logger()

    runner_logger.info(">>> Running GDC File Upload ....")

    # download the input manifest file
    file_dl(bucket, manifest_path)

    # extract file name before the workflow starts
    file_name = os.path.basename(manifest_path)

    runner_logger.info(f">>> Reading input file {file_name} ....")

    file_metadata = read_input(file_name)

    runner_logger.info(f">>> Extracted file metadata: {file_metadata.shape[0]} rows.")

    chunk_size = 50

    sub_manifest_results_concat = pd.DataFrame()

    runner_logger.info(f">>> Starting to process file metadata in chunks of {chunk_size} rows.")

    for i in range(0, file_metadata.shape[0], chunk_size):
        sub_manifest = file_metadata.iloc[i:i + chunk_size]
        runner_logger.info(f">>> Processing chunk {i // chunk_size + 1} of {len(file_metadata) // chunk_size + (1 if len(file_metadata) % chunk_size != 0 else 0)} with {sub_manifest.shape[0]} rows.")

        uploader_handler(
            sub_manifest=sub_manifest,
            project_id=project_id,
            secret_name_path=secret_name_path,
            secret_key_name=secret_key_name,
        )

        #runner_logger.info(f">>> Sub-manifest results: {sub_manifest_results.shape[0]} rows.")
        #sub_manifest_results_concat = pd.concat([sub_manifest_results_concat, sub_manifest_results], ignore_index=True)

    """runner_logger.info(f">>> All sub-manifest results concatenated: {sub_manifest_results_concat.shape[0]} rows.")
    file_upload_results_path = f"{os.path.splitext(file_name)[0]}_upload_results.csv"
    sub_manifest_results_concat.to_csv(file_upload_results_path, index=False)
    runner_logger.info(f">>> File upload results saved to {file_upload_results_path}.")

    file_ul(
        bucket=bucket,
        output_folder=runner,
        sub_folder="",
        newfile=file_upload_results_path,
    )"""