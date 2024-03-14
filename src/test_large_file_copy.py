from src.utils import set_s3_session_client
import os
from prefect import flow, get_run_logger


@flow
def copy_large_file(
    copy_parameter
):
    s3_client = set_s3_session_client()
    # create a logger
    logger = get_run_logger()

    # collect source bucket, source key, destination bucket, and destination key
    source_bucket, source_key = copy_parameter['CopySource'].split("/", 1)
    destination_bucket = copy_parameter['Bucket']
    destination_key = copy_parameter['Key']

    # Get the size of the source object
    response = s3_client.head_object(Bucket=source_bucket, Key=source_key)
    file_size = response["ContentLength"]

    # Define the size of each part (5MB)
    part_size = 100 * 1024 * 1024

    # Calculate the number of parts required
    num_parts = int(file_size / part_size) + 1

    # Initialize the multipart upload
    upload_id = s3_client.create_multipart_upload(
        Bucket=destination_bucket, Key=destination_key
    )["UploadId"]

    # Initialize parts list
    parts = []

    try:
        # Upload parts
        for part_number in range(1, num_parts + 1):
            # Calculate the byte range for this part
            start_byte = (part_number - 1) * part_size
            end_byte = min(part_number * part_size - 1, file_size - 1)
            byte_range = f"bytes={start_byte}-{end_byte}"

            # Upload a part
            response = s3_client.upload_part_copy(
                Bucket=destination_bucket,
                Key=destination_key,
                CopySource={"Bucket": source_bucket, "Key": source_key},
                PartNumber=part_number,
                UploadId=upload_id,
                CopySourceRange=byte_range,
            )

            # Append the response to the parts list
            parts.append(
                {"PartNumber": part_number, "ETag": response["CopyPartResult"]["ETag"]}
            )

        # Complete the multipart upload
        s3_client.complete_multipart_upload(
            Bucket=destination_bucket,
            Key=destination_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

        logger.info(
            f"File copied successfully from {source_bucket}/{source_key} to {destination_bucket}/{destination_key}"
        )
    except Exception as e:
        # Abort the multipart upload if an error occurs
        s3_client.abort_multipart_upload(
            Bucket=destination_bucket, Key=destination_key, UploadId=upload_id
        )
        logger.info(f"Failed to copy file: {e}")

