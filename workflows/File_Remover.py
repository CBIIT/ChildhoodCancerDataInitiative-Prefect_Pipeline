import sys
import os
from prefect import flow, pause_flow_run, get_run_logger
from prefect.input import RunInput
parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)

from src.utils import get_time


class FlowPath(RunInput):
    have_manifest: str

class ManifestPath(RunInput):
    bucket: str
    manifest_tsv_path: str
    delete_column_name: str
    runner: str


@flow
async def run_file_remover():
    logger = get_run_logger()
    current_time = get_time()

    description_md = f"""
**Welcome to the File Remover Flow!**
Today's Date: {current_time}

Please enter your preferred path below:
- **have_manifest**: y/n

"""

    description_manifest_md = f"""
**Please provide inputs as shown below**

- **bucket**: bucket name of where manifest lives
- **manifest_tsv_path**: path of manifest(tsv) in the bucket
- **delete_column_name**: column name of s3 uri to be deleted
- **runner**": your runner id

"""

    user_input = await pause_flow_run(
        wait_for_input=FlowPath.with_initial_data(
            description=description_md, have_manifest="y"
        )
    )

    if user_input.have_manifest == "y":
        logger.info("You have a manifest for File Remover")
        manifest_path_inputs = await pause_flow_run(
            wait_for_input=ManifestPath.with_initial_data(
                description=description_manifest_md, bucket="ccdi-validation"
            )
        )

        logger.info(f"bucket: {manifest_path_inputs.bucket}")
        logger.info(f"manifest_tsv_path: {manifest_path_inputs.manifest_tsv_path}")
        logger.info(f"delete_column_name: {manifest_path_inputs.delete_column_name}")
        logger.info(f"runner id: {manifest_path_inputs.runner}")

    else:
        logger.info(f"You don't have a manifest for File Remover!")
