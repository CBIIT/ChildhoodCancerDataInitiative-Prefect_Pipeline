from src.utils import get_time
from prefect import flow, task, get_run_logger
import os
import sys

from workflows.pull_neo4j_data import pull_neo4j_data
from workflows.db_tsv_to_manifest import join_tsv_to_manifest


@flow(
    name="Pull Neo4j data",
    log_prints=True,
    flow_run_name="pull-neo4j-{runner}-" + f"{get_time()}",
)
def pull_n_join_manifest_single_study(
    bucket: str,
    runner: str,
    study_id: str,
    ccdi_template_tag: str
):
    """Pipeline that pulls ingested study from a Neo4j database for study phs ID provided.

    Args:
        bucket (str): The S3 bucket to pull data from.
        runner (str): The runner to use for the workflow.
        study_id (str): The study ID to pull data for.
        ccdi_template_tag (str): Tag name of CCDI manifest
    """
    
    logger = get_run_logger()
    logger.info(f"Pulling joined DB for study {study_id} from bucket {bucket} using runner {runner}")

    op_folder = pull_neo4j_data(
        bucket=bucket,
        runner=runner,
        study_id=study_id,
    )
    
    op_folder_path = f"{op_folder}/{study_id}".replace("/./","/")

    logger.info(f"Pulled data stored at {op_folder_path}")
    
    join_tsv_to_manifest(
        bucket=bucket,
        runner=runner,
        tsv_folder_path=op_folder_path,
        ccdi_template_tag=ccdi_template_tag
    )

if __name__ == "__main__":

    pull_n_join_manifest_single_study(
        bucket="your-bucket-name",
        runner="your-runner-name",
        study_id="your-study-id",
    )
    