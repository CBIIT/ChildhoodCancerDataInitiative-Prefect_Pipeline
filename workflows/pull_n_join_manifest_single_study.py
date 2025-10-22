from src.utils import get_time
from prefect import flow, task, get_run_logger
import os
import sys

from workflows.pull_neo4j_data import pull_neo4j_data


@flow(
    name="Pull Neo4j data",
    log_prints=True,
    flow_run_name="pull-neo4j-{runner}-" + f"{get_time()}",
)
def pull_n_join_manifest_single_study(
    bucket: str,
    runner: str,
    study_id: str
):
    """Pipeline that pulls ingested study from a Neo4j database for study phs ID provided.

    Args:
        bucket (str): The S3 bucket to pull data from.
        runner (str): The runner to use for the workflow.
        study_id (str): The study ID to pull data for.
    """
    
    logger = get_run_logger()
    logger.info(f"Pulling joined DB for study {study_id} from bucket {bucket} using runner {runner}")

    op_folder = pull_neo4j_data_flow(
        bucket=bucket,
        runner=runner,
        study_id=study_id,
    )
    
    logger.info(f"Pulled data stored at {op_folder}")

if __name__ == "__main__":

    pull_n_join_manifest_single_study(
        bucket="your-bucket-name",
        runner="your-runner-name",
        study_id="your-study-id",
    )