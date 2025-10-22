from prefect import flow, task, get_run_logger
import os
import sys

from workflows.pull_neo4j_data import pull_neo4j_data_flow

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

    pull_neo4j_data_flow(
        bucket=bucket,
        runner=runner,
        study_id=study_id,
    )

if __name__ == "__main__":

    pull_n_join_manifest_single_study(
        bucket="your-bucket-name",
        runner="your-runner-name",
        study_id="your-study-id",
    )