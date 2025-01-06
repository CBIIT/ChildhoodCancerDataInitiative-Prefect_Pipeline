from prefect import flow, get_run_logger
import os
import sys

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.utils import get_secret, get_date, get_time, file_ul
from src.neo4j_data_tools import counts_DB_all_nodes_all_studies_w_secrets

@flow(name="Get diff between sandbox and dev neo4j instances", log_prints=True)
def diff_sandbox_dev_neo4j(bucket: str, runner: str) -> None:
    """Get counts of all nodes in all studies of sandbox and dev neo4j instances and save the difference to a file in the bucket

    Args:
        bucket (str): bucket name of where output goes to
        runner (str): unique runner name
    """    
    logger = get_run_logger()
    logger.info("Getting secrets for accessing sandbox and dev neo4j db instances")

    # sandbox secrets
    sandbox_ip = get_secret(secret_name_path="ccdi/nonprod/inventory/neo4j-db-creds", secret_key_name="sandbox_ip")
    sandbox_username = get_secret(secret_name_path="ccdi/nonprod/inventory/neo4j-db-creds", secret_key_name="sandbox_username")
    sandbox_password = get_secret(secret_name_path="ccdi/nonprod/inventory/neo4j-db-creds", secret_key_name="sandbox_password")

    # dev secrets
    dev_ip = get_secret(secret_name_path="ccdi/nonprod/inventory/neo4j-db-creds", secret_key_name="dev_ip")
    dev_username = get_secret(secret_name_path="ccdi/nonprod/inventory/neo4j-db-creds", secret_key_name="dev_username") 
    dev_password = get_secret(secret_name_path="ccdi/nonprod/inventory/neo4j-db-creds", secret_key_name="dev_password") 

    # retrieve counts for all nodes in all studies of both DBs
    counts_sandbox = counts_DB_all_nodes_all_studies_w_secrets(
        uri_parameter=sandbox_ip,
        username_parameter=sandbox_username,
        password_parameter=sandbox_password,
    )
    counts_sandbox.rename(columns={"DB_count": "sandbox_DB_count"}, inplace=True)
    logger.info("Retrieved counts for sandbox DB")
    counts_dev = counts_DB_all_nodes_all_studies_w_secrets(
        uri_parameter=dev_ip,
        username_parameter=dev_username,
        password_parameter=dev_password,
    )
    counts_dev.rename(columns={"DB_count": "dev_DB_count"}, inplace=True)
    logger.info("Retrieved counts for DEV DB")

    # merge two dataframes
    combined_df = counts_sandbox.merge(counts_dev, on=["study_id", "node"], how="outer", suffixes=("_sandbox", "_dev"))
    combined_df.fillna(0, inplace=True)
    combined_df["count_diff"] = combined_df["sandbox_DB_count"] - combined_df["dev_DB_count"]
    output_name = f"neo4j_db_diff_{get_date()}.tsv"
    combined_df.to_csv(output_name, sep="\t", index=False)

    # save output to bucket
    output_folder = os.path.join(runner, f"db_diff_sandbox_dev_{get_time()}")
    file_ul(bucket=bucket, newfile=output_name, output_folder=output_folder, sub_folder="")
    logger.info(f"Uploaded file {output_name} to bucket {bucket} folder {output_folder}")
