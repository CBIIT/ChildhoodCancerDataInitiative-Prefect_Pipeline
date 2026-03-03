from prefect import flow, get_run_logger
import os
import sys
from typing import Literal
from src.utils import get_secret_centralized_worker, get_date, get_time, file_ul
from src.neo4j_data_tools import counts_DB_all_nodes_all_studies_w_secrets

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)

@flow(name="Get diff between neo4j or memgraph instances", log_prints=True)
def db_diff(
    bucket: str,
    runner: str,
    database_1_account_id: str,
    database_1_secret_path: str,
    database_1_secret_key_ip: str,
    database_1_secret_key_username: str,
    database_1_secret_key_password: str,
    database_2_account_id: str,
    database_2_secret_path: str,
    database_2_secret_key_ip: str,
    database_2_secret_key_username: str,
    database_2_secret_key_password: str,
) -> None:
    '''This flow will pull credentials for two neo4j/memgraph db instances, get counts of all nodes in each db, and then output a tsv file with the counts and differences in counts between the two dbs. The output file will be saved to a specified bucket.
    Args:
        bucket (str): the bucket to save the output file to
        runner (str): the name of the runner executing the flow, used for naming output folder
        database_1_account_id (str): the account id for the first neo4j/memgraph db instance, used to pull aws secrets
        database_1_secret_path (str): the secret path for the first neo4j/memgraph db instance, used to pull aws secrets
        database_1_secret_key_ip (str): the secret key for the first neo4j/memgraph db instance ip, used to pull aws secrets
        database_1_secret_key_username (str): the secret key for the first neo4j/memgraph db instance username, used to pull aws secrets
        database_1_secret_key_password (str): the secret key for the first neo4j/memgraph db instance password, used to pull aws secrets
        database_2_account_id (str): the account id for the second neo4j/memgraph db instance, used to pull aws secrets
        database_2_secret_path (str): the secret path for the second neo4j/memgraph db instance, used to pull aws secrets
        database_2_secret_key_ip (str): the secret key for the second neo4j/memgraph db instance ip, used to pull aws secrets
        database_2_secret_key_username (str): the secret key for the second neo4j/memgraph db instance username, used to pull aws secrets
        database_2_secret_key_password (str): the secret key for the second neo4j/memgraph db instance password, used to pull aws secrets
    '''


    logger = get_run_logger()
    logger.info("Getting secrets for accessing neo4j or memgraph db instances")



    # pull from first db
    db1_ip = get_secret_centralized_worker(
        secret_path_name=database_1_secret_path,
        secret_key_name=database_1_secret_key_ip,
        account=database_1_account_id,
    )
    db1_username = get_secret_centralized_worker(
        secret_path_name=database_1_secret_path,
        secret_key_name=database_1_secret_key_username,
        account=database_1_account_id,
    )
    db1_password = get_secret_centralized_worker(
        secret_path_name=database_1_secret_path,
        secret_key_name=database_1_secret_key_password,
        account=database_1_account_id,
    )

    # pull from second db
    db2_ip = get_secret_centralized_worker(
        secret_path_name=database_2_secret_path,
        secret_key_name=database_2_secret_key_ip,
        account=database_2_account_id,
    )
    db2_username = get_secret_centralized_worker(
        secret_path_name=database_2_secret_path,
        secret_key_name=database_2_secret_key_username,
        account=database_2_account_id,
    )
    db2_password = get_secret_centralized_worker(
        secret_path_name=database_2_secret_path,
        secret_key_name=database_2_secret_key_password,
        account=database_2_account_id,
    )

    logger.info(f"Retrieved counts for DB1")
    count_db1 = counts_DB_all_nodes_all_studies_w_secrets(
        uri=db1_ip,
        username=db1_username,
        password=db1_password,
    )
    logger.info(f"Retrieved counts for DB2")
    count_db2 = counts_DB_all_nodes_all_studies_w_secrets(
        uri=db2_ip,
        username=db2_username,
        password=db2_password,
    )


    count_db1.rename(columns={"DB_count": "database_1_count"}, inplace=True)
    count_db2.rename(columns={"DB_count": "database_2_count"}, inplace=True)

    # merge two dataframes
    combined_df = count_db1.merge(
        count_db2, on=["study_id", "node"], how="outer", suffixes=("_database_1_count", "_database_2_count")
    )
    combined_df.fillna(0, inplace=True)
    combined_df["count_diff"] = combined_df["database_1_count"] - combined_df["database_2_count"]
    output_name = f"neo4j_db_diff_{get_date()}.tsv"
    combined_df.to_csv(output_name, sep="\t", index=False)

    # save output to bucket
    output_folder = os.path.join(runner, f"db_diff_sandbox_dev_{get_time()}")
    file_ul(
        bucket=bucket, newfile=output_name, output_folder=output_folder, sub_folder=""
    )
    logger.info(
        f"Uploaded file {output_name} to bucket {bucket} folder {output_folder}"
    )
