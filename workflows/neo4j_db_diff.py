from prefect import flow, get_run_logger
import os
import sys
from typing import Literal


parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.utils import get_secret, get_date, get_time, file_ul
from src.neo4j_data_tools import counts_DB_all_nodes_all_studies_w_secrets


DropDownChoices1 = Literal["Curation", "QA", "Dev"]
DropDownChoices2 = Literal["Curation", "QA", "Dev"]

@flow(name="Get diff between sandbox and dev neo4j instances", log_prints=True)
def diff_sandbox_dev_neo4j(bucket: str, runner: str, database_1: DropDownChoices1, database_2: DropDownChoices2) -> None:
    """Get counts of all nodes in all studies of sandbox and dev neo4j instances and save the difference to a file in the bucket

    Args:
        bucket (str): bucket name of where output goes to
        runner (str): unique runner name
        database_1 (DropDownChoices1): starting database to compare against
        database_2 (DropDownChoices2): other database to compare with
    """    
    logger = get_run_logger()
    logger.info("Getting secrets for accessing sandbox and dev neo4j db instances")

    credentials = {
        "Curation":{
            "ip_key":"sandbox_ip",
            "username_key":"sandbox_username",
            "password_key":"sandbox_password"
        },
        "Dev":{
            "ip_key":"dev_ip",
            "username_key":"dev_username",
            "password_key":"dev_password"
        },
        "QA":{
            "ip_key":"qa_ip",
            "username_key":"qa_username",
            "password_key":"qa_password"
        }
    }

    # pull from first db
    db1_ip = get_secret(secret_name_path="ccdi/nonprod/inventory/neo4j-db-creds", secret_key_name=credentials[database_1]["ip_key"])
    db1_username = get_secret(secret_name_path="ccdi/nonprod/inventory/neo4j-db-creds", secret_key_name=credentials[database_1]["username_key"])
    db1_password = get_secret(secret_name_path="ccdi/nonprod/inventory/neo4j-db-creds", secret_key_name=credentials[database_1]["password_key"])

    # pull from second db
    db2_ip = get_secret(secret_name_path="ccdi/nonprod/inventory/neo4j-db-creds", secret_key_name=credentials[database_2]["ip_key"])
    db2_username = get_secret(secret_name_path="ccdi/nonprod/inventory/neo4j-db-creds", secret_key_name=credentials[database_2]["username_key"])
    db2_password = get_secret(secret_name_path="ccdi/nonprod/inventory/neo4j-db-creds", secret_key_name=credentials[database_2]["password_key"])

    ## sandbox secrets
    #sandbox_ip = get_secret(secret_name_path="ccdi/nonprod/inventory/neo4j-db-creds", secret_key_name="sandbox_ip")
    #sandbox_username = get_secret(secret_name_path="ccdi/nonprod/inventory/neo4j-db-creds", secret_key_name="sandbox_username")
    #sandbox_password = get_secret(secret_name_path="ccdi/nonprod/inventory/neo4j-db-creds", secret_key_name="sandbox_password")
#
    ## dev secrets
    #dev_ip = get_secret(secret_name_path="ccdi/nonprod/inventory/neo4j-db-creds", secret_key_name="dev_ip")
    #dev_username = get_secret(secret_name_path="ccdi/nonprod/inventory/neo4j-db-creds", secret_key_name="dev_username") 
    #dev_password = get_secret(secret_name_path="ccdi/nonprod/inventory/neo4j-db-creds", secret_key_name="dev_password") 
#
    ## QA secrets
    #qa_ip = get_secret(secret_name_path="ccdi/nonprod/inventory/neo4j-db-creds", secret_key_name="qa_ip")
    #qa_username = get_secret(secret_name_path="ccdi/nonprod/inventory/neo4j-db-creds", secret_key_name="qa_username") 
    #qa_password = get_secret(secret_name_path="ccdi/nonprod/inventory/neo4j-db-creds", secret_key_name="qa_password") 


    ## retrieve counts for all nodes in all studies for all DBs
    #counts_sandbox = counts_DB_all_nodes_all_studies_w_secrets(
    #    uri=sandbox_ip,
    #    username=sandbox_username,
    #    password=sandbox_password,
    #)
    #counts_sandbox.rename(columns={"DB_count": "sandbox_DB_count"}, inplace=True)
    #logger.info("Retrieved counts for sandbox DB")
#
    #counts_dev = counts_DB_all_nodes_all_studies_w_secrets(
    #    uri=dev_ip,
    #    username=dev_username,
    #    password=dev_password,
    #)
    #counts_dev.rename(columns={"DB_count": "dev_DB_count"}, inplace=True)
    #logger.info("Retrieved counts for DEV DB")
#
    #counts_qa = counts_DB_all_nodes_all_studies_w_secrets(
    #    uri=qa_ip,
    #    username=qa_username,
    #    password=qa_password,
    #)
    #counts_qa.rename(columns={"DB_count": "qa_DB_count"}, inplace=True)
    #logger.info("Retrieved counts for QA DB")

    logger.info(f"Retrieved counts for DB1 {database_1}")
    count_db1 = counts_DB_all_nodes_all_studies_w_secrets(
        uri=db1_ip,
        username=db1_username,
        password=db1_password,
    )
    logger.info(f"Retrieved counts for DB2 {database_2}")
    count_db2 = counts_DB_all_nodes_all_studies_w_secrets(
        uri=db2_ip,
        username=db2_username,
        password=db2_password,
    )
    col_rename = {
        "Curation": "sandbox_DB_count",
        "QA": "qa_DB_count",
        "Dev": "dev_DB_count",
    }
    # rename DB_count column into [Database]_DB_count
    count_db1.rename(columns={"DB_count": col_rename[database_1]}, inplace=True)
    count_db2.rename(columns={"DB_count": col_rename[database_2]}, inplace=True)

    # set up logic to choose which DB to compare
    if database_1 == "Curation":
        suffix_1 = "_sandbox"
        column_1 = "sandbox_DB_count"
    elif database_1 == "QA":
        suffix_1 = "_qa"
        column_1 = "qa_DB_count"
    elif database_1 == "Dev":
        suffix_1 = "_dev"
        column_1 = "dev_DB_count"

    
    if database_2 == "Curation":
        suffix_2 = "_sandbox"
        column_2 = "sandbox_DB_count"
    elif database_2 == "QA":
        suffix_2 = "_qa"
        column_2 = "qa_DB_count"
    elif database_2 == "Dev":
        suffix_2 = "_dev"
        column_2 = "dev_DB_count"


    # merge two dataframes
    combined_df = count_db1.merge(count_db2, on=["study_id", "node"], how="outer", suffixes=(suffix_1, suffix_2))
    combined_df.fillna(0, inplace=True)
    combined_df["count_diff"] = combined_df[column_1] - combined_df[column_2]
    output_name = f"neo4j_db_diff_{get_date()}.tsv"
    combined_df.to_csv(output_name, sep="\t", index=False)

    # save output to bucket
    output_folder = os.path.join(runner, f"db_diff_sandbox_dev_{get_time()}")
    file_ul(bucket=bucket, newfile=output_name, output_folder=output_folder, sub_folder="")
    logger.info(f"Uploaded file {output_name} to bucket {bucket} folder {output_folder}")
