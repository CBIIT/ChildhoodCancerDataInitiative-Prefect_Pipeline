from neo4j import GraphDatabase
from memgraph_transfer import import_memgraph
from src.utils import get_time, file_dl, get_secret_centralized_worker
from prefect import flow, get_run_logger
import os

@flow(
    name="Data Base Index Management Flow",
    log_prints=True,
    flow_run_name="{runner}_" + f"{get_time()}",
)
def db_index_creation_flow(
    bucket: str,
    runner: str,
    file_path: str,
    database_target_account_id: str = None,
    database_target_secret_path: str = None,
    database_target_secret_key_ip: str = None,
    database_target_secret_key_username: str = None,
    database_target_secret_key_password: str = None,
):
    """
    Prefect flow for adding or removing an index in Memgraph.

    Args:
        bucket: Working cloud storage bucket name.
        runner: Identifier for the runner executing the flow and output file path.
        file_path: s3 file path to yaml file containing index management queries. One line per query, each line should be a complete Cypher statement.
        database_target_account_id (str): Account ID for the target database
        database_target_secret_path (str): Secret path for the target database
        database_target_secret_key_ip (str): Secret key for the IP of the target database
        database_target_secret_key_username (str): Secret key for the username of the target database
        database_target_secret_key_password (str): Secret key for the password of the target database
    """
    logger = get_run_logger()
    logger.info(f"Starting Memgraph Index Management Flow.")

    uri_target = get_secret_centralized_worker(
        secret_path_name=database_target_secret_path,
        secret_key_name=database_target_secret_key_ip,
        account=database_target_account_id,
    )
    username_target = get_secret_centralized_worker(
        secret_path_name=database_target_secret_path,
        secret_key_name=database_target_secret_key_username,
        account=database_target_account_id,
    )
    password_target = get_secret_centralized_worker(
        secret_path_name=database_target_secret_path,
        secret_key_name=database_target_secret_key_password,
        account=database_target_account_id,
    )

    # Retrieve secrets for connecting to the target database
    uri = uri_target
    username = username_target
    password = password_target

    # Download the CypherL file from S3
    file_dl(bucket, file_path)
    file_path_local = os.path.basename(file_path)

    driver = GraphDatabase.driver(uri, auth=(username, password))

    # Read in the yaml file with cypher commands and execute queries for index management
    for line in open(file_path_local, "r", encoding="utf-8"):
        query = line.strip()
        if not query:
            continue  # skip empty lines
        elif query.startswith("#"):
            logger.info(f"Skipping comment line: {query}")
            continue  # skip comment lines
        elif not query.endswith(";"):
            logger.warning(f"Query does not end with a semicolon: {query}. This may cause execution issues.")
            logger.warning(f"Skipping query: {query}")
            continue  # skip queries that do not end with a semicolon
        elif query.startswith("CREATE INDEX") or query.startswith("DROP INDEX"):
            logger.info(f"Processing index management query: {query}")
            # Execute index management queries in batches
            try:
                with driver.session() as session:
                    session.run(query)
                    logger.info(f"Successfully executed query: {query}")
            
            except Exception as e:
                logger.error(f"Failed to execute query: {query}, Error: {e}")
