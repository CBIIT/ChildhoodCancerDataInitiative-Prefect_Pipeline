from prefect import flow, get_run_logger
from websockets import uri
from src.memgraph_transfer import (
    export_memgraph,
    import_memgraph,
    export_memgraph_curation,
)
from typing import Literal
import os
from src.utils import (
    get_secret_centralized_worker,
    get_time,
    file_ul,
    file_dl,
)


# ------------------------------------------------------------------
# PREFECT FLOW: MEMGRAPH TRANSFER DCC
# ------------------------------------------------------------------
@flow(
    name="Memgraph Transfer DCC Flow",
    log_prints=True,
    flow_run_name="{runner}_" + f"{get_time()}",
)
def memgraph_transfer_dcc(
    bucket: str,
    runner: str,
    file_path: str,
    chunk_size: int = 1000,
    mode: Literal[
        "export", "import", "promotion", "curation promotion"
    ] = "export",  # dropdown choice
    database_source_account_name: str = None,
    database_source_account_id: str = None,
    database_source_secret_path: str = None,
    database_source_secret_key_ip: str = None,
    database_source_secret_key_username: str = None,
    database_source_secret_key_password: str = None,
    database_target_account_name: str = None,
    database_target_account_id: str = None,
    database_target_secret_path: str = None,
    database_target_secret_key_ip: str = None,
    database_target_secret_key_username: str = None,
    database_target_secret_key_password: str = None,
    wipe_db: bool = False,
):
    """
    Prefect flow for transferring/promoting a Memgraph database.

    Args:
        mode: Operation mode for the transfer. Must be one of 'export', 'import', 'promotion', or 'curation promotion'.
        bucket: Working cloud storage bucket name.
        runner: Identifier for the runner executing the flow and output file path.
        file_path: Path to the CypherL file for import.
        database_source_account_name (str): Account name/nickname/shorthand for the source database
        database_source_account_id (str): Account ID for the source database
        database_source_secret_path (str): Secret path for the source database
        database_source_secret_key_ip (str): Secret key for the IP of the source database
        database_source_secret_key_username (str): Secret key for the username of the source database
        database_source_secret_key_password (str): Secret key for the password of the source database
        database_target_account_name (str): Account name/nickname/shorthand for the target database (only required for promotion and import modes)
        database_target_account_id (str): Account ID for the target database (only required for promotion and import modes)
        database_target_secret_path (str): Secret path for the target database (only required for promotion and import modes)
        database_target_secret_key_ip (str): Secret key for the IP of the target database (only required for promotion and import modes)
        database_target_secret_key_username (str): Secret key for the username of the target database (only required for promotion and import modes)
        database_target_secret_key_password (str): Secret key for the password of the target database (only required for promotion and import modes)
        chunk_size: Number of statements to process per batch.
        wipe_db: If True during import, wipes the existing database before importing.
    """
    logger = get_run_logger()

    logger.info("Getting uri, username and password parameter from AWS")
    # get uri, username, and password value

    if mode in ["export", "promotion", "curation promotion"]:
        logger.info("Mode requires source database credentials, retrieving from AWS")
        uri_source = get_secret_centralized_worker(
            secret_path_name=database_source_secret_path,
            secret_key_name=database_source_secret_key_ip,
            account=database_source_account_id,
        )
        username_source = get_secret_centralized_worker(
            secret_path_name=database_source_secret_path,
            secret_key_name=database_source_secret_key_username,
            account=database_source_account_id,
        )
        password_source = get_secret_centralized_worker(
            secret_path_name=database_source_secret_path,
            secret_key_name=database_source_secret_key_password,
            account=database_source_account_id,
        )

    if mode in ["import", "promotion", "curation promotion"]:
        logger.info("Mode requires target database credentials, retrieving from AWS")
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

    # check the mode and run the corresponding flow

    if mode == "export":
        logger.info(f"Running export with chunk size {chunk_size}")
        logger.info(f"Source database account: {database_source_account_name}")
        output_file = (
            f"memgraph_dump_{database_source_account_name}_{get_time()}.cypherl"
        )
        export_memgraph(
            uri_source, username_source, password_source, output_file, chunk_size
        )
        # upload the cypherl file
        file_ul(bucket=bucket, output_folder=runner, sub_folder="", newfile=output_file)
        logger.info(f"Export completed: {output_file}")

    elif mode == "import":
        # download the cypherl file
        file_dl(bucket, file_path)
        file_path = os.path.basename(file_path)
        logger.info(f"Running import with chunk size {chunk_size}")
        logger.info(f"Target database account: {database_target_account_name}")
        import_memgraph(
            uri_target, username_target, password_target, file_path, chunk_size, wipe_db
        )
        logger.info(f"Import to {database_target_account_name} completed successfully")

    elif mode == "promotion":
        logger.info("Running regular promotion flow")
        logger.info(f"Source database account: {database_source_account_name}")
        logger.info(f"Target database account: {database_target_account_name}")
        # First export the source database to a local file
        logger.info(f"Running export with chunk size {chunk_size}")
        output_file = (
            f"memgraph_dump_{database_source_account_name}_{get_time()}.cypherl"
        )
        export_memgraph(
            uri_source, username_source, password_source, output_file, chunk_size
        )
        # upload the cypherl file
        file_ul(bucket=bucket, output_folder=runner, sub_folder="", newfile=output_file)
        logger.info(f"Export completed: {output_file}")
        logger.info(f"Running import with chunk size {chunk_size}")

        input_file = os.path.basename(output_file)

        import_memgraph(
            uri_target, username_target, password_target, input_file = input_file, chunk_size = chunk_size, wipe_db = wipe_db
        )
        logger.info(f"Import to {database_target_account_name} completed successfully")

    elif mode == "curation promotion":
        logger.info("Running curation promotion flow")
        logger.info("Exporting only the promoted studies and its connected subgraph")
        logger.info(f"Source database account: {database_source_account_name}")
        logger.info(f"Target database account: {database_target_account_name}")
        logger.info(f"Running export with chunk size {chunk_size}")
        output_file = f"memgraph_dump_curation_{database_source_account_name}_{get_time()}.cypherl"
        export_memgraph_curation(
            uri_source, username_source, password_source, output_file, chunk_size
        )
        # upload the cypherl file
        file_ul(bucket=bucket, output_folder=runner, sub_folder="", newfile=output_file)
        logger.info(f"Export completed: {output_file}")
        logger.info(f"Running import with chunk size {chunk_size}")

        input_file = os.path.basename(output_file)

        import_memgraph(
            uri_target, username_target, password_target, input_file = input_file, chunk_size = chunk_size, wipe_db = wipe_db
        )
        logger.info(f"Import to {database_target_account_name} completed successfully")

    else:
        logger.error(
            f"Invalid mode: {mode}. Must be 'export', 'import', 'promotion', or 'curation promotion'."
        )

    logger.info(f"{mode} flow completed successfully")
