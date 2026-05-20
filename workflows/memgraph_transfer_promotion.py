from neo4j import GraphDatabase
from prefect import flow, get_run_logger
from src.memgraph_transfer import (
    export_memgraph,
    import_memgraph,
    export_memgraph_curation_filter,
)
from typing import Literal, Optional
import os
from src.utils import (
    get_secret_centralized_worker,
    get_time,
    file_ul,
    file_dl,
)


# ------------------------------------------------------------------
# PREFECT FLOW: MEMGRAPH TRANSFER PROMOTION
# ------------------------------------------------------------------
@flow(
    name="Memgraph Transfer Promotion Flow",
    log_prints=True,
    flow_run_name="{runner}_" + f"{get_time()}",
)
def memgraph_transfer_promotion(
    bucket: str,
    runner: str,
    file_path: str,
    chunk_size: int = 1000,
    mode: Literal[
        "export",
        "import",
        "promotion",
        "curation promotion filter",
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
    promotion_filter_node_label: Optional[str] = None,
    promotion_filter_property: Optional[str] = None,
    promotion_filter_value: Optional[str] = None,
    wipe_db: bool = False,
):
    """
    Prefect flow for transferring/promoting a Memgraph database.

    Args:
        mode: Operation mode for the transfer. Must be one of 'export', 'import', 'promotion', or 'curation promotion filter'.
        bucket: Working cloud storage bucket name.
        runner: Identifier for the runner executing the flow and output file path.
        file_path: s3 file path to the CypherL file for import only pipeline.
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
        promotion_filter_node_label: The node label to filter on for the curation promotion filter mode
        promotion_filter_property: The node property to filter on for the curation promotion filter mode
        promotion_filter_value: The node property value to filter on for the curation promotion filter mode
    """
    logger = get_run_logger()

    logger.info("Getting uri, username and password parameter from AWS")
    # get uri, username, and password value

    if mode in [
        "export",
        "promotion",
        "curation promotion filter",
    ]:
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

    if mode in [
        "import",
        "promotion",
        "curation promotion filter",
    ]:
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
        driver_export = GraphDatabase.driver(uri_source, auth=(username_source, password_source))
        logger.info(f"Running export with chunk size {chunk_size}")
        logger.info(f"Source database account: {database_source_account_name}")
        output_file = (
            f"memgraph_dump_{database_source_account_name}_{get_time()}.cypherl"
        )
        export_memgraph(driver=driver_export, output_file=output_file, chunk_size=chunk_size)
        # upload the cypherl file
        file_ul(bucket=bucket, output_folder=runner, sub_folder="", newfile=output_file)
        logger.info(f"Export completed: {output_file}")

    elif mode == "import":
        driver_import = GraphDatabase.driver(uri_target, auth=(username_target, password_target))
        # download the cypherl file
        file_dl(bucket, file_path)
        file_path = os.path.basename(file_path)
        logger.info(f"Running import with chunk size {chunk_size}")
        logger.info(f"Target database account: {database_target_account_name}")
        import_memgraph(driver=driver_import, input_file=file_path, chunk_size=chunk_size, wipe_db=wipe_db)
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
        driver_export = GraphDatabase.driver(uri_source, auth=(username_source, password_source))

        export_memgraph(driver=driver_export, output_file=output_file, chunk_size=chunk_size)
        # upload the cypherl file
        file_ul(bucket=bucket, output_folder=runner, sub_folder="", newfile=output_file)
        logger.info(f"Export completed: {output_file}")
        logger.info(f"Running import with chunk size {chunk_size}")

        input_file = os.path.basename(output_file)

        driver_import = GraphDatabase.driver(uri_target, auth=(username_target, password_target))

        import_memgraph(driver=driver_import, input_file=input_file, chunk_size=chunk_size, wipe_db=wipe_db)
        logger.info(f"Import to {database_target_account_name} completed successfully")

    elif mode == "curation promotion filter":
        logger.info("Running curation promotion filter flow")
        logger.info(
            "Exporting all studies and then filtering only for Promoted studies and its connected subgraph"
        )
        logger.info(f"Source database account: {database_source_account_name}")
        logger.info(f"Target database account: {database_target_account_name}")
        logger.info(f"Running export with chunk size {chunk_size}")
        output_file = f"memgraph_dump_curation_filtered_{database_source_account_name}_{get_time()}.cypherl"
        driver_export = GraphDatabase.driver(uri_source, auth=(username_source, password_source))
        node_log, rel_log, study_log = export_memgraph_curation_filter(
            driver=driver_export,
            output_file=output_file,
            filter_label=promotion_filter_node_label,
            filter_property=promotion_filter_property,
            filter_value=promotion_filter_value,
        )
        # upload the cypherl file
        file_ul(bucket=bucket, output_folder=runner, sub_folder="", newfile=output_file)
        file_ul(bucket=bucket, output_folder=runner, sub_folder="", newfile=node_log)
        file_ul(bucket=bucket, output_folder=runner, sub_folder="", newfile=rel_log)
        file_ul(bucket=bucket, output_folder=runner, sub_folder="", newfile=study_log)
        logger.info(f"Export completed: {output_file}")
        logger.info(f"Running import with chunk size {chunk_size}")

        input_file = os.path.basename(output_file)

        driver_import = GraphDatabase.driver(uri_target, auth=(username_target, password_target))

        import_memgraph(driver=driver_import, input_file=input_file, chunk_size=chunk_size, wipe_db=wipe_db)
        logger.info(f"Import to {database_target_account_name} completed successfully")

    else:
        logger.error(
            f"Invalid mode: {mode}. Must be 'export', 'import', 'promotion', 'curation promotion', or 'curation promotion filtered file'."
        )

    logger.info(f"{mode} flow completed successfully")
