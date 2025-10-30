from prefect import flow, get_run_logger
from src.memgraph_transfer import export_memgraph, import_memgraph
from src.neo4j_data_tools import cypher_query_parameters
from typing import Literal
import os
from src.utils import (
    get_time,
    file_ul,
    file_dl,
)


# ------------------------------------------------------------------
# PREFECT FLOW: EXPORT OR IMPORT MODE
# ------------------------------------------------------------------
@flow(
    name="Memgraph Export and Import Flow",
    log_prints=True,
    flow_run_name="{runner}_" + f"{get_time()}",
)
def memgraph_export_import_flow(
    bucket: str,
    runner: str,
    file_path: str,
    uri_parameter: str,
    username_parameter: str,
    password_parameter: str,
    mode: Literal["export", "import"] = "export",  # dropdown choice
    chunk_size: int = 1000,
    wipe_db: bool = False,
):
    """
    Prefect flow for exporting or importing a Memgraph database.
    'mode' determines whether to export or import.

    Parameters:
    - bucket: Working cloud storage bucket name.
    - runner: Identifier for the runner executing the flow.
    - file_path: Path to the CypherL file for import/export.
    - uri_parameter: AWS Parameter name for Memgraph URI.
    - username_parameter: AWS Parameter name for Memgraph username.
    - password_parameter: AWS Parameter name for Memgraph password.
    - mode: "export" to export the database, "import" to import.
    - chunk_size: Number of statements to process per batch.
    - wipe_db: If True during import, wipes the existing database before importing.
    """
    logger = get_run_logger()
    logger.info(f"Starting Memgraph {mode.upper()} flow")

    # get uri, username, and password parameter values
    uri, username, password = cypher_query_parameters(
        uri_parameter=uri_parameter,
        username_parameter=username_parameter,
        password_parameter=password_parameter,
        logger=logger,
    )

    if mode == "export":
        logger.info(f"Running export with chunk size {chunk_size}")
        output_file = f"memgraph_dump_{get_time()}.cypherl"
        export_memgraph(uri, username, password, output_file, chunk_size)
        # upload the cypherl file
        file_ul(bucket=bucket, output_folder=runner, sub_folder="", newfile=output_file)
        logger.info(f"Export completed: {output_file}")

    elif mode == "import":
        # download the cypherl file
        file_dl(bucket, file_path)
        file_name = os.path.basename(file_path)
        logger.info(f"Running import with chunk size {chunk_size}")
        import_memgraph(uri, username, password, file_name, chunk_size, wipe_db)
        logger.info("Import completed successfully")

    else:
        logger.error(f"Invalid mode: {mode}. Must be 'export' or 'import'.")

    logger.info("Flow completed successfully")


# ------------------------------------------------------------------
# LOCAL EXECUTION ENTRY POINT
# ------------------------------------------------------------------
if __name__ == "__main__":
    # Example local test
    memgraph_export_import_flow(
        uri="bolt://000.0.0.1:8000",
        username="memgraph",
        password="memgraph",
        mode="export",  # or "import"
        file_path="memgraph_dump.cypherl",
        chunk_size=1000,
    )
