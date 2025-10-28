from typing import List
from neo4j import GraphDatabase
from prefect import task, get_run_logger

# ------------------------------------------------------------------
# TASK: EXPORT DATABASE
# ------------------------------------------------------------------
@task
def export_memgraph(uri: str, username: str, password: str, output_file: str, chunk_size: int = 1000) -> None:
    """
    Exports a Memgraph database in CypherL format to a file using DUMP DATABASE,
    writing in chunks for large databases.
    """
    logger = get_run_logger()
    driver = GraphDatabase.driver(uri, auth=(username, password))
    logger.info(f"Connected to Memgraph at {uri}")

    with driver.session() as session, open(output_file, "w", encoding="utf-8") as f:
        logger.info(f"Starting export with chunk size {chunk_size}")
        result = session.run("DUMP DATABASE;")

        buffer = []
        total = 0

        for record in result:
            query = record.get("query")
            if not query:
                continue
            buffer.append(query)
            total += 1

            if len(buffer) >= chunk_size:
                f.write(";\n".join(buffer) + ";\n")
                buffer.clear()
                logger.info(f"Exported {total} CypherL queries so far...")

        # Write remaining
        if buffer:
            f.write(";\n".join(buffer) + ";\n")

        logger.info(f"✅ Export complete — total queries exported: {total}")

    driver.close()
    logger.info("Memgraph connection closed.")


# ------------------------------------------------------------------
# INTERNAL TASK: RUN QUERY CHUNKS
# ------------------------------------------------------------------
@task
def _run_chunk(session, queries: List[str], logger) -> bool:
    """
    Executes a chunk of queries safely with rollback on failure.
    """
    tx = session.begin_transaction()
    try:
        for q in queries:
            tx.run(q)
        tx.commit()
        return True
    except Exception as e:
        logger.error(f"❌ Error executing query chunk: {e}")
        tx.rollback()
        return False


# ------------------------------------------------------------------
# TASK: IMPORT DATABASE
# ------------------------------------------------------------------
@task
def import_memgraph(uri: str, username: str, password: str, input_file: str, chunk_size: int = 500) -> None:
    """
    Imports CypherL dump into Memgraph in batches for large-scale uploads.
    """
    logger = get_run_logger()
    driver = GraphDatabase.driver(uri, auth=(username, password))
    logger.info(f"Connected to Memgraph at {uri}")
    logger.info(f"Starting import with chunk size {chunk_size}")

    with driver.session() as session, open(input_file, "r", encoding="utf-8") as f:
        buffer = []
        total = 0
        executed = 0

        for line in f:
            line = line.strip()
            if not line:
                continue

            buffer.append(line)

            # When a query ends (semicolon)
            if line.endswith(";"):
                query = " ".join(buffer).rstrip(";")
                buffer.clear()
                total += 1

                if total % chunk_size == 0:
                    success = _run_chunk.fn(session, [query], logger)  # call internal task directly
                    if success:
                        executed += 1
                        logger.info(f"Executed {executed} query chunks so far...")

        # Final flush
        if buffer:
            query = " ".join(buffer).rstrip(";")
            _run_chunk.fn(session, [query], logger)
            executed += 1

    driver.close()
    logger.info(f"✅ Import complete — total query chunks executed: {executed}")