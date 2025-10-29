from typing import List, Any, Iterable
from neo4j import GraphDatabase
from prefect import task, get_run_logger
from prefect.cache_policies import NO_CACHE


# ------------------------------------------------------------------
# TASK: EXPORT DATABASE
# ------------------------------------------------------------------
@task(cache_policy=NO_CACHE, name="export_memgraph")
def export_memgraph(
    uri: str, username: str, password: str, output_file: str, chunk_size: int = 1000
) -> None:
    """
    Export Memgraph using DUMP DATABASE; robustly extracts the returned CypherL statements
    and writes them to `output_file` in chunks to avoid memory blow-up.
    """
    logger = get_run_logger()
    driver = GraphDatabase.driver(uri, auth=(username, password))
    logger.info("Connected to Memgraph.")

    def _extract_query_from_record(record: Any) -> Iterable[str]:
        """
        Given a neo4j.Record (or other mapping/tuple), attempt to return one or more
        Cypher statements (as strings). Some drivers return single-column records,
        sometimes the key is 'query', other times it's the sole column value.
        Also handle cases where a record's value contains multiple statements
        separated by semicolons or newlines.
        """
        # Try common access patterns
        try:
            # If mapping-like and has 'query' key
            if hasattr(record, "get") and ("query" in record.keys()):
                value = record["query"]
            else:
                # If record behaves like a mapping with exactly one column
                keys = list(record.keys()) if hasattr(record, "keys") else []
                if len(keys) == 1:
                    value = record[keys[0]]
                else:
                    # Fall back to values() (list) or to str(record)
                    vals = list(record.values()) if hasattr(record, "values") else []
                    if vals:
                        value = vals[0]
                    else:
                        value = str(record)
        except Exception:
            # Absolute fallback
            value = str(record)

        # If the value is None, return empty
        if value is None:
            return []

        # If the driver returned bytes, decode
        if isinstance(value, (bytes, bytearray)):
            try:
                value = value.decode("utf-8")
            except Exception:
                value = str(value)

        # Normalize to string
        value = str(value).strip()

        # If empty after stripping, skip
        if not value:
            return []

        # Often the dump might be one long string with multiple statements separated by semicolons/newlines.
        # Split conservatively by semicolon, but keep semicolons consistent.
        statements = []
        # If there are semicolons, split on ';' and reconstruct statements
        if ";" in value:
            parts = [p.strip() for p in value.split(";")]
            for p in parts:
                if p:
                    statements.append(p)  # do not include trailing semicolon here
        else:
            # No semicolon — treat the whole value as one statement (may already be single CREATE)
            statements.append(value)

        return statements

    with driver.session() as session, open(output_file, "w", encoding="utf-8") as f:
        logger.info("Running DUMP DATABASE to export CypherL statements...")
        result = session.run("DUMP DATABASE;")

        buffer = []
        total_statements = 0

        for record in result:
            # Extract one or more statements from the record
            statements = _extract_query_from_record(record)

            for stmt in statements:
                # stmt is a statement WITHOUT trailing semicolon (by our extractor)
                buffer.append(stmt)
                total_statements += 1

                # When buffer reaches chunk_size, write them out
                if len(buffer) >= chunk_size:
                    # Join with semicolon + newline and add final semicolon/newline
                    f.write(";\n".join(buffer) + ";\n")
                    f.flush()
                    buffer.clear()
                    logger.info(f"Exported {total_statements} statements so far...")

        # Flush any remaining statements
        if buffer:
            f.write(";\n".join(buffer) + ";\n")
            f.flush()

        logger.info(f"Export complete. Total statements exported: {total_statements}")

    driver.close()
    logger.info("Memgraph connection closed.")


# ------------------------------------------------------------------
# INTERNAL TASK: RUN QUERY CHUNKS
# ------------------------------------------------------------------
@task(cache_policy=NO_CACHE, name="_run_chunk")
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
        logger.error(f"Error executing query chunk: {e}")
        tx.rollback()
        return False


# ------------------------------------------------------------------
# TASK: IMPORT DATABASE
# ------------------------------------------------------------------
@task(cache_policy=NO_CACHE, name="import_memgraph")
def import_memgraph(
    uri: str, username: str, password: str, input_file: str, chunk_size: int = 500
) -> None:
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
                    success = _run_chunk.fn(
                        session, [query], logger
                    )  # call internal task directly
                    if success:
                        executed += 1
                        logger.info(f"Executed {executed} query chunks so far...")

        # Final flush
        if buffer:
            query = " ".join(buffer).rstrip(";")
            _run_chunk.fn(session, [query], logger)
            executed += 1

    driver.close()
    logger.info(f"Import complete — total query chunks executed: {executed}")
