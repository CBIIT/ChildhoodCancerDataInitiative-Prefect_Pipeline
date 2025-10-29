from typing import List, Any, Iterable
from neo4j import GraphDatabase
from prefect import task, get_run_logger
from prefect.cache_policies import NO_CACHE
import re


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
    Extract one or more valid Cypher statements from a record, safely handling
    semicolons that appear inside string literals or map values.
    """

    # ---- Step 1: Get the record value ----
    try:
        if hasattr(record, "get") and ("query" in record.keys()):
            value = record["query"]
        else:
            keys = list(record.keys()) if hasattr(record, "keys") else []
            if len(keys) == 1:
                value = record[keys[0]]
            else:
                vals = list(record.values()) if hasattr(record, "values") else []
                value = vals[0] if vals else str(record)
    except Exception:
        value = str(record)

    if value is None:
        return []

    if isinstance(value, (bytes, bytearray)):
        try:
            value = value.decode("utf-8")
        except Exception:
            value = str(value)

    value = str(value).strip()
    if not value:
        return []

    # ---- Step 2: Split only on real statement-ending semicolons ----
    # We’ll use a regex to find semicolons that:
    #   - are not inside quotes, and
    #   - are followed by optional whitespace and a newline or end of string.
    #
    # Regex explanation:
    # (?:(?:(?<!\\)['"]).*?(?<!\\)['"])  → safely skip quoted strings
    # ;(?=\s*(?:\n|$))                   → semicolon at end of statement
    pattern = re.compile(
        r"""(
            (?:[^'";]+|'(?:\\'|[^'])*'|"(?:\\"|[^"])*")*     # match normal content or quoted text
        )
        ;
        (?=\s*(?:\n|$))                                       # semicolon before newline or end
        """,
        re.VERBOSE,
    )

    # Find all statement-like chunks
    statements = []
    last_end = 0
    for match in pattern.finditer(value):
        stmt = match.group(1).strip()
        if stmt:
            statements.append(stmt)
        last_end = match.end()

    # Add trailing statement if any leftover text exists
    if last_end < len(value):
        tail = value[last_end:].strip()
        if tail:
            statements.append(tail)

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
