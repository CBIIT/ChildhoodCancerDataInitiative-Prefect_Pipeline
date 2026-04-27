from typing import Any
from neo4j import GraphDatabase
from neo4j.exceptions import TransactionError
from prefect import task, get_run_logger
from prefect.cache_policies import NO_CACHE
import json
from requests import session
from src.utils import get_time
import time


# ------------------------------------------------------------------
# TASK: EXPORT DATABASE
# ------------------------------------------------------------------
@task(cache_policy=NO_CACHE, name="export_memgraph")
def export_memgraph(
    uri: str, username: str, password: str, output_file: str, chunk_size: int = 1000
) -> None:
    """
    Export a Memgraph database to a CypherL (.cypher) file using DUMP DATABASE.
    Each record is assumed to contain a complete Cypher statement.
    Writes to disk in chunks for large-scale databases.
    """
    logger = get_run_logger()
    driver = GraphDatabase.driver(uri, auth=(username, password))
    logger.info(f"Connected to Memgraph")

    def _extract_query_from_record(record: Any) -> str:
        """
        Safely extract the query text from a record.
        Assumes each record contains one complete Cypher query line.
        """
        try:
            if hasattr(record, "get") and "query" in record.keys():
                return record["query"]
            keys = list(record.keys()) if hasattr(record, "keys") else []
            if len(keys) == 1:
                return record[keys[0]]
            vals = list(record.values()) if hasattr(record, "values") else []
            if vals:
                return vals[0]
        except Exception:
            pass
        return str(record)

    with driver.session() as session, open(output_file, "w", encoding="utf-8") as f:
        logger.info("Running DUMP DATABASE to export CypherL statements...")
        result = session.run("DUMP DATABASE;")

        buffer = []
        total_statements = 0

        for record in result:
            query = _extract_query_from_record(record)
            if not query:
                continue

            # Keep the query as-is — do not strip or split
            buffer.append(query.strip())
            total_statements += 1

            if len(buffer) >= chunk_size:
                f.write("\n".join(buffer) + "\n")
                f.flush()
                buffer.clear()
                logger.info(f"Exported {total_statements} statements so far...")

        # Write any remaining statements
        if buffer:
            f.write("\n".join(buffer) + "\n")
            f.flush()

        logger.info(f"Export complete. Total statements exported: {total_statements}")

    driver.close()
    logger.info("Memgraph connection closed.")


# ------------------------------------------------------------------
# TASK: EXPORT DATABASE FOR CURATION PROMOTION
# ------------------------------------------------------------------
def format_properties(props):
    """Convert dict to Cypher map string"""
    if not props:
        return "{}"
    pairs = []
    for k, v in props.items():
        if isinstance(v, str):
            v = v.replace('"', '\\"')
            pairs.append(f'{k}: "{v}"')
        elif v is None:
            continue
        else:
            pairs.append(f"{k}: {json.dumps(v)}")
    return "{ " + ", ".join(pairs) + " }"


def format_labels(labels):
    return ":" + ":".join(labels) if labels else ""


def run_paginated_with_retry(session, query, params=None, page_size=1000, retries=3, delay=2):
    logger = get_run_logger()
    results = []
    skip = 0
    while True:
        paginated_query = query + f" SKIP {skip} LIMIT {page_size}"
        page = []
        for attempt in range(retries):
            try:
                page = list(session.run(paginated_query, params or {}))
                results.extend(page)
                break
            except TransactionError as e:
                if attempt < retries - 1:
                    logger.warning(f"Transaction error (attempt {attempt + 1}/{retries}) at SKIP {skip}, retrying in {delay * (attempt + 1)}s... Error: {e}")
                    time.sleep(delay * (attempt + 1))
                else:
                    logger.error(f"Transaction error after {retries} attempts at SKIP {skip}: {e}")
                    return results
            except Exception as e:
                if attempt < retries - 1:
                    logger.warning(f"Query failed (attempt {attempt + 1}/{retries}) at SKIP {skip}, retrying in {delay}s... Error: {e}")
                    time.sleep(delay)
                else:
                    logger.error(f"Query failed after {retries} attempts at SKIP {skip}: {e}")
                    raise e
        if len(page) < page_size:
            break
        skip += page_size
        time.sleep(0.5)
    return results


@task(cache_policy=NO_CACHE, name="export_nodes", persist_result=False)
def export_nodes(driver, output_file, node_vars):
    """
    Exports nodes directly to the output file as they are fetched.
    Populates node_vars dict in place for use by export_relationships.
    """
    logger = get_run_logger()
    logger.info("Fetching studies with promotion_status 'Promote'...")

    study_query = """
    MATCH (st:study)
    WHERE "Promote" IN st.promotion_status
    RETURN st.study_id AS study_id
    """
    with driver.session() as session:
        studies = run_paginated_with_retry(session, study_query)
    study_ids = [record["study_id"] for record in studies]
    logger.info(f"Found {len(study_ids)} studies to process: {study_ids}")

    seen_node_ids = set()
    node_counter = 0
    study_count = 0
    study_total = len(study_ids)
    log_file = f"nodes_export_{get_time()}.tsv"

    with open(log_file, "w") as log_f:
        log_f.write("study\tnode\tcount\n")

        for study_id in study_ids:
            study_count += 1
            logger.info(f"Processing study {study_count}/{study_total}: {study_id}...")

            with driver.session() as session:

                # Get all distinct node labels present in this study
                label_query = """
                MATCH (st:study {study_id : $study_id})-[*1..5]-(n)
                UNWIND labels(n) AS label
                RETURN DISTINCT label
                """
                try:
                    labels = [record["label"] for record in run_paginated_with_retry(session, label_query, {"study_id": study_id})]
                    logger.info(f"Found {len(labels)} node labels for study {study_id}: {labels}")
                except Exception as e:
                    logger.error(f"Failed to fetch node labels for study {study_id}: {e}")
                    continue

                # Add the study node itself
                study_node_query = """
                MATCH (st:study {study_id : $study_id})
                RETURN st AS n
                """
                try:
                    study_result = run_paginated_with_retry(session, study_node_query, {"study_id": study_id})
                    new_count = 0
                    for record in study_result:
                        n = record["n"]
                        if n.id in seen_node_ids:
                            continue
                        seen_node_ids.add(n.id)
                        var = f"n{node_counter}"
                        node_vars[n.id] = var
                        node_counter += 1
                        new_count += 1
                        labels_str = format_labels(list(n.labels))
                        props_str = format_properties(dict(n))
                        # Write directly to output file
                        output_file.write(f"CREATE ({var}{labels_str} {props_str});\n")
                    logger.info(f"Added study node for {study_id}")
                    log_f.write(f"{study_id}\tstudy\t{new_count}\n")
                    log_f.flush()
                except Exception as e:
                    logger.error(f"Failed to fetch study node for {study_id}: {e}")
                    log_f.write(f"{study_id}\tstudy\tERROR\n")
                    log_f.flush()

                # Query one label at a time
                for label in labels:
                    logger.info(f"Processing label '{label}' for study {study_id}...")
                    query = """
                    MATCH (st:study {study_id : $study_id})-[*1..5]-(n)
                    WHERE $label IN labels(n)
                    RETURN DISTINCT n
                    """
                    try:
                        result = run_paginated_with_retry(session, query, {"study_id": study_id, "label": label})
                        new_count = 0

                        for record in result:
                            n = record["n"]
                            if n.id in seen_node_ids:
                                continue
                            seen_node_ids.add(n.id)
                            var = f"n{node_counter}"
                            node_vars[n.id] = var
                            node_counter += 1
                            new_count += 1
                            labels_str = format_labels(list(n.labels))
                            props_str = format_properties(dict(n))
                            # Write directly to output file
                            output_file.write(f"CREATE ({var}{labels_str} {props_str});\n")

                        output_file.flush()  # flush after each label
                        logger.info(f"Found {new_count} new nodes of label '{label}' for study {study_id}")
                        log_f.write(f"{study_id}\t{label}\t{new_count}\n")
                        log_f.flush()

                    except Exception as e:
                        logger.error(f"Failed to process label '{label}' for study {study_id}: {e}")
                        log_f.write(f"{study_id}\t{label}\tERROR\n")
                        log_f.flush()
                        continue

                    time.sleep(0.1)

    logger.info(f"Total unique nodes exported: {node_counter}")
    return log_file


@task(cache_policy=NO_CACHE, name="export_relationships", persist_result=False)
def export_relationships(driver, output_file, node_vars):
    """
    Exports relationships directly to the output file as they are fetched.
    Requires node_vars to be populated by export_nodes first.
    """
    logger = get_run_logger()
    logger.info("Fetching studies with promotion_status 'Promote'...")

    study_query = """
    MATCH (st:study)
    WHERE "Promote" IN st.promotion_status
    RETURN st.study_id AS study_id
    """
    with driver.session() as session:
        studies = run_paginated_with_retry(session, study_query)
    study_ids = [record["study_id"] for record in studies]
    logger.info(f"Found {len(study_ids)} studies to process: {study_ids}")

    seen_rel_ids = set()
    study_count = 0
    study_total = len(study_ids)
    log_file = f"relationships_export_{get_time()}.tsv"

    with open(log_file, "w") as log_f:
        log_f.write("study\trel_type\tcount\n")

        for study_id in study_ids:
            study_count += 1
            logger.info(f"Processing study {study_count}/{study_total}: {study_id}...")

            with driver.session() as session:

                # Get all relationship types present in this study
                rel_type_query = """
                MATCH (st:study {study_id : $study_id})-[*1..5]-(n)-[r]-(m)
                RETURN DISTINCT type(r) AS rel_type
                """
                try:
                    rel_types = [record["rel_type"] for record in run_paginated_with_retry(session, rel_type_query, {"study_id": study_id})]
                    logger.info(f"Found {len(rel_types)} relationship types for study {study_id}: {rel_types}")
                except Exception as e:
                    logger.error(f"Failed to fetch relationship types for study {study_id}: {e}")
                    continue

                # Query one relationship type at a time
                for rel_type in rel_types:
                    logger.info(f"Processing relationship type '{rel_type}' for study {study_id}...")
                    query = """
                    MATCH (st:study {study_id : $study_id})-[*1..5]-(n)
                    WITH DISTINCT n
                    MATCH (n)-[r]-(m)
                    WHERE type(r) = $rel_type
                    RETURN DISTINCT r, startNode(r) AS start_node, endNode(r) AS end_node
                    """
                    try:
                        result = run_paginated_with_retry(session, query, {"study_id": study_id, "rel_type": rel_type})
                        new_count = 0

                        for record in result:
                            r = record["r"]
                            start_node = record["start_node"]
                            end_node = record["end_node"]

                            if r.id in seen_rel_ids:
                                continue
                            seen_rel_ids.add(r.id)

                            start_var = node_vars.get(start_node.id)
                            end_var = node_vars.get(end_node.id)

                            if not start_var or not end_var:
                                logger.warning(f"Skipping relationship {r.id} — missing node var for start={start_node.id} or end={end_node.id}")
                                continue

                            new_count += 1
                            props_str = format_properties(dict(r))
                            # Write directly to output file
                            output_file.write(f"CREATE ({start_var})-[:{r.type} {props_str}]->({end_var});\n")

                        output_file.flush()  # flush after each rel_type
                        logger.info(f"Found {new_count} new relationships of type '{rel_type}' for study {study_id}")
                        log_f.write(f"{study_id}\t{rel_type}\t{new_count}\n")
                        log_f.flush()

                    except Exception as e:
                        logger.error(f"Failed to process rel_type '{rel_type}' for study {study_id}: {e}")
                        log_f.write(f"{study_id}\t{rel_type}\tERROR\n")
                        log_f.flush()
                        continue

                    time.sleep(0.1)

    logger.info(f"Relationships export complete.")
    return log_file


@task(cache_policy=NO_CACHE, name="export_indices", persist_result=False)
def export_indices(session):
    logger = get_run_logger()
    logger.info("Exporting index information...")
    result = list(session.run("SHOW INDEX INFO;").data())

    indices = []
    for record in result:
        indices.append(
            {
                "label": record["label"],
                "property": record["property"],
                "index_type": record["index type"],
            }
        )
    return indices


@task(cache_policy=NO_CACHE, name="export_memgraph_curation", persist_result=False)
def export_memgraph_curation(
    uri: str, username: str, password: str, output_file: str, chunk_size: int = 1000
) -> None:
    logger = get_run_logger()
    driver = GraphDatabase.driver(uri, auth=(username, password))
    logger.info("Connected to Memgraph")

    node_vars = {}  # shared map of node id -> cypher variable name

    with open(output_file, "w") as out_f:

        # --- WRITE NODES ---
        out_f.write("// --- NODES ---\n")
        node_log = export_nodes(driver, out_f, node_vars)

        # --- WRITE RELATIONSHIPS ---
        out_f.write("// --- RELATIONSHIPS ---\n")
        rel_log = export_relationships(driver, out_f, node_vars)

        # --- WRITE INDEXES ---
        out_f.write("// --- INDEXES ---\n")
        with driver.session() as session:
            indices = export_indices(session)
        for index in indices:
            label = index["label"]
            property = index["property"]
            index_type = index["index_type"].lower()

            if "edge" in index_type:
                out_f.write(f"CREATE EDGE INDEX ON :{label};\n")
            elif property:
                prop = property[0] if isinstance(property, list) else property
                out_f.write(f"CREATE INDEX ON :{label}({prop});\n")
            else:
                out_f.write(f"CREATE INDEX ON :{label};\n")

        out_f.flush()

    driver.close()
    logger.info("Memgraph connection closed.")
    logger.info(f"Export complete: {output_file}")

    return node_log, rel_log


# ------------------------------------------------------------------
# INTERNAL TASK: RUN QUERY CHUNKS
# ------------------------------------------------------------------
def _execute_batch(session, queries, logger):
    """Executes a batch of Cypher queries safely and logs errors individually."""
    success_count = 0

    for q in queries:
        try:
            session.run(q)
            success_count += 1
        except Exception as e:
            logger.warning(f"Failed query: {q[:120]}... Error: {e}")

    logger.info(
        f"Executed batch of {len(queries)} queries ({success_count} succeeded)."
    )
    return success_count


# ------------------------------------------------------------------
# INTERNAL TASK: WIPE DATABASE
# ------------------------------------------------------------------
@task(cache_policy=NO_CACHE, name="wipe_memgraph_database")
def _wipe_database(session, logger):
    """Deletes all nodes, relationships and indexes from the database."""
    logger.warning(
        "Wiping Memgraph database: deleting all nodes, relationships, and indexes..."
    )
    try:
        logger.info("Deleting all nodes and relationships...")
        session.run("MATCH (n) DETACH DELETE n;")

        logger.info("Dropping all indexes...")
        # Fetch all indexes
        indexes = list(session.run("SHOW INDEX INFO;").data())
        logger.info(f"Found {len(indexes)} indexes to drop.")
        # Drop each index based on its type
        for index in indexes:
            label = index["label"]
            property = index["property"]
            index_type = index["index type"].lower()

            logger.info(
                f"Dropping index: {index['index type']} on label: {label} property: {property}"
            )

            if "edge" in index_type.lower():
                # Edge index: CREATE EDGE INDEX ON :label
                query = f"DROP EDGE INDEX ON :{label};"
            elif property:
                prop = property[0] if isinstance(property, list) else property
                query = f"DROP INDEX ON :{label}({prop});"
            else:
                # Label index: CREATE INDEX ON :label
                query = f"DROP INDEX ON :{label};"

            logger.info(f"Running: {query}")
            session.run(query)

        logger.info("All indexes dropped.")
        logger.info(
            "Database wipe complete. All nodes, relationships, and indexes removed."
        )
    except Exception as e:
        logger.error(f"Error wiping database: {e}")
        raise


# ------------------------------------------------------------------
# TASK: IMPORT DATABASE
# ------------------------------------------------------------------
@task(cache_policy=NO_CACHE, name="import_memgraph")
def import_memgraph(
    uri: str,
    username: str,
    password: str,
    input_file: str,
    chunk_size: int = 500,
    wipe_db: bool = False,
) -> None:
    """
    Imports a CypherL dump into Memgraph in batches.
    Each line in the file is assumed to be a complete Cypher statement.
    If wipe_db=True, the existing database contents are deleted first.
    """
    logger = get_run_logger()
    logger.info(f"Connecting to Memgraph.")
    driver = GraphDatabase.driver(uri, auth=(username, password))

    total_lines = 0
    executed = 0
    errors = 0

    try:
        with driver.session() as session:
            # Wipe DB if flag is set
            if wipe_db:
                _wipe_database(session, logger)

            # Start import
            logger.info(f"Starting import with chunk size {chunk_size}")
            with open(input_file, "r", encoding="utf-8") as f:
                batch = []

                for line in f:
                    query = line.strip()
                    if not query:
                        continue  # skip empty lines

                    batch.append(query)
                    total_lines += 1

                    if len(batch) >= chunk_size:
                        executed += _execute_batch(session, batch, logger)
                        batch = []

                        if total_lines % (chunk_size * 10) == 0:
                            logger.info(f"Processed {total_lines} queries so far...")

                # Final flush
                if batch:
                    executed += _execute_batch(session, batch, logger)

    except Exception as e:
        logger.error(f"Fatal error during import: {e}")
        raise
    finally:
        driver.close()
        logger.info(
            f"Import complete — total lines read: {total_lines}, total executed: {executed}, total errors: {errors}"
        )
