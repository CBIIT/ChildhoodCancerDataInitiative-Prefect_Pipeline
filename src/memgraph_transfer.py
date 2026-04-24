from typing import Any
from neo4j import GraphDatabase
from prefect import task, get_run_logger
from prefect.cache_policies import NO_CACHE
import json
from src.utils import get_time


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


@task(cache_policy=NO_CACHE, name="export_nodes")
def export_nodes(session):
    logger = get_run_logger()
    logger.info("Fetching studies with promotion_status 'Promote'...")

    study_query = """
    MATCH (st:study)
    WHERE "Promote" IN st.promotion_status
    RETURN st.study_id AS study_id
    """
    studies = list(session.run(study_query))
    study_ids = [record["study_id"] for record in studies]
    logger.info(f"Found {len(study_ids)} studies to process: {study_ids}")

    nodes = []
    seen_node_ids = set()
    log_file = f"nodes_export_{get_time()}.tsv"

    with open(log_file, "w") as f:
        f.write("study\tnode\tcount\n")

        for study_id in study_ids:
            logger.info(f"Processing nodes for study: {study_id}...")

            # Step 2: Get all distinct node labels present in this study
            label_query = """
            MATCH (st:study {study_id : $study_id})-[*1..6]-(n)
            UNWIND labels(n) AS label
            RETURN DISTINCT label
            """
            try:
                labels = [
                    record["label"]
                    for record in session.run(label_query, study_id=study_id)
                ]
                logger.info(
                    f"Found {len(labels)} node labels for study {study_id}: {labels}"
                )
            except Exception as e:
                logger.error(f"Failed to fetch node labels for study {study_id}: {e}")
                continue

            # Add the study node itself
            study_node_query = """
            MATCH (st:study {study_id : $study_id})
            RETURN st AS n
            """
            try:
                study_result = list(session.run(study_node_query, study_id=study_id))
                new_count = 0
                for record in study_result:
                    n = record["n"]
                    if n.id in seen_node_ids:
                        continue
                    seen_node_ids.add(n.id)
                    new_count += 1
                    nodes.append({
                        "id": n.id,
                        "labels": list(n.labels),
                        "properties": dict(n)
                    })
                logger.info(f"Added study node for {study_id}")
                f.write(f"{study_id}\tstudy\t{new_count}\n")
            except Exception as e:
                logger.error(f"Failed to fetch study node for {study_id}: {e}")
                f.write(f"{study_id}\tstudy\tERROR\n")

            # Step 3: Query one label at a time
            for label in labels:
                logger.info(f"Processing label '{label}' for study {study_id}...")
                query = """
                MATCH (st:study {study_id : $study_id})-[*1..6]-(n)
                WHERE $label IN labels(n)
                RETURN DISTINCT n
                """
                try:
                    result = list(session.run(query, study_id=study_id, label=label))
                    new_count = 0

                    for record in result:
                        n = record["n"]

                        if n.id in seen_node_ids:
                            continue
                        seen_node_ids.add(n.id)
                        new_count += 1

                        try:
                            nodes.append(
                                {
                                    "id": n.id,
                                    "labels": list(n.labels),
                                    "properties": dict(n),
                                }
                            )
                        except Exception as e:
                            logger.warning(
                                f"Failed to process node: {record}. Error: {e}"
                            )

                    logger.info(
                        f"Found {new_count} new nodes of label '{label}' for study {study_id}"
                    )
                    f.write(f"{study_id}\t{label}\t{new_count}\n")

                except Exception as e:
                    logger.error(
                        f"Failed to process label '{label}' for study {study_id}: {e}"
                    )
                    f.write(f"{study_id}\t{label}\tERROR\n")
                    continue

    logger.info(f"Total unique nodes exported: {len(nodes)}")
    return nodes, log_file


@task(cache_policy=NO_CACHE, name="export_relationships")
def export_relationships(session):
    logger = get_run_logger()
    logger.info("Fetching studies with promotion_status 'Promote'...")

    study_query = """
    MATCH (st:study)
    WHERE "Promote" IN st.promotion_status
    RETURN st.study_id AS study_id
    """
    studies = list(session.run(study_query))
    study_ids = [record["study_id"] for record in studies]
    logger.info(f"Found {len(study_ids)} studies to process: {study_ids}")

    rels = []
    seen_rel_ids = set()
    log_file = f"relationships_export_{get_time()}.tsv"

    with open(log_file, "w") as f:
        f.write("study\trel_type\tcount\n")

        for study_id in study_ids:
            logger.info(f"Processing relationships for study: {study_id}...")

            # Step 2: Get all relationship types present in this study
            rel_type_query = """
            MATCH (st:study {study_id : $study_id})-[*1..6]-(n)-[r]-(m)
            RETURN DISTINCT type(r) AS rel_type
            """
            try:
                rel_types = [
                    record["rel_type"]
                    for record in session.run(rel_type_query, study_id=study_id)
                ]
                logger.info(
                    f"Found {len(rel_types)} relationship types for study {study_id}: {rel_types}"
                )
            except Exception as e:
                logger.error(
                    f"Failed to fetch relationship types for study {study_id}: {e}"
                )
                continue

            # Step 3: Query one relationship type at a time
            for rel_type in rel_types:
                logger.info(
                    f"Processing relationship type '{rel_type}' for study {study_id}..."
                )
                query = """
                MATCH (st:study {study_id : $study_id})-[*1..6]-(n)
                WITH DISTINCT n
                MATCH (n)-[r]-(m)
                WHERE type(r) = $rel_type
                RETURN DISTINCT r, startNode(r) AS start_node, endNode(r) AS end_node
                """
                try:
                    result = list(
                        session.run(query, study_id=study_id, rel_type=rel_type)
                    )
                    new_count = 0

                    for record in result:
                        r = record["r"]
                        start_node = record["start_node"]
                        end_node = record["end_node"]

                        if r.id in seen_rel_ids:
                            continue
                        seen_rel_ids.add(r.id)
                        new_count += 1

                        try:
                            rels.append(
                                {
                                    "start": start_node.id,
                                    "end": end_node.id,
                                    "type": r.type,
                                    "properties": dict(r),
                                }
                            )
                        except Exception as e:
                            logger.warning(
                                f"Failed to process relationship: {record}. Error: {e}"
                            )

                    logger.info(
                        f"Found {new_count} new relationships of type '{rel_type}' for study {study_id}"
                    )
                    f.write(f"{study_id}\t{rel_type}\t{new_count}\n")

                except Exception as e:
                    logger.error(
                        f"Failed to process rel_type '{rel_type}' for study {study_id}: {e}"
                    )
                    f.write(f"{study_id}\t{rel_type}\tERROR\n")
                    continue

    logger.info(f"Total unique relationships exported: {len(rels)}")
    return rels, log_file


@task(cache_policy=NO_CACHE, name="export_indices")
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


@task(cache_policy=NO_CACHE, name="export_memgraph_curation")
def export_memgraph_curation(
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

    with driver.session() as session:
        nodes, node_log = session.execute_read(export_nodes)
        rels, rel_log = session.execute_read(export_relationships)

    # Map old IDs to variable names
    node_vars = {}
    cypher_lines = []
    total_statements = 0

    # --- CREATE NODES ---
    for i, node in enumerate(nodes):
        var = f"n{i}"
        node_vars[node["id"]] = var

        labels = format_labels(node["labels"])
        props = format_properties(node["properties"])

        cypher_lines.append(f"CREATE ({var}{labels} {props});")
        total_statements += 1

    # --- CREATE RELATIONSHIPS ---
    for rel in rels:
        start_var = node_vars.get(rel["start"])
        end_var = node_vars.get(rel["end"])

        if not start_var or not end_var:
            continue  # skip if node missing

        props = format_properties(rel["properties"])

        cypher_lines.append(
            f"CREATE ({start_var})-[:{rel['type']} {props}]->({end_var});"
        )
        total_statements += 1

    # --- CREATE INDEXES ---
    indices = session.execute_read(export_indices)
    logger.info(indices)
    for index in indices:
        label = index["label"]
        property = index["property"]
        index_type = index["index_type"].lower()

        if "edge" in index_type:
            # CREATE EDGE INDEX ON :label
            cypher_lines.append(f"CREATE EDGE INDEX ON :{label};")
        elif property:
            # Handle property being either a list or a string
            prop = property[0] if isinstance(property, list) else property
            cypher_lines.append(f"CREATE INDEX ON :{label}({prop});")
        else:
            # CREATE INDEX ON :label
            cypher_lines.append(f"CREATE INDEX ON :{label};")

        total_statements += 1

    # --- WRITE FILE ---
    with open(output_file, "w") as f:
        for line in cypher_lines:
            f.write(line + "\n")

        logger.info(
            f"Export complete {output_file}.\n\n Total statements exported: {total_statements}"
        )

    driver.close()
    logger.info("Memgraph connection closed.")

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
