from typing import Any
from neo4j import GraphDatabase
from prefect import task, get_run_logger
from prefect.cache_policies import NO_CACHE
import json


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


def export_nodes(tx):
    query = """
    MATCH (st:study {promotion_status: "Promote"})-[*0..]-(n)
    RETURN DISTINCT n
    """
    result = tx.run(query)

    nodes = []
    for record in result:
        n = record["n"]
        nodes.append({"id": n.id, "labels": list(n.labels), "properties": dict(n)})
    return nodes


def export_relationships(tx):
    query = """
    MATCH (st:study {promotion_status: "Promote"})-[r]->(n)
    RETURN DISTINCT r
    """
    result = tx.run(query)

    rels = []
    for record in result:
        r = record["r"]
        rels.append(
            {
                "start": r.start_node.id,
                "end": r.end_node.id,
                "type": r.type,
                "properties": dict(r),
            }
        )
    return rels

def export_indices(tx):
    result = list(tx.run("SHOW INDEX INFO;").data())
    indices = []
    for record in result:
        indices.append({
            "label": record["label"],
            "property": record["property"],
            "type": record["type"]
        })
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
        nodes = session.execute_read(export_nodes)
        rels = session.execute_read(export_relationships)

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
    for index in indices:
        label = index["label"]
        property = index["property"]
        index_type = index["type"].lower()

        if "edge" in index_type:
            # CREATE EDGE INDEX ON :label
            cypher_lines.append(f"CREATE EDGE INDEX ON :{label};")
        elif property:
            # CREATE INDEX ON :label(property)
            cypher_lines.append(f"CREATE INDEX ON :{label}({property});")
        else:
            # CREATE INDEX ON :label
            cypher_lines.append(f"CREATE INDEX ON :{label};")

        total_statements += 1

    # --- WRITE FILE ---
    with open(output_file, "w") as f:
        for line in cypher_lines:
            f.write(line + "\n")

        logger.info(
            f"Export complet {output_file}.\n\n Total statements exported: {total_statements}"
        )

    driver.close()
    logger.info("Memgraph connection closed.")


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
def _wipe_database(session, logger):
    """Deletes all nodes, relationships and indexes from the database."""
    logger.warning("Wiping Memgraph database: deleting all nodes, relationships, and indexes...")
    try:
        session.run("MATCH (n) DETACH DELETE n;")
        
        # Fetch all indexes
        indexes = list(session.run("SHOW INDEX INFO;").data())

        # Drop each index based on its type
        for index in indexes:
            label = index["label"]
            property = index["property"]
            index_type = index["type"]

            if "edge" in index_type.lower():
                # Edge index: CREATE EDGE INDEX ON :label
                query = f"DROP EDGE INDEX ON :{label};"
            elif property:
                # Label-property index: CREATE INDEX ON :label(property)
                query = f"DROP INDEX ON :{label}({property});"
            else:
                # Label index: CREATE INDEX ON :label
                query = f"DROP INDEX ON :{label};"

            print(f"Running: {query}")
            session.run(query)

        print("All indexes dropped.")
        logger.info("Database wipe complete. All nodes, relationships, and indexes removed.")
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
