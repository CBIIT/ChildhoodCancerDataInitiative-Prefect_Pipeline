import os
from typing import Any
from neo4j import GraphDatabase
from neo4j.exceptions import TransactionError
from prefect import flow, task, get_run_logger
from prefect.cache_policies import NO_CACHE
import json
from neo4j.time import DateTime, Date, Time, Duration
from requests import session
from src.utils import get_time
import time
import re


# ------------------------------------------------------------------
# TASK: EXPORT DATABASE
# ------------------------------------------------------------------
@flow(name="export_memgraph")
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


# # ------------------------------------------------------------------
# # TASK: EXPORT DATABASE FOR CURATION PROMOTION
# # ------------------------------------------------------------------
# def format_labels(labels):
#     return ":" + ":".join(labels) if labels else ""

# def format_properties(props):
#     """Convert dict to Cypher map string"""
#     if not props:
#         return "{}"
#     pairs = []
#     for k, v in props.items():
#         if v is None:
#             continue
#         elif isinstance(v, str):
#             v = v.replace('"', '\\"')
#             pairs.append(f'`{k}`: "{v}"')
#         elif isinstance(v, bool):
#             pairs.append(f'`{k}`: {str(v).lower()}')
#         elif isinstance(v, (int, float)):
#             pairs.append(f'`{k}`: {v}')
#         elif isinstance(v, DateTime):
#             # Strip nanoseconds to microseconds (6 decimal places) and append [Etc/UTC]
#             formatted = f"{v.year:04d}-{v.month:02d}-{v.day:02d}T{v.hour:02d}:{v.minute:02d}:{v.second:02d}.{v.nanosecond // 1000:06d}+00:00[Etc/UTC]"
#             pairs.append(f'`{k}`: DATETIME("{formatted}")')
#         elif isinstance(v, Date):
#             pairs.append(f'`{k}`: DATE("{v.iso_format()}")')
#         elif isinstance(v, Time):
#             pairs.append(f'`{k}`: TIME("{v.iso_format()}")')
#         elif isinstance(v, Duration):
#             pairs.append(f'`{k}`: DURATION("{str(v)}")')
#         elif isinstance(v, list):
#             serialized_items = []
#             for i in v:
#                 if isinstance(i, DateTime):
#                     # Strip nanoseconds to microseconds (6 decimal places) and append [Etc/UTC]
#                     formatted = f"{i.year:04d}-{i.month:02d}-{i.day:02d}T{i.hour:02d}:{i.minute:02d}:{i.second:02d}.{i.nanosecond // 1000:06d}+00:00[Etc/UTC]"
#                     serialized_items.append(f'DATETIME("{formatted}")')
#                 elif isinstance(i, Date):
#                     serialized_items.append(f'DATE("{i.iso_format()}")')
#                 elif isinstance(i, Time):
#                     serialized_items.append(f'TIME("{i.iso_format()}")')
#                 else:
#                     serialized_items.append(json.dumps(i))
#             pairs.append(f'`{k}`: [{", ".join(serialized_items)}]')
#         else:
#             pairs.append(f'`{k}`: {json.dumps(v)}')
#     return "{ " + ", ".join(pairs) + " }"


# def run_paginated_with_retry(session, query, params=None, page_size=5000, retries=3, delay=2):
#     """Increased default page_size from 1000 to 5000 for fewer round trips"""
#     logger = get_run_logger()
#     results = []
#     skip = 0
#     while True:
#         paginated_query = query + f" SKIP {skip} LIMIT {page_size}"
#         page = []
#         for attempt in range(retries):
#             try:
#                 page = list(session.run(paginated_query, params or {}))
#                 results.extend(page)
#                 break
#             except TransactionError as e:
#                 if attempt < retries - 1:
#                     logger.warning(f"Transaction error (attempt {attempt + 1}/{retries}) at SKIP {skip}, retrying in {delay * (attempt + 1)}s... Error: {e}")
#                     time.sleep(delay * (attempt + 1))
#                 else:
#                     logger.error(f"Transaction error after {retries} attempts at SKIP {skip}: {e}")
#                     return results
#             except Exception as e:
#                 if attempt < retries - 1:
#                     logger.warning(f"Query failed (attempt {attempt + 1}/{retries}) at SKIP {skip}, retrying in {delay}s... Error: {e}")
#                     time.sleep(delay)
#                 else:
#                     logger.error(f"Query failed after {retries} attempts at SKIP {skip}: {e}")
#                     raise e
#         if len(page) < page_size:
#             break
#         skip += page_size
#     return results  # removed time.sleep(0.1) between pages


# def get_promote_study_ids(driver):
#     """Fetch promote study IDs once and reuse across both export functions"""
#     study_query = """
#     MATCH (st:study)
#     WHERE "Promote" IN st.promotion_status
#     RETURN st.study_id AS study_id
#     """
#     with driver.session() as session:
#         studies = run_paginated_with_retry(session, study_query)
#     return [record["study_id"] for record in studies]


# @task(cache_policy=NO_CACHE, name="export_nodes", persist_result=False)
# def export_nodes(driver, output_file, node_vars, study_ids):
#     logger = get_run_logger()
#     seen_node_ids = set()
#     node_counter = 0
#     study_total = len(study_ids)
#     log_file = f"nodes_export_{get_time()}.tsv"

#     with open(log_file, "w") as log_f:
#         log_f.write("study\tnode\tcount\n")

#         for study_count, study_id in enumerate(study_ids, 1):
#             logger.info(f"Processing study {study_count}/{study_total}: {study_id}...")

#             with driver.session() as session:
#                 # Fetch ALL nodes for the study in one query instead of label by label
#                 node_query = """
#                 MATCH (st:study {study_id : $study_id})-[*1..6]-(n)
#                 RETURN DISTINCT n
#                 """
#                 # Also fetch the study node itself
#                 study_node_query = """
#                 MATCH (st:study {study_id : $study_id})
#                 RETURN st AS n
#                 """
#                 write_buffer = []
#                 label_counts = {}

#                 for query in [study_node_query, node_query]:
#                     try:
#                         result = run_paginated_with_retry(session, query, {"study_id": study_id})
#                         for record in result:
#                             n = record["n"]
#                             if n.id in seen_node_ids:
#                                 continue
#                             seen_node_ids.add(n.id)
#                             node_vars[n.id] = n.id
#                             node_counter += 1

#                             # Track counts per label for the log
#                             for lbl in n.labels:
#                                 label_counts[lbl] = label_counts.get(lbl, 0) + 1

#                             labels_str = "__mg_vertex__:" + ":".join([f"`{l}`" for l in list(n.labels)])
#                             props = dict(n)
#                             props["__mg_id__"] = n.id
#                             props_str = format_properties(props)
#                             write_buffer.append(f"CREATE (:{labels_str} {props_str});\n")

#                     except Exception as e:
#                         logger.error(f"Failed to fetch nodes for study {study_id}: {e}")
#                         continue

#                 # Write entire study's nodes in one batch
#                 output_file.writelines(write_buffer)
#                 output_file.flush()

#                 # Log counts per label
#                 for lbl, count in label_counts.items():
#                     log_f.write(f"{study_id}\t{lbl}\t{count}\n")
#                 log_f.flush()

#                 logger.info(f"Exported {len(write_buffer)} nodes for study {study_id}")

#     logger.info(f"Total unique nodes exported: {node_counter}")
#     return log_file


# @task(cache_policy=NO_CACHE, name="export_relationships", persist_result=False)
# def export_relationships(driver, output_file, node_vars, study_ids):
#     logger = get_run_logger()
#     seen_rel_ids = set()
#     study_total = len(study_ids)
#     log_file = f"relationships_export_{get_time()}.tsv"

#     with open(log_file, "w") as log_f:
#         log_f.write("study\trel_type\tcount\n")

#         for study_count, study_id in enumerate(study_ids, 1):
#             logger.info(f"Processing study {study_count}/{study_total}: {study_id}...")

#             with driver.session() as session:
#                 # Get all relationship types for this study
#                 rel_type_query = """
#                 MATCH (st:study {study_id : $study_id})-[*1..6]-(n)-[r]-(m)
#                 RETURN DISTINCT type(r) AS rel_type
#                 """
#                 try:
#                     rel_types = [record["rel_type"] for record in run_paginated_with_retry(session, rel_type_query, {"study_id": study_id})]
#                     logger.info(f"Found {len(rel_types)} relationship types for study {study_id}: {rel_types}")
#                 except Exception as e:
#                     logger.error(f"Failed to fetch relationship types for study {study_id}: {e}")
#                     continue

#                 for rel_type in rel_types:
#                     query = """
#                     MATCH (st:study {study_id : $study_id})-[*1..6]-(n)
#                     WITH DISTINCT n
#                     MATCH (n)-[r]->(m)
#                     WHERE type(r) = $rel_type
#                     RETURN DISTINCT r, startNode(r) AS start_node, endNode(r) AS end_node
#                     """
#                     try:
#                         result = run_paginated_with_retry(session, query, {"study_id": study_id, "rel_type": rel_type})
#                         write_buffer = []
#                         new_count = 0

#                         for record in result:
#                             r = record["r"]
#                             start_node = record["start_node"]
#                             end_node = record["end_node"]

#                             if r.id in seen_rel_ids:
#                                 continue
#                             seen_rel_ids.add(r.id)

#                             if start_node.id not in node_vars or end_node.id not in node_vars:
#                                 logger.warning(f"Skipping relationship {r.id} — missing node for start={start_node.id} or end={end_node.id}")
#                                 continue

#                             new_count += 1
#                             props_str = format_properties(dict(r))
#                             write_buffer.append(
#                                 f"MATCH (u:__mg_vertex__), (v:__mg_vertex__) "
#                                 f"WHERE u.__mg_id__ = {start_node.id} AND v.__mg_id__ = {end_node.id} "
#                                 f"CREATE (u)-[:`{rel_type}` {props_str}]->(v);\n"
#                             )

#                         # Write entire rel_type batch in one go
#                         output_file.writelines(write_buffer)
#                         output_file.flush()
#                         logger.info(f"Found {new_count} new relationships of type '{rel_type}' for study {study_id}")
#                         log_f.write(f"{study_id}\t{rel_type}\t{new_count}\n")
#                         log_f.flush()

#                     except Exception as e:
#                         logger.error(f"Failed to process rel_type '{rel_type}' for study {study_id}: {e}")
#                         log_f.write(f"{study_id}\t{rel_type}\tERROR\n")
#                         log_f.flush()
#                         continue

#     logger.info(f"Relationships export complete.")
#     return log_file


# @flow(name="export_indices", persist_result=False)
# def export_indices(session):
#     logger = get_run_logger()
#     logger.info("Exporting index information...")
#     result = list(session.run("SHOW INDEX INFO;").data())
#     return [
#         {
#             "label": record["label"],
#             "property": record["property"],
#             "index_type": record["index type"],
#         }
#         for record in result
#     ]


# @flow(name="export_memgraph_curation", persist_result=False)
# def export_memgraph_curation(
#     uri: str, username: str, password: str, output_file: str, chunk_size: int = 1000
# ) -> None:
#     logger = get_run_logger()
#     driver = GraphDatabase.driver(uri, auth=(username, password))
#     logger.info("Connected to Memgraph")

#     # Fetch study IDs once and pass to both functions
#     study_ids = get_promote_study_ids(driver)
#     logger.info(f"Found {len(study_ids)} studies to export: {study_ids}")

#     node_vars = {}

#     with open(output_file, "w") as out_f:

#         # --- WRITE NODES ---
#         out_f.write("// --- NODES ---\n")
#         out_f.write("CREATE INDEX ON :__mg_vertex__(__mg_id__);\n")
#         node_log = export_nodes(driver, out_f, node_vars, study_ids)

#         # --- WRITE RELATIONSHIPS ---
#         out_f.write("// --- RELATIONSHIPS ---\n")
#         rel_log = export_relationships(driver, out_f, node_vars, study_ids)

#         # --- WRITE INDEXES ---
#         out_f.write("// --- INDEXES ---\n")
#         with driver.session() as session:
#             indices = export_indices(session)
#         for index in indices:
#             label = index["label"]
#             property = index["property"]
#             index_type = index["index_type"].lower()

#             if "edge" in index_type:
#                 out_f.write(f"CREATE EDGE INDEX ON :{label};\n")
#             elif property:
#                 prop = property[0] if isinstance(property, list) else property
#                 out_f.write(f"CREATE INDEX ON :{label}({prop});\n")
#             else:
#                 out_f.write(f"CREATE INDEX ON :{label};\n")

#         # --- CLEANUP ---
#         out_f.write("// --- CLEANUP ---\n")
#         out_f.write("DROP INDEX ON :__mg_vertex__(__mg_id__);\n")
#         out_f.write("MATCH (u) REMOVE u:__mg_vertex__, u.__mg_id__;\n")

#         out_f.flush()

#     driver.close()
#     logger.info("Memgraph connection closed.")
#     logger.info(f"Export complete: {output_file}")

#     return node_log, rel_log

# ------------------------------------------------------------------
# CONSTANTS - adjust these as needed
# ------------------------------------------------------------------
FILTER_LABEL = "study"
FILTER_PROPERTY = "promotion_status"
FILTER_VALUE = "Promote"


# ------------------------------------------------------------------
# HELPER FUNCTIONS
# ------------------------------------------------------------------

def parse_mg_id(line):
    """Extract __mg_id__ value from a CREATE node line"""
    match = re.search(r'`__mg_id__`:\s*(\d+)', line)
    if match:
        return int(match.group(1))
    return None


def parse_node_labels(line):
    """Extract labels from a CREATE node line"""
    match = re.search(r'CREATE\s*\(:([\w`: ]+)\s*\{', line)
    if match:
        labels_str = match.group(1)
        labels = [l.strip().strip('`') for l in labels_str.split(':') if l.strip()]
        return labels
    return []


def parse_node_property(line, property_name):
    """Extract a specific property value from a CREATE node line"""
    match = re.search(rf'`{property_name}`:\s*"([^"]*)"', line)
    if match:
        return match.group(1)
    match = re.search(rf'`{property_name}`:\s*(\[[^\]]*\])', line)
    if match:
        try:
            return json.loads(match.group(1))
        except Exception:
            return match.group(1)
    return None


def parse_node_study_id(line):
    """Extract study_id property from a CREATE node line"""
    match = re.search(r'`study_id`:\s*"([^"]*)"', line)
    if match:
        return match.group(1)
    return None


def node_matches_filter(line, filter_label, filter_property, filter_value):
    """Check if a CREATE node line matches the filter criteria"""
    labels = parse_node_labels(line)
    if filter_label not in labels:
        return False
    value = parse_node_property(line, filter_property)
    if isinstance(value, list):
        return filter_value in value
    return value == filter_value


def parse_relationship_mg_ids(line):
    """Extract u.__mg_id__ and v.__mg_id__ from a MATCH/CREATE relationship line"""
    match = re.search(r'u\.__mg_id__\s*=\s*(\d+).*?v\.__mg_id__\s*=\s*(\d+)', line)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None, None


def parse_relationship_type(line):
    """Extract relationship type from a MATCH/CREATE relationship line"""
    match = re.search(r'CREATE \(u\)-\[:`([^`]+)`', line)
    if match:
        return match.group(1)
    return "unknown"


# ------------------------------------------------------------------
# TASK: DUMP DATABASE TO FILE
# ------------------------------------------------------------------

@task(cache_policy=NO_CACHE, name="dump_database", persist_result=False)
def dump_database(uri: str, username: str, password: str, dump_file: str):
    """
    Dumps the entire Memgraph database to a cypherl file using DUMP DATABASE.
    Streams line by line to avoid memory issues with large databases.
    """
    logger = get_run_logger()
    logger.info(f"Starting DUMP DATABASE to {dump_file}...")

    driver = GraphDatabase.driver(uri, auth=(username, password))
    lines_written = 0

    try:
        with driver.session() as session, open(dump_file, "w") as f:
            result = session.run("DUMP DATABASE;")
            for record in result:
                line = record[0]
                f.write(line + "\n")
                lines_written += 1
                if lines_written % 10000 == 0:
                    logger.info(f"Dumped {lines_written} lines so far...")
    finally:
        driver.close()

    logger.info(f"Dump complete. Total lines written: {lines_written} -> {dump_file}")
    return dump_file


# ------------------------------------------------------------------
# TASK: FILTER CYPHERL FILE FOR CURATION PROMOTION
# ------------------------------------------------------------------

@task(cache_policy=NO_CACHE, name="filter_cypherl", persist_result=False)
def filter_cypherl(
    input_file: str,
    output_file: str,
    filter_label: str = FILTER_LABEL,
    filter_property: str = FILTER_PROPERTY,
    filter_value: str = FILTER_VALUE,
):
    """
    Filters a Memgraph DUMP DATABASE cypherl file to only include nodes
    and relationships connected to qualifying studies.

    Pass 1 - Find all qualifying study mg_ids and study_ids
    Pass 2 - Find all node mg_ids connected to those studies via BFS
    Pass 3 - Write out only matching nodes, relationships, indexes, and cleanup
    """
    logger = get_run_logger()
    logger.info(f"Filtering {input_file} -> {output_file}")
    logger.info(f"Filter: {filter_label}.{filter_property} contains '{filter_value}'")

    timestamp = get_time()
    node_log_file = f"nodes_export_{timestamp}.tsv"
    rel_log_file = f"relationships_export_{timestamp}.tsv"
    study_log_file = f"studies_export_{timestamp}.tsv"

    # ------------------------------------------------------------------
    # PASS 1: Find all qualifying and excluded study node mg_ids
    # ------------------------------------------------------------------
    logger.info("Pass 1: Finding qualifying study nodes...")
    qualifying_study_mg_ids = set()
    excluded_study_mg_ids = set()

    # mg_id -> study_id mapping for logging
    all_study_mg_id_to_study_id = {}

    with open(input_file, "r") as f:
        for line in f:
            stripped = line.strip()
            if not stripped.startswith("CREATE (:"):
                continue
            labels = parse_node_labels(stripped)
            if filter_label not in labels:
                continue
            mg_id = parse_mg_id(stripped)
            study_id = parse_node_study_id(stripped)
            if mg_id is None:
                continue
            all_study_mg_id_to_study_id[mg_id] = study_id or f"unknown_mg_id_{mg_id}"
            if node_matches_filter(stripped, filter_label, filter_property, filter_value):
                qualifying_study_mg_ids.add(mg_id)
            else:
                excluded_study_mg_ids.add(mg_id)

    logger.info(f"Found {len(qualifying_study_mg_ids)} qualifying studies, {len(excluded_study_mg_ids)} excluded studies")

    # Write study log
    with open(study_log_file, "w") as f:
        f.write("study_id\tmg_id\tstatus\n")
        for mg_id in qualifying_study_mg_ids:
            f.write(f"{all_study_mg_id_to_study_id[mg_id]}\t{mg_id}\tincluded\n")
        for mg_id in excluded_study_mg_ids:
            f.write(f"{all_study_mg_id_to_study_id[mg_id]}\t{mg_id}\texcluded\n")

    if not qualifying_study_mg_ids:
        logger.warning("No qualifying study nodes found — output file will be empty.")
        return output_file, node_log_file, rel_log_file, study_log_file

    # ------------------------------------------------------------------
    # PASS 2: Find all node mg_ids connected to qualifying studies via BFS
    # ------------------------------------------------------------------
    logger.info("Pass 2: Finding all connected node mg_ids...")

    adjacency = {}
    with open(input_file, "r") as f:
        for line in f:
            stripped = line.strip()
            if not stripped.startswith("MATCH (u:__mg_vertex__)"):
                continue
            u_id, v_id = parse_relationship_mg_ids(stripped)
            if u_id is None or v_id is None:
                continue
            adjacency.setdefault(u_id, set()).add(v_id)
            adjacency.setdefault(v_id, set()).add(u_id)

    # BFS flood fill from qualifying study nodes
    connected_mg_ids = set(qualifying_study_mg_ids)
    frontier = set(qualifying_study_mg_ids)

    while frontier:
        next_frontier = set()
        for mg_id in frontier:
            for neighbor in adjacency.get(mg_id, set()):
                if neighbor not in connected_mg_ids:
                    connected_mg_ids.add(neighbor)
                    next_frontier.add(neighbor)
        frontier = next_frontier

    logger.info(f"Found {len(connected_mg_ids)} total connected nodes to export")

    # ------------------------------------------------------------------
    # PASS 3: Write filtered output and log files
    # ------------------------------------------------------------------
    logger.info("Pass 3: Writing filtered output...")

    nodes_written = 0
    nodes_skipped = 0
    rels_written = 0
    rels_skipped = 0

    # Track counts per label and rel_type per study for log files
    # mg_id -> study_id for connected nodes
    mg_id_to_study_id = {mg_id: all_study_mg_id_to_study_id[mg_id] for mg_id in qualifying_study_mg_ids}
    node_label_counts = {}  # (study_id, label) -> count
    rel_type_counts = {}    # (study_id, rel_type) -> count

    with open(input_file, "r") as in_f, open(output_file, "w") as out_f:
        for line in in_f:
            stripped = line.strip()

            # Always keep non-data lines
            if (
                not stripped
                or stripped.startswith("//")
                or stripped.startswith("CREATE INDEX")
                or stripped.startswith("CREATE EDGE INDEX")
                or stripped.startswith("DROP INDEX")
                or stripped.startswith("MATCH (u) REMOVE")
            ):
                out_f.write(line)
                continue

            # Filter node lines
            if stripped.startswith("CREATE (:"):
                mg_id = parse_mg_id(stripped)
                if mg_id is not None and mg_id in connected_mg_ids:
                    out_f.write(line)
                    nodes_written += 1

                    # Track label counts — attribute node to its study
                    labels = parse_node_labels(stripped)
                    # Find which study this node belongs to via adjacency
                    for label in labels:
                        if label in ("__mg_vertex__",):
                            continue
                        # Find the closest qualifying study
                        for study_mg_id, study_id in mg_id_to_study_id.items():
                            key = (study_id, label)
                            node_label_counts[key] = node_label_counts.get(key, 0) + 1
                            break  # attribute to first matching study to avoid duplication
                else:
                    nodes_skipped += 1
                continue

            # Filter relationship lines
            if stripped.startswith("MATCH (u:__mg_vertex__)"):
                u_id, v_id = parse_relationship_mg_ids(stripped)
                if u_id in connected_mg_ids and v_id in connected_mg_ids:
                    out_f.write(line)
                    rels_written += 1

                    # Track rel_type counts
                    rel_type = parse_relationship_type(stripped)
                    for study_mg_id, study_id in mg_id_to_study_id.items():
                        key = (study_id, rel_type)
                        rel_type_counts[key] = rel_type_counts.get(key, 0) + 1
                        break
                else:
                    rels_skipped += 1
                continue

            # Write anything else as-is
            out_f.write(line)

    # Write node log
    with open(node_log_file, "w") as f:
        f.write("study\tnode\tcount\n")
        for (study_id, label), count in sorted(node_label_counts.items()):
            f.write(f"{study_id}\t{label}\t{count}\n")

    # Write relationship log
    with open(rel_log_file, "w") as f:
        f.write("study\trel_type\tcount\n")
        for (study_id, rel_type), count in sorted(rel_type_counts.items()):
            f.write(f"{study_id}\t{rel_type}\t{count}\n")

    logger.info(f"Nodes written: {nodes_written}, skipped: {nodes_skipped}")
    logger.info(f"Relationships written: {rels_written}, skipped: {rels_skipped}")
    logger.info(f"Node log: {node_log_file}")
    logger.info(f"Relationship log: {rel_log_file}")
    logger.info(f"Study log: {study_log_file}")

    return output_file, node_log_file, rel_log_file, study_log_file


# ------------------------------------------------------------------
# FLOW: EXPORT MEMGRAPH CURATION FILTERED FILE
# ------------------------------------------------------------------

@flow(name="export_memgraph_curation_filtered_file", persist_result=False)
def export_memgraph_curation_filtered_file(
    uri: str,
    username: str,
    password: str,
    output_file: str,
    filter_label: str = FILTER_LABEL,
    filter_property: str = FILTER_PROPERTY,
    filter_value: str = FILTER_VALUE,
) -> tuple:
    """
    Main flow:
    1. Dumps the entire Memgraph database to a temp cypherl file
    2. Filters it down to only nodes and relationships connected to qualifying studies
    3. Returns the filtered output file and log files
    """
    logger = get_run_logger()

    # Derive the dump file name from the output file name
    base, ext = os.path.splitext(output_file)
    dump_file = f"{base}_full_dump{ext}"

    logger.info(f"Step 1: Dumping database to {dump_file}...")
    dump_database(uri, username, password, dump_file)

    logger.info(f"Step 2: Filtering dump to {output_file}...")
    output_file, node_log, rel_log, study_log = filter_cypherl(
        input_file=dump_file,
        output_file=output_file,
        filter_label=filter_label,
        filter_property=filter_property,
        filter_value=filter_value,
    )

    logger.info(f"Export complete.")
    logger.info(f"Filtered file: {output_file}")
    logger.info(f"Node log:      {node_log}")
    logger.info(f"Rel log:       {rel_log}")
    logger.info(f"Study log:     {study_log}")

    return output_file, node_log, rel_log, study_log


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
@flow(name="wipe_memgraph_database")
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
@flow(name="import_memgraph")
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

                    if query.startswith("//"):
                        logger.info(f"Skipping comment line: {query}")
                        continue  # skip comment lines

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
