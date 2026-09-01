import requests
import pandas as pd
import os
from prefect import flow, task, get_run_logger
from prefect.input import RunInput
from src.utils import get_secret_centralized_worker, get_time, file_dl, file_ul
from meval.parser import ModelParser
from bento_mdf import MDFReader
from bento_mdf.diff import diff_models
from dataclasses import dataclass, field
from neo4j import GraphDatabase, basic_auth


class InputValues(RunInput):
    node: str
    property: str


# ── dataclasses ───────────────────────────────────────────────────────────────

@dataclass
class PropertyRecord:
    """Represents a single property within a node."""
    name: str
    prop_type: str
    required: bool
    is_key: bool
    value_set_terms: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "property":        self.name,
            "type":            self.prop_type,
            "required":        self.required,
            "is_key":          self.is_key,
            "value_set_terms": ";".join(self.value_set_terms) if self.value_set_terms else "",
        }


@dataclass
class NodeRecord:
    """Represents a single node and all its properties and relationships."""
    name: str
    properties: dict[str, PropertyRecord] = field(default_factory=dict)
    parent_nodes: list[str] = field(default_factory=list)

    def get_property(self, prop_name: str) -> PropertyRecord | None:
        return self.properties.get(prop_name)

    def to_dict(self) -> list[dict]:
        rows = []
        for prop in self.properties.values():
            row = {"node": self.name, "parent_nodes": ";".join(self.parent_nodes)}
            row.update(prop.to_dict())
            rows.append(row)
        return rows


@dataclass
class ModelSnapshot:
    """Represents a full parsed snapshot of a data model at a given version."""
    version: str
    nodes: dict[str, NodeRecord] = field(default_factory=dict)

    def get_node(self, node_name: str) -> NodeRecord | None:
        return self.nodes.get(node_name)

    def get_property(self, node_name: str, prop_name: str) -> PropertyRecord | None:
        node = self.get_node(node_name)
        return node.get_property(prop_name) if node else None

    def get_all_node_names(self) -> list[str]:
        return list(self.nodes.keys())

    def get_all_properties(self) -> list[tuple[str, str]]:
        return [
            (node_name, prop_name)
            for node_name, node in self.nodes.items()
            for prop_name in node.properties
        ]

    def to_dataframe(self) -> pd.DataFrame:
        rows = []
        for node in self.nodes.values():
            rows.extend(node.to_dict())
        df = pd.DataFrame(rows)
        df["version"] = self.version
        return df

    def compare(self, other: "ModelSnapshot") -> pd.DataFrame:
        from_keys = set(self.get_all_properties())
        to_keys = set(other.get_all_properties())
        all_keys = from_keys | to_keys
        all_node_names = set(self.nodes.keys()) | set(other.nodes.keys())

        rows = []

        for node_name, prop_name in sorted(all_keys):
            from_prop = self.get_property(node_name, prop_name)
            to_prop = other.get_property(node_name, prop_name)

            if from_prop and not to_prop:
                rows.append({
                    "node":                 node_name,
                    "property":             prop_name,
                    "change_type":          "DELETION",
                    "from_type":            from_prop.prop_type,
                    "to_type":              "",
                    "from_required":        from_prop.required,
                    "to_required":          "",
                    "from_is_key":          from_prop.is_key,
                    "to_is_key":            "",
                    "from_value_set_terms": ";".join(from_prop.value_set_terms),
                    "to_value_set_terms":   "",
                    "from_version":         self.version,
                    "to_version":           other.version,
                })
                continue

            if not from_prop and to_prop:
                rows.append({
                    "node":                 node_name,
                    "property":             prop_name,
                    "change_type":          "ADDITION",
                    "from_type":            "",
                    "to_type":              to_prop.prop_type,
                    "from_required":        "",
                    "to_required":          to_prop.required,
                    "from_is_key":          "",
                    "to_is_key":            to_prop.is_key,
                    "from_value_set_terms": "",
                    "to_value_set_terms":   ";".join(to_prop.value_set_terms),
                    "from_version":         self.version,
                    "to_version":           other.version,
                })
                continue

            changes = []
            if from_prop.prop_type != to_prop.prop_type:
                changes.append("type")
            if from_prop.required != to_prop.required:
                changes.append("required")
            if from_prop.is_key != to_prop.is_key:
                changes.append("is_key")

            from_terms = set(from_prop.value_set_terms)
            to_terms = set(to_prop.value_set_terms)
            removed_terms = from_terms - to_terms
            added_terms = to_terms - from_terms
            if removed_terms or added_terms:
                changes.append("value_set_terms")

            if not changes:
                continue

            rows.append({
                "node":                 node_name,
                "property":             prop_name,
                "change_type":          "CHANGED:" + ",".join(changes),
                "from_type":            from_prop.prop_type,
                "to_type":              to_prop.prop_type,
                "from_required":        from_prop.required,
                "to_required":          to_prop.required,
                "from_is_key":          from_prop.is_key,
                "to_is_key":            to_prop.is_key,
                "from_value_set_terms": ";".join(sorted(removed_terms)),
                "to_value_set_terms":   ";".join(sorted(added_terms)),
                "from_version":         self.version,
                "to_version":           other.version,
            })

        for node_name in sorted(all_node_names):
            from_node = self.get_node(node_name)
            to_node = other.get_node(node_name)

            from_parents = set(from_node.parent_nodes) if from_node else set()
            to_parents = set(to_node.parent_nodes) if to_node else set()
            removed_parents = from_parents - to_parents
            added_parents = to_parents - from_parents

            if not removed_parents and not added_parents:
                continue

            rows.append({
                "node":                 node_name,
                "property":             "parent_nodes",
                "change_type":          "CHANGED:parent_nodes",
                "from_type":            "relationship",
                "to_type":              "relationship",
                "from_required":        "",
                "to_required":          "",
                "from_is_key":          "",
                "to_is_key":            "",
                "from_value_set_terms": ";".join(sorted(removed_parents)),
                "to_value_set_terms":   ";".join(sorted(added_parents)),
                "from_version":         self.version,
                "to_version":           other.version,
            })

        return pd.DataFrame(rows)


# ── helpers ───────────────────────────────────────────────────────────────────

def pull_model_data_files(model, version, file_type, output_file):
    if file_type == "model":
        url = f"https://raw.githubusercontent.com/CBIIT/{model}/{version}/model-desc/{model}.yml"
    elif file_type == "props":
        url = f"https://raw.githubusercontent.com/CBIIT/{model}/{version}/model-desc/{model}-{file_type}.yml"
    response = requests.get(url)
    response.raise_for_status()

    with open(output_file, "w") as f:
        f.write(response.text)

    return output_file


def flatten_diff_to_dataframe(diff_result: dict, from_version: str, to_version: str) -> pd.DataFrame:
    """
    Flatten the nested dict returned by diff_models into a TSV-friendly dataframe.
    Handles nodes, edges, props, and terms sections.
    """
    rows = []

    for ent_type in ["nodes", "edges", "props", "terms"]:
        section = diff_result.get(ent_type, {})

        # removed entities
        for key, val in section.get("removed", {}).items():
            rows.append({
                "entity_type":  ent_type,
                "key":          str(key),
                "change_type":  "DELETION",
                "attribute":    "",
                "from_value":   str(val),
                "to_value":     "",
                "from_version": from_version,
                "to_version":   to_version,
            })

        # added entities
        for key, val in section.get("added", {}).items():
            rows.append({
                "entity_type":  ent_type,
                "key":          str(key),
                "change_type":  "ADDITION",
                "attribute":    "",
                "from_value":   "",
                "to_value":     str(val),
                "from_version": from_version,
                "to_version":   to_version,
            })

        # changed entities
        for key, attr_dict in section.get("changed", {}).items():
            for attr, change in attr_dict.items():
                rows.append({
                    "entity_type":  ent_type,
                    "key":          str(key),
                    "change_type":  "CHANGED",
                    "attribute":    attr,
                    "from_value":   str(change.get("removed", "")),
                    "to_value":     str(change.get("added", "")),
                    "from_version": from_version,
                    "to_version":   to_version,
                })

    return pd.DataFrame(rows)


# ── model parsing ─────────────────────────────────────────────────────────────

def parse_model(model_parsed, version: str) -> ModelSnapshot:
    logger = get_run_logger()
    snapshot = ModelSnapshot(version=version)
    logger.info(f"Starting to parse model for version: {version}")

    node_list = model_parsed.get_node_list()

    for node_name in node_list:
        logger.info(f"Parsing node: {node_name}")

        parent_nodes = model_parsed.get_parent_nodes(node_name)
        logger.info(f"Parent nodes of node: {node_name} are: {parent_nodes}")

        valid_parents = []
        if not parent_nodes:
            logger.info(f"Node: {node_name} has no parent nodes.")
        else:
            for parent in parent_nodes:
                key_prop = model_parsed.get_node_key_prop(parent)
                if not key_prop:
                    logger.warning(
                        f"No key_prop found for parent '{parent}' of node '{node_name}', skipping."
                    )
                    continue
                valid_parents.append(parent)

        node_record = NodeRecord(name=node_name, parent_nodes=valid_parents)

        for prop in model_parsed.get_node_props_list(node_name):
            required  = model_parsed.if_prop_required(node_name, prop)
            is_key    = model_parsed.if_prop_key(node_name, prop)
            prop_type = model_parsed.get_prop_type(node_name, prop)

            value_set_terms = []
            if prop_type in ("value_set", "list"):
                raw_terms = model_parsed.get_permissible_values(node_name, prop)
                if raw_terms and raw_terms != "":
                    value_set_terms = raw_terms if isinstance(raw_terms, list) else [raw_terms]
                else:
                    value_set_terms = ["[NOT AN ENUMERATED VALUE]"]

            node_record.properties[prop] = PropertyRecord(
                name=prop,
                prop_type=prop_type,
                required=required,
                is_key=is_key,
                value_set_terms=value_set_terms,
            )

        snapshot.nodes[node_name] = node_record

    logger.info(
        f"Parsed {len(snapshot.nodes)} nodes and "
        f"{sum(len(n.properties) for n in snapshot.nodes.values())} properties "
        f"for version: {version}"
    )
    return snapshot


# ── database querying ─────────────────────────────────────────────────────────

def query_node_property(driver, node: str, prop: str) -> list[dict]:
    """
    Query all records for a given node and property from the database.
    Traverses up to the study node to retrieve study_id.
    Returns a list of dicts with: study_id, node, property, guid, current_value.
    """
    query = f"""
        MATCH (n:{node})
        WHERE n.{prop} IS NOT NULL
        OPTIONAL MATCH path = (n)-[*0..5]->(s:study)
        WITH n,
            coalesce(s.study_id, 'unknown') AS study_id
        RETURN
            study_id                    AS study_id,
            '{node}'                    AS node,
            '{prop}'                    AS property,
            coalesce(n.guid, n.id, '')  AS guid,
            n.{prop}                    AS current_value
    """
    with driver.session() as session:
        result = session.run(query)
        return [dict(record) for record in result]


@task(name="Check DB data against diff")
def check_data_against_diff(
    driver,
    diff_df: pd.DataFrame,
    snapshot_new: ModelSnapshot,
    logger,
) -> pd.DataFrame:
    """
    For each DELETION or CHANGED row in diff_df, query the database to find
    records whose current values may be invalid under the new model.

    Returns a line-level report dataframe with columns:
        study_id, node, property, guid, current_value, change_type, issue
    """
    # only check deletions and changes — additions don't affect existing data
    actionable = diff_df[
        diff_df["change_type"].str.startswith("DELETION") |
        diff_df["change_type"].str.startswith("CHANGED")
    ].copy()

    # skip parent_node relationship rows — not a queryable property
    actionable = actionable[actionable["property"] != "parent_nodes"]

    report_rows = []

    for _, row in actionable.iterrows():
        node   = row["node"]
        prop   = row["property"]
        change = row["change_type"]

        logger.info(f"Querying database for node={node}, property={prop}, change={change}")

        try:
            db_records = query_node_property(driver=driver, node=node, prop=prop)
        except Exception as e:
            logger.warning(f"Query failed for node={node}, property={prop}: {e}")
            continue

        if not db_records:
            logger.info(f"No records found in database for node={node}, property={prop}")
            continue

        for record in db_records:
            current_value = record.get("current_value")
            issue = None

            if change == "DELETION":
                issue = "Property deleted from model"

            elif "value_set_terms" in change:
                removed_terms = set(row["from_value_set_terms"].split(";")) if row["from_value_set_terms"] else set()
                new_prop = snapshot_new.get_property(node, prop)
                valid_terms = set(new_prop.value_set_terms) if new_prop else set()
                if str(current_value) in removed_terms and str(current_value) not in valid_terms:
                    issue = f"Value '{current_value}' removed from value set"

            elif "type" in change:
                issue = f"Property type changed from '{row['from_type']}' to '{row['to_type']}'"

            elif "required" in change:
                if current_value is None or current_value == "":
                    issue = "Property is now required but value is missing"

            if issue:
                report_rows.append({
                    "study_id":      record.get("study_id", "unknown"),
                    "node":          node,
                    "property":      prop,
                    "guid":          record.get("guid", ""),
                    "current_value": current_value,
                    "change_type":   change,
                    "issue":         issue,
                })

    return pd.DataFrame(report_rows)


# ── main flow ─────────────────────────────────────────────────────────────────

@flow(
    name="Model Data Compare",
    log_prints=True,
    flow_run_name="{runner}_" + f"{get_time()}",
)
def runner(
    bucket: str,
    runner: str,
    old_model_repository: str = "ccdi-dcc-model",
    new_model_repository: str = "ccdi-dcc-model",
    old_model_version: str = "1.0.0",
    new_model_version: str = "2.0.0",
    check_against_database: bool = False,
    database_source_account_id: str = None,
    database_source_secret_path: str = None,
    database_source_secret_key_ip: str = None,
    database_source_secret_key_username: str = None,
    database_source_secret_key_password: str = None,
):
    logger = get_run_logger()
    current_date = get_time()
    output_folder = os.path.join(runner, "model_data_compare_" + current_date)
    prefix = f"{old_model_repository}_{old_model_version}_{new_model_repository}_{new_model_version}"

    # ── fetch model files ─────────────────────────────────────────────────────
    logger.info(f"Fetching model files for {old_model_repository} at {old_model_version} and {new_model_repository} at {new_model_version}.")
    old_model_file_yaml = pull_model_data_files(
        model=old_model_repository, version=old_model_version,
        file_type="model", output_file="old_model.yaml",
    )
    logger.info(f"{old_model_repository} at {old_model_version} found.")

    old_props_file_yaml = pull_model_data_files(
        model=old_model_repository, version=old_model_version,
        file_type="props", output_file="old_props.yaml",
    )
    logger.info(f"{old_model_repository} properties at {old_model_version} found.")

    new_model_file_yaml = pull_model_data_files(
        model=new_model_repository, version=new_model_version,
        file_type="model", output_file="new_model.yaml",
    )
    logger.info(f"{new_model_repository} at {new_model_version} found.")

    new_props_file_yaml = pull_model_data_files(
        model=new_model_repository, version=new_model_version,
        file_type="props", output_file="new_props.yaml",
    )
    logger.info(f"{new_model_repository} properties at {new_model_version} found.")

    # ── parse models via meval ModelParser (for ModelSnapshot) ───────────────
    logger.info(f"Parsing models via meval ModelParser for {old_model_repository} at {old_model_version} and {new_model_repository} at {new_model_version}.")
    model_parsed_old = ModelParser(
        model_file=old_model_file_yaml, props_file=old_props_file_yaml,
        handle=old_model_version,
    )
    model_parsed_new = ModelParser(
        model_file=new_model_file_yaml, props_file=new_props_file_yaml,
        handle=new_model_version,
    )

    snapshot_old = parse_model(model_parsed_old, old_model_version)
    snapshot_new = parse_model(model_parsed_new, new_model_version)

    # ── our own structured comparison (used for DB querying) ─────────────────
    logger.info(f"Comparing snapshots for {old_model_repository} at {old_model_version} and {new_model_repository} at {new_model_version}.")
    diff_df = snapshot_old.compare(snapshot_new)

    # ── bento-mdf diff_models comparison (richer attribute-level diff) ────────
    logger.info(f"Performing bento-mdf diff_models comparison for {old_model_repository} at {old_model_version} and {new_model_repository} at {new_model_version}.")
    mdf_old = MDFReader(old_model_file_yaml, old_props_file_yaml, handle=old_model_version)
    mdf_new = MDFReader(new_model_file_yaml, new_props_file_yaml, handle=new_model_version)

    bento_diff_result = diff_models(
        mdf_old.model,
        mdf_new.model,
        objects_as_dicts=True,
        include_summary=True,
    )
    logger.info(f"bento-mdf diff summary: {bento_diff_result.get('summary', {})}")

    bento_diff_df = flatten_diff_to_dataframe(
        diff_result=bento_diff_result,
        from_version=old_model_version,
        to_version=new_model_version,
    )

    # ── save outputs ──────────────────────────────────────────────────────────
    logger.info(f"Saving comparison outputs for {old_model_repository} at {old_model_version} and {new_model_repository} at {new_model_version}.")
    # our structured comparison
    comparison_file_name = f"{prefix}_comparison_{current_date}.tsv"
    diff_df.to_csv(comparison_file_name, sep="\t", index=False)
    file_ul(bucket=bucket, output_folder=output_folder, sub_folder="", newfile=comparison_file_name)
    logger.info(f"Structured comparison written to {comparison_file_name}")

    # bento-mdf attribute-level diff
    bento_comparison_file_name = f"{prefix}_bento_diff_{current_date}.tsv"
    bento_diff_df.to_csv(bento_comparison_file_name, sep="\t", index=False)
    file_ul(bucket=bucket, output_folder=output_folder, sub_folder="", newfile=bento_comparison_file_name)
    logger.info(f"Bento-mdf diff written to {bento_comparison_file_name}")

    # ── check against database ────────────────────────────────────────────────
    if check_against_database:
        logger.info("Acquiring database credentials, retrieving from AWS")
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

        driver = GraphDatabase.driver(
            uri_source, auth=basic_auth(username_source, password_source)
        )
        logger.info("Database driver created successfully.")

        try:
            data_report_df = check_data_against_diff(
                driver=driver,
                diff_df=diff_df,
                snapshot_new=snapshot_new,
                logger=logger,
            )
        finally:
            driver.close()
            logger.info("Database driver closed.")

        if data_report_df.empty:
            logger.info("No data issues found against the new model.")
        else:
            logger.info(f"Found {len(data_report_df)} records with potential data issues.")

        data_report_file_name = f"{prefix}_data_report_{current_date}.tsv"
        data_report_df.to_csv(data_report_file_name, sep="\t", index=False)
        file_ul(
            bucket=bucket, output_folder=output_folder,
            sub_folder="", newfile=data_report_file_name,
        )
        logger.info(f"Data report written to {data_report_file_name}")

    logger.info(f"Done. Outputs written to {output_folder}")