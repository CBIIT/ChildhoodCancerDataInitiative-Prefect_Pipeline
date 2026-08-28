import requests
import pandas as pd
import os
from prefect import flow, task, get_run_logger, pause_flow_run
from prefect.input import RunInput
from src.utils import get_time, file_dl, file_ul
from meval.parser import ModelParser
import requests
from dataclasses import dataclass, field


class InputValues(RunInput):
    node: str
    property: str

@dataclass
class PropertyRecord:
    """Represents a single property within a node."""
    name: str
    prop_type: str
    required: bool
    is_key: bool
    value_set_terms: list[str] = field(default_factory=list)  # empty if not enum/list

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
    properties: dict[str, PropertyRecord] = field(default_factory=dict)  # {prop_name: PropertyRecord}
    parent_nodes: list[str] = field(default_factory=list)

    def get_property(self, prop_name: str) -> PropertyRecord | None:
        return self.properties.get(prop_name)

    def to_dict(self) -> list[dict]:
        """Flatten node into a list of row dicts, one per property."""
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
    nodes: dict[str, NodeRecord] = field(default_factory=dict)  # {node_name: NodeRecord}

    # ── accessors ─────────────────────────────────────────────────────────────

    def get_node(self, node_name: str) -> NodeRecord | None:
        return self.nodes.get(node_name)

    def get_property(self, node_name: str, prop_name: str) -> PropertyRecord | None:
        node = self.get_node(node_name)
        return node.get_property(prop_name) if node else None

    def get_all_node_names(self) -> list[str]:
        return list(self.nodes.keys())

    def get_all_properties(self) -> list[tuple[str, str]]:
        """Returns all (node, property) pairs in the snapshot."""
        return [
            (node_name, prop_name)
            for node_name, node in self.nodes.items()
            for prop_name in node.properties
        ]

    def to_dataframe(self) -> pd.DataFrame:
        """Flatten the entire snapshot into a dataframe."""
        rows = []
        for node in self.nodes.values():
            rows.extend(node.to_dict())
        df = pd.DataFrame(rows)
        df["version"] = self.version
        return df

    # ── comparison ────────────────────────────────────────────────────────────

    def compare(self, other: "ModelSnapshot") -> pd.DataFrame:
        """
        Compare this snapshot (from) against another snapshot (to).
        Detects: ADDITION, DELETION, CHANGED (type, required, is_key, value_set_terms).
        Returns a dataframe of differences only — identical entries are skipped.
        """
        from_keys = set(self.get_all_properties())
        to_keys = set(other.get_all_properties())
        all_keys = from_keys | to_keys

        rows = []
        for node_name, prop_name in sorted(all_keys):
            from_prop = self.get_property(node_name, prop_name)
            to_prop = other.get_property(node_name, prop_name)

            if from_prop and not to_prop:
                change_type = "DELETION"
            elif not from_prop and to_prop:
                change_type = "ADDITION"
            else:
                # both exist — check for differences
                changes = []
                if from_prop.prop_type != to_prop.prop_type:
                    changes.append("type")
                if from_prop.required != to_prop.required:
                    changes.append("required")
                if from_prop.is_key != to_prop.is_key:
                    changes.append("is_key")
                if set(from_prop.value_set_terms) != set(to_prop.value_set_terms):
                    changes.append("value_set_terms")
                if not changes:
                    continue  # identical — skip
                change_type = "CHANGED:" + ",".join(changes)

            rows.append({
                "node":                node_name,
                "property":            prop_name,
                "change_type":         change_type,
                "from_type":           from_prop.prop_type if from_prop else "",
                "to_type":             to_prop.prop_type if to_prop else "",
                "from_required":       from_prop.required if from_prop else "",
                "to_required":         to_prop.required if to_prop else "",
                "from_is_key":         from_prop.is_key if from_prop else "",
                "to_is_key":           to_prop.is_key if to_prop else "",
                "from_value_set_terms": ";".join(from_prop.value_set_terms) if from_prop else "",
                "to_value_set_terms":   ";".join(to_prop.value_set_terms) if to_prop else "",
                "from_version":        self.version,
                "to_version":          other.version,
            })

        return pd.DataFrame(rows)


def parse_model(model_parsed, version: str) -> ModelSnapshot:
    logger = get_run_logger()
    snapshot = ModelSnapshot(version=version)
    logger.info(f"Starting to parse model for version: {version}")

    node_list = model_parsed.get_node_list()

    for node_name in node_list:
        logger.info(f"Parsing node: {node_name}")

        # ── relationships ─────────────────────────────────────────────────────
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

        # ── properties ────────────────────────────────────────────────────────
        for prop in model_parsed.get_node_props_list(node_name):
            required    = model_parsed.get_prop_requiredness(node_name, prop)
            is_key      = model_parsed.if_prop_key(node_name, prop)
            prop_type   = model_parsed.get_prop_type(node_name, prop)

            value_set_terms = []
            if prop_type in ("value_set", "list"):
                raw_terms = model_parsed.get_value_set_terms(node_name, prop)
                if raw_terms and raw_terms != "":
                    value_set_terms = raw_terms if isinstance(raw_terms, list) else [raw_terms]
                else:
                    value_set_terms = ["not enumerated"]

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


# # ── extraction ────────────────────────────────────────────────────────────────

# @task
# def parse_model(model_parsed, version):
#     logger = get_run_logger()
#     rows = []
#     logger.info(f"Starting to parse model for version: {version}")
#     # Get list of nodes in the model
#     node_list = model_parsed.get_node_list()

#     # For each node
#     for node in node_list:
#         logger.info(f"Parsing node: {node}")
#         # Get list of properties for the node
#         props = model_parsed.get_node_props_list(node)
#         # Get list of parent nodes for the current node
#         parent_nodes = model_parsed.get_parent_nodes(node)

#         logger.info(f"Parent nodes of node: {node} are: {parent_nodes}")
#         if len(parent_nodes) == 0:
#             logger.info(
#                 f"Node: {node} has no parent nodes, skipping relationship parsing for this node."
#             )
#         else:
#             logger.info(
#                 f"Node: {node} has parent nodes, parsing relationships for this node."
#             )
#             for parent in parent_nodes:
#                 key_prop = model_parsed.get_node_key_prop(parent)
#                 if not key_prop:
#                     logger.warning(
#                         f"No key_prop found for parent '{parent}' of node '{node}', skipping."
#                     )
#                     continue

#         # For each property of the node
#         for prop in props:
#             # Get the requiredness of the property
#             requiredness = model_parsed.get_prop_requiredness(node, prop)
#             # Check if key property of the node
#             key_prop = model_parsed.if_prop_key(node, prop)
#             # Get the type of the property
#             prop_type = model_parsed.get_prop_type(node, prop)
#             #if property is a value set, get its terms
#             if prop_type == "value_set" or prop_type == "list":
#                 value_set_terms = model_parsed.get_value_set_terms(node, prop)
#                 if value_set_terms == "":
#                     value_set_terms = "not enumerated"

#             # Insert something about the CDE values and versions
#             # This will require going to the bento-mdf repo code to pull it



#     return 



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
    database_source_account_name: str = None,
    database_source_account_id: str = None,
    database_source_secret_path: str = None,
    database_source_secret_key_ip: str = None,
    database_source_secret_key_username: str = None,
    database_source_secret_key_password: str = None,
):
    logger = get_run_logger()
    current_date = get_time()
    output_folder = os.path.join(runner, "model_data_compare_" + current_date)

    # ── fetch models ──────────────────────────────────────────────────────────

    old_model_file_yaml = pull_model_data_files(
        model=old_model_repository,
        version=old_model_version,
        file_type="model",
        output_file="old_model.yaml",
    )
    logger.info(f"{old_model_repository} at {old_model_version} found.")

    old_props_file_yaml = pull_model_data_files(
        model=old_model_repository,
        version=old_model_version,
        file_type="props",
        output_file="old_props.yaml",
    )
    logger.info(f"{old_model_repository} properties at {old_model_version} found.")

    new_model_file_yaml = pull_model_data_files(
        model=new_model_repository,
        version=new_model_version,
        file_type="model",
        output_file="new_model.yaml",
    )
    logger.info(f"{new_model_repository} at {new_model_version} found.")

    new_props_file_yaml = pull_model_data_files(
        model=new_model_repository,
        version=new_model_version,
        file_type="props",
        output_file="new_props.yaml",
    )
    logger.info(f"{new_model_repository} properties at {new_model_version} found.")

    # ── Create MDF objects via MEVAL (mdf) parsing ─────────────────────────────────
    model_parsed_old = ModelParser(
        model_file=old_model_file_yaml,
        props_file=old_props_file_yaml,
        handle=old_model_version,
    )

    model_parsed_new = ModelParser(
        model_file=new_model_file_yaml,
        props_file=new_props_file_yaml,
        handle=new_model_version,
    )

    snapshot_old = parse_model(model_parsed_old, old_model_version)
    snapshot_new = parse_model(model_parsed_new, new_model_version)

    # # query individual nodes or properties
    # node = snapshot_old.get_node("sample")
    # prop = snapshot_old.get_property("sample", "sample_id")

    # flat dataframe for the full model
    df_old = snapshot_old.to_dataframe()

    # diff between versions — only changed/added/deleted rows
    diff_df = snapshot_old.compare(snapshot_new)

    # ── save & upload ─────────────────────────────────────────────────────────
    prefix = f"{old_model_repository}_{old_model_version}_{new_model_repository}_{new_model_version}"

    # mapping_file_name = f"{prefix}_MAPPING_{current_date}.tsv"
    # mapping_df.to_csv(mapping_file_name, sep="\t", index=False)
    # file_ul(
    #     bucket=bucket,
    #     output_folder=output_folder,
    #     sub_folder="",
    #     newfile=mapping_file_name,
    # )

    comparison_file_name = f"{prefix}_comparison_{current_date}.tsv"
    diff_df.to_csv(comparison_file_name, sep="\t", index=False)
    file_ul(
        bucket=bucket,
        output_folder=output_folder,
        sub_folder="",
        newfile=comparison_file_name,
    )

    logger.info(f"Done. Outputs written to {output_folder}")
