import yaml
import requests
import pandas as pd
import os
from prefect import flow, task, get_run_logger, pause_flow_run
from prefect.input import RunInput
from src.utils import get_time, file_dl, file_ul
from meval.parser import ModelParser
import requests



class InputValues(RunInput):
    node: str
    property: str


COLUMNS = [
    "lift_from_node", "lift_from_property", "lift_from_version",
    "lift_to_node",   "lift_to_property",   "lift_to_version",
]

# ── helpers ───────────────────────────────────────────────────────────────────

def pull_model_data_files(model, version, file_type, output_file):
    if file_type == "model":
        url = f"https://raw.githubusercontent.com/CBIIT/{model}/{version}/model-desc/{model}.yml"
    elif file_type == "props":
        url = f"https://raw.githubusercontent.com/CBIIT/{model}/{version}/model-desc/{model}-{file_type}.yml"
    response = requests.get(url)
    response.raise_for_status()

    with open(output_file, 'w') as f:
        f.write(response.text)

    return output_file

# ── extraction ────────────────────────────────────────────────────────────────

def parse_model(model_parsed, version):
    rows = []
    print(f"Starting to parse model for version: {version}")
    node_list = model_parsed.get_node_list()

    for node in node_list:
        print(f"Parsing node: {node}")
        for prop in model_parsed.get_node_props_list(node):
            rows.append({"node": node, "property": prop, "version": version})

    for node in node_list:
        print(f"Things are getting pushed, right?")
        print(f"Parsing relationships for node: {node}")
        parent_nodes = model_parsed.get_parent_nodes(node)
        print(f"Parent nodes of node: {node} are: {parent_nodes}")
        if len(parent_nodes) == 0:
            print(f"Node: {node} has no parent nodes, skipping relationship parsing for this node.")
        else:
            print(
                f"Node: {node} has parent nodes, parsing relationships for this node."
            )
            for parent in parent_nodes:
                key_prop = model_parsed.get_node_key_prop(parent)
                if not key_prop:
                    print(f"No key_prop found for parent '{parent}' of node '{node}', skipping.")
                    continue
                rows.append({"node": node, "property": f"{parent}.{key_prop}", "version": version})

    return pd.DataFrame(rows, columns=["node", "property", "version"])


# ── merging ───────────────────────────────────────────────────────────────────

def build_mapping(df_from: pd.DataFrame, df_to: pd.DataFrame) -> pd.DataFrame:
    merged = pd.merge(
        df_from,
        df_to,
        left_on=["lift_from_node", "lift_from_property"],
        right_on=["lift_to_node", "lift_to_property"],
        how="outer",
    )
    return merged[COLUMNS]


# ── user input ────────────────────────────────────────────────────────────────

def user_input_location(df, value_node_col, value_property_col,
                        missing_node_col, missing_property_col,
                        missing_version_col, base_mode, direction):
    logger = get_run_logger()

    df_missing = df[df[missing_property_col].isna()]
    for index, row in df_missing.iterrows():
        existing_node = row[value_node_col]
        existing_property = row[value_property_col]
        logger.info(f"{index}. node: {existing_node}, property: {existing_property}")

        if base_mode:
            user_input_node = user_input_prop = "remove"
        else:
            header = "Old values to map to new" if direction == "fromto" else "New values to map to old"
            value_inputs = pause_flow_run(
                wait_for_input=InputValues.with_initial_data(
                    description=f"""
# **Active Input**

## **Instructions**
- If a value is staying the same, write 'same'.
- If a value is removed, write 'remove'.
- For multiple nodes, use ';' as separator.

## **{header}**
**node**: {existing_node}
**property**: {existing_property}
                    """
                )
            )
            logger.info(f"Inputs received:\nnode: {value_inputs.node}\nproperty: {value_inputs.property}")
            user_input_node = value_inputs.node
            user_input_prop = value_inputs.property

        if user_input_node.lower() == "same":
            user_input_node = row[value_node_col]
        if user_input_prop.lower() == "same":
            user_input_prop = row[value_property_col]
        if user_input_node.lower() == "remove":
            user_input_node = None
        if user_input_prop.lower() == "remove":
            user_input_prop = None

        new_version = df[missing_version_col].dropna().unique()[0]
        df.at[index, missing_node_col] = user_input_node
        df.at[index, missing_property_col] = user_input_prop
        df.at[index, missing_version_col] = new_version


# ── cleanup ───────────────────────────────────────────────────────────────────

def expand_semicolon_nodes(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in df.iterrows():
        node_to = row["lift_to_node"]
        if pd.isna(node_to) or node_to in ["NA", "none", ""]:
            rows.append(row)
        else:
            for value in node_to.split(";"):
                new_row = row.copy()
                new_row["lift_to_node"] = value.strip()
                rows.append(new_row)
    return pd.DataFrame(rows).reset_index(drop=True)


def clean_up_partial_dups(df, empty_node_col, empty_prop_col,
                        value_node_col, value_prop_col) -> pd.DataFrame:
    indexes_to_remove = []
    for index, row in df.iterrows():
        if pd.isna(row[empty_node_col]) or pd.isna(row[empty_prop_col]):
            mask = (
                (df[value_node_col] == df.at[index, value_node_col]) &
                (df[value_prop_col] == df.at[index, value_prop_col])
            )
            matching = df.index[mask].tolist()
            if len(matching) > 1:
                for other_index in matching:
                    other = df.iloc[other_index]
                    if pd.isna(other[empty_node_col]) and pd.isna(other[empty_prop_col]):
                        indexes_to_remove.append(index)
    return df.drop(list(set(indexes_to_remove))).fillna("")


# ── comparison ────────────────────────────────────────────────────────────────

def build_comparison(df: pd.DataFrame, old_version: str, new_version: str) -> pd.DataFrame:
    results = []
    for _, row in df.iterrows():
        from_vals = (row["lift_from_node"], row["lift_from_property"])
        to_vals   = (row["lift_to_node"],   row["lift_to_property"])
        from_na = any(v == "" for v in from_vals)
        to_na   = any(v == "" for v in to_vals)

        if to_na and not from_na:
            state = "DELETION"
        elif from_na and not to_na:
            state = "ADDITION"
        elif from_vals != to_vals:
            state = "CHANGED"
        else:
            state = "SAME"

        results.append({
            "state":              state,
            "lift_from_node":     from_vals[0],
            "lift_from_property": from_vals[1],
            "lift_from_version":  old_version,
            "lift_to_node":       to_vals[0],
            "lift_to_property":   to_vals[1],
            "lift_to_version":    new_version,
        })

    return (
        pd.DataFrame(results)
        .query("state != 'SAME'")
        .fillna("")
        .drop_duplicates()
    )


# ── main flow ─────────────────────────────────────────────────────────────────

@flow(
    name="Model Mapping Maker",
    log_prints=True,
    flow_run_name="{runner}_" + f"{get_time()}",
)
def runner(
    bucket: str,
    runner: str,
    old_model_repository: str = "ccdi-dcc-model",
    new_model_repository: str = "cds-model",
    old_model_version: str = "1.0.0",
    new_model_version: str = "11.0.4-GC_Release",
    base_mode: bool = True,
    mapping_file: str = "path_to/mapping_file/in/s3_bucket.tsv",
):
    logger = get_run_logger()
    current_date = get_time()
    output_folder = os.path.join(runner, "model_mapping_maker_" + current_date)

    # if mapping file path is not updated from default or is empty, skip downloading and instead build mapping from scratch
    if mapping_file == "path_to/mapping_file/in/s3_bucket.tsv" or mapping_file.strip() == "":
        mapping_file = None
    if mapping_file:
        file_dl(bucket, mapping_file)

    # ── fetch models ──────────────────────────────────────────────────────────

    old_model_file_yaml = pull_model_data_files(model=old_model_repository, version=old_model_version, file_type="model", output_file="old_model.yaml")
    logger.info(f"{old_model_repository} at {old_model_version} found.")
    
    old_props_file_yaml = pull_model_data_files(model=old_model_repository, version=old_model_version, file_type="props", output_file="old_props.yaml")
    logger.info(f"{old_model_repository} properties at {old_model_version} found.")
        
    new_model_file_yaml = pull_model_data_files(model=new_model_repository, version=new_model_version, file_type="model", output_file="new_model.yaml")
    logger.info(f"{new_model_repository} at {new_model_version} found.")    
    
    new_props_file_yaml = pull_model_data_files(model=new_model_repository, version=new_model_version, file_type="props", output_file="new_props.yaml")
    logger.info(f"{new_model_repository} properties at {new_model_version} found.")


    # ── Create MDF objects via MEVAL (mdf) parsing ─────────────────────────────────
    model_parsed_old = ModelParser(
        model_file=old_model_file_yaml,
        props_file=old_props_file_yaml,
        handle=old_model_version
    )

    model_parsed_new = ModelParser(
        model_file=new_model_file_yaml,
        props_file=new_props_file_yaml,
        handle=new_model_version
    )

    df_old = parse_model(model_parsed_old, old_model_version)
    df_new = parse_model(model_parsed_new, new_model_version)


    # ── build or load mapping ─────────────────────────────────────────────────
    if mapping_file:
        mapping_df = pd.read_csv(os.path.basename(mapping_file), sep="\t")
        mapping_df.columns = COLUMNS
    else:
        df_from = df_old.rename(columns={"node": "lift_from_node", "property": "lift_from_property", "version": "lift_from_version"})
        df_to   = df_new.rename(columns={"node": "lift_to_node",   "property": "lift_to_property",   "version": "lift_to_version"})

        mapping_df = build_mapping(df_from, df_to)

        user_input_location(mapping_df, "lift_from_node", "lift_from_property", "lift_to_node",   "lift_to_property",   "lift_to_version",   base_mode, "fromto")
        user_input_location(mapping_df, "lift_to_node",   "lift_to_property",   "lift_from_node", "lift_from_property", "lift_from_version", base_mode, "tofrom")

        mapping_df = mapping_df.drop_duplicates()

    # ── post-process ──────────────────────────────────────────────────────────
    mapping_df = expand_semicolon_nodes(mapping_df)
    mapping_df = clean_up_partial_dups(mapping_df, "lift_from_node", "lift_from_property", "lift_to_node",   "lift_to_property")
    mapping_df = clean_up_partial_dups(mapping_df, "lift_to_node",   "lift_to_property",   "lift_from_node", "lift_from_property")

    mapping_df = mapping_df.fillna("").drop_duplicates()

    # ── comparison ────────────────────────────────────────────────────────────
    comparison_df = build_comparison(mapping_df, old_model_version, new_model_version)

    # ── save & upload ─────────────────────────────────────────────────────────
    prefix = f"{old_model_repository}_{old_model_version}_{new_model_repository}_{new_model_version}"

    mapping_file_name = f"{prefix}_MAPPING_{current_date}.tsv"
    mapping_df.to_csv(mapping_file_name, sep="\t", index=False)
    file_ul(bucket=bucket, output_folder=output_folder, sub_folder="", newfile=mapping_file_name)

    comparison_file_name = f"{prefix}_comparison_{current_date}.tsv"
    comparison_df.to_csv(comparison_file_name, sep="\t", index=False)
    file_ul(bucket=bucket, output_folder=output_folder, sub_folder="", newfile=comparison_file_name)

    logger.info(f"Done. Outputs written to {output_folder}")