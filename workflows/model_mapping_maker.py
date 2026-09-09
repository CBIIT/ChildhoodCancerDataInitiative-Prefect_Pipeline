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
    "lift_from_node",
    "lift_from_property",
    "lift_from_version",
    "lift_to_node",
    "lift_to_property",
    "lift_to_version",
]

# ── helpers ───────────────────────────────────────────────────────────────────


def pull_model_data_files(model, version, file_type, output_file):
    if file_type == "model":
        url = f"https://raw.githubusercontent.com/CBIIT/{model}/{version}/model-desc/{model}.yml"
    elif file_type == "props":
        url = f"https://raw.githubusercontent.com/CBIIT/{model}/{version}/model-desc/{model}-{file_type}.yml"
    else:
        raise ValueError(f"Unknown file_type: {file_type}")

    logger = get_run_logger()
    logger.info(f"Fetching {file_type} file from: {url}")

    response = requests.get(url)
    if not response.ok:
        logger.error(
            f"Failed to fetch {file_type} file for {model}@{version}. "
            f"URL: {url} | Status: {response.status_code} | Body: {response.text[:500]}"
        )
    response.raise_for_status()

    with open(output_file, "w") as f:
        f.write(response.text)

    return output_file


# ── extraction ────────────────────────────────────────────────────────────────


@task
def parse_model(model_parsed, version):
    logger = get_run_logger()
    rows = []
    logger.info(f"Starting to parse model for version: {version}")
    node_list = model_parsed.get_node_list()

    for node in node_list:
        logger.info(f"Parsing node: {node}")
        for prop in model_parsed.get_node_props_list(node):
            rows.append({"node": node, "property": prop, "version": version})

    for node in node_list:
        logger.info(f"Parsing relationships for node: {node}")
        parent_nodes = model_parsed.get_parent_nodes(node)
        logger.info(f"Parent nodes of node: {node} are: {parent_nodes}")
        if len(parent_nodes) == 0:
            logger.info(
                f"Node: {node} has no parent nodes, skipping relationship parsing for this node."
            )
        else:
            logger.info(
                f"Node: {node} has parent nodes, parsing relationships for this node."
            )
            for parent in parent_nodes:
                key_prop = model_parsed.get_node_key_prop(parent)
                if not key_prop:
                    logger.warning(
                        f"No key_prop found for parent '{parent}' of node '{node}', skipping."
                    )
                    continue
                rows.append(
                    {
                        "node": node,
                        "property": f"{parent}.{key_prop}",
                        "version": version,
                    }
                )

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


# ── reconciliation ────────────────────────────────────────────────────────────


def reconcile_mapping(
    mapping_provided: pd.DataFrame, mapping_built: pd.DataFrame
) -> pd.DataFrame:
    """
    Reconcile a user-provided mapping file against a freshly built one.
    - Rows in the provided file take precedence (they contain curated mappings).
    - Rows in the built file that are already covered by the provided file are dropped.
    - Rows in the built file that are NOT covered are appended (net-new nodes/properties).
    A row is considered "covered" if its lift_from_node + lift_from_property pair
    already exists in the provided mapping.

    NaN values are normalized to empty strings before building the key set so that
    rows with missing lift_from values (NaN != NaN) are compared correctly instead
    of always evaluating as "not covered".
    """
    provided_normalized = mapping_provided[
        ["lift_from_node", "lift_from_property"]
    ].fillna("")
    provided_keys = set(
        zip(provided_normalized["lift_from_node"], provided_normalized["lift_from_property"])
    )

    built_normalized = mapping_built[["lift_from_node", "lift_from_property"]].fillna("")

    # only keep built rows whose from-key isn't already handled in the provided file
    is_covered = [
        (n, p) in provided_keys
        for n, p in zip(built_normalized["lift_from_node"], built_normalized["lift_from_property"])
    ]
    net_new = mapping_built[~pd.Series(is_covered, index=mapping_built.index)]

    reconciled = pd.concat([mapping_provided, net_new], ignore_index=True)
    return reconciled


# ── user input ────────────────────────────────────────────────────────────────


def user_input_location(
    df,
    value_node_col,
    value_property_col,
    missing_node_col,
    missing_property_col,
    missing_version_col,
    base_mode,
    direction,
):
    logger = get_run_logger()

    df_missing = df[df[missing_property_col].isna()]
    for index, row in df_missing.iterrows():
        existing_node = row[value_node_col]
        existing_property = row[value_property_col]
        logger.info(f"{index}. node: {existing_node}, property: {existing_property}")

        if base_mode:
            user_input_node = user_input_prop = "remove"
        else:
            header = (
                "Old values to map to new"
                if direction == "fromto"
                else "New values to map to old"
            )
            value_inputs = pause_flow_run(
                wait_for_input=InputValues.with_initial_data(description=f"""
# **Active Input**

## **Instructions**
- If a value is staying the same, write 'same'.
- If a value is removed, write 'remove'.
- For multiple nodes, use ';' as separator.

## **{header}**
**node**: {existing_node}
**property**: {existing_property}
                    """)
            )
            logger.info(
                f"Inputs received:\nnode: {value_inputs.node}\nproperty: {value_inputs.property}"
            )
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


def clean_up_partial_dups(
    df, empty_node_col, empty_prop_col, value_node_col, value_prop_col
) -> pd.DataFrame:
    """
    Drops rows that have missing (node, property) values on one side when another
    row already covers the same value-side pair with a complete match on the
    empty side. Uses .loc (label-based) instead of .iloc (position-based) when
    looking up candidate matches, since df.index[mask] returns labels, and those
    labels are not guaranteed to be contiguous positions - especially after a
    prior drop() call. The index is reset at the end so subsequent calls
    (e.g. a second clean_up_partial_dups pass) always work with a clean,
    contiguous index too.
    """
    indexes_to_remove = []
    for index, row in df.iterrows():
        if pd.isna(row[empty_node_col]) or pd.isna(row[empty_prop_col]):
            mask = (df[value_node_col] == df.at[index, value_node_col]) & (
                df[value_prop_col] == df.at[index, value_prop_col]
            )
            matching = df.index[mask].tolist()
            if len(matching) > 1:
                for other_index in matching:
                    other = df.loc[other_index]
                    if pd.isna(other[empty_node_col]) and pd.isna(
                        other[empty_prop_col]
                    ):
                        indexes_to_remove.append(index)
    return (
        df.drop(list(set(indexes_to_remove)))
        .reset_index(drop=True)
        .fillna("")
    )


# ── comparison ────────────────────────────────────────────────────────────────


def build_comparison(
    df: pd.DataFrame, old_version: str, new_version: str
) -> pd.DataFrame:
    results = []
    for _, row in df.iterrows():
        from_vals = (row["lift_from_node"], row["lift_from_property"])
        to_vals = (row["lift_to_node"], row["lift_to_property"])
        from_na = any(v == "" for v in from_vals)
        to_na = any(v == "" for v in to_vals)

        if to_na and not from_na:
            state = "DELETION"
        elif from_na and not to_na:
            state = "ADDITION"
        elif from_vals != to_vals:
            state = "CHANGED"
        else:
            state = "SAME"

        results.append(
            {
                "state": state,
                "lift_from_node": from_vals[0],
                "lift_from_property": from_vals[1],
                "lift_from_version": old_version,
                "lift_to_node": to_vals[0],
                "lift_to_property": to_vals[1],
                "lift_to_version": new_version,
            }
        )

    return pd.DataFrame(results).query("state != 'SAME'").fillna("").drop_duplicates()


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
    if (
        mapping_file == "path_to/mapping_file/in/s3_bucket.tsv"
        or mapping_file.strip() == ""
    ):
        mapping_file = None
    if mapping_file:
        file_dl(bucket, mapping_file)
        logger.info(f"Downloaded mapping file from S3: {mapping_file}")

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

    df_old = parse_model(model_parsed_old, old_model_version)
    df_new = parse_model(model_parsed_new, new_model_version)

    # ── build or load mapping ─────────────────────────────────────────────────

    # always build a fresh mapping from the parsed models
    df_from = df_old.rename(
        columns={
            "node": "lift_from_node",
            "property": "lift_from_property",
            "version": "lift_from_version",
        }
    )
    df_to = df_new.rename(
        columns={
            "node": "lift_to_node",
            "property": "lift_to_property",
            "version": "lift_to_version",
        }
    )
    mapping_built = build_mapping(df_from, df_to)

    if mapping_file:
        logger.info("Obtaining mapping file.")
        local_path = os.path.basename(mapping_file)

        # Validation of file existing
        if not os.path.exists(local_path):
            raise FileNotFoundError(
                f"Expected mapping file at '{local_path}' after file_dl, but it wasn't found. "
                f"Check that file_dl downloads to the current working directory."
            )

        logger.info("Reading mapping file")
        mapping_provided = pd.read_csv(local_path, sep="\t")
        logger.info(
            f"Loaded provided mapping file with columns: {list(mapping_provided.columns)} "
            f"({len(mapping_provided.columns)} columns, {len(mapping_provided)} rows)"
        )

        logger.info("Handling mapping file columns")
        if len(mapping_provided.columns) != len(COLUMNS):
            raise ValueError(
                f"Provided mapping file has {len(mapping_provided.columns)} columns "
                f"{list(mapping_provided.columns)}, but expected {len(COLUMNS)}: {COLUMNS}. "
                f"Check the file's delimiter and column structure."
            )
        mapping_provided.columns = COLUMNS
        mapping_df = reconcile_mapping(mapping_provided, mapping_built)
        logger.info(
            f"Reconciled provided mapping with freshly built mapping. "
            f"Provided: {len(mapping_provided)} rows, "
            f"Built: {len(mapping_built)} rows, "
            f"Reconciled: {len(mapping_df)} rows."
        )
    else:
        mapping_df = mapping_built

    # NOTE: previously these three calls were nested under `if not base_mode:`,
    # which meant:
    #   1. In base_mode=True runs, rows missing a value on either side were
    #      never resolved at all (the `if base_mode:` branch inside
    #      user_input_location that auto-fills "remove" was unreachable code,
    #      since the function was never even called in that mode).
    #   2. mapping_df.drop_duplicates() never ran in base_mode=True runs either,
    #      since it lived in the same skipped block.
    # user_input_location already branches internally on base_mode (auto-fill
    # "remove" vs. pause for interactive input), so these calls need to run
    # unconditionally in both modes.
    user_input_location(
        mapping_df,
        "lift_from_node",
        "lift_from_property",
        "lift_to_node",
        "lift_to_property",
        "lift_to_version",
        base_mode,
        "fromto",
    )
    user_input_location(
        mapping_df,
        "lift_to_node",
        "lift_to_property",
        "lift_from_node",
        "lift_from_property",
        "lift_from_version",
        base_mode,
        "tofrom",
    )

    mapping_df = mapping_df.drop_duplicates()

    # ── post-process ──────────────────────────────────────────────────────────
    mapping_df = expand_semicolon_nodes(mapping_df)
    mapping_df = clean_up_partial_dups(
        mapping_df,
        "lift_from_node",
        "lift_from_property",
        "lift_to_node",
        "lift_to_property",
    )
    mapping_df = clean_up_partial_dups(
        mapping_df,
        "lift_to_node",
        "lift_to_property",
        "lift_from_node",
        "lift_from_property",
    )

    mapping_df = mapping_df.fillna("").drop_duplicates()

    # ── comparison ────────────────────────────────────────────────────────────
    comparison_df = build_comparison(mapping_df, old_model_version, new_model_version)

    # ── save & upload ─────────────────────────────────────────────────────────
    prefix = f"{old_model_repository}_{old_model_version}_{new_model_repository}_{new_model_version}"

    mapping_file_name = f"{prefix}_MAPPING_{current_date}.tsv"
    mapping_df.to_csv(mapping_file_name, sep="\t", index=False)
    file_ul(
        bucket=bucket,
        output_folder=output_folder,
        sub_folder="",
        newfile=mapping_file_name,
    )

    comparison_file_name = f"{prefix}_comparison_{current_date}.tsv"
    comparison_df.to_csv(comparison_file_name, sep="\t", index=False)
    file_ul(
        bucket=bucket,
        output_folder=output_folder,
        sub_folder="",
        newfile=comparison_file_name,
    )

    logger.info(f"Done. Outputs written to {output_folder}")