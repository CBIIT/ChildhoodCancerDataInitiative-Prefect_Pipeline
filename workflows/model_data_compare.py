import requests
import pandas as pd
import os
from prefect import flow, task, get_run_logger
from prefect.input import RunInput
from src.utils import get_secret_centralized_worker, get_time, file_dl, file_ul
from bento_mdf import MDFReader
from bento_mdf.diff import diff_models
from dataclasses import dataclass, field
from neo4j import GraphDatabase, basic_auth
from prefect.cache_policies import NO_CACHE


class InputValues(RunInput):
    node: str
    property: str


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


def _serialize_value(val) -> str:
    """Safely serialize a diff value to a readable string."""
    if val is None:
        return ""
    if isinstance(val, dict):
        # for dicts of bento_meta objects (e.g. terms, props), show keys only
        return ";".join(str(k) for k in val.keys())
    if isinstance(val, (list, tuple)):
        return ";".join(str(v) for v in val)
    return str(val)

def truncate_value_field(value: str, max_entries: int = 10, delimiter: str = ";") -> str:
    """
    Truncate a delimited value field to max_entries items.
    Appends a note if truncation occurred.
    """
    if not value or not isinstance(value, str):
        return value
    parts = value.split(delimiter)
    if len(parts) <= max_entries:
        return value
    truncated = delimiter.join(parts[:max_entries])
    return f"{truncated} ... [{len(parts) - max_entries} more values truncated]"


def truncate_diff_dataframe(df: pd.DataFrame, max_entries: int = 10) -> pd.DataFrame:
    """
    Truncate from_value and to_value columns in the diff dataframe
    to avoid Excel line-break issues caused by long semicolon-delimited lists.
    """
    df = df.copy()
    for col in ["from_value", "to_value"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: truncate_value_field(x, max_entries))
    return df

@task(name="flatten diff to dataframe", log_prints=True)
def flatten_diff_to_dataframe(
    diff_result: dict,
    from_version: str,
    to_version: str,
) -> pd.DataFrame:
    """
    Flatten the nested dict returned by diff_models into a clean TSV-friendly dataframe.

    Filtering rules applied:
    - nodes/edges/props/terms with change_type ADDITION or DELETION get attribute='node_existence'
    - For CHANGED entries:
        - attribute='model' is dropped (just records model handle change, not meaningful)
        - attribute='props' on nodes/edges is dropped (redundant — props entity_type covers it)
        - all other attributes are kept
    """
    rows = []

    for ent_type in ["nodes", "edges", "props", "terms"]:
        section = diff_result.get(ent_type, {})
        if not section:
            continue

        # ── removed entities (present in old, absent in new) ──────────────────
        for key, val in (section.get("removed") or {}).items():
            rows.append({
                "entity_type":  ent_type,
                "key":          str(key),
                "change_type":  "DELETION",
                "attribute":    f"{ent_type}_existence",
                "from_value":   _serialize_value(val),
                "to_value":     "",
                "from_version": from_version,
                "to_version":   to_version,
            })

        # ── added entities (absent in old, present in new) ────────────────────
        for key, val in (section.get("added") or {}).items():
            rows.append({
                "entity_type":  ent_type,
                "key":          str(key),
                "change_type":  "ADDITION",
                "attribute":    f"{ent_type}_existence",
                "from_value":   "",
                "to_value":     _serialize_value(val),
                "from_version": from_version,
                "to_version":   to_version,
            })

        # ── changed entities ──────────────────────────────────────────────────
        for key, attr_dict in (section.get("changed") or {}).items():
            if not isinstance(attr_dict, dict):
                continue
            for attr, change in attr_dict.items():
                # drop 'model' attribute — just records handle name change, not useful
                if attr == "model":
                    continue

                # drop 'props' attribute on nodes/edges — covered by props entity_type
                if attr == "props" and ent_type in ("nodes", "edges"):
                    continue

                # skip if change is not a dict with removed/added keys
                if not isinstance(change, dict):
                    continue

                from_val = _serialize_value(change.get("removed"))
                to_val   = _serialize_value(change.get("added"))

                # skip if both sides are empty after serialization
                if not from_val and not to_val:
                    continue

                rows.append({
                    "entity_type":  ent_type,
                    "key":          str(key),
                    "change_type":  "CHANGED",
                    "attribute":    attr,
                    "from_value":   from_val,
                    "to_value":     to_val,
                    "from_version": from_version,
                    "to_version":   to_version,
                })

    df = pd.DataFrame(rows, columns=[
        "entity_type", "key", "change_type", "attribute",
        "from_value", "to_value", "from_version", "to_version",
    ])

    # sort for readability: entity_type, then change_type, then key
    df = df.sort_values(
        by=["entity_type", "change_type", "key", "attribute"],
        ignore_index=True,
    )

    return df


# ── database querying ─────────────────────────────────────────────────────────
@task(name="Query node property", log_prints=True)
def query_node_property(driver, node: str, prop: str) -> list[dict]:
    """
    Query all records for a given node and property from the database.
    Traverses up to the study node to retrieve study_id.
    Returns a list of dicts with: study_id, node, property, guid, current_value.
    """
    query = f"""
        MATCH (n:{node})
        WHERE n.{prop} IS NOT NULL
        OPTIONAL MATCH (n)-[*0..5]->(s:study)
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


@task(name="Check DB data against diff", cache_policy=NO_CACHE)
def check_data_against_diff(
    driver,
    diff_df: pd.DataFrame,
    mdf_new: MDFReader,
    logger,
) -> pd.DataFrame:
    """
    For each DELETION or CHANGED row in diff_df that relates to a prop,
    query the database to find records whose current values may be invalid
    under the new model.

    Returns a line-level report dataframe with columns:
        study_id, node, property, guid, current_value, change_type, attribute, issue
    """
    # only props are directly queryable against node data
    actionable = diff_df[
        (diff_df["entity_type"] == "props") &
        (
            diff_df["change_type"].isin(["DELETION", "CHANGED"])
        )
    ].copy()

    report_rows = []

    for _, row in actionable.iterrows():
        key       = row["key"]       # e.g. ('node_name', 'prop_name') as string
        attr      = row["attribute"]
        change    = row["change_type"]
        from_val  = row["from_value"]
        to_val    = row["to_value"]

        # parse node and prop from the key — bento-mdf uses tuple keys like ('node', 'prop')
        try:
            key_parsed = eval(key)  # converts "('node', 'prop')" string back to tuple
            if isinstance(key_parsed, tuple) and len(key_parsed) == 2:
                node, prop = key_parsed
            else:
                logger.warning(f"Unexpected key format: {key}, skipping.")
                continue
        except Exception:
            logger.warning(f"Could not parse key: {key}, skipping.")
            continue

        logger.info(f"Querying database for node={node}, property={prop}, change={change}, attribute={attr}")

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

            elif change == "CHANGED":
                if attr in ("value_set", "concept"):
                    # check if current value was in the removed terms
                    removed_terms = set(from_val.split(";")) if from_val else set()
                    valid_terms   = set(to_val.split(";")) if to_val else set()
                    if str(current_value) in removed_terms and str(current_value) not in valid_terms:
                        issue = f"Value '{current_value}' removed from value set"

                elif attr == "value_domain":
                    issue = f"Property type changed from '{from_val}' to '{to_val}'"

                elif attr == "is_required":
                    if to_val == "True" and (current_value is None or current_value == ""):
                        issue = "Property is now required but value is missing"

            if issue:
                report_rows.append({
                    "study_id":      record.get("study_id", "unknown"),
                    "node":          node,
                    "property":      prop,
                    "guid":          record.get("guid", ""),
                    "current_value": current_value,
                    "change_type":   change,
                    "attribute":     attr,
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
    logger.info(
        f"Fetching model files for {old_model_repository} at {old_model_version} "
        f"and {new_model_repository} at {new_model_version}."
    )

    old_model_file_yaml = pull_model_data_files(
        model=old_model_repository, version=old_model_version,
        file_type="model", output_file="old_model.yaml",
    )
    logger.info(f"{old_model_repository} model at {old_model_version} found.")

    old_props_file_yaml = pull_model_data_files(
        model=old_model_repository, version=old_model_version,
        file_type="props", output_file="old_props.yaml",
    )
    logger.info(f"{old_model_repository} properties at {old_model_version} found.")

    new_model_file_yaml = pull_model_data_files(
        model=new_model_repository, version=new_model_version,
        file_type="model", output_file="new_model.yaml",
    )
    logger.info(f"{new_model_repository} model at {new_model_version} found.")

    new_props_file_yaml = pull_model_data_files(
        model=new_model_repository, version=new_model_version,
        file_type="props", output_file="new_props.yaml",
    )
    logger.info(f"{new_model_repository} properties at {new_model_version} found.")

    # ── load models via MDFReader ─────────────────────────────────────────────
    logger.info("Loading models via MDFReader.")
    mdf_old = MDFReader(old_model_file_yaml, old_props_file_yaml, handle=old_model_version)
    mdf_new = MDFReader(new_model_file_yaml, new_props_file_yaml, handle=new_model_version)
    logger.info("MDFReader models loaded successfully.")

    # ── run bento-mdf diff ────────────────────────────────────────────────────
    logger.info("Running bento-mdf diff_models comparison.")
    try:
        bento_diff_result = diff_models(
            mdf_old.model,
            mdf_new.model,
            objects_as_dicts=True,
            include_summary=True,
        )
        logger.info(f"bento-mdf diff summary: {bento_diff_result.get('summary', {})}")
    except Exception as e:
        logger.exception(f"diff_models failed: {type(e).__name__}: {e}")
        raise

    # ── flatten and clean diff output ─────────────────────────────────────────
    logger.info("Flattening and cleaning diff output.")
    diff_df = flatten_diff_to_dataframe(
        diff_result=bento_diff_result,
        from_version=old_model_version,
        to_version=new_model_version,
    )
    logger.info(f"Diff contains {len(diff_df)} meaningful rows after filtering.")

    # ── save diff output ──────────────────────────────────────────────────────
    comparison_file_name = f"{prefix}_comparison_{current_date}.tsv"
    diff_df.to_csv(comparison_file_name, sep="\t", index=False)
    file_ul(bucket=bucket, output_folder=output_folder, sub_folder="", newfile=comparison_file_name)
    logger.info(f"Comparison written to {comparison_file_name}")

    # ── check against database (uses full untruncated diff_df) ───────────────
    if check_against_database:
        logger.info("Acquiring database credentials from AWS.")
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
                mdf_new=mdf_new,
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

    # ── truncate for Excel readability before saving ──────────────────────────
    diff_df_truncated = truncate_diff_dataframe(diff_df, max_entries=10)

    comparison_file_name = f"{prefix}_comparison_truncated_{current_date}.tsv"
    diff_df_truncated.to_csv(comparison_file_name, sep="\t", index=False)
    file_ul(bucket=bucket, output_folder=output_folder, sub_folder="", newfile=comparison_file_name)
    logger.info(f"Comparison written to {comparison_file_name}")

    logger.info(f"Done. Outputs written to {output_folder}")