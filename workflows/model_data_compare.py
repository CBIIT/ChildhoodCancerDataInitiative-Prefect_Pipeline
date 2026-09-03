import requests
import pandas as pd
import os
from prefect import flow, task, get_run_logger
from prefect.input import RunInput
from src.utils import get_secret_centralized_worker, get_time, file_dl, file_ul
from bento_mdf import MDFReader
from bento_mdf.diff import diff_models
from neo4j import GraphDatabase, basic_auth
from prefect.cache_policies import NO_CACHE


class InputValues(RunInput):
    node: str
    property: str

# ── helpers ───────────────────────────────────────────────────────────────────

@task(name="Pull model data files", log_prints=True, cache_policy=NO_CACHE)
def pull_model_data_files(model, version, file_type, output_file):
    logger = get_run_logger()
    if file_type == "model":
        url = f"https://raw.githubusercontent.com/CBIIT/{model}/{version}/model-desc/{model}.yml"
    elif file_type == "props":
        url = f"https://raw.githubusercontent.com/CBIIT/{model}/{version}/model-desc/{model}-{file_type}.yml"
    response = requests.get(url)
    response.raise_for_status()

    with open(output_file, "w") as f:
        f.write(response.text)

    logger.info(f"Downloaded {file_type} file for {model} at {version} to {output_file}")
    return output_file


def _serialize_value(val) -> str:
    """Safely serialize a diff value to a readable string."""
    if val is None:
        return ""
    if isinstance(val, dict):
        return ";".join(str(k) for k in val.keys())
    if isinstance(val, (list, tuple)):
        return ";".join(str(v) for v in val)
    return str(val)


def truncate_value_field(value: str, max_entries: int = 10, delimiter: str = ";") -> str:
    """Truncate a delimited value field to max_entries items, appending a note if truncated."""
    if not value or not isinstance(value, str):
        return value
    parts = value.split(delimiter)
    if len(parts) <= max_entries:
        return value
    truncated = delimiter.join(parts[:max_entries])
    return f"{truncated} ... [{len(parts) - max_entries} more values truncated]"


def truncate_diff_dataframe(df: pd.DataFrame, max_entries: int = 10) -> pd.DataFrame:
    """Truncate from_value and to_value columns to avoid Excel line-break issues."""
    df = df.copy()
    for col in ["from_value", "to_value"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: truncate_value_field(x, max_entries))
    return df


def _parse_key(key: str, ent_type: str = "") -> tuple[str, str]:
    """
    Parse a bento-mdf diff key string into (node, property).
    Handles tuples of length 2 (props), 3 (edges), and plain strings (nodes/terms).

    For edges, the node column is the source node of the relationship,
    and the property column is derived from the destination node as [dst].[dst]_id.

    Example:
        key = "('of_cell_line', 'cell_line', 'participant')"
        returns: ("of_cell_line --[cell_line]--> participant", "participant.participant_id")
    """
    try:
        key_parsed = eval(key)
        if isinstance(key_parsed, tuple) and len(key_parsed) == 2:
            return str(key_parsed[0]), str(key_parsed[1])
        elif isinstance(key_parsed, tuple) and len(key_parsed) == 3:
            # edges: (src_relationship, relationship_label, dst_node)
            src, rel, dst = key_parsed
            node = f"{src} --[{rel}]--> {dst}"
            prop = f"{dst}.{dst}_id"
            return node, prop
        else:
            return str(key_parsed), ""
    except Exception:
        return key, ""


# ── model loading ─────────────────────────────────────────────────────────────

@task(name="Load MDFReader models", log_prints=True, cache_policy=NO_CACHE)
def load_mdf_models(
    old_model_file: str,
    old_props_file: str,
    old_version: str,
    new_model_file: str,
    new_props_file: str,
    new_version: str,
) -> tuple[MDFReader, MDFReader]:
    logger = get_run_logger()
    logger.info("Loading models via MDFReader.")
    mdf_old = MDFReader(old_model_file, old_props_file, handle=old_version)
    mdf_new = MDFReader(new_model_file, new_props_file, handle=new_version)
    logger.info("MDFReader models loaded successfully.")
    return mdf_old, mdf_new


# ── diff ──────────────────────────────────────────────────────────────────────

@task(name="Run bento-mdf diff", log_prints=True, cache_policy=NO_CACHE)
def run_bento_diff(mdf_old: MDFReader, mdf_new: MDFReader) -> dict:
    logger = get_run_logger()
    logger.info("Running bento-mdf diff_models comparison.")
    try:
        bento_diff_result = diff_models(
            mdf_old.model,
            mdf_new.model,
            objects_as_dicts=True,
            include_summary=True,
        )
        logger.info(f"bento-mdf diff summary: {bento_diff_result.get('summary', {})}")
        return bento_diff_result
    except Exception as e:
        logger.exception(f"diff_models failed: {type(e).__name__}: {e}")
        raise


@task(name="Flatten diff to dataframe", log_prints=True, cache_policy=NO_CACHE)
def flatten_diff_to_dataframe(
    diff_result: dict,
    from_version: str,
    to_version: str,
) -> pd.DataFrame:
    """
    Flatten the nested dict returned by diff_models into a clean TSV-friendly dataframe.

    Filtering rules:
    - ADDITION/DELETION rows get attribute='[entity_type]_existence'
    - attribute='model' is dropped (just records model handle change, not meaningful)
    - attribute='props' on nodes/edges is dropped (redundant — props entity_type covers it)
    - rows where both from_value and to_value are empty after serialization are dropped
    """
    logger = get_run_logger()
    rows = []

    for ent_type in ["nodes", "edges", "props", "terms"]:
        section = diff_result.get(ent_type, {})
        if not section:
            continue

        # ── removed entities ──────────────────────────────────────────────────
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

        # ── added entities ────────────────────────────────────────────────────
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
                if attr == "model":
                    continue
                if attr == "props" and ent_type in ("nodes", "edges"):
                    continue
                if not isinstance(change, dict):
                    continue

                from_val = _serialize_value(change.get("removed"))
                to_val   = _serialize_value(change.get("added"))

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
    df = df.sort_values(
        by=["entity_type", "change_type", "key", "attribute"],
        ignore_index=True,
    )

    logger.info(f"Diff contains {len(df)} meaningful rows after filtering.")
    return df


# ── comparison report ─────────────────────────────────────────────────────────

@task(name="Build comparison report", log_prints=True, cache_policy=NO_CACHE)
def build_comparison_report(
    diff_df: pd.DataFrame,
    from_version: str,
    to_version: str,
) -> pd.DataFrame:
    """
    Build a clean human-readable comparison report from the diff dataframe.
    - Drops terms entity_type rows (too granular for this report)
    - Parses tuple keys into readable node/property columns
    - Generates a human-readable description column for each change
    - Truncates from_value/to_value to 10 entries for Excel readability
    """
    logger = get_run_logger()

    # drop terms — too granular and noisy for the comparison report
    df = diff_df[diff_df["entity_type"] != "terms"].copy()

    rows = []
    for _, row in df.iterrows():
        ent_type    = row["entity_type"]
        key         = row["key"]
        change_type = row["change_type"]
        attribute   = row["attribute"]
        from_val    = row["from_value"]
        to_val      = row["to_value"]

        node, prop = _parse_key(key)

        # build a human-readable description of the change
        if change_type == "DELETION":
            description = f"{ent_type.rstrip('s').capitalize()} removed from model"

        elif change_type == "ADDITION":
            description = f"{ent_type.rstrip('s').capitalize()} added to model"

        elif change_type == "CHANGED":
            if attribute == "value_domain":
                description = f"Type changed: '{from_val}' → '{to_val}'"

            elif attribute in ("value_set", "concept"):
                from_terms = set(from_val.split(";")) if from_val else set()
                to_terms   = set(to_val.split(";")) if to_val else set()
                removed    = from_terms - to_terms
                added      = to_terms - from_terms
                parts = []
                if removed:
                    parts.append(f"{len(removed)} term(s) removed")
                if added:
                    parts.append(f"{len(added)} term(s) added")
                description = "Value set changed: " + ", ".join(parts)

            elif attribute == "is_required":
                if to_val == "True":
                    description = "Property became required"
                else:
                    description = "Property became optional"

            elif attribute == "is_key":
                description = f"Key status changed: '{from_val}' → '{to_val}'"

            elif attribute == "is_deprecated":
                description = f"Deprecated status changed: '{from_val}' → '{to_val}'"

            elif attribute in ("src", "dst"):
                description = f"Edge endpoint changed ({attribute}): '{from_val}' → '{to_val}'"

            elif attribute.endswith("_existence"):
                description = f"{ent_type.rstrip('s').capitalize()} existence changed"

            else:
                description = f"Attribute '{attribute}' changed: '{from_val}' → '{to_val}'"
        else:
            description = change_type

        rows.append({
            "entity_type":  ent_type,
            "node":         node,
            "property":     prop,
            "change_type":  change_type,
            "attribute":    attribute,
            "description":  description,
            "from_value":   truncate_value_field(from_val, max_entries=10),
            "to_value":     truncate_value_field(to_val, max_entries=10),
            "from_version": from_version,
            "to_version":   to_version,
        })

    result = pd.DataFrame(rows, columns=[
        "entity_type", "node", "property", "change_type",
        "attribute", "description", "from_value", "to_value",
        "from_version", "to_version",
    ])
    result = result.sort_values(
        by=["entity_type", "change_type", "node", "property"],
        ignore_index=True,
    )

    logger.info(f"Comparison report built with {len(result)} rows.")
    return result


# ── data report summary ───────────────────────────────────────────────────────

@task(name="Build data report summary", log_prints=True, cache_policy=NO_CACHE)
def build_data_report_summary(data_report_df: pd.DataFrame) -> pd.DataFrame:
    """
    Summarize the line-level data report into a higher-level view showing
    which study, node, and property combinations need attention, and how many
    records are affected.
    """
    logger = get_run_logger()

    if data_report_df.empty:
        logger.info("Data report is empty — no summary to build.")
        return pd.DataFrame(columns=[
            "study_id", "node", "property", "change_type",
            "attribute", "issue", "affected_record_count",
        ])

    summary = (
        data_report_df.groupby(
            ["study_id", "node", "property", "change_type", "attribute", "issue"],
            dropna=False,
        )
        .size()
        .reset_index(name="affected_record_count")
        .sort_values(
            by=["study_id", "node", "property"],
            ignore_index=True,
        )
    )

    logger.info(
        f"Data report summary built: {len(summary)} unique study/node/property/issue combinations "
        f"across {data_report_df['study_id'].nunique()} studies."
    )
    return summary


# ── database querying ─────────────────────────────────────────────────────────

def query_node_property(driver, node: str, prop: str) -> list[dict]:
    """Query all records for a given node/property, traversing up to study for study_id."""
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


@task(name="Check DB data against diff", log_prints=True, cache_policy=NO_CACHE)
def check_data_against_diff(
    driver,
    diff_df: pd.DataFrame,
    mdf_new: MDFReader,
) -> pd.DataFrame:
    """
    For each DELETION or CHANGED props row in diff_df, query the database
    to find records whose current values may be invalid under the new model.

    Returns a line-level report with:
        study_id, node, property, guid, current_value, change_type, attribute, issue
    """
    logger = get_run_logger()

    actionable = diff_df[
        (diff_df["entity_type"] == "props") &
        (diff_df["change_type"].isin(["DELETION", "CHANGED"]))
    ].copy()

    logger.info(f"Found {len(actionable)} actionable prop rows to check against database.")
    report_rows = []

    for _, row in actionable.iterrows():
        key      = row["key"]
        attr     = row["attribute"]
        change   = row["change_type"]
        from_val = row["from_value"]
        to_val   = row["to_value"]

        # parse node and prop from the tuple key string
        try:
            key_parsed = eval(key)
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

    logger.info(f"Database check complete. Found {len(report_rows)} records with potential issues.")
    return pd.DataFrame(report_rows)


@task(name="Save and upload file", log_prints=True, cache_policy=NO_CACHE)
def save_and_upload(
    df: pd.DataFrame,
    file_name: str,
    bucket: str,
    output_folder: str,
) -> None:
    logger = get_run_logger()
    df.to_csv(file_name, sep="\t", index=False)
    file_ul(bucket=bucket, output_folder=output_folder, sub_folder="", newfile=file_name)
    logger.info(f"Saved and uploaded: {file_name}")


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
    old_props_file_yaml = pull_model_data_files(
        model=old_model_repository, version=old_model_version,
        file_type="props", output_file="old_props.yaml",
    )
    new_model_file_yaml = pull_model_data_files(
        model=new_model_repository, version=new_model_version,
        file_type="model", output_file="new_model.yaml",
    )
    new_props_file_yaml = pull_model_data_files(
        model=new_model_repository, version=new_model_version,
        file_type="props", output_file="new_props.yaml",
    )

    # ── load models ───────────────────────────────────────────────────────────
    mdf_old, mdf_new = load_mdf_models(
        old_model_file=old_model_file_yaml,
        old_props_file=old_props_file_yaml,
        old_version=old_model_version,
        new_model_file=new_model_file_yaml,
        new_props_file=new_props_file_yaml,
        new_version=new_model_version,
    )

    # ── run diff ──────────────────────────────────────────────────────────────
    bento_diff_result = run_bento_diff(mdf_old=mdf_old, mdf_new=mdf_new)

    # ── flatten diff ──────────────────────────────────────────────────────────
    diff_df = flatten_diff_to_dataframe(
        diff_result=bento_diff_result,
        from_version=old_model_version,
        to_version=new_model_version,
    )

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
            )
        finally:
            driver.close()
            logger.info("Database driver closed.")

        # ── save full line-level data report (existing output) ────────────────
        if data_report_df.empty:
            logger.info("No data issues found against the new model.")
        else:
            logger.info(f"Found {len(data_report_df)} records with potential data issues.")

        save_and_upload(
            df=data_report_df,
            file_name=f"{prefix}_data_report_{current_date}.tsv",
            bucket=bucket,
            output_folder=output_folder,
        )

        # ── save summarized data report (new output) ──────────────────────────
        data_report_summary = build_data_report_summary(data_report_df)
        save_and_upload(
            df=data_report_summary,
            file_name=f"{prefix}_data_report_summary_{current_date}.tsv",
            bucket=bucket,
            output_folder=output_folder,
        )

    # ── save full untruncated diff (existing output) ──────────────────────────
    save_and_upload(
        df=diff_df,
        file_name=f"{prefix}_comparison_{current_date}.tsv",
        bucket=bucket,
        output_folder=output_folder,
    )

    # ── save truncated diff for Excel (existing output) ───────────────────────
    diff_df_truncated = truncate_diff_dataframe(diff_df, max_entries=10)
    save_and_upload(
        df=diff_df_truncated,
        file_name=f"{prefix}_comparison_truncated_{current_date}.tsv",
        bucket=bucket,
        output_folder=output_folder,
    )

    # ── save human-readable comparison report (new output) ───────────────────
    comparison_report = build_comparison_report(
        diff_df=diff_df,
        from_version=old_model_version,
        to_version=new_model_version,
    )
    save_and_upload(
        df=comparison_report,
        file_name=f"{prefix}_comparison_report_{current_date}.tsv",
        bucket=bucket,
        output_folder=output_folder,
    )

    logger.info(f"Done. Outputs written to {output_folder}")