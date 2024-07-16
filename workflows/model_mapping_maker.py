# model_mapping_maker

import yaml
import requests
import pandas as pd
from datetime import date
import os
from dataclasses import dataclass
from prefect import flow, get_run_logger, pause_flow_run
from prefect.input import RunInput
from src.utils import (
    get_time,
    get_date,
    get_manifest_phs,
    file_dl,
    file_ul,
    folder_dl,
    view_all_s3_objects,
    markdown_input_task,
    markdown_output_task,
    check_ccdi_version,
    dl_ccdi_template,
    dl_sra_template,
    get_ccdi_latest_release,
    ccdi_wf_inputs_ul,
    ccdi_wf_outputs_ul,
    identify_data_curation_log_file,
    ccdi_to_dcf_index,
)


class InputValues(RunInput):
    node: str
    property: str


@dataclass
class InputDescription:
    index: int
    node: str
    property: str

    """dataclass for wait for input description MD"""

    old_to_new_input: str = (
        f"""
**Please provide inputs as shown below**

If these values were moved to a new location, please enter the new node and/or property.

- If a value is staying the same, write 'same'.
- If a value is removed, write 'remove'.

{index}. node: {node}, property: {property}

- **node**: the new node/nodes the property is located in. For lists, use ';' as the separator.
- **property**: the new property name.

"""
    )
    new_to_old_input: str = (
        f"""
**Please provide inputs as shown below**

If these values are being pulled from an older location, please enter the old node and/or property.

- If a value is staying the same, write 'same'.
- If a value is removed, write 'remove'.

{index}. node: {node}, property: {property}

- **node**: the old node/nodes the property is located in.
- **property**: the old property name.

"""
    )


# obtain the date
def refresh_date():
    today = date.today()
    today = today.strftime("%Y%m%d")
    return today


def read_yaml_from_github(url):
    # Fetch the content of the file from the given URL
    response = requests.get(url)
    response.raise_for_status()  # Raise an HTTPError for bad responses

    # Load the YAML content
    yaml_content = yaml.safe_load(response.text)

    return yaml_content


@flow(
    name="Extract_properties",
    log_prints=True,
)
def extract_properties(yaml_data):
    properties = []

    if "Version" in yaml_data:
        version = yaml_data["Version"]
    else:
        version = "insert version"

    for Nodes, Props in yaml_data["Nodes"].items():
        for Prop in Props["Props"]:
            properties.append({"node": Nodes, "property": Prop, "version": version})

    return pd.DataFrame(properties)


@flow(
    name="Extract_relationships",
    log_prints=True,
)
def extract_relationships(yaml_data):

    relationships = []

    if "Version" in yaml_data:
        version = yaml_data["Version"]
    else:
        version = "insert version"

    for relationship in yaml_data["Relationships"].items():
        relationship = relationship[1]["Ends"]

        for ends in relationship:
            src = ends.get("Src")
            dst = ends.get("Dst")
            relationships.append({"Src": src, "Dst": dst, "version": version})

    return pd.DataFrame(relationships)


@flow(
    name="Source_Destination_to_Node_Properties",
    log_prints=True,
)
def src_dst_to_node_prop(df, src_col, dst_col):
    # quick logic to determine if the node / prop column is new or old.
    if "old" in src_col.lower():
        node = "node_old"
    else:
        node = "node_new"

    if "old" in dst_col.lower():
        property = "property_old"
    else:
        property = "property_new"

    # for each row create a node / prop_id entry based on the src / dst mapping.
    for index, row in df.iterrows():
        if pd.isna(df.at[index, src_col]) or pd.isna(df.at[index, dst_col]):
            df.at[index, node] = None
            df.at[index, property] = None
        else:
            df.at[index, node] = df.at[index, src_col]
            df.at[index, property] = f"{df.at[index,dst_col]}.{df.at[index,dst_col]}_id"

    return df


def clean_up_partial_dups(
    df, empty_col_node, empty_col_prop, value_col_node, value_col_prop
):
    # Do final fixes to remove new mappings that have been accounted for.
    indexes_to_remove = []
    for index, row in df.iterrows():
        # if the old node or property is blank
        if pd.isna(row[empty_col_node]) or pd.isna(row[empty_col_prop]):

            # look at the new node and property
            new_node_value = df.at[index, value_col_node]
            new_property_value = df.at[index, value_col_prop]

            # then create a filter that looks for any other instances where there are duplicates of the new node/property value found in other places.
            mask = (df[value_col_node] == new_node_value) & (
                df[value_col_prop] == new_property_value
            )

            # add those indexes of duplicate new node/property values to a list
            indexes = df.index[mask].tolist()

            # If other instances exist for this, see if there is a conversion from an older version
            # If there is, then remove this blanked version for the new node and property.
            if len(indexes) > 1:
                for other_index in indexes:
                    other_row = df.iloc[other_index]
                    if pd.isna(other_row[empty_col_node]) and pd.isna(
                        other_row[empty_col_prop]
                    ):
                        indexes_to_remove.append(index)
                        indexes_to_remove = list(set(indexes_to_remove))

            else:
                pass

    # remove redundant or incomplete rows compared to already existing rows
    df = df.drop(indexes_to_remove)
    df = df.fillna("")

    return df


# user determines where deleted properties go
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
    runner_logger = get_run_logger()

    if base_mode:
        runner_logger.info("You are running in base mode, no need for input.")
    else:
        runner_logger.info("You are not running in base mode, input needed.")

    # looking at the missing values in one column, determine values to map to each node and property.
    df_missing = df[df[missing_property_col].isna()]
    # for each row with missing information in the column of interest
    for index, row in df_missing.iterrows():
        node= row[value_node_col]
        property=row[value_property_col]
        runner_logger.info(
            f"{index}. node: {node}, property: {property}"
        )

        # if in base mode, skip inputs and keep it all blank
        if base_mode:
            user_input_node = "remove"
            user_input_prop = "remove"
        # else, allow user input
        else:
            if direction == "oldnew":
                value_inputs = pause_flow_run(
                    wait_for_input=InputValues.with_initial_data(
                        description=InputDescription.old_to_new_input(index=index,node=node,property=property)
                    )
                )
            elif direction == "newold":
                value_inputs = pause_flow_run(
                    wait_for_input=InputValues.with_initial_data(
                        description=InputDescription.new_to_old_input(index=index,node=node,property=property)
                    )
                )

            runner_logger.info(
                "Inputs received:" + "\n"
                + f"node: {value_inputs.node}"
                + "\n"
                + f"property: {value_inputs.property}"
            )
            user_input_node = value_inputs.node
            user_input_prop = value_inputs.property

        # make things easier, if it is the same value, say "same".
        if user_input_node.lower() == "same":
            user_input_node = row[value_node_col]
        if user_input_prop.lower() == "same":
            user_input_prop = row[value_property_col]

        # make things easier, if it is no longer needed, ignore and leave blank.
        if user_input_node.lower() == "remove":
            user_input_node = None
        if user_input_prop.lower() == "remove":
            user_input_prop = None

        # obtain the model version based on the input missing column
        new_model_version = df[missing_version_col].dropna().unique()[0]

        # apply values to the data frame
        df.at[index, missing_node_col] = user_input_node
        df.at[index, missing_property_col] = user_input_prop
        df.at[index, missing_version_col] = new_model_version


@flow(
    name="Model Mapping Maker",
    log_prints=True,
    flow_run_name="{runner}_" + f"{get_time()}",
)
def runner(
    bucket: str,
    runner: str,
    model_repository: str = "ccdi-model",
    old_model_version: str = "",
    new_model_version: str = "",
    nodes_mapping_file: str = "path_to/nodes_file/in/s3_bucket",
    relationship_mapping_file: str = "path_to/nodes_file/in/s3_bucket",
    base_mode: bool = False,
):

    # create a logging object
    runner_logger = get_run_logger()

    # Null out the suggested file path if not overwritten
    if nodes_mapping_file == "path_to/nodes_file/in/s3_bucket":
        nodes_mapping_file = None
    if relationship_mapping_file == "path_to/nodes_file/in/s3_bucket":
        relationship_mapping_file = None

    # download the configuration file inputs if they are given.
    if nodes_mapping_file:
        file_dl(bucket, nodes_mapping_file)
    if relationship_mapping_file:
        file_dl(bucket, relationship_mapping_file)

    # EVERYTHING

    # obtain date
    current_date = refresh_date()

    # URL of the raw YAML file on GitHub
    new_model_url = f"https://raw.githubusercontent.com/CBIIT/{model_repository}/{new_model_version}/model-desc/ccdi-model.yml"
    old_model_url = f"https://raw.githubusercontent.com/CBIIT/{model_repository}/{old_model_version}/model-desc/ccdi-model.yml"

    # Read the YAML content from the URL
    yaml_content_new = read_yaml_from_github(new_model_url)
    runner_logger.info(f"{model_repository} at {new_model_version} found.")
    yaml_content_old = read_yaml_from_github(old_model_url)
    runner_logger.info(f"{model_repository} at {old_model_version} found.")

    # Extract properties from the YAML data
    df_new = extract_properties(yaml_content_new)
    df_old = extract_properties(yaml_content_old)

    df_new.columns = ["node_new", "property_new", "version_new"]
    df_old.columns = ["node_old", "property_old", "version_old"]

    # Merge the dataframes with outer join
    merged_df = pd.merge(
        df_old,
        df_new,
        left_on=["node_old", "property_old"],
        right_on=["node_new", "property_new"],
        how="outer",
    )

    # Extract relationships from the YAML data
    relationships_new = extract_relationships(yaml_content_new)
    relationships_old = extract_relationships(yaml_content_old)

    relationships_new.columns = ["src_new", "dst_new", "version_new"]
    relationships_old.columns = ["src_old", "dst_old", "version_old"]

    # Merge the dataframes with outer join
    merged_df_relate = pd.merge(
        relationships_old,
        relationships_new,
        left_on=["src_old", "dst_old"],
        right_on=["src_new", "dst_new"],
        how="outer",
    )

    # convert source/destination values into node/property values
    merged_df_relate = src_dst_to_node_prop(merged_df_relate, "src_old", "dst_old")
    merged_df_relate = src_dst_to_node_prop(merged_df_relate, "src_new", "dst_new")

    # get rid of old columns
    merged_df_relate.drop(
        columns=["src_old", "dst_old", "src_new", "dst_new"], inplace=True
    )

    # if there isn't an input file, run through asking for input
    if not nodes_mapping_file:
        # Take input to create the base mapping file for nodes and properties
        user_input_location(
            merged_df,
            "node_old",
            "property_old",
            "node_new",
            "property_new",
            "version_new",
            base_mode,
            "oldnew",
        )
    # use the mapping file
    else:
        merged_df = pd.read_csv(os.path.basename(nodes_mapping_file), sep="\t")

    # Create new df based on input for the diffs that were noted.
    new_merged = []

    # for the property node mapping file, clean it up
    for index, row in merged_df.iterrows():
        # Add rows where 'node_new' is NA or None
        if pd.isna(row["node_new"]) or row["node_new"] in ["NA", "none"]:
            new_row = row.copy()
            new_merged.append(new_row)
        else:
            # check for node values that are a list and split them out
            node_values = (
                row["node_new"].split(";")
                if ";" in row["node_new"]
                else [row["node_new"]]
            )
            first_value = True
            for value in node_values:
                if first_value:
                    # Add the original row with the first split value
                    new_row = row.copy()
                    new_row["node_new"] = value
                    new_merged.append(new_row)
                    first_value = False
                else:
                    # Add new rows with subsequent split values
                    new_row = row.copy()
                    new_row["node_new"] = value
                    new_merged.append(new_row)

    # Create a new DataFrame from the new rows
    new_merged_df = pd.DataFrame(new_merged)

    # Reset the index of the new DataFrame
    new_merged_df.reset_index(drop=True, inplace=True)

    # Do final fixes to remove new mappings that have been accounted for.
    new_merged_df = clean_up_partial_dups(
        new_merged_df, "node_old", "property_old", "node_new", "property_new"
    )

    # fix version columns, because the check is only one way, old to new,
    # it doesn't know about columns that have new values but no value for the old one,
    # thus it doesn't fill in the old version number
    new_merged_df["version_old"] = (
        new_merged_df["version_old"].dropna().unique().tolist()[0]
    )

    nodes_mapping_file_name = (
        f"{old_model_version}_{new_model_version}_nodes_{current_date}.tsv"
    )

    new_merged_df.to_csv(
        nodes_mapping_file_name,
        sep="\t",
        index=False,
    )

    # UPLOAD NODES file

    output_folder = os.path.join(runner, "model_mapping_maker_" + get_time())

    file_ul(
        bucket=bucket,
        output_folder=output_folder,
        sub_folder="",
        newfile=nodes_mapping_file_name,
    )

    # final setup to make sure that [node].[node]_ids get moved when a property moves from
    # one node to a new node, especially an established node, it is likely this is needed.

    if not relationship_mapping_file:
        # Take inputs for relationship values that are found in the old model but are not located in the new model.
        user_input_location(
            merged_df_relate,
            "node_old",
            "property_old",
            "node_new",
            "property_new",
            "version_new",
            base_mode,
            "oldnew",
        )

        # Take inputs for relationship values that are found in the new model but are not located in the old model.
        user_input_location(
            merged_df_relate,
            "node_new",
            "property_new",
            "node_old",
            "property_old",
            "version_old",
            base_mode,
            "newold",
        )

        merged_df_relate = merged_df_relate.drop_duplicates()

    else:
        merged_df_relate = pd.read_csv(
            os.path.basename(relationship_mapping_file), sep="\t"
        )

    # Do final fixes to remove new mappings that have been accounted for.
    merged_df_relate = clean_up_partial_dups(
        merged_df_relate, "node_old", "property_old", "node_new", "property_new"
    )
    merged_df_relate = clean_up_partial_dups(
        merged_df_relate, "node_new", "property_new", "node_old", "property_old"
    )

    # reorder relationship df to match the node property one.
    merged_df_relate = merged_df_relate[new_merged_df.columns]
    merged_df_relate = merged_df_relate.fillna("")

    relationship_mapping_file_name = (
        f"{old_model_version}_{new_model_version}_relationship_{current_date}.tsv"
    )

    # write out of relationship file
    merged_df_relate.to_csv(
        relationship_mapping_file_name,
        sep="\t",
        index=False,
    )

    # UPLOAD RELATIONSHIP file

    file_ul(
        bucket=bucket,
        output_folder=output_folder,
        sub_folder="",
        newfile=relationship_mapping_file_name,
    )

    # Create concatenation of mapping and nodes plus clean up
    final_merged = pd.concat([new_merged_df, merged_df_relate], ignore_index=True)
    final_merged = final_merged.fillna("")
    final_merged = final_merged.drop_duplicates()

    final_mapping_file_name = (
        f"{old_model_version}_{new_model_version}_MAPPING_{current_date}.tsv"
    )

    # add the linkage properties onto the property data frame

    final_merged.to_csv(
        final_mapping_file_name,
        sep="\t",
        index=False,
    )

    # UPLOAD RELATIONSHIP file

    file_ul(
        bucket=bucket,
        output_folder=output_folder,
        sub_folder="",
        newfile=final_mapping_file_name,
    )

    # Last step to create a more human readable format
    results = []

    # logic to create simplified output for human use
    for index, row in final_merged.iterrows():
        new_values = (
            final_merged.at[index, "node_new"],
            final_merged.at[index, "property_new"],
        )
        old_values = (
            final_merged.at[index, "node_old"],
            final_merged.at[index, "property_old"],
        )

        # Check for NA/None values in new_values and old_values
        new_values_na = any(value == "" for value in new_values)
        old_values_na = any(value == "" for value in old_values)

        # logic flow to note if there are deletions, additions, rearrangements or static

        if new_values_na and not old_values_na:
            state = "DELETION"
        elif old_values_na and not new_values_na:
            state = "ADDITION"
        elif new_values != old_values:
            state = "CHANGED"
        else:
            state = "SAME"

        # Append the row to the results list
        results.append(
            {
                "state": state,
                "node_old": old_values[0],
                "property_old": old_values[1],
                "version_old": old_model_version,
                "node_new": new_values[0],
                "property_new": new_values[1],
                "version_new": new_model_version,
            }
        )

    # Create a new DataFrame from the results list
    comparison_df = pd.DataFrame(results)

    # Drop rows where state is 'SAME' and clean up
    comparison_df = comparison_df[comparison_df["state"] != "SAME"]
    comparison_df = comparison_df.fillna("")
    comparison_df = comparison_df.drop_duplicates()

    comparison_mapping_file_name = (
        f"{old_model_version}_{new_model_version}_comparison_{current_date}.tsv"
    )

    comparison_df.to_csv(
        comparison_mapping_file_name,
        sep="\t",
        index=False,
    )

    # UPLOAD RELATIONSHIP file

    file_ul(
        bucket=bucket,
        output_folder=output_folder,
        sub_folder="",
        newfile=comparison_mapping_file_name,
    )


if __name__ == "__main__":
    bucket = "my-source-bucket"

    # test new version manifest and latest version template
    # file_path = "inputs/test_file.xlsx"
    # template_path = "inputs/CCDI_Submission_Template_v1.7.1.xlsx"
    # sra_template_path = "path_to/sra_template/in/ccdi-curation/bucket"
    # sra_previous_file_path = "QL/phs002790_outputs_20240129_T113511/3_SRA_submisison_output/phs002790_SRA_submission.xlsx"
    # dbgap_previous_path = "QL/phs002790_outputs_20240129_T113511/4_dbGaP_submisison_output/phs002790_dbGaP_submission_2024-01-29"

    runner(
        bucket=bucket,
        # file_path=file_path,
        # template_path=template_path,
        # sra_template_path=sra_template_path,
        runner="SVB",
        # sra_previous_file_path=sra_previous_file_path,
        # dbgap_previous_dir_path=dbgap_previous_path,
    )
