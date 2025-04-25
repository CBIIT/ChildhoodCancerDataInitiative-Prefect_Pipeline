import os
import sys
import pandas as pd
from prefect import flow, task, get_run_logger
from typing import TypeVar
from src.utils import get_logger, get_date, get_time

sys.path.insert(0, os.path.abspath("./prefect-toolkit"))
from src.commons.datamodel import ReadDataModel

DataFrame = TypeVar("DataFrame")


def find_non_empty_props(tsv_folder: str) -> dict:
    """This returns a dictionary of non-empty props with node as key and a list of non-empty props as value

    Args:
        tsv_folder (str): Folder path of the tsv files

    Returns:
        dict: a dictionary of non-empty props with node as key and a list of non-empty props as value
    """

    tsv_files = [
        os.path.join(tsv_folder, f)
        for f in os.listdir(tsv_folder)
        if f.endswith(".tsv")
    ]
    return_dict = {}
    for tsv in tsv_files:
        tsv_df = pd.read_csv(
            tsv,
            sep="\t",
            header=0,
            na_values=["NA", "na", "N/A", "n/a", ""],
            dtype="string",
        )
        # identiy the node type and remove the type column
        tsv_type = tsv_df.loc[0, "type"]
        tsv_df = tsv_df.drop(columns=["type"])
        # find any column that is not emoty
        non_empty_columns = tsv_df.columns[tsv_df.notna().any()]
        return_dict[tsv_type] = non_empty_columns.tolist()

    return return_dict


@task(log_prints=True)
def find_unlifted_props(
    tsv_folder: str, liftover_mapping_filepath: str, logger
) -> dict:
    """This returns a dictionary of unlifted props with node as key and a list of unlifted props as value

    Args:
        tsv_folder (str): Folder path of the tsv files
        liftover_mapping_filepath (str): Mapping file path

    Returns:
        dict: a dictionary of unlifted props with node as key and a list of unlifted props as value
    """
    non_empty_props = find_non_empty_props(tsv_folder=tsv_folder)
    mapping_df = pd.read_csv(liftover_mapping_filepath, sep="\t", header=0)
    mapping_df_lift_from = mapping_df[["lift_from_node", "lift_from_property"]]
    mapping_df_lift_from = mapping_df_lift_from.dropna(how="all", ignore_index=True)

    unlifted_props = pd.DataFrame(columns=["node", "unlifted_property"])

    for node in non_empty_props.keys():
        node_nonempty_props = non_empty_props[node]
        for prop in node_nonempty_props:
            filtered_mapping_df = mapping_df_lift_from[
                (mapping_df_lift_from["lift_from_node"] == node)
                & mapping_df_lift_from["lift_from_property"]
                == prop
            ]
            if filtered_mapping_df.shape[0] == 0:
                unlifted_new_row = pd.DataFrame(
                    {"node": [node], "unlifted_property": [prop]}
                )
                unlifted_props = pd.concat(
                    [unlifted_props, unlifted_new_row], ignore_index=True
                )
            else:
                pass
    if unlifted_props.shape[0] > 0:
        logger.warning(
            f"There are non-empty props that in submission tsv files that are not lifted because no mapping was found for these props."
        )
        print(
            f"There are non-empty props that in submission tsv files that are not lifted because no mapping was found for these props."
        )
        print_unlifted_df = unlifted_props.to_markdown(
            tablefmt="rounded_grid", index=False
        ).replace("\n", "\n\t")
        logger.warning(f"Unlifted props: \n\t{print_unlifted_df}")
        print(f"Unlifted props: \n\t{print_unlifted_df}")
    else:
        logger.info(f"All non-empty props in submission tsv files are lifted.")
        print(f"All non-empty props in submission tsv files are lifted.")

    return None


def model_to_df(model_file: str, props_file: str, node_type: str) -> DataFrame:
    """Generates a dataframe from the model files, and returns an empty dataframe with columns names of a node

    Args:
        model_file (str): model yaml file
        props_file (str): model props yaml file
        node_type (str): node name type

    Returns:
        DataFrame: a dataframe
    """
    readmodel = ReadDataModel(model_file=model_file, prop_file=props_file)
    model_obj = readmodel._get_model()
    node_prop_list = readmodel.get_node_props_list(
        model_obj=model_obj, node_name=node_type
    )
    node_prop_list = ["type"] + node_prop_list
    return_df = pd.DataFrame(columns=node_prop_list)
    return return_df


def identify_file_type(tsv_folder: str) -> dict:
    """This function identifies the file type of the tsv files in the folder

    Args:
        tsv_folder (str): Folder path of the tsv files

    Returns:
        dict: a dictionary of file type with node as key and a list of file type as value
    """
    tsv_files = [
        os.path.join(tsv_folder, f)
        for f in os.listdir(tsv_folder)
        if f.endswith(".tsv")
    ]
    return_dict = {}
    for tsv in tsv_files:
        tsv_df = pd.read_csv(
            tsv,
            sep="\t",
            header=0,
            na_values=["NA", "na", "N/A", "n/a", ""],
            dtype="string",
        )
        # identiy the node type and remove the type column
        tsv_type = tsv_df.loc[0, "type"]
        return_dict[tsv_type] = tsv
    return return_dict


def single_node_liftover(
    mapping_df: DataFrame,
    full_mapping_file: str,
    lift_to_model_file: str,
    lift_to_props_file: str,
    lift_to_node: str,
    lift_from_folder: str,
    output_folder: str,
    logger,
) -> None:
    """Creates a tsv file of lift to node type with the lifted values based off mapping file provided

    Args:
        mapping_df (DataFrame): mapping file dataframe only relavant oto lift_to_node
        full_mapping_file (str): full(complete) mapping file path
        lift_to_model_file (str): lift to model file path
        lift_to_props_file (str): lift to props file path
        lift_to_node (str): lift to node type
        output_folder (str): output folder path that stores tsv files
        logger (_type_): logger object
    """
    # create an empty dataframe with the column name of the node type
    lift_to_df = model_to_df(
        model_file=lift_to_model_file,
        props_file=lift_to_props_file,
        node_type=lift_to_node,
    )
    # if multiple nodes in manifest is associated with template node
    # get a list of manifest nodes, in case there are multiple lift from nodes
    manifest_nodes = (
        mapping_df[mapping_df["lift_to_node"] == lift_to_node]["lift_from_node"]
        .dropna()
        .unique()
        .tolist()
    )
    if len(manifest_nodes) > 1:
        logger.warning(
            f"Lift to sheet {lift_to_node} has lifted value of more than one sheet in manifest: {*manifest_nodes,}"
        )
        print(
            f"Lift to sheet {lift_to_node} has lifted value of more than one sheet in manifest: {*manifest_nodes,}"
        )
    else:
        logger.info(
            f"Lift to sheet {lift_to_node} has lifted value from one sheet in manifest: {*manifest_nodes,}"
        )
        print(
            f"Lift to sheet {lift_to_node} has lifted value from one sheet in manifest: {*manifest_nodes,}"
        )

    lift_from_mapping_dict = identify_file_type(tsv_folder=lift_from_folder)
    # for each manifest node, create a separate dataframe in the mapped template node
    # after liftover, append the df to the concatenate_df
    for n in manifest_nodes:
        print(f"adding {n} to {lift_to_node} sheet")
        # n is the manifest node name, not necessarily equals to template node
        lift_to_n_df = model_to_df(
            model_file=lift_to_model_file,
            props_file=lift_to_props_file,
            node_type=lift_to_node,
        )
        lift_from_tsv = lift_from_mapping_dict[n]
        lift_from_df = pd.read_csv(
            lift_from_tsv,
            sep="\t",
            header=0,
            na_values=["NA", "na", "N/A", "n/a", ""],
            dtype="string",
        )
        n_mapping = mapping_df[
            (mapping_df["lift_to_node"] == lift_to_node)
            & (mapping_df["lift_from_node"] == n)
        ]
        for index, row in n_mapping.iterrows():
            row_property_from = row["lift_from_property"]
            row_property_to = row["lift_to_property"]
            # if column row_property_to has no value assigned to it
            if len(lift_to_n_df[row_property_to].dropna()) == 0:
                lift_to_n_df[row_property_to] = lift_from_df[row_property_from]
            else:
                lift_to_n_df[row_property_to] = (
                    lift_to_n_df[row_property_to]
                    + ";"
                    + lift_from_df[row_property_from].astype(str)
                )
                # strip off any ";"
                lift_to_n_df[row_property_to] = lift_to_n_df[row_property_to].str.strip(
                    ";"
                )
                logger.warning(
                    f"Property {row_property_to} in template node {lift_to_node} contains concatenated values from multiple properties from the same node in manifest"
                )
                print(
                    f"Property {row_property_to} in template node {lift_to_node} contains concatenated values from multiple properties from the same node in manifest"
                )
        # remove any row with all missing value
        lift_to_n_df.dropna(axis=0, how="all", inplace=True)
        # add value to the type node
        lift_to_n_df["type"] = lift_to_node
        logger.info(
            f"After dropping empty rows, {lift_to_n_df.shape[0]} row(s) from lift_from node {n} will be added to {lift_to_node}"
        )
        print(f"After dropping empty rows, {lift_to_n_df.shape[0]} row(s) from lift_from node {n} will be added to {lift_to_node}")
        # only append the lift_to_n_df to the lift_to_df if it has any unempty rows
        if lift_to_n_df.shape[0] == 0:
            pass
        else:
            # add lift_to_n_df to the lift_to_df
            lift_to_df = pd.concat(
                [lift_to_df, lift_to_n_df], ignore_index=True, axis=0
            )
    # handle default value
    mapping_df_complete = pd.read_csv(full_mapping_file, sep="\t", header=0)
    mapping_df_complete_lift_to = mapping_df_complete[
        mapping_df_complete["lift_to_node"] == lift_to_node
    ]
    if "default_value" in mapping_df_complete.columns:
        # identify any column that contains missing value
        nan_columns = lift_to_df.columns[lift_to_df.isna().any()].tolist()
        for column in nan_columns:
            column_default_value_series = mapping_df_complete_lift_to[
                mapping_df_complete_lift_to["lift_to_property"] == column
            ]["default_value"]
            # some times there might be no mapping for that column/prop in the mapping file
            if len(column_default_value_series) > 0:
                column_default_value = column_default_value_series.values[0]
                lift_to_df[column] = lift_to_df[column].fillna(column_default_value)
            else:
                pass
    else:
        # no action if no default_value column found in the mapping file
        pass
    # save the dataframe lift_to_df to a tsv file
    tsv_name = lift_to_node + "_" + get_date() + ".tsv"
    lift_to_df.to_csv(
        os.path.join(output_folder, tsv_name),
        sep="\t",
        index=False,
        header=True,
    )

    return None


@flow(name="Liftover to tsv files", log_prints=True)
def liftover_to_tsv(
    mapping_file: str,
    submission_folder: str,
    lift_to_model: str,
    lift_to_props: str,
) -> str:
    """This function liftover the submission metadata tsv files

    Args:
        mapping_file (str): mapping file path
        submission_folder (str): folder path of submission tsv files
        lift_to_model (str): liftover to model file path
        list_to_props (str): liftover to props file path
        output_tsv_folder (str): output folder path that stores tsv files

    Returns:
        str: folder path of the liftovered tsv files
    """
    logger = get_logger(loggername=f"liftover_workflow", log_level="info")
    log_name = "liftover_workflow_" + get_date() + ".log"

    output_folder = (
        os.path.basename(submission_folder.strip("/")) + "_liftover_output_" + get_time()
    )
    os.makedirs(output_folder, exist_ok=True)
    logger.info(f"Created output folder: {output_folder}")

    # may be used in the future
    # lift_to_model_read = ReadDataModel(model_file=lift_to_model, prop_file=lift_to_props)

    # find non empty metadata tsv files (lift from)
    non_empty_props = find_non_empty_props(tsv_folder=submission_folder)

    # find unlifted props, and report in log file
    find_unlifted_props(
        tsv_folder=submission_folder,
        liftover_mapping_filepath=mapping_file,
        logger=logger,
    )

    # filter mapping df based on nonempty nodes in manifest
    # we only need to look at the sheet that are not empty
    mapping_df = pd.read_csv(
        mapping_file,
        sep="\t",
        header=0,
    )
    mapping_df = mapping_df[
        mapping_df["lift_from_node"].isin(list(non_empty_props.keys()))
    ]
    # how many unique lift_to_node found in filtered mapping df
    # these are the nodes we need to be filled with info
    lift_to_nodes = mapping_df["lift_to_node"].dropna().unique().tolist()
    logger.info(f"Nodes in lift to that will have lifted value: {*lift_to_nodes,}")
    print(f"Nodes in lift to that will have lifted value: {*lift_to_nodes,}")

    for node in lift_to_nodes:
        print(f"lifting value for lift to node {node}")
        mapping_df_node = mapping_df[mapping_df["lift_to_node"] == node]
        single_node_liftover(
            mapping_df=mapping_df_node,
            full_mapping_file=mapping_file,
            lift_to_model_file=lift_to_model,
            lift_to_props_file=lift_to_props,
            lift_to_node=node,
            lift_from_folder=submission_folder,
            output_folder=output_folder,
            logger=logger,
        )

    return output_folder, log_name
