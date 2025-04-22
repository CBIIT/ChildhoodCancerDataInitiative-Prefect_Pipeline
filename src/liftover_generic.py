import os
import pandas as pd
from prefect import flow, task, get_run_logger
from typing import TypeVar
from src.utils import get_logger, get_date

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

    tsv_files = [os.path.join(tsv_folder, f) for f in os.listdir(tsv_folder) if f.endswith(".tsv")]
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
def find_unlifted_props(tsv_folder: str, liftover_mapping_filepath: str, logger) -> dict:
    """This returns a dictionary of unlifted props with node as key and a list of unlifted props as value

    Args:
        tsv_folder (str): Folder path of the tsv files
        liftover_mapping_filepath (str): Mapping file path
    
    Returns:
        dict: a dictionary of unlifted props with node as key and a list of unlifted props as value
    """
    non_empty_props = find_non_empty_props(tsv_folder=tsv_folder) 
    mapping_df = pd.read_csv(
        liftover_mapping_filepath,
        sep="\t",
        header=0)
    mapping_df_lift_from =  mapping_df[["lift_from_node","lift_from_property"]]
    mapping_df_lift_from = mapping_df_lift_from.dropna(how="all", ignore_index=True)

    unlifted_props = pd.DataFrame(columns=["node", "unlifted_property"])

    for node in non_empty_props.keys():
        node_nonempty_props = non_empty_props[node]
        for prop in node_nonempty_props:
            filtered_mapping_df = mapping_df_lift_from[(mapping_df_lift_from["lift_from_node"] == node) & mapping_df_lift_from["lift_from_property"]== prop]
            if filtered_mapping_df.shape[0] == 0:
                unlifted_new_row = pd.DataFrame({"node": [node], "unlifted_property": [prop]})
                unlifted_props = pd.concat([unlifted_props, unlifted_new_row], ignore_index=True)
            else:
                pass
    if unlifted_props.shape[0] > 0:
        logger.warning(f"There are non-empty props that in submission tsv files that are not lifted because no mapping was found for these props.")
        print(
            f"There are non-empty props that in submission tsv files that are not lifted because no mapping was found for these props."
        )
        print_unlifted_df = unlifted_props.to_markdown(
            tablefmt="rounded_grid", index=False
        ).replace("\n", "\n\t")
        logger.warning(f"Unlifted props: \n{print_unlifted_df}")
        print(f"Unlifted props: \n{print_unlifted_df}")
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
    readmodel = ReadDataModel(model_file=model_file, props_file=props_file)
    model_obj = readmodel._get_model()
    node_prop_list = readmodel._get_node_prop_list(model_obj=model_obj, node_name=node_type)
    node_prop_list = ["type"] + node_prop_list
    return_df = pd.DataFrame(columns=node_prop_list)
    return return_df


def single_node_liftover():
    return None



@flow(name="Liftover to tsv files", log_prints=True)
def liftover_to_tsv(mapping_file:str, submission_folder: str, lift_to_model: str, lift_to_props: str) -> str:
    """This function liftover the submission metadata tsv files 

    Args:
        mapping_file (str): mapping file path
        submission_folder (str): folder path of submission tsv files
        lift_to_model (str): liftover to model file path
        list_to_props (str): liftover to props file path

    Returns:
        str: folder path of the liftovered tsv files
    """    
    logger = get_logger(loggername=f"liftover_workflow", log_level="info")
    log_name = "liftover_workflow_" + get_date() + ".log"

    output_folder = os.path.basename(submission_folder) + "_liftover_output_" + get_date()
    os.makedirs(output_folder, exist_ok=True)
    logger.info(f"Created output folder: {output_folder}")

    # may be used in the future
    # lift_to_model_read = ReadDataModel(model_file=lift_to_model, prop_file=lift_to_props)

    # find non empty metadata tsv files (lift from)
    non_empty_props = find_non_empty_props(tsv_folder=submission_folder)

    # find unlifted props, and report in log file
    find_unlifted_props(tsv_folder=submission_folder, liftover_mapping_filepath=mapping_file, logger=logger)

    # filter mapping df based on nonempty nodes in manifest
    # we only need to look at the sheet that are not empty
    mapping_df = pd.read_csv(
        mapping_file,
        sep="\t",
        header=0,
    )
    mapping_df = mapping_df[mapping_df["lift_from_node"].isin(list(non_empty_props.keys()))]
    # how many unique lift_to_node found in filtered mapping df
    # these are the nodes we need to be filled with info
    lift_to_nodes = mapping_df["lift_to_node"].dropna().unique().tolist()
    logger.info(f"Nodes in lift to that will have lifted value: {*lift_to_nodes,}")
    print(f"Nodes in lift to that will have lifted value: {*lift_to_nodes,}")

    for node in lift_to_nodes:
        print(f"lifting value for lift to node {node}")


    return output_folder, log_name
