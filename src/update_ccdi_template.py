from prefect import flow, task, get_run_logger
from prefect.blocks.notifications import SlackWebhook
import os
import sys
import pandas as pd
from shutil import copy
from typing import List, TypeVar, Dict, Tuple
import warnings
from src.utils import get_time, get_logger, get_date


ExcelFile = TypeVar("ExcelFile")
DataFrame = TypeVar("DataFrame")


def dropSheets(mylist: List) -> List:
    sheets_to_rm = ["README and INSTRUCTIONS", "Dictionary", "Terms and Value Sets"]
    newlist = [i for i in mylist if i not in sheets_to_rm]
    return newlist


@task
def manifest_workbook(excelfile: ExcelFile) -> Dict:
    """
    Returns a dictionary with sheet names as keys and dataframes
    as values

    The function checks the emptiness of the dataframe
    besides the type column and if all properties in
    the dataframe are linking properties
    """
    sheet_list = excelfile.sheet_names
    sheet_list_lean = dropSheets(sheet_list)
    workbook_dict = {}
    na_bank = ["NA", "na", "N/A", "n/a", ""]
    for sheet in sheet_list_lean:
        warnings.simplefilter(action="ignore", category=UserWarning)
        sheet_df = pd.read_excel(
            excelfile, sheet_name=sheet, na_values=na_bank, dtype="string"
        )
        df_content = sheet_df.drop(columns=["type"])
        # check if the df is empty after dropping type column
        if not df_content.dropna(how="all").empty:
            # if not empty, check if properties are all linking properties
            columns_wo_type = df_content.columns
            if len([h for h in columns_wo_type if "." in h]) < len(columns_wo_type):
                workbook_dict[sheet] = sheet_df
            else:
                pass
        else:
            pass
    return workbook_dict


@task
def template_workbook(excelfile: ExcelFile) -> Dict:
    """
    Returns a dictionary with sheet names as keys and dataframes
    as values

    This function is going to create an empty df for each sheet
    """
    sheet_list = excelfile.sheet_names
    sheet_list_lean = dropSheets(sheet_list)
    workbook_dict = {}
    na_bank = ["NA", "na", "N/A", "n/a", ""]
    for sheet in sheet_list_lean:
        warnings.simplefilter(action="ignore", category=UserWarning)
        sheet_df = pd.read_excel(
            excelfile, sheet_name=sheet, na_values=na_bank, dtype="string"
        )
        sheet_df_colnames = sheet_df.columns
        # create an empty df
        empty_sheet = pd.DataFrame(columns=sheet_df_colnames)
        # the new empty df only has one row
        # to assign with column value in the future,
        # needs to reindex before assigning with new value
        # df.reindex(range(len(<somelist>))) to match the new column len
        empty_sheet["type"] = [sheet]
        workbook_dict[sheet] = empty_sheet
    return workbook_dict


@task
def getNodeProp(workbook_dict: Dict, dict_type: str) -> DataFrame:
    node_prop = pd.DataFrame(columns=["node", "property"])
    for sheet, df in workbook_dict.items():
        df_columns = df.columns
        append_df = pd.DataFrame(
            {"node": [sheet] * len(df_columns), "property": df_columns}
        )
        node_prop = pd.concat([node_prop, append_df], ignore_index=True)
    node_prop["source"] = [dict_type] * node_prop.shape[0]
    return node_prop


@task
def populate_template_workbook(
    workbook_dict: Dict,
    template_dict: Dict,
    workbook_node_prop: DataFrame,
    template_node_prop: DataFrame,
    logger,
) -> Tuple:
    logger.info("Starting populating template file with manifest value")
    changed_property_df = pd.DataFrame(
        columns=["node", "property", "change", "new_node", "populated_in_new_node"]
    )
    for i in range(workbook_node_prop.shape[0]):
        i_row_node = workbook_node_prop.iloc[i]["node"]
        i_row_property = workbook_node_prop.iloc[i]["property"]

        workbook_i_column = workbook_dict[i_row_node][i_row_property]
        # skip the column with all missing value
        if not workbook_i_column.dropna().empty:
            # check if i_row_node is found in template nodes
            # and if i_row_property is found template node properties
            if (
                i_row_node in template_node_prop["node"].unique().tolist()
                and i_row_property
                in template_node_prop[template_node_prop["node"] == i_row_node][
                    "property"
                ].tolist()
            ):
                # if the df of i_row_node in template_dict has different row size
                # compare to len of workbook_i_column, reindex the df
                if template_dict[i_row_node].shape[0] != len(
                    workbook_i_column.tolist()
                ):
                    template_dict[i_row_node] = template_dict[i_row_node].reindex(
                        range(len(workbook_i_column.tolist()))
                    )
                else:
                    pass
                template_dict[i_row_node][i_row_property] = workbook_i_column.tolist()
            elif i_row_property in template_node_prop["property"].tolist():
                i_property_in_template = template_node_prop[
                    template_node_prop["property"] == i_row_property
                ]
                i_property_in_template = i_property_in_template[
                    ~i_property_in_template["node"].isin(["file", "diagnosis"])
                ]
                if i_property_in_template.shape[0] == 1:
                    i_property_new_node = i_property_in_template.iloc[0]["node"]
                    logger.warning(
                        f"Property {i_row_property} from node {i_row_node} has been reloacted to node {i_property_new_node}"
                    )
                    # puplate the workbook_i_column to the new node in template
                    template_dict[i_property_new_node][
                        i_row_property
                    ] = workbook_i_column.tolist()
                    i_property_change_info = pd.DataFrame(
                        {
                            "node": i_row_node,
                            "property": i_row_property,
                            "change": "Relocated",
                            "new_node": i_property_new_node,
                            "populated_in_new_node": "Yes",
                        },
                        index=[0],
                    )
                    changed_property_df = pd.concat(
                        [changed_property_df, i_property_change_info], ignore_index=True
                    )
                elif i_property_in_template.shape[0] > 1:
                    i_property_new_nodes = i_property_in_template["node"].tolist()
                    logger.warning(
                        f"Property {i_row_property} from node {i_row_node} has been reloacted to nodes {*i_property_new_nodes,}"
                    )
                    i_property_change_info = pd.DataFrame(
                        {
                            "node": i_row_node,
                            "property": i_row_property,
                            "change": "Relocated",
                            "new_node": ",".join(i_property_new_nodes),
                            "populated_in_new_node": "No",
                        },
                        index=[0],
                    )
                    changed_property_df = pd.concat(
                        [changed_property_df, i_property_change_info], ignore_index=True
                    )
                else:
                    logger.warning(
                        f"Property {i_row_property} from node {i_row_node} can't be located in template"
                    )
                    i_property_change_info = pd.DataFrame(
                        {
                            "node": i_row_node,
                            "property": i_row_property,
                            "change": "Not transfered",
                            "new_node": "",
                            "populated_in_new_node": "No",
                        },
                        index=[0],
                    )
                    changed_property_df = pd.concat(
                        [changed_property_df, i_property_change_info], ignore_index=True
                    )
            else:
                logger.warning(
                    f"Property {i_row_property} from node {i_row_node} can't be located in template"
                )
                i_property_change_info = pd.DataFrame(
                    {
                        "node": i_row_node,
                        "property": i_row_property,
                        "change": "Not transfered",
                        "new_node": "",
                        "populated_in_new_node": "No",
                    },
                    index=[0],
                )
                changed_property_df = pd.concat(
                    [changed_property_df, i_property_change_info], ignore_index=True
                )
        else:
            pass

    return (template_dict, changed_property_df)


@flow(
    name="Update_CCDI_Manifest",
    flow_run_name="Update_CCDI_Manifest_" + f"{get_time()}",
)
def updateManifest(manifest: str, template: str, template_version: str) -> Tuple:
    logger = get_logger(loggername="Update_CCDI_manifest", log_level="info")
    output_logger = "Update_CCDI_manifest_" + get_date() + ".log"

    try:
        manifest_f = pd.ExcelFile(manifest)
        logger.info(f"Reading the validated CCDI manifest {manifest}")
    except FileNotFoundError as err:
        logger.error(err)
        return None, output_logger
    except ValueError as err:
        logger.error(err)
        return None, output_logger
    except:
        logger.error(f"Issue occurred while openning file {manifest}")
        return None, output_logger

    try:
        template_f = pd.ExcelFile(template)
        logger.info(f"Reading the CCDI tempalte {template}")
    except FileNotFoundError as err:
        logger.error(err)
        return None, output_logger
    except ValueError as err:
        logger.error(err)
        return None, output_logger
    except:
        logger.error(f"Issue occurred while openning file {manifest}")
        return None, output_logger

    # Generate dict and node property df for both workbook/manifest and template
    workbook_dict = manifest_workbook(manifest_f)
    workbook_node_property = getNodeProp(
        workbook_dict=workbook_dict, dict_type="manifest"
    )
    manifest_f.close()

    template_dict = template_workbook(template_f)
    template_node_property = getNodeProp(
        workbook_dict=template_dict, dict_type="template"
    )
    template_f.close()

    # populate the template dict with manifest value
    populated_template_dict, changed_property_df = populate_template_workbook(
        workbook_dict=workbook_dict,
        template_dict=template_dict,
        workbook_node_prop=workbook_node_property,
        template_node_prop=template_node_property,
        logger=logger,
    )
    logger.info(
        "Additonal information of changed properties:\n"
        + changed_property_df.to_markdown(
            tablefmt="fancy_grid",
            index=False,
        )
    )

    # writng out the output
    manifest_filename = os.path.basename(manifest)[0:-5]
    output_name = (
        manifest_filename + "_Updater_v" + template_version + "_" + get_date() + ".xlsx"
    )
    copy(template, output_name)
    logger.info(f"Generating output")
    with pd.ExcelWriter(
        output_name, mode="a", engine="openpyxl", if_sheet_exists="overlay"
    ) as writer:
        for key in populated_template_dict.keys():
            logger.info(f"Writing sheet {key} to output")
            key_df = populated_template_dict[key]
            key_df.to_excel(writer, sheet_name=key, index=False, header=True)
    logger.info(f"Updated manifest can be found at: {output_name}")

    return output_name, output_logger
