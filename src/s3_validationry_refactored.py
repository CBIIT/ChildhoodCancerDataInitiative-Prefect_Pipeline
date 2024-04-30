from prefect import flow, task, get_run_logger, unmapped
import os
import pandas as pd
from datetime import date
import sys
import numpy as np
import warnings
import re
from src.utils import set_s3_session_client, get_time, get_date, CheckCCDI
import boto3
from botocore.exceptions import ClientError
from prefect.task_runners import ConcurrentTaskRunner
from typing import TypeVar


DataFrame = TypeVar("DataFrame")


def if_string_float(mystr: str) -> bool:
    try:
        float(mystr)
        return True
    except ValueError:
        return False


def if_string_int(mystr: str) -> bool:
    try:
        int(mystr)
        return True
    except ValueError:
        return False


def if_template_valid(template_path: str) -> None:
    template_file = pd.ExcelFile(template_path)
    template_sheets = template_file.sheet_names
    print(template_sheets)
    print(len(template_sheets))
    if not (
        ("Dictionary" in template_sheets)
        and ("Terms and Value Sets" in template_sheets)
    ):
        raise ValueError(
            'Template must include sheet "Dictionary" and "Terms and Value Sets"'
        )
    else:
        pass
    template_file.close()
    return None


@task
def cleanup_manifest_nodes(
    file_path: str, template_node_list: list[str], logger
) -> list[str]:
    """Removes sheets that are either instructional or contains empty df"""
    instructional_sheets = [
        "README and INSTRUCTIONS",
        "Dictionary",
        "Terms and Value Sets",
    ]
    manifest_file = CheckCCDI(ccdi_manifest=file_path)
    manifest_sheetnames = manifest_file.get_sheetnames()
    # remove instructional sheetnames
    manifest_sheetnames = [
        i for i in manifest_sheetnames if i not in instructional_sheets
    ]
    # remove sheetnames if not found in template node list
    unrecognized_nodes = [i for i in manifest_sheetnames if i not in template_node_list]
    if len(unrecognized_nodes) >= 1:
        logger.warning(
            f"Nodes {*unrecognized_nodes,} are not recognized in the given CCDI template, and will removed from further validation."
        )
        manifest_sheetnames = [
            i for i in manifest_sheetnames if i not in unrecognized_nodes
        ]
    else:
        logger.info("All nodes can be found in template dictionary sheet")
    # check if sheet is empty
    if_not_empty_list = []
    for k in manifest_sheetnames:
        k_df = manifest_file.read_sheet_na(sheetname=k)
        # remove type col
        k_df = k_df.drop("type", axis=1)
        # remove empty rows and cols
        k_df = k_df.dropna(how="all").dropna(how="all", axis=1)
        if not k_df.empty:
            if_not_empty_list.append(True)
        else:
            if_not_empty_list.append(False)
        del k_df
    sheetnames_filtered = [
        i for (i, v) in zip(manifest_sheetnames, if_not_empty_list) if v
    ]
    sheetnames_ordered = sorted(
        sheetnames_filtered,
        key=lambda x: (
            template_node_list.index(x) if x in template_node_list else float("inf")
        ),
    )
    logger.info(
        f"The following nodes were found not empty and are subject to further validation: {*sheetnames_ordered,}"
    )
    return sheetnames_ordered


@task(name="Validate required properties of a single sheet", log_prints=True)
def validate_required_properties_one_sheet(
    node_name, checkccdi_object, required_properties
) -> str:
    node_df = checkccdi_object.read_sheet_na(sheetname=node_name)
    properties = node_df.columns
    line_length = 25
    print_str = ""
    print_str = print_str + f"\n\t{node_name}\n\t----------\n"
    for property in properties:
        WARN_FLAG = True
        if property in required_properties:
            if node_df[property].isna().any():
                bad_positions = np.where(node_df[property].isna())[0] + 2

                # Flag to turn on explanation of error/warning
                if WARN_FLAG:
                    WARN_FLAG = False
                    print_str = (
                        print_str
                        + f"\tERROR: The values for the node, {node_name}, in the the required property, {property}, are missing:\n"
                    )
                # print out the row number contains missing value
                pos_print = ""
                for i, pos in enumerate(bad_positions):
                    if i % line_length == 0:
                        pos_print = pos_print + "\n\t"
                        # print_str = print_str + "\n\t\t\n"
                    else:
                        pass
                    pos_print = pos_print + str(pos) + ","
                print_str = print_str + pos_print + "\n"
            else:
                print_str = (
                    print_str
                    + f"\tPASS: For the node, {node_name}, the required property, {property}, contains values for all expexted entries.\n"
                )
        else:
            pass
    print(print_str)
    return print_str


@flow(
    task_runner=ConcurrentTaskRunner(),
    name="Validate required properties",
    log_prints=True,
)
def validate_required_properties(
    file_path: str, node_list: list, required_properties: list, output_file: str
):
    section_title = """\n\nThis section is for required properties for all nodes that contain data.\nFor information
    on required properties per node, please see the 'Dictionary' page of the template file.\nFor each entry, 
    it is expected that all required information has a value:\n----------\n
    """
    file_object = CheckCCDI(ccdi_manifest=file_path)
    validate_str_future = validate_required_properties_one_sheet.map(
        node_list, file_object, unmapped(required_properties)
    )
    validate_str = "".join([i.result() for i in validate_str_future])
    return_str = section_title + validate_str
    with open(output_file, "a+") as outf:
        outf.write(return_str)
    print(return_str)
    return None


@task(name="Validate whitespace of a single sheet", log_prints=True)
def validate_whitespace_one_sheet(node_name: str, checkccdi_object) -> str:
    node_df = checkccdi_object.read_sheet_na(sheetname=node_name)
    properties = node_df.columns
    line_length = 25
    print_str = ""
    print_str = print_str + f"\n\t{node_name}\n\t----------\n"
    for property in properties:
        WARN_FLAG = True
        # if the property is not completely empty:
        if not node_df[property].isna().all():
            # if there are some values that do not match when positions are stripped of white space
            if (
                node_df[property].fillna("") != node_df[property].str.strip().fillna("")
            ).any():
                bad_positions = (
                    np.where(
                        node_df[property].fillna("")
                        != node_df[property].str.strip().fillna("")
                    )[0]
                    + 2
                )
                # Flag to turn on explanation of error/warning
                if WARN_FLAG:
                    WARN_FLAG = False
                    print_str = (
                        print_str
                        + f"\tERROR: The values for the node, {node_name}, in the property, {property}, have white space issues:\n"
                    )

                # itterate over that list and print out the values
                pos_print = ""
                for i, pos in enumerate(bad_positions):
                    if i % line_length == 0:
                        pos_print = pos_print + "\n\t"
                    else:
                        pass
                    pos_print = pos_print + str(pos) + ","
                print_str = print_str + pos_print + "\n"
            else:
                pass
        else:
            pass
    print(print_str)
    return print_str


@flow(
    task_runner=ConcurrentTaskRunner(),
    name="Validate whitespace issue",
    log_prints=True,
)
def validate_whitespace(node_list: list[str], file_path: str, output_file: str) -> None:
    section_title = """\n\nThis section checks for white space issues in all properties.\n----------\n
    """
    file_object = CheckCCDI(ccdi_manifest=file_path)
    validate_str_future = validate_whitespace_one_sheet.map(node_list, file_object)
    validate_str = "".join([i.result() for i in validate_str_future])
    return_str = section_title + validate_str
    with open(output_file, "a+") as outf:
        outf.write(return_str)
    print(return_str)
    return None


@task(name="Validate terms and value sets of one sheet", log_prints=True)
def validate_terms_value_sets_one_sheet(
    node_name: str,
    checkccdi_object,
    template_object,
):
    node_df = checkccdi_object.read_sheet_na(sheetname=node_name)
    properties = node_df.columns
    line_length = 5
    print_str = ""
    print_str = print_str + f"\n\t{node_name}\n\t----------\n"

    # create tavs_df and dict_df
    dict_df = template_object.read_sheet_na(sheetname="Dictionary")
    tavs_df = template_object.read_sheet_na(sheetname="Terms and Value Sets")
    tavs_df = tavs_df.dropna(how="all").dropna(how="all", axis=1)

    # create an enum property list
    # For newer versions of the submission template, obtain the arrays from the Dictionary tab
    if any(dict_df["Type"].str.contains("array")):
        enum_arrays = dict_df[dict_df["Type"].str.contains("array")][
            "Property"
        ].tolist()
    else:
        enum_arrays = [
            "therapeutic_agents",
            "treatment_type",
            "study_data_types",
            "morphology",
            "primary_site",
            "race",
        ]

    for property in properties:
        WARN_FLAG = True
        tavs_df_prop = tavs_df[tavs_df["Value Set Name"] == property]
        # if the property is in the TaVs data frame
        if len(tavs_df_prop) > 0:
            # if the property is not completely empty:
            if not node_df[property].isna().all():
                # if the property is an enum
                if property in enum_arrays:
                    # obtain a list of value strings
                    unique_values = node_df[property].dropna().unique()
                    # pull out a complete list of all values in sub-arrays
                    for unique_value in unique_values:
                        # if there is a semi-colon
                        if ";" in unique_value:
                            # make sure the semi-colon is not part of a pre-existing term. (This will help with most use cases, but there could be arrays that also have enums with semi-colons and that will just have to be handled manually.)
                            if (
                                unique_value
                                not in tavs_df_prop["Term"].unique().tolist()
                            ):
                                # find the position
                                unique_value_pos = np.where(
                                    unique_values == unique_value
                                )[0][0]
                                # delete entry
                                unique_values = np.delete(
                                    unique_values, unique_value_pos
                                )
                                # rework the entry and apply back to list
                                unique_value = list(set(unique_value.split(";")))
                                for value in unique_value:
                                    unique_values = np.append(unique_values, value)
                    # make sure list is unique
                    unique_values = list(set(unique_values))

                    if set(unique_values).issubset(set(tavs_df_prop["Term"])):
                        # if yes, then
                        print_str = (
                            print_str
                            + f"\tPASS: {property}, property contains all valid values.\n"
                        )
                    else:
                        # if no, then
                        # for each unique value
                        bad_enum_list = []

                        # Flag to turn on explanation of error/warning
                        if WARN_FLAG:
                            WARN_FLAG = False
                            # test to see if string;enum
                            enum_strings = dict_df[
                                dict_df["Type"].str.contains("string")
                                & dict_df["Type"].str.contains("enum")
                            ]["Property"].tolist()
                            # if the enum is an string;enum
                            if property in enum_strings:
                                print_str = (
                                    print_str
                                    + f"\tWARNING: {property} property contains a value that is not recognized, but can handle free strings:\n",
                                )
                            else:
                                print_str = (
                                    print_str
                                    + f"\tERROR: {property} property contains a value that is not recognized:\n",
                                )
                        else:
                            pass

                        # for each value that is not found, add to a list
                        for unique_value in unique_values:
                            if unique_value not in tavs_df_prop["Term"].values:
                                bad_enum_list.append(unique_value)

                        # itterate over that list and print out the values
                        enum_print = ""
                        for i, enum in enumerate(bad_enum_list):
                            if i % line_length == 0:
                                enum_print = enum_print + "\n\t"
                            else:
                                pass
                            enum_print = enum_print + str(enum) + ","
                        print_str = print_str + enum_print + "\n"
                # if the property is not an enum
                else:
                    unique_values = node_df[property].dropna().unique()
                    # as long as there are unique values
                    if len(unique_values) > 0:
                        # are all the values found in the TaVs terms
                        if set(unique_values).issubset(set(tavs_df_prop["Term"])):
                            # if yes, then
                            print_str = (
                                print_str
                                + f"\tPASS: {property}, property contains all valid values.\n",
                            )
                        else:
                            # if no, then
                            bad_enum_list = []

                            # Flag to turn on explanation of error/warning
                            if WARN_FLAG:
                                WARN_FLAG = False
                                # test to see if string;enum
                                enum_strings = dict_df[
                                    dict_df["Type"].str.contains("string")
                                    & dict_df["Type"].str.contains("enum")
                                ]["Property"].tolist()
                                # if the enum is an string;enum
                                if property in enum_strings:
                                    print_str = (
                                        print_str
                                        + f"\tWARNING: {property} property contains a value that is not recognized, but can handle free strings:\n",
                                    )

                                else:
                                    print_str = (
                                        print_str
                                        + f"\tERROR: {property} property contains a value that is not recognized:\n"
                                    )

                            # for each unique value, check it against the TaVs data frame
                            for unique_value in unique_values:
                                if unique_value not in tavs_df_prop["Term"].values:
                                    bad_enum_list.append(unique_value)

                            enum_print = ""
                            for i, enum in enumerate(bad_enum_list):
                                if i % line_length == 0:
                                    enum_print = enum_print + "\n\t"
                                else:
                                    pass
                                enum_print = enum_print + str(enum) + ","
                            print_str = print_str + enum_print + "\n"
    print(print_str)
    return print_str


@flow(name="Validate terms and value sets", log_prints=True)
def validate_terms_value_sets(
    file_path: str,
    template_path: str,
    node_list: list[str],
    #dict_df: DataFrame,
    #tavs_df: DataFrame,
    output_file: str,
) -> None:
    section_title = """he following columns have controlled vocabulary on the 'Terms and Value Sets' 
    page of the template file. If the values present do not match, they will noted and in some cases 
    the values will be replaced:\n----------\n
    """
    template_object = CheckCCDI(ccdi_manifest=template_path)
    file_object = CheckCCDI(ccdi_manifest=file_path)
    validate_str_future = validate_terms_value_sets_one_sheet.map(
        node_list, file_object, template_object
    )
    validate_str = "".join([i.result() for i in validate_str_future])
    return_str = section_title + validate_str
    with open(output_file, "a+") as outf:
        outf.write(return_str)
    print(return_str)
    return None


@flow(
    name="CCDI_ValidationRy_refactor",
    log_prints=True,
    flow_run_name="CCDI_ValidationRy_refactor" + f"{get_time()}",
)
def ValidationRy_new(file_path: str, template_path: str):
    validation_logger = get_run_logger()

    todays_date = get_date()

    # Output file name based on input file name and date/time stamped.
    file_basename = os.path.basename(file_path).rsplit(".", 1)[0]
    output_file = file_basename + "_Validate" + todays_date + ".txt"

    # check if template file is valid, "Dictionary" and "Terms and Value Sets"
    # sheets can be found.
    if_template_valid(template_path=template_path)

    # Extract sheet df of template "Dictionary" and "Terms and Value Sets" sheets
    template_file = CheckCCDI(ccdi_manifest=template_path)
    dict_df = template_file.read_sheet_na(sheetname="Dictionary")
    tavs_df = template_file.read_sheet_na(sheetname="Terms and Value Sets")
    tavs_df = tavs_df.dropna(how="all").dropna(how="all", axis=1)

    # crete a list of all properties and a list of required properties
    all_properties = set(dict_df["Property"])
    required_properties = set(dict_df[dict_df["Required"].notna()]["Property"])

    # Try not to read every df into a single dict variable
    # the size gets too big. Instead, filter nodes by removing instruction
    # node and empty df nodes. and we can create task by taking, manifest path, node/sheet name,
    # and template info as necessary, and we can run task concurrently to speed up
    # the process

    # list of nodes based on template Dictionary sheet
    template_node_list = dict_df["Node"].unique().tolist()

    # get sheetnames in the manifest that requires validation
    nodes_to_validate = cleanup_manifest_nodes(
        file_path=file_path,
        template_node_list=template_node_list,
        logger=validation_logger,
    )
    validation_logger.info(f"Nodes will be validated: {*nodes_to_validate,}")

    # starts validation of unempty node sheets
    validation_logger.info("Checking if required properties were filled")
    validate_required_properties(
        file_path,
        nodes_to_validate,
        list(required_properties),
        output_file,
    )

    # validate whitespace in proprety values
    validate_whitespace(nodes_to_validate, file_path, output_file)

    # validate terms and value sets
    validate_terms_value_sets(
        file_path, template_path, nodes_to_validate, output_file
    )

    return output_file
