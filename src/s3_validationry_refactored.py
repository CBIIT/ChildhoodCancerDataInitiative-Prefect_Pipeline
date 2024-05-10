from prefect import flow, task, get_run_logger, unmapped
import os
import pandas as pd
from datetime import date
import sys
import numpy as np
import warnings
import re
from src.utils import set_s3_session_client, get_time, get_date, CheckCCDI
from src.file_mover import parse_file_url_in_cds
from src.file_remover import list_to_chunks
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
    # if template doesn't have
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


@task(
    name="Validate required properties of a single sheet",
    log_prints=True,
    task_run_name="Validate required properties of node {node_name}",
)
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
                        pos_print = pos_print + "\n\t\t"
                        # print_str = print_str + "\n\t\t\n"
                    else:
                        pass
                    pos_print = pos_print + str(pos) + ","
                print_str = print_str + pos_print + "\n\n"
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
    section_title = """\n\nThis section is for required properties for all nodes that contain data.
For information on required properties per node, please see the 'Dictionary' page of the template file.
For each entry, it is expected that all required information has a value:\n----------\n
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


@task(
    name="Validate whitespace of a single sheet",
    log_prints=True,
    task_run_name="Validate whitespace of node {node_name}",
)
def validate_whitespace_one_sheet(node_name: str, checkccdi_object) -> str:
    node_df = checkccdi_object.read_sheet_na(sheetname=node_name)
    properties = node_df.columns
    line_length = 25
    print_str = f"\n\t{node_name}\n\t----------\n"
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
                        pos_print = pos_print + "\n\t\t"
                    else:
                        pass
                    pos_print = pos_print + str(pos) + ","
                print_str = print_str + pos_print + "\n\n"
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


@task(
    name="Validate terms and value sets of one sheet",
    log_prints=True,
    task_run_name="Validate terms and value sets of node {node_name}",
)
def validate_terms_value_sets_one_sheet(
    node_name: str,
    checkccdi_object,
    template_object,
):
    node_df = checkccdi_object.read_sheet_na(sheetname=node_name)
    properties = node_df.columns
    line_length = 5
    print_str = f"\n\t{node_name}\n\t----------\n"

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
                                    + f"\tWARNING: {property} property contains a value that is not recognized, but can handle free strings:\n"
                                )
                            else:
                                print_str = (
                                    print_str
                                    + f"\tERROR: {property} property contains a value that is not recognized:\n"
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
                                enum_print = enum_print + "\n\t\t"
                            else:
                                pass
                            enum_print = enum_print + str(enum) + ","
                        print_str = print_str + enum_print + "\n\n"
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
                                + f"\tPASS: {property}, property contains all valid values.\n"
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
                                        + f"\tWARNING: {property} property contains a value that is not recognized, but can handle free strings:\n"
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
                                    enum_print = enum_print + "\n\t\t"
                                else:
                                    pass
                                enum_print = enum_print + str(enum) + ","
                            print_str = print_str + enum_print + "\n\n"
                    else:
                        pass
    print(print_str)
    return print_str


@flow(
    name="Validate terms and value sets",
    log_prints=True,
    task_runner=ConcurrentTaskRunner(),
)
def validate_terms_value_sets(
    file_path: str,
    template_path: str,
    node_list: list[str],
    # dict_df: DataFrame,
    # tavs_df: DataFrame,
    output_file: str,
) -> None:
    section_title = """\n\nThe following columns have controlled vocabulary on the 'Terms and Value Sets' page of the template file. 
If the values present do not match, they will noted and in some cases the values will be replaced:\n----------\n
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


@task(
    name="Validate integer and numeric value of one sheet",
    log_prints=True,
    task_run_name="Validate integer and numeric value of node {node_name}",
)
def validate_integer_numeric_checks_one_sheet(
    node_name: str, file_object, template_object
):
    node_df = file_object.read_sheet_na(sheetname=node_name)
    properties = node_df.columns
    line_length = 25
    print_str = ""
    print_str = print_str + f"\n\t{node_name}\n\t----------\n"

    # read dict_df, create int_props, and num_props
    dict_df = template_object.read_sheet_na(sheetname="Dictionary")
    int_props = dict_df[dict_df["Type"] == "integer"]["Property"].unique().tolist()
    num_props = dict_df[dict_df["Type"] == "number"]["Property"].unique().tolist()

    for property in properties:
        WARN_FLAG = False
        # NUMBER PROPS CHECK
        # if that property is a number property
        if property in num_props:
            if len(node_df[property].dropna().tolist()) > 0:
                error_rows = []
                # go throw each row
                for row in list(range(len(node_df))):
                    # obtain the value
                    value = node_df[property][row]
                    # if it is not NA
                    if pd.notna(value):
                        # test whether it is a float
                        if not if_string_float(value):
                            # if not, add to list, row number offset by 2
                            error_rows.append(row + 2)
                            WARN_FLAG = True
                        else:
                            pass
                    else:
                        pass
            else:
                pass

            # if the warning flag was tripped
            if WARN_FLAG:
                WARN_FLAG = False
                print_str = (
                    print_str
                    + f"\tERROR: {property} property contains a value that is not a number:\n"
                )
                # itterate over that list and print out the values
                enum_print = ""
                for i, row in enumerate(error_rows):
                    if i % line_length == 0:
                        enum_print = enum_print + "\n\t\t"
                    else:
                        pass
                    enum_print = enum_print + str(row) + ","
                print_str = print_str + enum_print + "\n\n"
            else:
                pass
        # property not an num_props
        else:
            pass

        # INTEGER props check
        # if that property is a integer property
        if property in int_props:
            # if there are atleast one value
            if len(node_df[property].dropna().tolist()) > 0:
                error_rows = []
                # go through each row
                for row in list(range(len(node_df))):
                    # obtain the value
                    value = node_df[property][row]
                    # if it is not NA
                    if pd.notna(value):
                        # test whether it is a int
                        if not if_string_int(value):
                            # if not, add to list, row number offset by 2
                            error_rows.append(row + 2)
                            WARN_FLAG = True
                        else:
                            pass
                    else:
                        pass
            else:
                pass

            # if the warning flag was tripped
            if WARN_FLAG:
                WARN_FLAG = False
                print_str = (
                    print_str
                    + f"\tERROR: {property} property contains a value that is not an integer:\n"
                )
                # itterate over that list and print out the values
                enum_print = ""
                for i, row in enumerate(error_rows):
                    if i % line_length == 0:
                        enum_print = enum_print + "\n\t\t"
                    else:
                        pass
                    enum_print = enum_print + str(row) + ","
                print_str = print_str + enum_print + "\n\n"
            else:
                pass
        else:
            pass
    print(print_str)
    return print_str


@flow(
    name="Validate integer and numeric value",
    log_prints=True,
    task_runner=ConcurrentTaskRunner(),
)
def validate_integer_numeric_checks(
    file_path: str, template_path: str, node_list: list[str], output_file: str
):
    section_title = "\n\nThis section will display any values in properties that are expected to be either numeric or integer based on the Dictionary, but have values that are not:\n----------\n"
    template_object = CheckCCDI(ccdi_manifest=template_path)
    file_object = CheckCCDI(ccdi_manifest=file_path)

    validate_str_future = validate_integer_numeric_checks_one_sheet.map(
        node_list, file_object, template_object
    )
    validate_str = "".join([i.result() for i in validate_str_future])
    return_str = section_title + validate_str
    with open(output_file, "a+") as outf:
        outf.write(return_str)
    print(return_str)
    return None


@task(
    name="Validate regex of one sheet",
    log_prints=True,
    task_run_name="Validate regex of node {node_name}",
)
def validate_regex_one_sheet(
    node_name: str, file_object, template_object, all_regex: list
):
    # read dict_df
    dict_df = template_object.read_sheet_na(sheetname="Dictionary")
    # pull out a data frame that only applies to string values
    string_df = dict_df[dict_df["Type"].str.lower().str.contains("string")]

    node_df = file_object.read_sheet_na(sheetname=node_name)
    string_node = string_df[string_df["Node"].isin([node_name])]
    string_props = string_node["Property"].values

    # logic to remove both GUID and md5sum from the check, as these are random/semi-random strings that are created and would never have a date placed in them.
    if "md5sum" in string_props:
        string_props = string_props[string_props != "md5sum"]
    else:
        pass

    if "dcf_indexd_guid" in string_props:
        string_props = string_props[string_props != "dcf_indexd_guid"]
    else:
        pass

    line_length = 5
    print_str = ""
    print_str = print_str + f"\n\t{node_name}\n\t----------\n"
    for string_prop in string_props:
        WARN_FLAG = True
        # find all unique values
        string_values = node_df[string_prop].dropna().unique()

        bad_regex_strings = []
        # each unique value
        for string_value in string_values:
            # if that value matches any of the regex
            for regex in all_regex:
                if re.match(regex, string_value):
                    bad_regex_strings.append(string_value)
                else:
                    pass

        if len(bad_regex_strings) > 0:
            # Flag to turn on explanation of error/warning
            if WARN_FLAG:
                WARN_FLAG = False
                print_str = (
                    print_str
                    + f"\tERROR: For the {node_name} node, the {string_prop} property contains a value that matches a regular expression for dates/social security number/phone number/zip code:\n"
                )
            else:
                pass
            # itterate over that list and print out the values
            enum_print = ""
            for i, string_val in enumerate(bad_regex_strings):
                if i % line_length == 0:
                    enum_print = enum_print + "\n\t\t"
                else:
                    pass
                enum_print = enum_print + str(string_val) + ","
            print_str = print_str + enum_print + "\n\n"

        else:
            pass
    print(print_str)
    return print_str


@flow(name="Validate regex", log_prints=True, task_runner=ConcurrentTaskRunner())
def validate_regex(
    node_list: list[str], file_path: str, template_path: str, output_file: str
):
    section_title = """\n\nThis section will display any values in properties that can accept strings, which are thought to contain PII/PHI based on regex suggestions from dbGaP:\n----------\n"""
    # create file_object and template_object
    template_object = CheckCCDI(ccdi_manifest=template_path)
    file_object = CheckCCDI(ccdi_manifest=file_path)
    date_regex = [
        "(0?[1-9]|1[0-2])[-\\/.](0?[1-9]|[12][0-9]|3[01])[-\\/.](19[0-9]{2}|2[0-9]{3}|[0-9]{2})",
        "(19[0-9]{2}|2[0-9]{3})[-\\/.](0?[1-9]|1[0-2])[-\\/.](0?[1-9]|[12][0-9]|3[01])",
        "(0?[1-9]|[12][0-9]|3[01])[\\/](19[0-9]{2}|2[0-9]{3})",
        "(0?[1-9]|[12][0-9])[\\/]([0-9]{2})",
        "(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01])[0-9]{2}",
        "(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01])19[0-9]{2}",
        "(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01])2[0-9]{3}",
        "19[0-9]{2}(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01])",
        "2[0-9]{3}(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01])",
    ]
    # Problematic regex
    # A month name or abbreviation and a 1, 2, or 4-digit number, in either order, separated by some non-letter, non-number characters or not separated, e.g., "JAN '93", "FEB64", "May 3rd" (but not "May be 14").
    # ```'[a-zA-Z]{3}[\ ]?([0-9]|[0-9]{2}|[0-9]{4})[a-zA-Z]{0,2}'```
    socsec_regex = ["[0-9]{3}[-][0-9]{2}[-][0-9]{4}"]
    phone_regex = ["[(]?[0-9]{3}[-)\ ][0-9]{3}[-][0-9]{4}"]
    zip_regex = ["(^[0-9]{5}$)|(^[0-9]{9}$)|(^[0-9]{5}-[0-9]{4}$)"]
    all_regex = date_regex + socsec_regex + phone_regex + zip_regex

    validate_str_future = validate_regex_one_sheet.map(
        node_list, file_object, template_object, unmapped(all_regex)
    )
    validate_str = "".join([i.result() for i in validate_str_future])
    return_str = section_title + validate_str
    with open(output_file, "a+") as outf:
        outf.write(return_str)
    print(return_str)
    return None


@task(
    name="Validate unique key of one sheet",
    log_prints=True,
    task_run_name="Validate unique key of node {node_name}",
)
def validate_unique_key_one_sheet(node_name: str, file_object, template_object):
    node_df = file_object.read_sheet_na(sheetname=node_name)

    # read dict_df
    dict_df = template_object.read_sheet_na(sheetname="Dictionary")
    # pull out all key value properties
    key_value_props = dict_df[
        (dict_df["Key"] == "True") & (dict_df["Node"] == node_name)
    ]["Property"].values

    line_length = 5
    print_str = ""
    print_str = print_str + f"\n\t{node_name}\n\t----------\n"

    for key_value_prop in key_value_props:
        WARN_FLAG = True
        # if a property is found in the data frame
        if key_value_prop in node_df.columns.tolist():
            # as long as there are some values in the key column
            if node_df[key_value_prop].notna().any():
                # if the length of the data frame is not the same length of the unique key property values, then we have some non-unique values
                if len(node_df[key_value_prop].dropna()) != len(
                    node_df[key_value_prop].dropna().unique()
                ):
                    if WARN_FLAG:
                        WARN_FLAG = False
                        print_str = (
                            print_str
                            + f"\tERROR: The {node_name} node, has multiple instances of the same key value, which should be unique, in the property, {key_value_prop}:\n"
                        )
                    else:
                        pass
                    # create a table of values and counts
                    freq_key_values = node_df[key_value_prop].value_counts()
                    # pull out a unique list of values that have more than one instance
                    not_unique_key_values = (
                        node_df[
                            node_df[key_value_prop].isin(
                                freq_key_values[freq_key_values > 1].index
                            )
                        ][key_value_prop]
                        .unique()
                        .tolist()
                    )

                    # itterate over that list and print out the values
                    enum_print = ""
                    for i, not_unique_key_value in enumerate(not_unique_key_values):
                        if i % line_length == 0:
                            enum_print = enum_print + "\n\t\t"
                        else:
                            pass
                        enum_print = enum_print + str(not_unique_key_value) + ","
                    print_str = print_str + enum_print + "\n\n"
                else:
                    pass

            else:
                pass
        else:
            pass
    print(print_str)
    return print_str


@flow(name="Valiedate unique key", log_prints=True, task_runner=ConcurrentTaskRunner())
def validate_unique_key(
    node_list: list[str], file_path: str, template_path: str, output_file: str
):
    section_title = "\n\nThe following will check for multiples of key values, which are expected to be unique.\nIf there are any unexpected values, they will be reported below:\n----------\n"
    # create file_object and template_object
    template_object = CheckCCDI(ccdi_manifest=template_path)
    file_object = CheckCCDI(ccdi_manifest=file_path)

    validate_str_future = validate_unique_key_one_sheet.map(
        node_list, file_object, template_object
    )
    validate_str = "".join([i.result() for i in validate_str_future])
    return_str = section_title + validate_str
    # print the return_str to the output file
    with open(output_file, "a+") as outf:
        outf.write(return_str)
    print(return_str)
    return None


def extract_object_file_meta(nodes_list: list[str], file_object):
    file_node_props = [
        "file_id",
        "file_name",
        "file_size",
        "md5sum",
        "file_url_in_cds",
        "node",
    ]
    df_file = pd.DataFrame(columns=file_node_props)
    for node in nodes_list:
        node_df = file_object.read_sheet_na(sheetname=node)
        node_df["node"] = node
        node_df["file_id"] = node_df[f"{node}_id"]
        df_file = pd.concat([df_file, node_df[file_node_props]], ignore_index=True)
    df_file["file_url_in_cds"] = df_file["file_url_in_cds"].map(
        lambda x: (
            x.replace("%20", " ").replace("%2C", ",").replace("%23", "#")
            if isinstance(x, str)
            else x
        )
    )
    return df_file


def check_file_size_zero(file_df: DataFrame) -> str:
    WARN_FLAG = True
    size_zero_match = file_df[file_df["file_size"] == "0"]
    print_str = ""
    if size_zero_match.shape[0] > 0:
        if WARN_FLAG:
            WARN_FLAG = False
            print_str = (
                print_str + "\tWARNING: There are files that have a size value of 0:\n"
            )
            print_str = (
                print_str
                + "\n\t"
                + size_zero_match[["node", "file_name", "file_size"]]
                .to_markdown(tablefmt="rounded_grid", index=False)
                .replace("\n", "\n\t")
                + "\n\n"
            )
        else:
            pass
    else:
        print_str = print_str + "\tINFO: No files were found with 0 file_size.\n"
    return print_str


def check_file_md5sum_regex(file_df: DataFrame) -> str:
    WARN_FLAG = True
    print_str = ""
    file_md5sum_list = file_df["md5sum"].tolist()
    if_md5sum_regex = [
        True if re.match(pattern=r"^[a-f0-9]{32}$", string=i) else False
        for i in file_md5sum_list
    ]
    if sum(if_md5sum_regex) < file_df.shape[0]:
        if WARN_FLAG:
            WARN_FLAG = False
            print_str = (
                print_str
                + "\tWARNING: There are files that have a md5sum value that does not follow the md5sum regular expression:\n"
            )
            failed_df = file_df[np.logical_not(if_md5sum_regex)]
            print_str = (
                print_str
                + "\n\t"
                + failed_df[["node", "file_name", "md5sum"]]
                .to_markdown(tablefmt="rounded_grid", index=False)
                .replace("\n", "\n\t")
                + "\n\n"
            )
        else:
            pass
    else:
        print_str = (
            print_str
            + "\tINFO: all files were found with md5sum value that follows md5sum regular expression.\n"
        )
    return print_str


def check_file_basename(file_df: DataFrame) -> str:
    """Checks if the file_name value matches to the basename of file_url_in_cds"""
    WARN_FLAG = True
    print_str = ""
    url_list = file_df["file_url_in_cds"].tolist()
    file_df["url_basename"] = [os.path.split(os.path.relpath(i))[1] for i in url_list]
    filename_not_match = file_df[
        np.logical_not(file_df["file_name"] == file_df["url_basename"])
    ]
    if filename_not_match.shape[0] > 0:
        if WARN_FLAG:
            WARN_FLAG = False
            print_str = (
                print_str
                + f"\tWARNING: There are files that have a file_name that does not match the file name in the url:\n"
            )
            print_str = (
                print_str
                + "\n\t"
                + filename_not_match[["node", "file_name", "file_url_in_cds"]]
                .to_markdown(tablefmt="rounded_grid", index=False)
                .replace("\n", "\n\t")
                + "\n\n"
            )
        else:
            pass
    else:
        print_str = (
            print_str + "\tINFO: all file names were found in their file_url_in_cds.\n"
        )
    return print_str


def count_buckets(df_file: DataFrame) -> list:
    df_file["bucket"] = df_file["file_url_in_cds"].str.split("/").str[2]
    bucket_list = list(set(df_file["bucket"].values.tolist()))
    return bucket_list


def check_buckets_access(bucket_list: list[str]) -> dict:
    invalid_buckets = {"bucket": [], "error_message": []}
    s3_client = set_s3_session_client()
    for bucket in bucket_list:
        try:
            s3_client.head_bucket(Bucket=bucket)
        except ClientError as err:
            err_code = err.response["Error"]["Code"]
            err_message = err.response["Error"]["Message"]
            # with open(output_file, "a+") as outf:
            #    outf.write(
            #        f"\tFail to read bucket {bucket}: {err_code} {err_message}\n"
            #    )
            # invalid_buckets.append(bucket)
            invalid_buckets["bucket"].append(bucket)
            invalid_buckets["error_message"].append(err_code + " " + err_message)
    s3_client.close()
    return invalid_buckets


@task(name="if a single obj exists in bucket", retries=3, retry_delay_seconds=0.5, tags=["validation-tag"])
def validate_single_manifest_obj_in_bucket(s3_uri: str, s3_client) -> bool:
    """Checks if an obj exists in AWS by using a s3 uri
    Returns (True, {file size}) if exist, or (False, np.nan) if not exist
    """
    obj_bucket, obj_key = parse_file_url_in_cds(url=s3_uri)
    try:
        object_meta = s3_client.head_object(Bucket=obj_bucket, Key=obj_key)
        object_size = object_meta["ContentLength"]
        return True, object_size
    except ClientError as err:
        return False, np.nan


@flow(task_runner=ConcurrentTaskRunner(), name="If objects exist in bucket")
def validate_manifest_objs_in_bucket(s3_uri_list: list, s3_client) -> list:
    """Checks if a list of uri exists in AWS by using a s3 uri
    Returns a list of True if exist, False if not exist, and None
    """
    exist_list_future = validate_single_manifest_obj_in_bucket.map(
        s3_uri_list, s3_client
    )
    return [i.result() for i in exist_list_future]


@flow(name="if bucket objects exist in manifest")
def validate_bucket_objs_in_manifest(
    file_object: str, file_node_list: list[str], readable_buckets: list[str]
) -> list:
    """Returns a list of object found in bucket, but not found in the manifest"""
    df_file = extract_object_file_meta(
        nodes_list=file_node_list, file_object=file_object
    )
    df_file_urls = df_file["file_url_in_cds"].tolist()
    del df_file
    s3_client = set_s3_session_client()
    # create a paginator to itterate through each 1000 objs
    paginator = s3_client.get_paginator("list_objects_v2")
    bucket_obj_unfound = []
    for bucket in readable_buckets:
        response_iterator = paginator.paginate(Bucket=bucket)
        # pull out each response and obtain file name and size
        for response in response_iterator:
            if "Contents" in response:
                for obj in response["Contents"]:
                    obj_uri = "s3://" + bucket + "/" + obj["Key"]
                    if obj_uri not in df_file_urls:
                        bucket_obj_unfound.append(obj_uri)
                    else:
                        pass
            else:
                pass
    s3_client.close()
    del df_file_urls
    return bucket_obj_unfound


@flow(name="Validate manifest objs bucket loc and size")
def validate_objs_loc_size(
    file_object, file_node_list: list[str], readable_bucket_list: list[str]
) -> DataFrame:
    """Validate if the manifest obj can be found in the bucket location and check if the file size
    matches to bucket obj size
    """
    df_file = extract_object_file_meta(
        nodes_list=file_node_list, file_object=file_object
    )
    df_file["if_bucket_readable"] = df_file["file_url_in_cds"].apply(
        lambda x: True if parse_file_url_in_cds(x)[0] in readable_bucket_list else False
    )
    # extract a list of url with readable bucket list only
    uri_list = df_file.loc[
        df_file["if_bucket_readable"] == True, "file_url_in_cds"
    ].tolist()
    if_exist = []
    bucket_obj_size = []
    s3_client = set_s3_session_client()
    print(f"Number of uri to be tested: {len(uri_list)}")

    """
    if len(uri_list) > 100:
        uri_list_chunk = list_to_chunks(uri_list, 100)
        for i in range(len(uri_list_chunk)):
            i_validation = validate_manifest_objs_in_bucket(
                s3_uri_list=uri_list_chunk[i], s3_client=s3_client
            )
            print(f"Progress: {i+1}/{len(uri_list_chunk)}")
            # add validation results to if_exist and bucket_obj_size list
            for j in i_validation:
                if_exist.append(j[0])
                bucket_obj_size.append(j[1])

    else:
        uri_list_validation = validate_manifest_objs_in_bucket(
            s3_uri_list=uri_list, s3_client=s3_client
        )
        for k in uri_list_validation:
            if_exist.append(k[0])
            bucket_obj_size.append(k[1])
    """
    progress_bar = 1
    for i in uri_list:
        # avoid using task, try to avoid crash in prefect
        i_if_exist, i_size = validate_single_manifest_obj_in_bucket.fn(s3_uri=i, s3_client=s3_client)
        if_exist.append(i_if_exist)
        bucket_obj_size.append(i_size)
        if progress_bar % 100 == 0:
            print(f"progress: {progress_bar}/{len(uri_list)}")
        else:
            pass
        progress_bar += 1

    df_file.loc[df_file["if_bucket_readable"] == True, "if_exist"] = if_exist
    df_file.loc[df_file["if_bucket_readable"] == True, "bucket_obj_size"] = (
        bucket_obj_size
    )
    df_file["size_compare"] = df_file["file_size"] == df_file["bucket_obj_size"]
    s3_client.close()
    return df_file


@flow(name="Validate file metadata", log_prints=True)
def validate_file_metadata(
    node_list: list[str], file_path: str, template_path: str, output_file: str
):
    """Validate if manifest file objs have none zero file size, correct md5sum regex
    and if file name matches to s3 uri
    """
    section_title = "\n\nThe following section will check the manifest for expected file metadata.\nIf there are any unexpected values, they will be reported below:\n----------\n"
    return_str = "" + section_title
    # create file_object and template_object
    template_object = CheckCCDI(ccdi_manifest=template_path)
    file_object = CheckCCDI(ccdi_manifest=file_path)

    # read dict_df
    dict_df = template_object.read_sheet_na(sheetname="Dictionary")
    file_nodes = dict_df[dict_df["Property"] == "file_url_in_cds"][
        "Node"
    ].values.tolist()
    file_nodes_to_check = [i for i in node_list if i in file_nodes]
    df_file = extract_object_file_meta(
        nodes_list=file_nodes_to_check, file_object=file_object
    )

    # check for file_size == 0
    return_str = return_str + check_file_size_zero(file_df=df_file)

    # check for md5sum regular expression
    return_str = return_str + check_file_md5sum_regex(file_df=df_file)

    # check for file basename in url
    return_str = return_str + check_file_basename(file_df=df_file)

    # print the return_str to output_file
    with open(output_file, "a+") as outf:
        outf.write(return_str)
    print(return_str)
    return None


@flow(name="Validate AWS bucket content", log_prints=True)
def validate_bucket_content(
    node_list: list[str], file_path: str, template_path: str, output_file: str
):
    section_title = "\n\nThe following section will compare the manifest against the reported buckets and note if there are unexpected results where the file is represented equally in both sources.\nIf there are any unexpected values, they will be reported below:\n----------\n"
    with open(output_file, "a+") as outf:
        outf.write(section_title)
    # create file_object and template_object
    template_object = CheckCCDI(ccdi_manifest=template_path)
    file_object = CheckCCDI(ccdi_manifest=file_path)

    # read dict_df
    dict_df = template_object.read_sheet_na(sheetname="Dictionary")
    file_nodes = dict_df[dict_df["Property"] == "file_url_in_cds"][
        "Node"
    ].values.tolist()
    file_nodes_to_check = [i for i in node_list if i in file_nodes]
    if len(file_nodes_to_check) == 0:
        with open(output_file, "a+") as outf:
            outf.write(
                "\tAll file nodes in this manifest were found empty. No further bucket content validation\n"
            )
        return None
    else:
        pass
    df_file = extract_object_file_meta(
        nodes_list=file_nodes_to_check, file_object=file_object
    )
    # report a list of buckets found within the manifest
    bucket_list = count_buckets(df_file=df_file)
    del df_file
    if len(bucket_list) > 1:
        with open(output_file, "a+") as outf:
            outf.write(
                f"\tINFO: There are more than one aws bucket that is associated with this metadata file:\n\t{*bucket_list,}\n\n"
            )
    else:
        with open(output_file, "a+") as outf:
            outf.write(
                f"\tINFO: Only one aws bucket is associated with this metadata file:\n\t\t{*bucket_list,}\n\n"
            )
    invalid_buckets = check_buckets_access(bucket_list=bucket_list)
    invalid_buckets_df = pd.DataFrame.from_dict(invalid_buckets)
    if len(invalid_buckets) > 0:
        with open(output_file, "a+") as outf:
            outf.write(
                f"\tAWS bucket content validation won't perform validation for buckets:\n\t"
                + invalid_buckets_df.to_markdown(
                    tablefmt="rounded_grid", index=False
                ).replace("\n", "\n\t")
                + "\n\n"
            )
    else:
        pass
    readable_buckets = [i for i in bucket_list if i not in invalid_buckets["bucket"]]
    print(f"readable buckets are: {*readable_buckets,}")
    if len(readable_buckets) > 0:
        print(
            "start checking if manifest objs exist in bucket and if exist, whether the bucket obj size matches to the manifest"
        )

        df_file_validated = validate_objs_loc_size(
            file_object=file_object,
            file_node_list=file_nodes_to_check,
            readable_bucket_list=readable_buckets,
        )
        # write summary of if manifest obj exist in bucket
        if sum(df_file_validated["if_exist"] == False) > 0:
            not_exist_str = "\tWARNING: There are files that are not found in the readable bucket list, but are in the manifest:\n\n"
            not_exist_summary = (
                df_file_validated[df_file_validated["if_bucket_readable"] == True]
                .groupby("if_exist")
                .size()
                .reset_index(name="counts")
            )
            not_exist_node_filename = df_file_validated[
                df_file_validated["if_exist"] == False
            ][["node", "file_name"]]
            not_exist_str = (
                not_exist_str
                + not_exist_summary.to_markdown(
                    tablefmt="rounded_grid", index=False
                ).replace("\n", "\n\t")
                + "\n\n"
                + not_exist_node_filename.to_markdown(
                    tablefmt="rounded_grid", index=False
                ).replace("\n", "\n\t")
                + "\n\n"
            )
            print(not_exist_summary.to_markdown(tablefmt="rounded_grid", index=False))
            print(not_exist_node_filename.to_markdown(tablefmt="rounded_grid", index=False))
            with open(output_file, "a+") as outf:
                outf.write(not_exist_str)
        else:
            with open(output_file, "a+") as outf:
                outf.write(
                    f"\tAll files under READABLE buckets {*readable_buckets,} can be found in AWS bucket\n"
                )
            print(
                f"\tAll files under READABLE buckets {*readable_buckets,} can be found in AWS bucket\n"
            )

        # write summary of manifest obj size comparison in bucket
        if (
            sum(
                df_file_validated[df_file_validated["if_exist"] == True]["size_compare"]
                == False
            )
            > 0
        ):
            size_comparison_summary = (
                df_file_validated[df_file_validated["if_exist"] == True]
                .groupby("size_compare")
                .size()
                .reset_index(name="counts")
            )
            size_compare_fail_df = df_file_validated[
                df_file_validated["if_exist"] == True
                & df_file_validated["size_compare"]
                == False
            ][["node", "file_name", "file_size", "bucket_obj_size"]]
            size_compare_fail_str = "\tWARNING: There are manifest files that exist in AWS bucket, but failed size comparison check.\n\n"
            size_compare_fail_str = (
                size_compare_fail_str
                + size_comparison_summary.to_markdown(
                    tablefmt="rounded_grid", index=False
                ).replace("\n", "\n\t")
                + "\n\n"
                + size_compare_fail_df.to_markdown(
                    tablefmt="rounded_grid", index=False
                ).replace("\n", "\n\t")
                + "\n\n"
            )
            print(
                size_comparison_summary.to_markdown(
                    tablefmt="rounded_grid", index=False
                )
            )
            print(
                size_compare_fail_df.to_markdown(tablefmt="rounded_grid", index=False)
            )
            with open(output_file, "a+") as outf:
                outf.write(size_compare_fail_str)
        else:
            with open(output_file, "a+") as outf:
                outf.write(
                    f"\tAll files that passed the exist check PASSED the file size check\n"
                )
            print(
                f"\tAll files that passed the exist check PASSED the file size check\n"
            )
        del df_file_validated

        # Check if the bucket content can be found in the manifest
        print("Start checking if all bucket objs can be found in the manifest")
        bucket_objs_unfound = validate_bucket_objs_in_manifest(
            file_object=file_object,
            file_node_list=file_nodes_to_check,
            readable_buckets=readable_buckets,
        )
        if len(bucket_objs_unfound) > 0:
            bucket_objs_unfound_str = "\tWARNING: There are files that are found in the bucket but not the manifest:\n\t\t"
            bucket_objs_unfound_str = (
                bucket_objs_unfound_str + "\n\t\t".join(bucket_objs_unfound) + "\n"
            )
            with open(output_file, "a+") as outf:
                outf.write(bucket_objs_unfound_str)
        else:
            with open(output_file, "a+") as outf:
                outf.write(
                    "\tAll files in the accessible buckets can be found in the manifest\n"
                )

    else:
        with open(output_file, "a+") as outf:
            outf.write(
                "\n\tWARNING: No accessible buckets for AWS bucket content validation\n"
            )
    return None


@task(
    name="Validate cross links of one sheet",
    log_prints=True,
    task_run_name="Validate cross links of node {node_name}",
)
def validate_cross_links_single_sheet(node_name: str, file_object) -> str:
    """Performs cross links validation between nodes of a single sheet"""
    print_str = f"\n\t{node_name}:\n\t----------\n"

    # get node df
    node_df = file_object.read_sheet_na(sheetname=node_name)
    # pull out all the linking properties
    link_props = node_df.filter(like="_id", axis=1)
    link_props = link_props.filter(like=".", axis=1).columns.tolist()

    # if there are more than one linking property
    if len(link_props) > 1:
        for _, row in node_df.iterrows():
            row_values = row[link_props].dropna().tolist()
            # if there are entries that have more than one linking property value
            if len(set(row_values)) > 1:
                print_str = (
                    print_str
                    + "\tWARNING: The entry on row {index+1} contains multiple links. While multiple links can occur, they are often not needed or best practice.\n"
                )
            else:
                pass
    else:
        # skip multiple linking property check if only one parent node found
        pass

    # for the linking property
    for link_prop in link_props:
        # find the unique values of that linking property
        link_values = node_df[link_prop].dropna().unique().tolist()

        # if there are values in parent link
        if len(link_values) > 0:
            # determine the linking node and property.
            linking_node = str.split(link_prop, ".")[0]
            linking_prop = str.split(link_prop, ".")[1]
            df_link = file_object.read_sheet_na(sheetname=linking_node)
            linking_values = df_link[linking_prop].dropna().unique().tolist()

            # test to see if all the values are found
            # all True if all values in link_values can be found in linking values(parent node sheet id)
            matching_links = [
                True if id in linking_values else False for id in link_values
            ]

            # if not all values match, determined the mismatched values
            if not all(matching_links):
                mis_match_values = np.array(link_values)[
                    ~np.array(matching_links)
                ].tolist()

                # for each mismatched value, throw an error.
                for mis_match_value in mis_match_values:
                    print_str = (
                        print_str
                        + f"\tERROR: For the node, {node_name}, the following linking property, {link_prop}, has a value that is not found in the parent node: {mis_match_value}\n"
                    )
            else:
                print_str = (
                    print_str
                    + f"\tPASS: The links for the node, {node_name}, have corresponding values in the parent node, {linking_node}.\n"
                )
        else:
            pass
    print(print_str)
    return print_str


@flow(name="Validate cross links", log_prints=True)
def validate_cross_links(
    file_path: str, output_file: str, node_list: list[str]
) -> None:
    """Performs cross link validation between nodes of entire manifest file"""
    section_title = "\n\nIf there are unexpected or missing values in the linking values between nodes, they will be reported below:\n----------\n"

    # create file_object and template_object
    file_object = CheckCCDI(ccdi_manifest=file_path)
    cross_validate_future = validate_cross_links_single_sheet.map(
        node_list, file_object
    )
    cross_validate_str = "".join([i.result() for i in cross_validate_future])
    return_str = section_title + cross_validate_str
    with open(output_file, "a+") as outf:
        outf.write(return_str)
    return None


@task(
    name="Validate Key ID of a single sheet",
    log_prints=True,
    task_run_name="Validate Key ID of node {node_name}",
)
def validate_key_id_single_sheet(node_name: str, file_object, template_object) -> str:
    """Validate key id of a single sheet"""
    print_str = f"\n\t{node_name}:\n\t----------\n"

    # get node df
    node_df = file_object.read_sheet_na(sheetname=node_name)
    dict_df = template_object.read_sheet_na(sheetname="Dictionary")
    # pull out all the id properties in the node
    id_props = node_df.filter(like="_id", axis=1).columns.tolist()
    key_id_props = dict_df[dict_df["Key"] == "True"]["Property"].unique().tolist()
    # pull out only the key ids that are present in the node
    key_ids = list(set(id_props) & set(key_id_props))

    for key_id in key_ids:
        # find the unique values of that linking property
        id_values = node_df[key_id].dropna().unique().tolist()

        # if there are values with ";", unpack them
        # for instance ["apple;orange", "milk;yogurt", "cabbage;carrot"] -> ['apple', 'orange', 'milk', 'yogurt', 'cabbage', 'carrot']
        if len(id_values) > 0:
            # if there is an array of link values, pull the array apart and delete the old value.
            remove_id_value = []
            append_id_value = []
            for id_value in id_values:
                if ";" in id_value:
                    value_splits = str.split(id_value, ";")
                    append_id_value.extend(value_splits)
                    remove_id_value.append(id_value)
                else:
                    pass
            id_values.extend(append_id_value)
            new_id_values = [i for i in id_values if i not in remove_id_value]
            id_values = new_id_values

            WARN_FLAG = True
            # for each id value
            troubled_id_value = []
            for id_value in id_values:
                # if it does not match the following regex, throw an error.
                if not re.match(pattern=r"^[a-zA-Z0-9_.@#;-]*$", string=id_value):
                    if WARN_FLAG:
                        WARN_FLAG = False
                        print_str = (
                            print_str
                            + f"\tERROR: The following IDs have an illegal character (acceptable: A-z,0-9,_,.,-,@,#) in the property:\n"
                        )
                    else:
                        pass
                    troubled_id_value.append(id_value)
                else:
                    pass

            if len(troubled_id_value) > 0:
                print_str = print_str + "\t\t" + ", ".join(troubled_id_value) + "\n"
            else:
                pass
        else:
            pass

    return print_str


@flow(name="Validate Key ID", log_prints=True)
def validate_key_id(
    file_path: str, template_path: str, node_list: list[str], output_file
) -> None:
    """Validate key id of entire manifest"""
    section_title = "\n\nFor the '_id' key properties, only the following characters can be included: English letters, Arabic numerals, period (.), hyphen (-), underscore (_), at symbol (@), and the pound sign (#).\nFor values that do not match, they will be reported below:\n----------\n"

    # create file_object and template_object
    file_object = CheckCCDI(ccdi_manifest=file_path)
    template_object = CheckCCDI(ccdi_manifest=template_path)
    validate_key_id_future = validate_key_id_single_sheet.map(
        node_list, file_object, template_object
    )
    validate_key_id_str = "".join([i.result() for i in validate_key_id_future])
    return_str = section_title + validate_key_id_str
    with open(output_file, "a+") as outf:
        outf.write(return_str)
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
    # all_properties = set(dict_df["Property"]) # this variable not used in original script
    required_properties = set(dict_df[dict_df["Required"].notna()]["Property"])

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
    validation_logger.info("Checking whitespace in property values")
    validate_whitespace(nodes_to_validate, file_path, output_file)

    # validate terms and value sets
    validation_logger.info("Checking term and value sets")
    validate_terms_value_sets(file_path, template_path, nodes_to_validate, output_file)

    # validate integer and numeric vlaues
    validation_logger.info("Checking integer and numeric values")
    validate_integer_numeric_checks(
        file_path, template_path, nodes_to_validate, output_file
    )

    # validate regex
    validation_logger.info("Checking regular expression")
    validate_regex(nodes_to_validate, file_path, template_path, output_file)

    # validate unique keys
    validation_logger.info("Checking unique keys")
    validate_unique_key(nodes_to_validate, file_path, template_path, output_file)

    # validate file metadata (size, md5sum regex, and file basename in url)
    validation_logger.info(
        "Checking object file metadata, size, md5sum regex, and file basename"
    )
    validate_file_metadata(
        node_list=nodes_to_validate,
        file_path=file_path,
        template_path=template_path,
        output_file=output_file,
    )

    # validate bucket content
    validation_logger.info("Checking bucket contents against manifest file objects")
    validate_bucket_content(
        node_list=nodes_to_validate,
        file_path=file_path,
        template_path=template_path,
        output_file=output_file,
    )

    # validate cross links
    validation_logger.info("Checking cross links between nodes")
    validate_cross_links(
        node_list=nodes_to_validate, file_path=file_path, output_file=output_file
    )

    # validate key id pattern
    validation_logger.info("Checking key id patterns")
    validate_key_id(
        file_path=file_path,
        template_path=template_path,
        node_list=nodes_to_validate,
        output_file=output_file,
    )

    validation_logger.info(
        f"Process Complete. The output file can be found here: {output_file}"
    )

    return output_file
