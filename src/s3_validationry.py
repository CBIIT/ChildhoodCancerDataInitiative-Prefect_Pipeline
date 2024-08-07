from prefect import flow, get_run_logger
import os
import pandas as pd
from datetime import date
import sys
import numpy as np
import warnings
import re
from src.utils import set_s3_session_client, get_time
import boto3
from botocore.exceptions import ClientError


@flow(
    name="CCDI_ValidationRy",
    log_prints=True,
    flow_run_name="CCDI_ValidationRy_" + f"{get_time()}",
)
def ValidationRy(file_path: str, template_path: str):  # removed profile
    validation_logger = get_run_logger()
    validation_logger.info("The CCDI submission template is being checked for errors")

    ##############
    #
    # File name rework
    #
    ##############

    # Determine file ext and abs path
    file_name = os.path.splitext(os.path.split(os.path.relpath(file_path))[1])[0]
    file_dir_path = os.path.split(os.path.abspath(file_path))[0]

    if file_dir_path == "":
        file_dir_path = "."

    # obtain the date
    def refresh_date():
        today = date.today()
        today = today.strftime("%Y%m%d")
        return today

    todays_date = refresh_date()

    # Output file name based on input file name and date/time stamped.
    output_file = file_name + "_Validate" + todays_date

    # function to determine if a string value is a float
    def isFloat(s):
        try:
            float(s)
            return True
        except ValueError:
            return False

    # function to determine if a string value is an int
    def isInt(s):
        try:
            int(s)
            return True
        except ValueError:
            return False

    ##############
    #
    # Pull Dictionary Page to create node pulls
    #
    ##############

    def read_xlsx(file_path: str, sheet: str):
        # Read in excel file
        warnings.simplefilter(action="ignore", category=UserWarning)
        return pd.read_excel(file_path, sheet, dtype=str)

    # create workbook
    xlsx_model = pd.ExcelFile(template_path)

    # create dictionary for dfs
    model_dfs = {}

    # check to make sure Dictionary and Terms and Value Sets are in the template
    if (
        not "Dictionary" in xlsx_model.sheet_names
        or not "Terms and Value Sets" in xlsx_model.sheet_names
    ):
        validation_logger.error(
            "The template file needs to contain both a 'Dictionary' and 'Terms and Value Sets' tab"
        )
        sys.exit(1)

    # read in dfs and apply to dictionary
    for sheet_name in xlsx_model.sheet_names:
        model_dfs[sheet_name] = read_xlsx(xlsx_model, sheet_name)

    # close xlsx_model
    xlsx_model.close()

    # pull out the non-metadata table and then remove them from the dictionary
    readme_df = model_dfs["README and INSTRUCTIONS"]
    dict_df = model_dfs["Dictionary"]
    tavs_df = model_dfs["Terms and Value Sets"]

    # create a list of all properties and a list of required properties
    all_properties = set(dict_df["Property"])
    required_properties = set(dict_df[dict_df["Required"].notna()]["Property"])

    ##############
    #
    # Read in TaVS page to create value checks
    #
    ##############

    # Read in Terms and Value sets page to obtain the required value set names.
    tavs_df = tavs_df.dropna(how="all").dropna(how="all", axis=1)

    ##############
    #
    # Read in data
    #
    ##############

    # create workbook
    xlsx_data = pd.ExcelFile(file_path)

    # create dictionary for dfs
    meta_dfs = {}

    # read in dfs and apply to dictionary
    for sheet_name in xlsx_data.sheet_names:
        meta_dfs[sheet_name] = read_xlsx(xlsx_data, sheet_name)

    # close xlsx_data
    xlsx_data.close()

    # remove model tabs from the meta_dfs
    not_needed_data_nodes = [
        "README and INSTRUCTIONS",
        "Dictionary",
        "Terms and Value Sets",
    ]
    for not_needed_data_node in not_needed_data_nodes:
        if not_needed_data_node in list(meta_dfs.keys()):
            del meta_dfs[not_needed_data_node]

    # create a list of present tabs
    dict_nodes = list(meta_dfs.keys())

    # Do a check to make sure that nodes present are nodes that are expected based on the Dictionary
    dictionary_node_check = dict_df["Node"].unique().tolist()

    removed_nodes = list(set(dict_nodes) - set(dictionary_node_check))
    dict_nodes = list(set(dict_nodes) & set(dictionary_node_check))

    if len(removed_nodes) > 0:
        validation_logger.warning(
            f"The following nodes are not recognized in the 'Dictionary' tab, and were removed from the validation checks: {*removed_nodes,}"
        )
        for removed_node in removed_nodes:
            del meta_dfs[removed_node]

    ##############
    #
    # Go through each tab and remove completely empty tabs
    #
    ##############
    node_to_remove = []
    for node in dict_nodes:
        # see if the tab contain any data
        test_df = meta_dfs[node]
        test_df = test_df.drop("type", axis=1)
        test_df = test_df.dropna(how="all").dropna(how="all", axis=1)
        # if there is no data, add the node name to the node_to_remove
        if test_df.empty:
            node_to_remove.append(node)
        else:
            pass
    validation_logger.info(f"{node_to_remove} tabs are empty")
    dict_nodes = [i for i in dict_nodes if i not in node_to_remove]
    meta_dfs = {key: meta_dfs[key] for key in meta_dfs if key not in node_to_remove}

    # Final reordering of present nodes to show up in tab order in the output.
    dict_nodes = sorted(
        dict_nodes,
        key=lambda x: (
            dictionary_node_check.index(x)
            if x in dictionary_node_check
            else float("inf")
        ),
    )

    ##############
    #
    # Start Log Printout
    #
    ##############
    validation_out_file = f"{output_file}.txt"
    with open(f"{file_dir_path}/{validation_out_file}", "w") as outf:
        ##############
        #
        # Required Properties completeness
        #
        ##############

        print(
            "\n\nThis section is for required properties for all nodes that contain data.\nFor information on required properties per node, please see the 'Dictionary' page of the template file.\nFor each entry, it is expected that all required information has a value:\n----------",
            file=outf,
        )

        # for each tab
        for node in dict_nodes:
            print(f"\n\t{node}\n\t----------", file=outf)
            df = meta_dfs[node]
            properties = df.columns
            line_length = 25

            for property in properties:
                WARN_FLAG = True
                if property in required_properties:
                    if df[property].isna().any():
                        # if there are missing values
                        # locate them
                        bad_positions = np.where(df[property].isna())[0] + 2

                        # Flag to turn on explanation of error/warning
                        if WARN_FLAG:
                            WARN_FLAG = False
                            print(
                                f"\tERROR: The values for the node, {node}, in the the required property, {property}, are missing:",
                                file=outf,
                            )

                        # itterate over that list and print out the values
                        for i, pos in enumerate(bad_positions):
                            if i % line_length == 0:
                                print("\n\t\t", end="", file=outf)
                            print(pos, end=", ", file=outf)

                        print("\n", file=outf)

                    else:
                        print(
                            f"\tPASS: For the node, {node}, the required property, {property}, contains values for all expexted entries.",
                            file=outf,
                        )

        ##############
        #
        # Properties value whitespace
        #
        ##############

        print(
            "\n\nThis section checks for white space issues in all properties.\n----------",
            file=outf,
        )

        # for each tab
        for node in dict_nodes:
            df = meta_dfs[node]
            properties = df.columns
            line_length = 25

            for property in properties:
                WARN_FLAG = True
                # if the property is not completely empty:
                if not df[property].isna().all():
                    # if there are some values that do not match when positions are stripped of white space
                    if (
                        df[property].fillna("") != df[property].str.strip().fillna("")
                    ).any():
                        # print node
                        print(f"\n\t{node}\n\t----------", file=outf)
                        # if there are missing values
                        # locate them
                        bad_positions = (
                            np.where(
                                df[property].fillna("")
                                != df[property].str.strip().fillna("")
                            )[0]
                            + 2
                        )

                        # Flag to turn on explanation of error/warning
                        if WARN_FLAG:
                            WARN_FLAG = False
                            print(
                                f"\tERROR: The values for the node, {node}, in the the required property, {property}, have white space issues:",
                                file=outf,
                            )

                        # itterate over that list and print out the values
                        for i, pos in enumerate(bad_positions):
                            if i % line_length == 0:
                                print("\n\t\t", end="", file=outf)
                            print(pos, end=", ", file=outf)

                        print("\n", file=outf)

        ##############
        #
        # Terms and Value sets checks
        #
        ##############

        print(
            "The following columns have controlled vocabulary on the 'Terms and Value Sets' page of the template file. If the values present do not match, they will noted and in some cases the values will be replaced:\n----------",
            file=outf,
        )

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

        # for each tab
        for node in dict_nodes:
            print(f"\n\t{node}\n\t----------", file=outf)
            df = meta_dfs[node]
            properties = df.columns
            line_length = 5

            # for each property
            for property in properties:
                WARN_FLAG = True
                tavs_df_prop = tavs_df[tavs_df["Value Set Name"] == property]
                # if the property is in the TaVs data frame
                if len(tavs_df_prop) > 0:
                    # if the property is not completely empty:
                    if not df[property].isna().all():
                        # if the property is an enum
                        if property in enum_arrays:
                            # obtain a list of value strings
                            unique_values = df[property].dropna().unique()

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
                                        unique_value = list(
                                            set(unique_value.split(";"))
                                        )
                                        for value in unique_value:
                                            unique_values = np.append(
                                                unique_values, value
                                            )

                            # make sure list is unique
                            unique_values = list(set(unique_values))

                            if set(unique_values).issubset(set(tavs_df_prop["Term"])):
                                # if yes, then
                                print(
                                    f"\tPASS: {property}, property contains all valid values.",
                                    file=outf,
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
                                        print(
                                            f"\tWARNING: {property} property contains a value that is not recognized, but can handle free strings:",
                                            file=outf,
                                        )
                                    else:
                                        print(
                                            f"\tERROR: {property} property contains a value that is not recognized:",
                                            file=outf,
                                        )

                                # for each value that is not found, add to a list
                                for unique_value in unique_values:
                                    if unique_value not in tavs_df_prop["Term"].values:
                                        bad_enum_list.append(unique_value)

                                # itterate over that list and print out the values
                                for i, enum in enumerate(bad_enum_list):
                                    if i % line_length == 0:
                                        print("\n\t\t", end="", file=outf)
                                    print(enum, end=", ", file=outf)

                                print("\n", file=outf)

                        # if the property is not an enum
                        else:
                            unique_values = df[property].dropna().unique()
                            # as long as there are unique values
                            if len(unique_values) > 0:
                                # are all the values found in the TaVs terms
                                if set(unique_values).issubset(
                                    set(tavs_df_prop["Term"])
                                ):
                                    # if yes, then
                                    print(
                                        f"\tPASS: {property}, property contains all valid values.",
                                        file=outf,
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
                                            print(
                                                f"\tWARNING: {property} property contains a value that is not recognized, but can handle free strings:",
                                                file=outf,
                                            )
                                        else:
                                            print(
                                                f"\tERROR: {property} property contains a value that is not recognized:",
                                                file=outf,
                                            )

                                    # for each unique value, check it against the TaVs data frame
                                    for unique_value in unique_values:
                                        if (
                                            unique_value
                                            not in tavs_df_prop["Term"].values
                                        ):
                                            bad_enum_list.append(unique_value)

                                    # itterate over that list and print out the values
                                    for i, enum in enumerate(bad_enum_list):
                                        if i % line_length == 0:
                                            print("\n\t\t", end="", file=outf)
                                        print(enum, end=", ", file=outf)

                                    print("\n", file=outf)

        ##############
        #
        # Integer and numeric checks
        #
        ##############

        print(
            "\nThis section will display any values in properties that are expected to be either numeric or integer based on the Dictionary, but have values that are not:\n----------\n",
            file=outf,
        )

        # Since the files are read in as "all strings" to ensure that the file can be ingested, this can hide issues with integers and numbers.
        # This check will look at the dictionary to determine which properties should be integers and numbers and then force the strings into those types and make checks.

        int_props = dict_df[dict_df["Type"] == "integer"]["Property"].unique().tolist()
        num_props = dict_df[dict_df["Type"] == "number"]["Property"].unique().tolist()
        # for each tab
        for node in dict_nodes:
            print(f"\n\t{node}\n\t----------", file=outf)
            df = meta_dfs[node]
            properties = df.columns
            line_length = 25

            # for each property
            for property in properties:
                WARN_FLAG = False

                # NUMBER PROPS CHECK

                # if that property is a number property
                if property in num_props:
                    # if there are atleast one value
                    if len(df[property].dropna().tolist()) > 0:
                        error_rows = []
                        # go through each row
                        for row in list(range(len(df))):
                            # obtain the value
                            value = df[property][row]
                            # if it is not NA
                            if pd.notna(value):
                                # test whether it is a float
                                if not isFloat(value):
                                    # if not, add to list, row number offset by 2
                                    error_rows.append(row + 2)
                                    WARN_FLAG = True

                    # if the warning flag was tripped
                    if WARN_FLAG:
                        WARN_FLAG = False

                        print(
                            f"\tERROR: {property} property contains a value that is not a number:",
                            file=outf,
                        )
                        # itterate over that list and print out the values
                        for i, row in enumerate(error_rows):
                            if i % line_length == 0:
                                print("\n\t\t", end="", file=outf)
                            print(row, end=", ", file=outf)

                        print("\n", file=outf)

                # INTEGER PROPS CHECK

                # if that property is a integer property
                if property in int_props:
                    # if there are atleast one value
                    if len(df[property].dropna().tolist()) > 0:
                        error_rows = []
                        # go through each row
                        for row in list(range(len(df))):
                            # obtain the value
                            value = df[property][row]
                            # if it is not NA
                            if pd.notna(value):
                                # test whether it is a int
                                if not isInt(value):
                                    # if not, add to list, row number offset by 2
                                    error_rows.append(row + 2)
                                    WARN_FLAG = True

                    # if the warning flag was tripped
                    if WARN_FLAG:
                        WARN_FLAG = False

                        print(
                            f"\tERROR: {property} property contains a value that is not a number:",
                            file=outf,
                        )
                        # itterate over that list and print out the values
                        for i, row in enumerate(error_rows):
                            if i % line_length == 0:
                                print("\n\t\t", end="", file=outf)
                            print(row, end=", ", file=outf)

                        print("\n", file=outf)

        ##############
        #
        # Regex Checks
        #
        ##############

        print(
            "\nThis section will display any values in properties that can accept strings, which are thought to contain PII/PHI based on regex suggestions from dbGaP:\n----------\n",
            file=outf,
        )

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

        # pull out a data frame that only applies to string values
        string_df = dict_df[dict_df["Type"].str.lower().str.contains("string")]

        for node in dict_nodes:
            df = meta_dfs[node]
            string_node = string_df[string_df["Node"].isin([node])]
            string_props = string_node["Property"].values

            # logic to remove both GUID and md5sum from the check, as these are random/semi-random strings that are created and would never have a date placed in them.
            if "md5sum" in string_props:
                string_props = string_props[string_props != "md5sum"]

            if "dcf_indexd_guid" in string_props:
                string_props = string_props[string_props != "dcf_indexd_guid"]

            for string_prop in string_props:
                WARN_FLAG = True
                # find all unique values
                string_values = df[string_prop].dropna().unique()

                bad_regex_strings = []
                # each unique value
                for string_value in string_values:
                    # if that value matches any of the regex
                    for regex in all_regex:
                        if re.match(regex, string_value):
                            bad_regex_strings.append(string_value)

                if len(bad_regex_strings) > 0:
                    # Flag to turn on explanation of error/warning
                    if WARN_FLAG:
                        WARN_FLAG = False
                        print(
                            f"\tERROR: For the {node} node, the {string_prop} property contains a value that matches a regular expression for dates/social security number/phone number/zip code:",
                            file=outf,
                        )

                    # itterate over that list and print out the values
                    for i, string_val in enumerate(bad_regex_strings):
                        if i % 5 == 0:
                            print("\n\t\t", end="", file=outf)
                        print(string_val, end=", ", file=outf)

                    print("\n", file=outf)

        ##############
        #
        # Unique Key check
        #
        ##############

        print(
            "\n\nThe following will check for multiples of key values, which are expected to be unique.\nIf there are any unexpected values, they will be reported below:\n----------",
            file=outf,
        )

        for node in dict_nodes:
            df = meta_dfs[node]

            # pull out all key value properties
            key_value_props = dict_df[
                (dict_df["Key"] == "True") & (dict_df["Node"] == node)
            ]["Property"].values

            # for each key value property in a node (should only be one, but just in case)
            for key_value_prop in key_value_props:
                WARN_FLAG = True
                # if a property is found in the data frame
                if key_value_prop in df.columns.tolist():
                    # as long as there are some values in the key column
                    if df[key_value_prop].notna().any():
                        # if the length of the data frame is not the same length of the unique key property values, then we have some non-unique values
                        if len(df[key_value_prop].dropna()) != len(
                            df[key_value_prop].dropna().unique()
                        ):
                            if WARN_FLAG:
                                WARN_FLAG = False
                                print(
                                    f"\tERROR: The {node} node, has multiple instances of the same key value, which should be unique, in the property, {key_value_prop}:",
                                    file=outf,
                                )

                            # create a table of values and counts
                            freq_key_values = df[key_value_prop].value_counts()

                            # pull out a unique list of values that have more than one instance
                            not_unique_key_values = (
                                df[
                                    df[key_value_prop].isin(
                                        freq_key_values[freq_key_values > 1].index
                                    )
                                ][key_value_prop]
                                .unique()
                                .tolist()
                            )

                            # itterate over that list and print out the values
                            for i, not_unique_key_value in enumerate(
                                not_unique_key_values
                            ):
                                if i % 5 == 0:
                                    print("\n\t\t", end="", file=outf)
                                print(not_unique_key_value, end=", ", file=outf)

                            print("\n", file=outf)

        ##############
        #
        # File checks, both metdata and buckets.
        #
        ##############
        # Make one large flattened data frame that contains all files from each node. This will make it easier to not only determine errors, but might catch errors that would not be noticed as they dont exist on the same page.
        file_nodes = dict_df[dict_df["Property"] == "file_url"][
            "Node"
        ].values.tolist()
        file_node_props = [
            "file_id",
            "file_name",
            "file_size",
            "md5sum",
            "file_url",
            "node",
        ]
        df_file = pd.DataFrame(columns=file_node_props)
        df_file = df_file.sort_values("node").reset_index(drop=True)

        # print(dict_nodes)
        for node in dict_nodes:
            if node in file_nodes:
                df = meta_dfs[node]
                df["node"] = node
                df["file_id"] = df[f"{node}_id"]
                df_file = pd.concat([df_file, df[file_node_props]], ignore_index=True)

        # revert HTML code changes that might exist so that it can be handled with correct AWS calls
        df_file["file_url"] = df_file["file_url"].map(
            lambda x: (
                x.replace("%20", " ").replace("%2C", ",").replace("%23", "#")
                if isinstance(x, str)
                else x
            )
        )

        file_ids = df_file["file_id"].dropna().unique().tolist()
        file_names = df_file["file_name"].dropna().unique().tolist()
        file_urls = df_file["file_url"].dropna().unique().tolist()

        ##########################################
        # file metadata checks
        ##########################################
        print(
            "\nThe following section will check the manifest for expected file metadata.\nIf there are any unexpected values, they will be reported below:\n----------\n",
            file=outf,
        )

        WARN_FLAG = True

        # check for file_size == 0
        for file_name in file_names:
            # determine file size
            file_size = df_file[df_file["file_name"] == file_name]["file_size"].values[
                0
            ]
            if file_size == "0":
                if WARN_FLAG:
                    WARN_FLAG = False
                    print(
                        f"\t\tWARNING: There are files that have a size value of 0:",
                        file=outf,
                    )

                current_node = df_file[df_file["file_name"] == file_name][
                    "node"
                ].values[0]

                print(f"\t\t\t{current_node} : {file_name}", file=outf)

        WARN_FLAG = True

        # check for md5sum regex
        for file_name in file_names:
            # determine file md5sum
            file_md5sum = df_file[df_file["file_name"] == file_name]["md5sum"].values[0]
            if not re.match(pattern=r"^[a-f0-9]{32}$", string=file_md5sum):
                if WARN_FLAG:
                    WARN_FLAG = False
                    print(
                        f"\t\tWARNING: There are files that have a md5sum value that does not follow the md5sum regular expression:",
                        file=outf,
                    )

                current_node = df_file[df_file["file_name"] == file_name][
                    "node"
                ].values[0]

                print(f"\t\t\t{current_node} : {file_name}", file=outf)

        WARN_FLAG = True

        # check for file basename in url
        for file_name in file_names:
            # determine file url
            file_url = df_file[df_file["file_name"] == file_name][
                "file_url"
            ].values[0]
            if file_name != os.path.split(os.path.relpath(file_url))[1]:
                if WARN_FLAG:
                    WARN_FLAG = False
                    print(
                        f"\t\tWARNING: There are files that have a file_name that does not match the file name in the url:",
                        file=outf,
                    )

                current_node = df_file[df_file["file_name"] == file_name][
                    "node"
                ].values[0]

                print(f"\t\t\t{current_node} : {file_name}", file=outf)

        WARN_FLAG = True

        # This check is likely unneeded for CCDI validation, as it was a hack for CDS downstream validation.
        # It will remain commented out for a while and if someone comes across this section and finds that no one
        # has complained since it was commented out on March 21, 2024, then it can be deleted.
        # check for file uniqueness for name and url
        # for file_name in file_names:
        #     # determine file url
        #     file_url = (
        #         df_file[df_file["file_name"] == file_name]["file_url"]
        #         .unique()
        #         .tolist()
        #     )
        #     if len(file_url) > 1:
        #         if WARN_FLAG:
        #             WARN_FLAG = False
        #             print(
        #                 f"\t\tWARNING: There are files that are associated with more than one url.\n\t\tWARNING: If they only have one associated file_id, then they are acceptable.",
        #                 file=outf,
        #             )

        #         current_node = df_file[df_file["file_name"] == file_name][
        #             "node"
        #         ].values[0]
        #         print(f"\t\t\t{current_node} : {file_name} --> {file_url}", file=outf)

        #         # check to see if file_id is unique even if file name isn't
        #         multi_urls = []
        #         for per_file_url in file_url:
        #             multi_urls = df_file[df_file["file_url"] == per_file_url][
        #                 "file_name"
        #             ].unique().tolist()
        #             print(
        #                 f"\t\t\t\t{per_file_url}\n\t\t\t\t\tfile_ids:\n\t\t\t\t\t\t{multi_urls}",
        #                 file=outf,
        #             )

        ##########################################
        # AWS bucket file checks
        ##########################################
        print(
            "\nThe following section will compare the manifest against the reported buckets and note if there are unexpected results where the file is represented equally in both sources.\nIf there are any unexpected values, they will be reported below:\n----------\n",
            file=outf,
        )

        WARN_FLAG = True

        # create bucket column from file data frame
        df_file["bucket"] = df_file["file_url"].str.split("/").str[2]
        # print(df_file[df_file.isna().any(axis=1)][["file_name","node","bucket"]].to_markdown())

        # return the unique list of buckets
        buckets = list(set(df_file["bucket"].values.tolist()))
        validation_logger.info(f"List of buckets: {*buckets,}")

        # if there are more than one bucket, warning
        if len(buckets) > 1:
            print(
                "\tThere are more than one aws bucket that is associated with this metadata file:",
                file=outf,
            )
            print(f"\t\t{buckets}", file=outf)
            print("\n", file=outf)

        # Get s3 session setup
        s3_client = set_s3_session_client()

        # initialize file metadata from bucket
        s3_file_path = []
        s3_file_size = []

        # for the bucket
        for bucket in buckets:
            # try and see if the bucket exists, if it does, obtain the metadata from it
            try:
                s3_client.head_bucket(Bucket=bucket)

                # create a paginator to itterate through each 1000 objs
                paginator = s3_client.get_paginator("list_objects_v2")
                response_iterator = paginator.paginate(Bucket=bucket)

                # pull out each response and obtain file name and size
                for response in response_iterator:
                    if "Contents" in response:
                        for obj in response["Contents"]:
                            s3_file_path.append("s3://" + bucket + "/" + obj["Key"])
                            s3_file_size.append(obj["Size"])

            except ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    print(
                        f"\tThe following bucket either does not exist or you do not have read access for it: {bucket}",
                        file=outf,
                    )

        # create a metadata data frame from the bucket
        df_bucket = pd.DataFrame({"url": s3_file_path, "file_size": s3_file_size})

        if len(df_bucket) > 0:
            # for each line in the file manifest, check to see if it the file is in the bucket
            WARN_FLAG = True

            for file_url in file_urls:
                if not file_url in set(df_bucket["url"]):
                    # if the files are not in the bucket, throw an ERROR.
                    if WARN_FLAG:
                        WARN_FLAG = False
                        print(
                            f"\t\tWARNING: There are files that are not found in the bucket, but are in the manifest:",
                            file=outf,
                        )

                    current_node = df_file[df_file["file_url"] == file_url][
                        "node"
                    ].values[0]
                    file_name = df_file[df_file["file_url"] == file_url][
                        "file_name"
                    ].values[0]

                    print(
                        f"\t\t\t{current_node} : {file_name} --> {file_url}", file=outf
                    )

            # for each line in the bucket manifest, check to see if it the file is in the manifest
            WARN_FLAG = True

            for file_url in list(set(df_bucket["url"])):
                if not file_url in file_urls:
                    # if the files are not in the bucket, throw an ERROR.
                    if WARN_FLAG:
                        WARN_FLAG = False
                        print(
                            f"\tWARNING: There are files that are found in the bucket, but not the manifest:",
                            file=outf,
                        )

                    print(f"\t\t{file_url}", file=outf)

            # for each line in the file manifest, check to see if it is the right file size
            WARN_FLAG = True

            for file_url in file_urls:
                if file_url in set(df_bucket["url"]):
                    file_size_test = str(
                        df_file[df_file["file_url"] == file_url]["file_size"]
                        .unique()
                        .tolist()[0]
                    )
                    bucket_size_test = str(
                        df_bucket[df_bucket["url"] == file_url]["file_size"].values[0]
                    )
                    # if the files are not the same size, throw an ERROR.
                    if file_size_test != bucket_size_test:
                        if WARN_FLAG:
                            WARN_FLAG = False
                            print(
                                f"\t\tWARNING: There are files that have file_size values that do not match the bucket metadata:",
                                file=outf,
                            )

                        current_node = df_file[df_file["file_url"] == file_url][
                            "node"
                        ].values[0]
                        file_name = df_file[df_file["file_url"] == file_url][
                            "file_name"
                        ].values[0]

                        print(
                            f"\t\t\t{current_node} : {file_name}: {file_size_test} --> {file_url}: {bucket_size_test}",
                            file=outf,
                        )

        # just to make sure we don't have too much sitting in memory.
        del df_bucket

        ###############
        #
        # Cross node validation (do linking values have corresponding values)
        #
        ###############

        print(
            "\n\nIf there are unexpected or missing values in the linking values between nodes, they will be reported below:\n----------",
            file=outf,
        )

        # for each node
        for node in dict_nodes:
            print(f"\n\t{node}:\n\t----------", file=outf)
            df = meta_dfs[node]
            # pull out all the linking properties
            link_props = df.filter(like="_id", axis=1)
            link_props = link_props.filter(like=".", axis=1).columns.tolist()

            # if there are more than one linking property
            if len(link_props) > 1:
                for index, row in df.iterrows():
                    row_values = row[link_props].dropna().tolist()
                    # if there are entries that have more than one linking property value
                    if len(set(row_values)) > 1:
                        print(
                            f"\tWARNING: The entry on row {index+1} contains multiple links. While multiple links can occur, they are often not needed or best practice.\n",
                            file=outf,
                        )

            # for the linking property
            for link_prop in link_props:
                # find the unique values of that linking property
                link_values = df[link_prop].dropna().unique().tolist()

                # if there are values
                if len(link_values) > 0:
                    # determine the linking node and property.
                    linking_node = str.split(link_prop, ".")[0]
                    linking_prop = str.split(link_prop, ".")[1]
                    df_link = meta_dfs[linking_node]
                    linking_values = df_link[linking_prop].dropna().unique().tolist()

                    # if there is an array of link values, pull the array apart and delete the old value.
                    # this should not happen as we are not accepting submissions where there are arrays in the <node>.<node>_id column
                    # if there are multiple connections to different parent entities, they should be in multiple rows.
                    # remove_link_values=[]
                    # for link_value in link_values:
                    #     if ";" in link_value:
                    #         value_splits = str.split(link_value, ";")
                    #         for value_split_value in value_splits:
                    #             link_values.append(value_split_value)
                    #         remove_link_values.append(link_value)
                    #     link_values.remove(remove_link_values)

                    # test to see if all the values are found
                    matching_links = [
                        True if id in linking_values else False for id in link_values
                    ]

                    # matching_links = [
                    #     [id in linking_values for id in link_values]
                    #     for _ in range(len(linking_values))
                    # ]
                    # matching_links = matching_links[0]

                    # if not all values match, determined the mismatched values
                    if not all(matching_links):
                        mis_match_values = np.array(link_values)[
                            ~np.array(matching_links)
                        ].tolist()

                        # for each mismatched value, throw an error.
                        for mis_match_value in mis_match_values:
                            print(
                                f"\tERROR: For the node, {node}, the following linking property, {link_prop}, has a value that is not found in the parent node: {mis_match_value}",
                                file=outf,
                            )

                    else:
                        print(
                            f"\tPASS: The links for the node, {node}, have corresponding values in the parent node, {linking_node}.",
                            file=outf,
                        )

        ###############
        #
        # Key ID validation
        #
        # For the '_id' properties, make sure there are no illegal characters and it only has "Only the following characters can be included in the ID: English letters, Arabic numerals, period (.), hyphen (-), underscore (_), at symbol (@), and the pound sign (#)."
        #
        ###############

        print(
            "\n\nFor the '_id' key properties, only the following characters can be included: English letters, Arabic numerals, period (.), hyphen (-), underscore (_), at symbol (@), and the pound sign (#).\nFor values that do not match, they will be reported below:\n----------",
            file=outf,
        )

        # for each node
        for node in dict_nodes:
            print(f"\n\t{node}:\n\t----------", file=outf)
            df = meta_dfs[node]
            # pull out all the id properties in the node
            id_props = df.filter(like="_id", axis=1).columns.tolist()
            key_id_props = (
                dict_df[dict_df["Key"] == "True"]["Property"].unique().tolist()
            )

            # pull out only the key ids that are present in the node
            key_ids = list(set(id_props) & set(key_id_props))

            # for the linking property
            for key_id in key_ids:
                # find the unique values of that linking property
                id_values = df[key_id].dropna().unique().tolist()

                # if there are values
                if len(id_values) > 0:
                    # if there is an array of link values, pull the array apart and delete the old value.
                    for id_value in id_values:
                        if ";" in id_value:
                            value_splits = str.split(id_value, ";")
                            for value_split_value in value_splits:
                                id_values.append(value_split_value)
                            id_values.remove(id_value)

                    WARN_FLAG = True
                    # for each id value
                    for id_value in id_values:
                        # if it does not match the following regex, throw an error.
                        if not re.match(
                            pattern=r"^[a-zA-Z0-9_.@#;-]*$", string=id_value
                        ):
                            if WARN_FLAG:
                                WARN_FLAG = False
                                print(
                                    f"\tERROR: The following IDs have an illegal character (acceptable: A-z,0-9,_,.,-,@,#) in the property:",
                                    file=outf,
                                )

                            print(f"\t\t{id_value}", file=outf)

    validation_logger.info(
        f"Process Complete. The output file can be found here: {file_dir_path}"
    )

    return validation_out_file
