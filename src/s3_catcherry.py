from prefect import flow, get_run_logger
import os
import warnings
import pandas as pd
import numpy as np
import boto3
import re
from datetime import date
from src.utils import set_s3_session_client, get_time
from botocore.exceptions import ClientError
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows
import uuid
from shutil import copy


@flow(
    name="CCDI_CatchERRy",
    log_prints=True,
    flow_run_name="CCDI_CatchERRy_" + f"{get_time()}",
)
def CatchERRy(file_path: str, template_path: str):  # removed profile
    catcherr_logger = get_run_logger()
    ##############
    #
    # File name rework
    #
    ##############
    # Determine file ext and abs path
    file_name = os.path.splitext(os.path.split(os.path.relpath(file_path))[1])[0]
    file_ext = os.path.splitext(file_path)[1]
    file_dir_path = os.path.split(os.path.relpath(file_path))[0]

    if file_dir_path == "":
        file_dir_path = "."

    # obtain the date
    def refresh_date():
        today = date.today()
        today = today.strftime("%Y%m%d")
        return today

    todays_date = refresh_date()

    # Output file name based on input file name and date/time stamped.
    output_file = file_name + "_CatchERR" + todays_date

    ##############
    #
    # Pull Dictionary Page to create node pulls
    #
    ##############

    catcherr_logger.info("Reading CCDI template file")

    def read_xlsx(file_path: str, sheet: str):
        # Read in excel file
        warnings.simplefilter(action="ignore", category=UserWarning)
        df = pd.read_excel(
            file_path,
            sheet,
            dtype="string",
            keep_default_na=False,
            na_values=[
                "",
                "#N/A",
                "#N/A N/A",
                "#NA",
                "-1.#IND",
                "-1.#QNAN",
                "-NaN",
                "-nan",
                "1.#IND",
                "1.#QNAN",
                "<NA>",
                "N/A",
                "NA",
                "NULL",
                "NaN",
                # "None",
                "n/a",
                "nan",
                "null",
            ],
        )

        # Remove leading and trailing whitespace from all cells
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        return df

    # create workbook
    xlsx_model = pd.ExcelFile(template_path)

    # create dictionary for dfs
    model_dfs = {}

    # read in dfs and apply to dictionary
    for sheet_name in xlsx_model.sheet_names:
        model_dfs[sheet_name] = read_xlsx(xlsx_model, sheet_name)

    # pull out the non-metadata table and then remove them from the dictionary
    # readme_df=model_dfs["README and INSTRUCTIONS"]
    dict_df = model_dfs["Dictionary"]
    tavs_df = model_dfs["Terms and Value Sets"]

    # create a list of all properties and a list of required properties
    # all_properties=set(dict_df['Property'])
    # required_properties=set(dict_df[dict_df["Required"].notna()]["Property"])

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

    catcherr_logger.info("Reading CCDI manifest file")

    # create workbook
    xlsx_data = pd.ExcelFile(file_path)

    # create dictionary for dfs
    meta_dfs = {}

    # read in dfs and apply to dictionary
    for sheet_name in xlsx_data.sheet_names:
        meta_dfs[sheet_name] = read_xlsx(xlsx_data, sheet_name)
    # close xlsx_data object
    xlsx_data.close()

    # remove model tabs from the meta_dfs
    del meta_dfs["README and INSTRUCTIONS"]
    del meta_dfs["Dictionary"]
    del meta_dfs["Terms and Value Sets"]

    # create a list of present tabs
    dict_nodes = set(list(meta_dfs.keys()))

    ##############
    #
    # Go through each tab and remove completely empty tabs
    #
    ##############
    catcherr_logger.info("Removing empty tabs from the manifest file")

    for node in dict_nodes:
        # see if the tab contain any data
        test_df = meta_dfs[node]
        test_df = test_df.drop("type", axis=1)
        test_df = test_df.dropna(how="all").dropna(how="all", axis=1)
        # if there is no data, drop the node/tab
        if test_df.empty:
            del meta_dfs[node]

    # determine nodes again
    dict_nodes = set(list(meta_dfs.keys()))


# CCDIDC-1775
# The following two sections are commented out as they are not currently used.
# These could be useful in the future, so they are left here for reference.
# These were meant to fix issues with empty rows and regex issues in the nodes.
# They haven't fixed the issues that were encountered, so they are not currently used.
# But these might be helpful in the future if similar issues arise.

    # ##############
    # #
    # # Check nodes for extra empty rows that might cause issues
    # #
    # ##############
    # catcherr_logger.info("Removing rows where 'type' is NaN from each node")

    # for node in dict_nodes:
    #     df = meta_dfs[node]
    #     # if there is a row that does not have a value in the column "type", drop that row, and send a warning
    #     if "type" in df.columns:
    #         empty_rows = df[df["type"].isna()]
    #         if not empty_rows.empty:
    #             catcherr_logger.warning(
    #                 f"Node {node} has empty rows that will be removed: {empty_rows.index.tolist()}"
    #             )
    #             print(
    #                 f"Node {node} has empty rows that will be removed: {empty_rows.index.tolist()}",
    #                 file=outf,
    #             )
    #             df = df.dropna(subset=["type"])
    #             meta_dfs[node] = df


    # ##############
    # #
    # # Check nodes for any formatting regex issues, such as \n, \r, \t, multiple spaces or just a space as a value.
    # #
    # ##############
    # catcherr_logger.info("Formatting each node to remove regex issues")

    # for node in dict_nodes:
    #     catcherr_logger.info(f"Formatting node: {node}")
    #     df = meta_dfs[node]
    #     # for each column in the dataframe, replace the regex issues with empty strings
    #     for col in df.columns:
    #         catcherr_logger.info(f"Formatting column: {col}")
    #         # replace \n, \r, \t with a space
    #         df[col] = df[col].replace(r"[\n\r\t]", " ", regex=True)
    #         # replace multiple spaces with a single space
    #         df[col] = df[col].replace(r" {2,}", " ", regex=True)
    #         # replace only a space with an empty string
    #         df[col] = df[col].replace(r"^\s*$", "", regex=True)
    #     meta_dfs[node] = df

    ##############
    #
    # Start Log Printout
    #
    ##############
    catcherr_logger.info("Starting log printout")
    catcherr_out_log = f"{output_file}.txt"
    with open(f"{file_dir_path}/{catcherr_out_log}", "w") as outf:

        ##############
        #
        # Pre-unique check on all nodes
        #
        ##############

        catcherr_logger.info("Checking for unique values in each node")

        for node in dict_nodes:
            df = meta_dfs[node]
            df = df.drop_duplicates(ignore_index=True)
            meta_dfs[node] = df

        ##############
        #
        # Diagnosis clean up, remove '###(#)/# : ' from the diagnosis column
        #
        ##############

        catcherr_logger.info("Cleaning up diagnosis column, removing '###(#)/(#) : ' if present")

        for node in dict_nodes:
            df = meta_dfs[node]
            if "diagnosis" in df.columns:
                df["diagnosis"] = df["diagnosis"].replace(r"^\s*\d+(?:/\d+)?\s*:\s*", "", regex=True)
            meta_dfs[node] = df



        ##############
        #
        # Terms and Value sets checks
        #
        ##############

        catcherr_logger.info("Checking Terms and Value Sets")
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
                "diagnosis_category",
            ]

        # for each tab
        for node in dict_nodes:
            catcherr_logger.info(f"TaVS, Checking node: {node}")
            print(f"\n{node}\n----------", file=outf)
            df = meta_dfs[node]
            properties = df.columns

            # for each property
            for property in properties:
                catcherr_logger.info(f"TaVS, Checking property: {property}")
                tavs_df_prop = tavs_df[tavs_df["Value Set Name"] == property]
                # if the property is in the TaVs data frame
                if len(tavs_df_prop) > 0:
                    # if the property is not completely empty:
                    if not df[property].isna().all():
                        # if the property is an enum
                        if property in enum_arrays:
                            # reorder the array to be in alphabetical order
                            for value_pos in range(0, len(df[property])):
                                value = df[property].iloc[value_pos]
                                if pd.notna(value):
                                    if ";" in value:
                                        value = ";".join(
                                            sorted(
                                                set(value.split(";")),
                                                key=lambda s: s.casefold(),
                                            )
                                        )
                                        df[property][value_pos] = value

                            # obtain a list of value strings
                            unique_values = df[property].dropna().unique()

                            # pull out a complete list of all values in sub-arrays
                            cmplt_unique_values = []
                            for unique_value in unique_values:
                                if ";" in unique_value:
                                    unique_value_list = list(
                                        set(unique_value.split(";"))
                                    )
                                    cmplt_unique_values.extend(unique_value_list)
                                else:
                                    cmplt_unique_values.append(unique_value)
                            unique_values = cmplt_unique_values

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
                                for unique_value in unique_values:
                                    if unique_value not in tavs_df_prop["Term"].dropna().values:
                                        print(
                                            f"\tERROR: {property} property contains a value that is not recognized: {unique_value}",
                                            file=outf,
                                        )
                                        # fix if lower cases match
                                        if (
                                            tavs_df_prop["Term"].dropna().str.lower().values
                                            == unique_value.lower()
                                        ).any():
                                            new_value = tavs_df_prop[
                                                (
                                                    tavs_df_prop["Term"]
                                                    .dropna()
                                                    .str.lower()
                                                    .values
                                                    == unique_value.lower()
                                                )
                                            ]["Term"].values[0]
                                            df[property] = df[property].apply(
                                                lambda x: (
                                                    re.sub(
                                                        rf"\b{unique_value}\b",
                                                        new_value,
                                                        x,
                                                    )
                                                    if (
                                                        np.all(pd.notnull(df[property]))
                                                    )
                                                    else x
                                                )
                                            )

                                            print(
                                                f"\t\tThe value in {property} was changed: {unique_value} ---> {new_value}",
                                                file=outf,
                                            )

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
                                    # for each unique value, check it against the TaVs data frame
                                    for unique_value in unique_values:
                                        if (
                                            unique_value
                                            not in tavs_df_prop["Term"].dropna().values
                                        ):
                                            print(
                                                f"\tERROR: {property} property contains a value that is not recognized: {unique_value}",
                                                file=outf,
                                            )
                                            # fix if lower cases match
                                            if (
                                                tavs_df_prop["Term"].dropna().str.lower().values
                                                == unique_value.lower()
                                            ).any():
                                                new_value = tavs_df_prop[
                                                    (
                                                        tavs_df_prop["Term"]
                                                        .dropna()
                                                        .str.lower()
                                                        .values
                                                        == unique_value.lower()
                                                    )
                                                ]["Term"].values[0]
                                                df[property] = df[property].replace(
                                                    unique_value, new_value
                                                )
                                                print(
                                                    f"\t\tThe value in {property} was changed: {unique_value} ---> {new_value}",
                                                    file=outf,
                                                )

        ##############
        #
        # Check and replace for non-UTF-8 characters
        #
        ##############

        print(
            "\nCertain characters (®, ™, ©) do not handle being transformed into certain file types, due to this, the following characters were changed.\n----------",
            file=outf,
        )


        non_utf_8_array = ["®", "™", "©", "–", "—"]

        non_utf_8_array = "|".join(non_utf_8_array)

        catcherr_logger.info("Checking for non-UTF-8 characters")

        # check each node
        for node in dict_nodes:
            catcherr_logger.info(f"non-UTF-8, checking node: {node}")
            df = meta_dfs[node]
            # for each column
            for col in df.columns:
                # check to see if there are any non-UTF-8 characters in the column
                catcherr_logger.info(f"non-UTF-8, checking column: {col}")
                if df[col].str.contains(non_utf_8_array).any():
                    # only if they have an issue, then print out the node.
                    print(f"\n{node}\n----------", file=outf)
                    rows = np.where(df[col].str.contains(non_utf_8_array))[0]
                    for i in range(0, len(rows)):
                        print(
                            f"\tWARNING: The property, {col}, contained a non-UTF-8 character on row: {rows[i]+1}\n",
                            file=outf,
                        )

            # for each column in the dataframe, replace the non-UTF-8 characters with their UTF-8 equivalents
            df = df.map(
                lambda x: (
                    x.replace("®", "(R)")
                    .replace("™", "(TM)")
                    .replace("©", "(C)")
                    .replace("–", "-")
                    .replace("—", "-")
                    if isinstance(x, str)
                    else x
                )
            )
            meta_dfs[node] = df

        ##############
        #
        # Diagnosis to Diagnosis category transformation
        #
        ##############

        # This section will use information found in the diagnosis.diagnosis property and based on the cross-reference file
        # found on GitHub, it will create the diagnosis category values.

        # directory path to the cross-reference file
        diagnosis_mapping_path = ("docs/uniqDx2Dx_cat_2025_07_15.tsv")

        # read in the cross-reference file
        diagnosis_mapping = pd.read_csv(diagnosis_mapping_path, sep="\t", dtype=str)

        # Create a mapping dictionary
        diagnosis_mapping = diagnosis_mapping.set_index('diagnosis')['diagnosis_category'].to_dict()

        catcherr_logger.info("Transforming diagnosis to diagnosis category")
        for node in dict_nodes:
            df = meta_dfs[node]
            # for the diagnosis node
            if "diagnosis" in df.columns:
                # map the diagnosis column to the diagnosis_category column, based on the dataframe diagnosis_mapping,
                # which has the same column headers found in the df object, diagnosis and diagnosis_category.
                # Only update where diagnosis_category is null
                df['diagnosis_category'] = df.apply(
                    lambda row: diagnosis_mapping.get(row['diagnosis'], 'Not Reported')
                    if pd.isna(row['diagnosis_category']) else row['diagnosis_category'],
                    axis=1
                )
            meta_dfs[node] = df
        
        
        
        ##############
        #
        # Check and replace non-html encoded characters in URLs
        #
        ##############

        catcherr_logger.info("HTML encoding fixing")

        print(
            "\nCertain characters (comma, space) do not handle being used in HTML, due to this, the following characters were changed.\n----------",
            file=outf,
        )

        non_html_array = [" ", ",", "#"]

        non_html_array = "|".join(non_html_array)

        for node in dict_nodes:
            # for a column called file_url
            if "file_url" in meta_dfs[node].columns:
                catcherr_logger.info(f"non-html encoding, checking node: {node}")
                df = meta_dfs[node]
                # check for any of the values in the array
                if df["file_url"].str.contains(non_html_array).any():
                    # only if they have an issue, then print out the node.
                    print(f"\n{node}\n----------", file=outf)
                    rows = np.where(df["file_url"].str.contains(non_html_array))[0]
                    for i in range(0, len(rows)):
                        print(
                            f"\tWARNING: The url contained a non-HTML encoded character on row and was fixed: {rows[i]+1}\n",
                            file=outf,
                        )
                df["file_url"] = df["file_url"].map(
                    lambda x: (
                        x.replace(" ", "%20").replace(",", "%2C").replace("#", "%23")
                        if isinstance(x, str)
                        else x
                    )
                )
                meta_dfs[node] = df

        ##############
        #
        # File_access check and ACL/authz creation
        #
        ##############

        catcherr_logger.info("File access checks and ACL/authz creation")

        print(
            "\nThe following section will check the file_access values, and create derived values for acl and authz.\n----------",
            file=outf,
        )

        # pull consent number and dbgap accession from study node
        # This is ASSUMING that the study is only ONE consent number.
        # THIS IS A QUICK FIX AND SHOULD BE REWORKED IN THE FUTURE.
        # THIS WILL BREAK ON MULTI-CONSENT STUDIES.
        consent_number = meta_dfs["consent_group"]["consent_group_suffix"][0]
        dbgap_accession = meta_dfs["study"]["dbgap_accession"][0]

        # check each node to find the acl property (it has been in study and study_admin)
        for node in dict_nodes:
            if "file_access" in meta_dfs[node].columns:
                catcherr_logger.info(f"ACL/Authz, checking node: {node}")
                df = meta_dfs[node]
                acl_value = f"['{dbgap_accession}.{consent_number}']"
                authz_value = f"['/programs/{dbgap_accession}.{consent_number}']"

                # for each row, determine if the ACL is properly formed and fix otherwise
                for index, row in df.iterrows():
                    file_access_value = df.at[index, "file_access"]

                    if file_access_value == "Open":
                        df.at[index, "acl"] = "['*']"
                        df.at[index, "authz"] = "['/open']"

                    elif file_access_value == "Controlled":
                        df.at[index, "acl"] = acl_value
                        df.at[index, "authz"] = authz_value

                    else:
                        print(
                            f"\tERROR: The value for file_access is missing for {node} node at row {index +1}.\n",
                            file=outf,
                        )

                meta_dfs[node] = df

        print(
            "\nFile access checks and value creation complete.\n",
            file=outf,
        )

        ##############
        #
        # File_mapping_level check
        #
        ##############
        catcherr_logger.info("File mapping level checks and value creation")

        print(
            "\nThe following section will check the file_mapping_level (fml) values, and create values when blank.\n----------",
            file=outf,
        )

        # check each node to find the acl property (it has been in study and study_admin)
        for node in dict_nodes:
            if "file_mapping_level" in meta_dfs[node].columns:
                catcherr_logger.info(f"file mapping level, checking node: {node}")
                df = meta_dfs[node]

                # empty rows
                error_index = df.index[pd.isna(df["file_mapping_level"])].tolist()

                # for each row, determine if the fml value is present and if not, determine the value
                for index, row in df.iterrows():
                    fml_value = df.at[index, "file_mapping_level"]

                    if pd.isna(fml_value):
                        for column in df.columns:
                            if "." in column and pd.notna(row[column]):
                                key_name = column
                                fml_value = str.split(key_name, sep=".")[0].capitalize()
                                df.at[index, "file_mapping_level"] = fml_value

                meta_dfs[node] = df
                print(
                    f"\n\t{node}\n\t----------\n\t\t{error_index}",
                    file=outf,
                )

        print(
            "\nFile mapping level checks and value creation complete.\n",
            file=outf,
        )

        ##############
        #
        # Fix URL paths
        #
        ##############

        catcherr_logger.info("Fixing file urls")

        print(
            "\nCheck the following url columns (file_url), to make sure the full file url is present and fix entries that are not:\n----------\n\nWARNING: If you are seeing a large number of 'ERROR: There is an unresolvable issue...', it is likely there are two or more buckets and this is the script trying and failing at checks against the other bucket for the file.",
            file=outf,
        )

        # check each node
        for node in dict_nodes:
            # for a column called file_url
            if "file_url" in meta_dfs[node].columns:
                catcherr_logger.info(f"Fix url paths, checking node: {node}")
                df = meta_dfs[node]

                # revert HTML code changes that might exist so that it can be handled with correct AWS calls
                # this is then reverted after this section, which allows for this check to be made multiple times against the same file.

                df["file_url"] = df["file_url"].map(
                    lambda x: (
                        x.replace("%20", " ").replace("%2C", ",").replace("%23", "#")
                        if isinstance(x, str)
                        else x
                    )
                )

                print(f"{node}\n----------", file=outf)

                # discover all possible base bucket urls in the file node

                node_all_urls = df["file_url"].dropna()
                node_urls = pd.DataFrame(node_all_urls)

                node_urls["bucket"] = node_urls["file_url"].apply(
                    lambda x: x.split("/")[2]
                )

                node_urls = node_urls["bucket"].unique().tolist()

                # for each possible bucket based on the base urls in file_url
                # go through and see if the values for the url can be filled in based on file_name and size
                if len(node_urls) > 0:
                    for node_url in node_urls:
                        # create a blank list for bad url_locations
                        bad_url_locs = []

                        # pull bucket metadata

                        # Get s3 session setup
                        s3_client = set_s3_session_client()

                        # initialize file metadata from bucket
                        s3_file_path = []
                        s3_file_name = []
                        s3_file_size = []

                        # try and see if the bucket exists, if it does, obtain the metadata from it
                        try:
                            s3_client.head_bucket(Bucket=node_url)

                            # create a paginator to itterate through each 1000 objs
                            paginator = s3_client.get_paginator("list_objects_v2")
                            response_iterator = paginator.paginate(Bucket=node_url)

                            # pull out each response and obtain file name and size
                            for response in response_iterator:
                                if "Contents" in response:
                                    for obj in response["Contents"]:
                                        s3_file_path.append(
                                            "s3://" + node_url + "/" + obj["Key"]
                                        )
                                        s3_file_name.append(
                                            os.path.basename(obj["Key"])
                                        )
                                        s3_file_size.append(obj["Size"])

                        except ClientError as e:
                            if e.response["Error"]["Code"] == "404":
                                print(
                                    f"\tThe following bucket either does not exist or you do not have read access for it: {node_url}",
                                    file=outf,
                                )

                    # create a metadata data frame from the bucket
                    df_bucket = pd.DataFrame(
                        {
                            "file_path": s3_file_path,
                            "file_name": s3_file_name,
                            "file_size": s3_file_size,
                        }
                    )

                    # find bad url locs based on the full file path and whether it can be found in the url bucket manifest.
                    bad_url_locs = df["file_url"].isin(df_bucket["file_path"])

                    # Go through each bad location and determine if the correct url location can be determined on file_name and file_size.
                    for loc in range(len(bad_url_locs)):
                        # if the value is bad then fix
                        if not bad_url_locs.iloc[loc]:
                            file_name_find = df["file_name"][loc]
                            file_size_find = df["file_size"][loc]

                            # filter the bucket df to see if there is exactly one file value that matches both name and file size
                            filtered_df = df_bucket[
                                df_bucket["file_name"] == file_name_find
                            ]
                            filtered_df = filtered_df[
                                filtered_df["file_size"] == int(file_size_find)
                            ]

                            if len(filtered_df) == 1:
                                # output of url change

                                print(
                                    f"\tWARNING: The file location for the file, {file_name_find}, has been changed:",
                                    file=outf,
                                )
                                print(
                                    f"\t\t{df['file_url'][loc]} ---> {filtered_df['file_path'].values[0]}",
                                    file=outf,
                                )

                                df["file_url"][loc] = filtered_df["file_path"].values[0]

                            else:
                                print(
                                    f"\tERROR: There is an unresolvable issue with the file url for file: {file_name_find}",
                                    file=outf,
                                )

                    # write back to the meta_dfs list
                    meta_dfs[node] = df

                else:
                    print(
                        "ERROR: There is not a bucket associated with this node's files.",
                        file=outf,
                    )

        ##############
        #
        # Reapply HTML encoding changes without throwing errors after AWS check.
        #
        ##############

        catcherr_logger.info("Reapplying HTML encoding changes")

        for node in dict_nodes:
            # for a column called file_url
            if "file_url" in meta_dfs[node].columns:
                df = meta_dfs[node]
                # check for any of the values in the array and make changes.
                df["file_url"] = df["file_url"].map(
                    lambda x: (
                        x.replace(" ", "%20").replace(",", "%2C").replace("#", "%23")
                        if isinstance(x, str)
                        else x
                    )
                )
                meta_dfs[node] = df

        ##############
        #
        # Assign guids to files
        #
        ##############

        catcherr_logger.info("Assigning GUIDs to files")

        catcherr_logger.info(
            "The file based nodes will now have a guid assigned to each unique file"
        )

        # check each node
        for node in dict_nodes:
            catcherr_logger.info(f"Checking node: {node}")
            # if file_url exists in the node
            if "file_url" in meta_dfs[node].columns:
                catcherr_logger.info(f"GUID application, checking node: {node}")
                df = meta_dfs[node]
                # identify posistions without guids
                no_guids = df["dcf_indexd_guid"].isna()
                if no_guids.any():
                    # apply guids to files that don't have guids
                    new_guids = (
                        df[no_guids]
                        .groupby(["file_url", "md5sum"])
                        .apply(lambda x: "dg.4DFC/" + str(uuid.uuid4()))
                        .reset_index()
                        .rename(columns={0: "dcf_indexd_guid"})
                    )
                    # merge the new UUIDs back into the original dataframe but not via merge as it replaces one version over another
                    for row in range(0, len(new_guids)):
                        fuic_value = new_guids.loc[row].file_url
                        md5_value = new_guids.loc[row].md5sum
                        dig_value = new_guids.loc[row].dcf_indexd_guid

                        # locate the row position via file_url and md5sum values and then apply the guid
                        df.loc[
                            (df["file_url"] == fuic_value)
                            & (df["md5sum"] == md5_value),
                            "dcf_indexd_guid",
                        ] = dig_value

    ##############
    #
    # Replace any NaN with "" before writing output
    #
    ##############

    catcherr_logger.info("Replacing NaN values with empty strings")

    for node in dict_nodes:
        df = meta_dfs[node]
        df = df.fillna("")
        df = df.drop_duplicates(ignore_index=True)
        meta_dfs[node] = df

    ##############
    #
    # Replace any no-break space before writing output
    #
    ##############

    catcherr_logger.info("Replacing no-break spaces with regular spaces")

    def replace_no_break_space(meta_dfs: dict, dict_nodes: list[str]) -> dict:
        for node in dict_nodes:
            node_df = meta_dfs[node]
            node_df_str_cols = [
                col for col, dt in node_df.dtypes.items() if dt == object
            ]
            for col_i in node_df_str_cols:
                node_df[col_i] = node_df[col_i].str.replace("\u00A0", " ")
            meta_dfs[node] = node_df
        return meta_dfs

    meta_dfs = replace_no_break_space(meta_dfs=meta_dfs, dict_nodes=dict_nodes)

    ##############
    #
    # Write out
    #
    ##############
    catcherr_logger.info("Reordering dataframes to match template")

    def reorder_dataframe(dataframe, column_list: list, sheet_name: str, logger):
        reordered_df = pd.DataFrame(columns=column_list)
        for i in column_list:
            if i in dataframe.columns:
                reordered_df[i] = dataframe[i].tolist()
            else:
                logger.warning(f"Column {i} in sheet {sheet_name} was left empty")
        return reordered_df

    catcherr_logger.info("Writing out the CatchERR using pd.ExcelWriter")
    # save out template
    catcherr_out_file = f"{output_file}.xlsx"
    copy(src=template_path, dst=catcherr_out_file)
    with pd.ExcelWriter(
        catcherr_out_file, mode="a", engine="openpyxl", if_sheet_exists="overlay"
    ) as writer:
        # for each sheet df
        for sheet_name in meta_dfs.keys():
            sheet_df = meta_dfs[sheet_name]
            sheet_df_col = sheet_df.columns.tolist()
            template_sheet_df = pd.read_excel(template_path, sheet_name=sheet_name)
            template_sheet_col = template_sheet_df.columns.tolist()
            if sheet_df_col != template_sheet_col:
                sheet_df = reorder_dataframe(
                    dataframe=sheet_df,
                    column_list=template_sheet_col,
                    sheet_name=sheet_name,
                    logger=catcherr_logger,
                )
            else:
                pass
            sheet_df.to_excel(
                writer, sheet_name=sheet_name, index=False, header=False, startrow=1
            )

    catcherr_logger.info(
        f"Process Complete. The output file can be found here: {file_dir_path}/{catcherr_out_file}"
    )

    return (catcherr_out_file, catcherr_out_log)
