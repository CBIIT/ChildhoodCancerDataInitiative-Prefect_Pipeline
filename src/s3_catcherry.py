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

    def determine_file_name(file_path):
        file_name = os.path.splitext(os.path.split(os.path.relpath(file_path))[1])[0]
        file_ext = os.path.splitext(file_path)[1]
        file_dir_path = os.path.split(os.path.relpath(file_path))[0]

        if file_dir_path == "":
            file_dir_path = "."

        return file_name, file_ext, file_dir_path

    def refresh_date():
        today = date.today()
        today = today.strftime("%Y%m%d")
        return today

    def create_output_file(file_name, todays_date):
        return f"{file_name}_CatchERR{todays_date}"

    def read_xlsx(file_path, sheet):
        warnings.simplefilter(action="ignore", category=UserWarning)
        return pd.read_excel(file_path, sheet, dtype="string")

    def read_template(template_path):
        xlsx_model = pd.ExcelFile(template_path)
        model_dfs = {}
        for sheet_name in xlsx_model.sheet_names:
            model_dfs[sheet_name] = read_xlsx(xlsx_model, sheet_name)
        return model_dfs

    def remove_empty_tabs(meta_dfs):
        dict_nodes = set(list(meta_dfs.keys()))
        for node in dict_nodes:
            test_df = meta_dfs[node].drop("type", axis=1).dropna(how="all").dropna(how="all", axis=1)
            if test_df.empty:
                del meta_dfs[node]
        return meta_dfs

#    def log_printout(output_file, file_dir_path, catcherr_out_log, dict_nodes, meta_dfs, tavs_df, dict_df):
#        catcherr_out_log_path = f"{file_dir_path}/{catcherr_out_log}"
#        with open(catcherr_out_log_path, "w") as outf:
#            # Terms and Value sets checks
#           print("The following columns have controlled vocabulary on the 'Terms and Value Sets' page of the template file. If the values present do not match, they will be noted and, in some cases, the values will be replaced:\n----------", file=outf)
#            # ... (remaining log_printout code)

    def process_property(property, df, tavs_df_prop, enum_arrays, outf):
        if property in enum_arrays:
            process_enum_property(property, df, tavs_df_prop, outf)
        else:
            process_non_enum_property(property, df, tavs_df_prop, outf)

    def process_enum_property(property, df, tavs_df_prop, outf):
        for value_pos in range(len(df[property])):
            value = df[property][value_pos]
            if pd.notna(value):
                if ";" in value:
                    value = ";".join(sorted(set(value.split(";")), key=lambda s: s.casefold()))
                    df[property][value_pos] = value
    
        unique_values = df[property].dropna().unique()
        unique_values = process_sub_arrays(unique_values)
    
        if set(unique_values).issubset(set(tavs_df_prop["Term"])):
            print(f"\tPASS: {property}, property contains all valid values.", file=outf)
        else:
            process_invalid_values(property, unique_values, tavs_df_prop, df, outf)
    
    def process_non_enum_property(property, df, tavs_df_prop, outf):
        unique_values = df[property].dropna().unique()
    
        if set(unique_values).issubset(set(tavs_df_prop["Term"])):
            print(f"\tPASS: {property}, property contains all valid values.", file=outf)
        else:
            process_invalid_values(property, unique_values, tavs_df_prop, df, outf)
    
    def process_invalid_values(property, unique_values, tavs_df_prop, df, outf):
        for unique_value in unique_values:
            if unique_value not in tavs_df_prop["Term"].values:
                print(f"\tERROR: {property} property contains a value that is not recognized: {unique_value}", file=outf)
                if (tavs_df_prop["Term"].str.lower().values == unique_value.lower()).any():
                    new_value = tavs_df_prop[
                        (tavs_df_prop["Term"].str.lower().values == unique_value.lower())
                    ]["Term"].values[0]
                    df[property] = df[property].apply(
                        lambda x: re.sub(rf"\b{unique_value}\b", new_value, x)
                        if (np.all(pd.notnull(df[property])))
                        else x
                    )
                    print(f"\t\tThe value in {property} was changed: {unique_value} ---> {new_value}", file=outf)
    
    def process_sub_arrays(unique_values):
        for unique_value in unique_values:
            if ";" in unique_value:
                unique_value_pos = np.where(unique_values == unique_value)[0][0]
                unique_values = np.delete(unique_values, unique_value_pos)
                unique_value = list(set(unique_value.split(";")))
                for value in unique_value:
                    unique_values = np.append(unique_values, value)
        return list(set(unique_values))
    
    def check_terms_and_value_sets(meta_dfs, dict_nodes, tavs_df, dict_df, outf):
        print("The following columns have controlled vocabulary on the 'Terms and Value Sets' page of the template file. If the values present do not match, they will be noted and in some cases, the values will be replaced:\n----------", file=outf)
    
        if any(dict_df["Type"].str.contains("array")):
            enum_arrays = dict_df[dict_df["Type"].str.contains("array")]["Property"].tolist()
        else:
            enum_arrays = ["therapeutic_agents", "treatment_type", "study_data_types", "morphology", "primary_site", "race"]
    
        for node in dict_nodes:
            print(f"\n{node}\n----------", file=outf)
            df = meta_dfs[node]
            properties = df.columns
    
            for property in properties:
                tavs_df_prop = tavs_df[tavs_df["Value Set Name"] == property]
    
                if len(tavs_df_prop) > 0 and not df[property].isna().all():
                    process_property(property, df, tavs_df_prop, enum_arrays, outf)

    def check_and_replace_non_utf_8_characters(meta_dfs, dict_nodes, outf):
        print("\nCertain characters (®, ™, ©) do not handle being transformed into certain file types, due to this, the following characters were changed.\n----------", file=outf)
    
        non_utf_8_characters = ["®", "™", "©"]
        non_utf_8_pattern = "|".join(non_utf_8_characters)
    
        for node in dict_nodes:
            df = meta_dfs[node]
    
            for col in df.columns:
                if df[col].str.contains(non_utf_8_pattern).any():
                    print(f"\n{node}\n----------", file=outf)
                    rows_with_issue = np.where(df[col].str.contains(non_utf_8_pattern))[0]
                    for row in rows_with_issue:
                        print(f"\tWARNING: The property, {col}, contained a non-UTF-8 character on row: {row + 1}\n", file=outf)
    
            df = df.applymap(lambda x: x.replace("®", "(R)").replace("™", "(TM)").replace("©", "(C)") if isinstance(x, str) else x)
            meta_dfs[node] = df


    def acl_pattern_check(meta_dfs, dict_nodes, outf):
        print("\nThe value for ACL will be checked to determine if it follows the required structure, ['.*'].\n----------", file=outf)
    
        for node in dict_nodes:
            if "acl" in meta_dfs[node].columns:
                df = meta_dfs[node]
                acl_values = df["acl"]
    
                if len(acl_values) > 1:
                    print(
                        f"\tERROR: There is more than one ACL associated with this study and workbook. Please only submit one ACL and corresponding data to a workbook.\n",
                        file=outf,
                    )
                elif len(acl_values) == 1:
                    acl_value = acl_values[0]
                    if pd.isna(acl_value):
                        print(
                            f"\tERROR: Please submit an ACL value to the 'acl' property in the {node} node.\n",
                            file=outf,
                        )
                    elif not pd.isna(acl_value):
                        acl_test = acl_value.startswith("['") and acl_value.endswith("']")
                        if acl_test:
                            print(
                                f"\tThe ACL found in the {node} node matches the required structure: {acl_value}",
                                file=outf,
                            )
                        else:
                            fix_and_print_acl(df, acl_value, node, outf)
                else:
                    print(
                        f"\tERROR: Something is wrong with the ACL value submitted in the {node} node.\n",
                        file=outf,
                    )
    
    def fix_and_print_acl(df, acl_value, node, outf):
        acl_fix = f"['{acl_value}']"
        df["acl"] = acl_fix
        print(
            f"\tThe ACL found in the {node} node does not match the required structure, it will be changed:",
            file=outf,
        )
        print(f"\t\t{acl_value} ---> {acl_fix}", file=outf)


    
    def get_s3_file_metadata(s3_client, node_url):
        try:
            s3_client.head_bucket(Bucket=node_url)
    
            paginator = s3_client.get_paginator("list_objects_v2")
            response_iterator = paginator.paginate(Bucket=node_url)
    
            s3_file_path = []
            s3_file_name = []
            s3_file_size = []
    
            for response in response_iterator:
                if "Contents" in response:
                    for obj in response["Contents"]:
                        s3_file_path.append("s3://" + node_url + "/" + obj["Key"])
                        s3_file_name.append(os.path.basename(obj["Key"]))
                        s3_file_size.append(obj["Size"])
    
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                print(f"\tThe following bucket either does not exist or you do not have read access for it: {node_url}", file=outf)
    
        return pd.DataFrame({"file_path": s3_file_path, "file_name": s3_file_name, "file_size": s3_file_size})

    def fix_url_paths(meta_dfs, dict_nodes, outf):
        print("\nCheck the following URL columns (file_url_in_cds) to ensure the full file URL is present and fix entries that are not:\n----------\n\nWARNING: If you are seeing a large number of 'ERROR: There is an unresolvable issue...', it is likely there are two or more buckets, and this is the script trying and failing at checks against the other bucket for the file.", file=outf)
    
        for node in dict_nodes:
            if "file_url_in_cds" in meta_dfs[node].columns:
                df = meta_dfs[node]
                print(f"{node}\n----------", file=outf)
    
                node_all_urls = df["file_url_in_cds"].dropna()
                node_urls = pd.DataFrame(node_all_urls)
    
                node_urls["bucket"] = node_urls["file_url_in_cds"].apply(lambda x: x.split("/")[2])
    
                node_urls = node_urls["bucket"].unique().tolist()
    
                if len(node_urls) > 0:
                    for node_url in node_urls:
                        bad_url_locs = []
    
                        s3_client = set_s3_session_client()
                        df_bucket = get_s3_file_metadata(s3_client, node_url)
    
                        bad_url_locs = df["file_url_in_cds"].isin(df_bucket["file_path"])
    
                        for loc in range(len(bad_url_locs)):
                            if not bad_url_locs[loc]:
                                file_name_find = df["file_name"][loc]
                                file_size_find = df["file_size"][loc]
    
                                filtered_df = df_bucket[df_bucket["file_name"] == file_name_find]
                                filtered_df = filtered_df[filtered_df["file_size"] == int(file_size_find)]
    
                                if len(filtered_df) == 1:
                                    print(f"\tWARNING: The file location for the file, {file_name_find}, has been changed:", file=outf)
                                    print(f"\t\t{df['file_url_in_cds'][loc]} ---> {filtered_df['file_path'].values[0]}", file=outf)
                                    df["file_url_in_cds"][loc] = filtered_df["file_path"].values[0]
                                else:
                                    print(f"\tERROR: There is an unresolvable issue with the file URL for file: {file_name_find}", file=outf)
    
                        meta_dfs[node] = df
                else:
                    print("ERROR: There is not a bucket associated with this node's files.", file=outf)



    def assign_guids_to_files(meta_dfs, dict_nodes, catcherr_logger):
    catcherr_logger.info("The file-based nodes will now have a GUID assigned to each unique file")

        for node in dict_nodes:
            if "file_url_in_cds" in meta_dfs[node].columns:
                df = meta_dfs[node]
    
                # Identify positions without GUIDs
                no_guids = df["dcf_indexd_guid"].isna()
    
                if no_guids.any():
                    # Apply GUIDs to files that don't have GUIDs
                    new_guids = assign_guids(df[no_guids])
    
                    # Merge the new UUIDs back into the original dataframe
                    for row in range(len(new_guids)):
                        fuic_value, md5_value, dig_value = new_guids.loc[row]
    
                        # Locate the row position via file_url and md5sum values and then apply the GUID
                        df.loc[
                            (df["file_url_in_cds"] == fuic_value)
                            & (df["md5sum"] == md5_value),
                            "dcf_indexd_guid",
                        ] = dig_value
    
                    # Update the meta_dfs list with the modified dataframe
                    meta_dfs[node] = df

    def assign_guids(df):
        return (
            df.groupby(["file_url_in_cds", "md5sum"])
            .apply(lambda x: "dg.4DFC/" + str(uuid.uuid4()))
            .reset_index(name="dcf_indexd_guid")
        )


    def replace_nan(meta_dfs, dict_nodes):
        for node in dict_nodes:
            df = meta_dfs[node]
            df = df.fillna("")
            df = df.drop_duplicates()
            meta_dfs[node] = df
        return meta_dfs

    def write_out(catcherr_out_file, template_path, meta_dfs):
        catcherr_logger.info("Writing out the CatchERR using pd.ExcelWriter")
        copy(src=template_path, dst=catcherr_out_file)
        with pd.ExcelWriter(catcherr_out_file, mode="a", engine="openpyxl", if_sheet_exists="overlay") as writer:
            for sheet_name in meta_dfs.keys():
                sheet_df = meta_dfs[sheet_name]
                sheet_df.to_excel(writer, sheet_name=sheet_name, index=False, header=True)
        catcherr_logger.info(f"Process Complete. The output file can be found here: {file_dir_path}/{catcherr_out_file}")

    # Main function logic
    file_name, _, file_dir_path = determine_file_name(file_path)
    todays_date = refresh_date()
    output_file = create_output_file(file_name, todays_date)
    
    catcherr_logger.info("Reading CCDI template file")
    model_dfs = read_template(template_path)
    tavs_df = model_dfs["Terms and Value Sets"]
    dict_df = model_dfs["Dictionary"]

    catcherr_logger.info("Reading CCDI manifest file")
    xlsx_data = pd.ExcelFile(file_path)
    meta_dfs = {sheet_name: read_xlsx(xlsx_data, sheet_name) for sheet_name in xlsx_data.sheet_names}
    xlsx_data.close()

    dict_nodes = remove_empty_tabs(meta_dfs)

    log_printout(output_file, file_dir_path, f"{output_file}.txt", dict_nodes, meta_dfs, tavs_df, dict_df)
    fix_url_paths(meta_dfs, dict_nodes)
    assign_guids(meta_dfs, dict_nodes)
    meta_dfs = replace_nan(meta_dfs, dict_nodes)

    catcherr_out_file = f"{output_file}.xlsx"
    write_out(catcherr_out_file, template_path, meta_dfs)

    return (catcherr_out_file, f"{output_file}.txt")
