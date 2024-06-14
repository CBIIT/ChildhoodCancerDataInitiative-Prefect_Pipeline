from prefect import flow, task, get_run_logger
from src.utils import CheckCCDI, get_date, get_logger
from enum import Enum
from typing import TypeVar
import os
import pandas as pd
import numpy as np
from shutil import copy

DataFrame = TypeVar("DataFrame")


@task(name="version validation", log_prints=True)
def tags_validation(manifest_path: str, tag: str, logger) -> bool:
    """Test if manifest_path equals to tag value"""
    manifest_object = CheckCCDI(ccdi_manifest=manifest_path)
    manifest_version = manifest_object.get_version()
    logger.info(f"Version in manifest {manifest_path}: {manifest_version}")
    if manifest_version == tag:
        return True
    else:
        return False


@task(name="extract tags in mapping file", log_prints=True)
def liftover_tags(liftover_mapping_path: str) -> tuple:
    """Extract lift from tag and lift to tag from the mapping file

    Only one unique tag is expected in column lift_from_version and
    lift_to_version columns, respectively
    """
    mapping_df = pd.read_csv(liftover_mapping_path, sep="\t")
    lift_from = mapping_df["lift_from_version"].dropna().unique().tolist()
    lift_to = mapping_df["lift_to_version"].dropna().unique().tolist()
    if len(lift_from) > 1:
        print(
            f"More than one lift from versions were found in mapping file: {*lift_from,}"
        )
        raise ValueError(
            f"More than one lift from versions were found in mapping file: {*lift_from,}"
        )
    else:
        pass
    if len(lift_to) > 1:
        print(
            f"More than one lift to versions were found in mapping file: {*lift_to,}"
        )
        raise ValueError(
            f"More than one lift to versions were found in mapping file: {*lift_to,}"
        )
    else:
        pass
    return lift_from[0], lift_to[0]


@task(name="mapping file coverage check")
def mapping_coverage(
    mapping_df: DataFrame, node_colname: str, prop_colname: str, manifest_object
) -> str:
    """Find any property in the model but not found in the mapping file
    Find any property in the mapping file but not found in the model
    """
    sheet_list = manifest_object.get_sheetnames()
    sheet_list = [
        i
        for i in sheet_list
        if i not in ["README and INSTRUCTIONS", "Dictionary", "Terms and Value Sets"]
    ]
    missing_prop = []
    extra_prop = []
    for i in sheet_list:
        i_df = manifest_object.read_sheet_na(sheetname=i)
        i_df_cols = i_df.columns.tolist()
        # remove type, and any column ends with ".id"
        i_df_cols_short = [
            j for j in i_df_cols if not (j == "type") and not (j.endswith(".id"))
        ]
        i_mappped_prop = mapping_df[mapping_df[node_colname] == i][
            prop_colname
        ].tolist()
        i_missing = [k for k in i_df_cols_short if k not in i_mappped_prop]
        if len(i_missing) > 0:
            for l in i_missing:
                missing_prop.append({"node": i, "prop": l})
        else:
            pass
        i_extra = [m for m in i_mappped_prop if m not in i_df_cols_short]
        if len(i_extra) > 0:
            for n in i_extra:
                extra_prop.append({"node": i, "prop": n})
        else:
            pass
    missing_prop_df = pd.DataFrame.from_records(missing_prop, columns=["node", "prop"])
    extra_prop_df = pd.DataFrame.from_records(extra_prop, columns=["node", "prop"])
    return missing_prop_df, extra_prop_df


@task(name="find unmapped props", log_prints=True)
def evaluate_mapping_props(mapping_df: DataFrame, mapping_col_dict: dict) -> tuple:
    """Find unmapped properties between two models
    Find many to one and one to many properties between two models
    """
    mapping_from_cols = mapping_col_dict["lift_from"]
    mapping_to_cols = mapping_col_dict["lift_to"]

    # find props not mapped to
    empty_in_template_rows = mapping_df[mapping_to_cols].isna().all(axis=1)
    manifest_unmapped_df = mapping_df.loc[
        empty_in_template_rows,
        ["lift_from_version", "lift_from_node", "lift_from_property"],
    ]
    # only manifest unmapped property need to pay attention to unmapped linking property
    # especially for nodes which have fewer number of parent nodes in the newer model
    manifest_unmapped_df["If_lose_links"] = [
        "YES" if ("." in i) and (i.endswith("_id")) else ""
        for i in manifest_unmapped_df["lift_from_property"].tolist()
    ]

    # find props not mapped from
    empty_in_manifest_rows = mapping_df[mapping_from_cols].isna().all(axis=1)
    template_unmapped_df = mapping_df.loc[
        empty_in_manifest_rows, ["lift_to_version", "lift_to_node", "lift_to_property"]
    ]
    # template unmapped property need will have no value
    template_unmapped_df["if_links"] = [
        "YES" if ("." in i) and (i.endswith("_id")) else ""
        for i in template_unmapped_df["lift_to_property"].tolist()
    ]

    # find if multiple template props maps to manifest prop
    manifest_props_counts_df = (
        mapping_df.dropna(
            subset=["lift_from_version", "lift_from_node", "lift_from_property"],
            how="all",
        )
        .groupby(["lift_from_node", "lift_from_property"])
        .size()
        .reset_index(name="count")
    )
    manifest_props_multiple_df = manifest_props_counts_df[
        manifest_props_counts_df["count"] > 1
    ]
    manifest_props_multiple_summary = mapping_df.merge(
        manifest_props_multiple_df, on=["lift_from_node", "lift_from_property"]
    ).drop(columns=["count"])

    # find if multiple manifest props maps to template prop
    template_props_counts_df = (
        mapping_df.dropna(
            subset=["lift_to_version", "lift_to_node", "lift_to_property"],
            how="all",
        )
        .groupby(["lift_to_node", "lift_to_property"])
        .size()
        .reset_index(name="count")
    )
    template_props_multiple_df = template_props_counts_df[
        template_props_counts_df["count"] > 1
    ]
    template_props_multiple_summary = mapping_df.merge(
        template_props_multiple_df, on=["lift_to_node", "lift_to_property"]
    ).drop(columns=["count"])

    return (
        manifest_unmapped_df,
        template_unmapped_df,
        manifest_props_multiple_summary,
        template_props_multiple_summary,
    )


def multiple_mapping_summary_cleanup(
    df: DataFrame, manifest_version: str, template_version: str
) -> DataFrame:
    """Rename a mapping df with name column names
    """
    df.rename(
        columns={
            "lift_from_node": f"{manifest_version}_node",
            "lift_from_property": f"{manifest_version}_property",
            "lift_to_node": f"{template_version}_node",
            "lift_to_property": f"{template_version}_property",
        },
        inplace=True,
    )
    df.drop(columns=["lift_from_version", "lift_to_version"], inplace=True)
    return df


@flow(name="validate mapping", log_prints=True)
def validate_mapping(manifest_path: str, template_path: str, mapping_path: str) -> None:
    """Validate if the mapping file includes all properties found in the manifest

    The mapping file excludes properties type, id.*
    """
    mapping_col_dict = {
        "lift_to": ["lift_to_version", "lift_to_node", "lift_to_property"],
        "lift_from": ["lift_from_version", "lift_from_node", "lift_from_property"],
    }
    todaydate = get_date()

    mapping_df = pd.read_csv(mapping_path, sep="\t")
    mapping_from_df = mapping_df[mapping_col_dict["lift_from"]]
    mapping_to_df = mapping_df[mapping_col_dict["lift_to"]]

    manifest_object = CheckCCDI(ccdi_manifest=manifest_path)
    manifest_version = manifest_object.get_version()
    template_object = CheckCCDI(ccdi_manifest=template_path)
    template_version = template_object.get_version()

    mapping_report = f"mapping_validation_report_{todaydate}.txt"

    # check if the propreties in manifest xlsx can be found in mapping file
    manifest_mapping_missing, manifest_mapping_extra = mapping_coverage(
        mapping_df=mapping_from_df,
        node_colname="lift_from_node",
        prop_colname="lift_from_property",
        manifest_object=manifest_object,
    )
    template_mapping_missing, template_mapping_extra = mapping_coverage(
        mapping_df=mapping_to_df,
        node_colname="lift_to_node",
        prop_colname="lift_to_property",
        manifest_object=template_object,
    )

    # checking mapping file itself (if multiple props from old version are mapped to new version, vice versa)
    (
        manifest_unmapped_df,
        template_unmapped_df,
        manifest_props_multiple_summary,
        template_props_multiple_summary,
    ) = evaluate_mapping_props(
        mapping_df=mapping_df, mapping_col_dict=mapping_col_dict
    )

    with open(mapping_report, "w") as report_file:
        report_file.write(
            f"Manifest file: {manifest_path}\nTemplate file: {template_path}\nMapping file: {mapping_path}\n\n\n"
        )

        # manifest file against mapping
        report_file.write(
            f"If the mapping file misses any proprety in the manifest\n{manifest_path}\n\n"
        )
        report_file.write(
            manifest_mapping_missing.to_markdown(index=False, tablefmt="rounded_grid")
            + "\n\n"
        )
        report_file.write(
            f"If the mapping file contains any extra proprety in the manifest\n{manifest_path}\n\n"
        )
        report_file.write(
            manifest_mapping_extra.to_markdown(index=False, tablefmt="rounded_grid")
            + "\n\n"
        )

        # template file against mapping
        report_file.write(
            f"If the mapping file misses any proprety in the template\n{template_path}\n\n"
        )
        report_file.write(
            template_mapping_missing.to_markdown(index=False, tablefmt="rounded_grid")
            + "\n\n"
        )
        report_file.write(
            f"If the mapping file contains any extra proprety in the template\n{template_path}\n\n"
        )
        report_file.write(
            template_mapping_extra.to_markdown(index=False, tablefmt="rounded_grid")
            + "\n\n"
        )

        # evaluate mapping file
        report_file.write(
            f"Properties in {manifest_version} model that are unmapped in the {template_version} model\nUnmapped propreties wouldn't be lifted over\nWARNING: Unmapped linking properties will LOSE LINKS between nodes\n\n"
        )
        report_file.write(
            manifest_unmapped_df.to_markdown(index=False, tablefmt="rounded_grid")
            + "\n\n"
        )
        report_file.write(
            f"Properties in {template_version} model that are unmapped in the {manifest_version} model\nWARNING: Unmapped linking properties will have no links in the final liftover output because this is a new linkage compared to lift_from data model\n\n"
        )
        report_file.write(
            template_unmapped_df.to_markdown(index=False, tablefmt="rounded_grid")
            + "\n\n"
        )
        report_file.write(
            f"Multiple props in {template_version} model mapped to {manifest_version}\n\n"
        )
        manifest_props_multiple_summary = multiple_mapping_summary_cleanup(
            df=manifest_props_multiple_summary,
            manifest_version=manifest_version,
            template_version=template_version,
        )
        report_file.write(
            manifest_props_multiple_summary.to_markdown(
                index=False, tablefmt="rounded_grid"
            )
            + "\n\n"
        )
        report_file.write(
            f"Multiple props in {manifest_version} model mapped to {template_version}\n\n"
        )
        template_props_multiple_summary = multiple_mapping_summary_cleanup(
            df=template_props_multiple_summary,
            manifest_version=manifest_version,
            template_version=template_version,
        )
        report_file.write(
            template_props_multiple_summary.to_markdown(
                index=False, tablefmt="rounded_grid"
            )
            + "\n\n"
        )
    return mapping_report


def remove_index_cols(col_list: list) -> list:
    """Removes type, id and id.* column names from col_list
    Returns a list
    """
    cleanup_col = []
    for i in col_list:
        if i != "type" and i != "id":
            if not i.startswith("id."):
                cleanup_col.append(i)
            else:
                pass
        else:
            pass
    return cleanup_col


def find_nonempty_nodes(checkccdi_object) -> list[str]:
    """Look through sheets of ccdi manifest object and
    find the sheet names that are not empty
    """
    node_names = checkccdi_object.get_sheetnames()
    instruction_nodes = [
        "README and INSTRUCTIONS",
        "Dictionary",
        "Terms and Value Sets",
    ]
    node_names = [i for i in node_names if i not in instruction_nodes]
    nonempty_list = []
    for i in node_names:
        i_df = checkccdi_object.read_sheet_na(sheetname=i)
        i_cols = remove_index_cols(col_list=i_df.columns.tolist())
        i_df_subset = i_df[i_cols].dropna(how="all")
        if not i_df_subset.empty:
            nonempty_list.append(i)
        else:
            pass
        del i_df
    return nonempty_list


def find_unlifted_properties(checkccdi_object, nonempty_nodes: list[str], mapping_file: str) -> DataFrame:
    """Report any property that are unmapped in the lift to template, but has value in the manifest
    These are the info that will be lost during liftover
    """
    mapping_df = pd.read_csv(mapping_file, sep="\t")
    empty_in_template_rows = mapping_df[["lift_to_version","lift_to_node","lift_to_property"]].isna().all(axis=1)
    manifest_unmapped_df = mapping_df.loc[
        empty_in_template_rows,
        ["lift_from_version", "lift_from_node", "lift_from_property"],
    ]
    unlift_prop_list = []
    for index, row in  manifest_unmapped_df.iterrows():
        row_node = row["lift_from_node"]
        if row_node in nonempty_nodes:
            row_prop = row["lift_from_property"]
            row_df = checkccdi_object.read_sheet_na(sheetname=row_node)
            row_prop_list =  row_df[row_prop].dropna().tolist()
            if len(row_prop_list) > 0:
                unlift_prop_list.append({"node":row_node,"property":row_prop})
            else:
                pass
        else:
            # node found empty in the manifest, pass
            pass
    unlift_prop_df = pd.DataFrame.from_records(unlift_prop_list, columns=["node","property"])
    return unlift_prop_df


def single_node_liftover(
    mapping_df: DataFrame, template_node: str, template_object, manifest_object, logger
) -> DataFrame:
    """Lift values from manifest file to a sheet of templat file

    # the mapping_df needs to be subset of original mapping df. it only contains
    nodes that are found not empty in the manifest
    """
    # create an empty dataframe
    template_node_df = template_object.read_sheet_na(sheetname=template_node)
    # if multiple nodes in manifest is associated with template node
    # get a list of manifest nodes
    manifest_nodes = (
        mapping_df[mapping_df["lift_to_node"] == template_node]["lift_from_node"]
        .dropna()
        .unique()
        .tolist()
    )
    concatenate_df = pd.DataFrame(columns=template_node_df.columns)
    if len(manifest_nodes) > 1:
        logger.warning(
            f"Template sheet {template_node} has lifted value of more than one sheet in manifest: {*manifest_nodes,}"
        )
    else:
        logger.info(
            f"Template sheet {template_node} has lifted value from one sheet in manifest: {*manifest_nodes,}"
        )
    # for each manifest node, create a separate dataframe in the mapped template node
    # after liftover, append the df to the concatenate_df
    for n in manifest_nodes:
        # n is the manifest node name, not necessarily equals to template node
        template_n_df = pd.DataFrame(columns=template_node_df.columns)
        manifest_n_df = manifest_object.read_sheet_na(sheetname=n)
        n_mapping = mapping_df[
            (mapping_df["lift_to_node"] == template_node)
            & (mapping_df["lift_from_node"] == n)
        ]
        for index, row in n_mapping.iterrows():
            row_property_from = row["lift_from_property"]
            row_property_to = row["lift_to_property"]
            # if column row_property_to has no value assigned to it
            if len(template_n_df[row_property_to].dropna()) == 0:
                template_n_df[row_property_to] = manifest_n_df[row_property_from]
            else:
                template_n_df[row_property_to] = (
                    template_n_df[row_property_to] + manifest_n_df[row_property_from].astype(str)
                )
                logger.warning(f"Property {row_property_to} in template node {template_node} contains concatenated values from multiple properties from the same node in the manifest")

        # remove any row with all missing value
        template_n_df.dropna(axis=0, how="all", inplace=True)
        # add value to the type node
        template_n_df["type"] = template_node
        # append template_n_df to the concatenate_df
        concatenate_df = pd.concat([concatenate_df, template_n_df], axis=0)
    return concatenate_df


@flow(name="lifting value between two files", log_prints=True)
def liftover_to_template(
    mapping_file: str, manifest_file: str, template_file: str
) -> tuple:
    """Lift the content of manifest to the template based off mappping fil

    The function returns a lifted template file and a log file
    """
    logger = get_logger(loggername=f"ccdi_liftover_workflow", log_level="info")
    log_name = "ccdi_liftover_workflow_" + get_date() + ".log"

    template_object = CheckCCDI(ccdi_manifest=template_file)
    template_version = template_object.get_version()

    # copy template file to output_file
    output_file = (
        os.path.basename(manifest_file).rsplit(".", 1)[0]
        + f"_liftover_{template_version}_"
        + get_date()
        + ".xlsx"
    )
    logger.info(f"The name of manifest with lifted value: {output_file}")
    print(f"output name is {output_file}")
    copy(template_file, output_file)

    manifest_object = CheckCCDI(ccdi_manifest=manifest_file)
    nonempty_nodes_manifest = find_nonempty_nodes(checkccdi_object=manifest_object)
    logger.info(
        f"Nonempty nodes in the manifest {manifest_file}: {*nonempty_nodes_manifest,}"
    )
    print(
        f"Nonempty nodes in the manifest {manifest_file}: {*nonempty_nodes_manifest,}"
    )

    unlifted_properties = find_unlifted_properties(
        checkccdi_object=manifest_object, nonempty_nodes=nonempty_nodes_manifest, mapping_file=mapping_file
    )
    unlifted_properties_str = unlifted_properties.to_markdown(
        index=False, tablefmt="rounded_grid"
    )
    if unlifted_properties.shape[0] > 0:
        logger.warning(f"There are properties with values in the manifest that won't be lifted because of mapping absence:\n{unlifted_properties_str}\n")
    else:
        logger.info("Unmapped properties in the manifest are found empty. No vlaue is lost during liftover.")

    mapping_df = pd.read_csv(mapping_file, sep="\t")
    # filter mapping df based on nonempty nodes in manifest
    # we only need to look at the sheet that are not empty
    mapping_df = mapping_df[mapping_df["lift_from_node"].isin(nonempty_nodes_manifest)]

    # how many unique lift_to_node found in filtered mapping df
    # these are the nodes we need to be filled with info
    template_nodes = mapping_df["lift_to_node"].dropna().unique().tolist()
    logger.info(
        f"Nodes in template file {template_file} that will have lifted value: {*template_nodes,}"
    )
    print(
        f"Nodes in template file {template_file} that will have lifted value: {*template_nodes,}"
    )

    # now go through every item of template_nodes
    for node in template_nodes:
        print(f"lifting value for template node {node}")
        template_df_to_add = single_node_liftover(
            mapping_df=mapping_df,
            template_node=node,
            template_object=template_object,
            manifest_object=manifest_object,
            logger=logger,
        )
        with pd.ExcelWriter(
            output_file, mode="a", engine="openpyxl", if_sheet_exists="overlay"
        ) as writer:
            template_df_to_add.to_excel(
                writer, sheet_name=node, index=False, header=False, startrow=1
            )
    return output_file, log_name
