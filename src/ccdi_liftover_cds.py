from prefect import flow, task, get_run_logger
from typing import TypeVar
import pandas as pd
from pathlib import Path
import os
import sys

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.utils import get_date, CheckCCDI, get_logger
from src.manifest_liftover import (
    mapping_coverage,
    evaluate_mapping_props,
    multiple_mapping_summary_cleanup,
    find_nonempty_nodes,
    find_unlifted_properties,
)


DataFrame = TypeVar("DataFrame")


@task(name="mapping file coverage check with cds templates")
def mapping_coverage_cds(
    mapping_df: DataFrame,
    node_colname: str,
    prop_colname: str,
    cds_template_folder: str,
) -> str:
    """Find any property in the model but not found in the mapping file
    Find any property in the mapping file but not found in the model
    """
    # extract files from cds template folder ends with tsv
    filename_list = os.listdir(cds_template_folder)
    file_lists = [
        os.path.join(cds_template_folder, i) for i in filename_list if i.endswith("tsv")
    ]

    missing_prop = []
    extra_prop = []

    for i in file_lists:

        i_df = pd.read_csv(
            i, sep="\t", na_values=["NA", "na", "N/A", "n/a", ""], dtype="string"
        )
        i_node = i_df["type"].tolist()[0]
        i_df_cols = i_df.columns.tolist()

        # remove type, and any column ends with ".id"
        i_df_cols_short = [
            j for j in i_df_cols if not (j == "type") and not (j.endswith(".id"))
        ]
        i_mappped_prop = mapping_df[mapping_df[node_colname] == i_node][
            prop_colname
        ].tolist()
        i_missing = [k for k in i_df_cols_short if k not in i_mappped_prop]
        if len(i_missing) > 0:
            for l in i_missing:
                missing_prop.append({"node": i_node, "prop": l})
        else:
            pass
        i_extra = [m for m in i_mappped_prop if m not in i_df_cols_short]
        if len(i_extra) > 0:
            for n in i_extra:
                extra_prop.append({"node": i_node, "prop": n})
        else:
            pass
    missing_prop_df = pd.DataFrame.from_records(missing_prop, columns=["node", "prop"])
    extra_prop_df = pd.DataFrame.from_records(extra_prop, columns=["node", "prop"])
    return missing_prop_df, extra_prop_df


def extract_cds_node_type(cds_template_folderpath: str) -> None:
    """Takes a cds templates folder that contains tsv templates
    and find the node type of each file
    It returns a dict of mapping between node type and file
    """
    # extract files from cds template folder ends with tsv
    filename_list = os.listdir(cds_template_folderpath)
    file_lists = [
        os.path.join(cds_template_folderpath, i)
        for i in filename_list
        if i.endswith("tsv")
    ]
    node_file_mapping_dict = {}
    for i in file_lists:
        i_df = pd.read_csv(
            i, sep="\t", na_values=["NA", "na", "N/A", "n/a", ""], dtype="string"
        )
        i_node = i_df["type"].tolist()[0]
        node_file_mapping_dict[i_node] = i
    return node_file_mapping_dict


@flow(name="validate mapping with ccdi manifest and cds templates", log_prints=True)
def validate_mapping_cds(
    manifest_path: str,
    template_version: str,
    template_folderpath: str,
    mapping_path: str,
) -> None:
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

    mapping_report = f"mapping_validation_report_{todaydate}.txt"

    # check if the propreties in manifest xlsx can be found in mapping file
    manifest_mapping_missing, manifest_mapping_extra = mapping_coverage(
        mapping_df=mapping_from_df,
        node_colname="lift_from_node",
        prop_colname="lift_from_property",
        manifest_object=manifest_object,
    )
    template_mapping_missing, template_mapping_extra = mapping_coverage_cds(
        mapping_df=mapping_to_df,
        node_colname="lift_to_node",
        prop_colname="lift_to_property",
        cds_template_folder=template_folderpath,
    )

    # checking mapping file itself (if multiple props from old version are mapped to new version, vice versa)
    (
        manifest_unmapped_df,
        template_unmapped_df,
        manifest_props_multiple_summary,
        template_props_multiple_summary,
    ) = evaluate_mapping_props(mapping_df=mapping_df, mapping_col_dict=mapping_col_dict)

    with open(mapping_report, "w") as report_file:
        report_file.write(
            f"Manifest file: {manifest_path}\nCDS Template folder path: {template_folderpath}\nMapping file: {mapping_path}\n\n\n"
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
            f"If the mapping file misses any proprety in the CDS template\n{template_folderpath}\n\n"
        )
        report_file.write(
            template_mapping_missing.to_markdown(index=False, tablefmt="rounded_grid")
            + "\n\n"
        )
        report_file.write(
            f"If the mapping file contains any extra proprety in the CDS template\n{template_folderpath}\n\n"
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


def single_cds_node_liftover(
    mapping_df: DataFrame,
    cds_node_file: str,
    cds_node_name: str,
    tsv_output_folder: str,
    ccdi_maniest,
    logger,
) -> None:
    # generate output tsv name
    phs_accession = ccdi_maniest.get_study_id()
    out_tsv_name = phs_accession + "_" + cds_node_name + "_" + get_date() + ".tsv"
    out_tsv_path = os.path.join(tsv_output_folder, out_tsv_name)

    # if multiple nodes in manifest is associated with template node
    # get a list of manifest nodes
    manifest_nodes = (
        mapping_df[mapping_df["lift_to_node"] == cds_node_name]["lift_from_node"]
        .dropna()
        .unique()
        .tolist()
    )
    if len(manifest_nodes) > 1:
        logger.warning(
            f"Template sheet {cds_node_name} has lifted value of more than one sheet in manifest: {*manifest_nodes,}"
        )
    else:
        logger.info(
            f"Template sheet {cds_node_name} has lifted value from one sheet in manifest: {*manifest_nodes,}"
        )

    # create an empty dataframe with colnames of cds node
    template_node_df = pd.read_csv(cds_node_file, sep="\t")
    concatenate_df = pd.DataFrame(columns=template_node_df.columns)

    # for each manifest node, create a separate dataframe in the mapped template node
    # after liftover, append the df to the concatenate_df
    for n in manifest_nodes:
        # n is the manifest node name, not necessarily equals to template node
        template_n_df = pd.DataFrame(columns=template_node_df.columns)
        manifest_n_df = ccdi_maniest.read_sheet_na(sheetname=n)
        n_mapping = mapping_df[
            (mapping_df["lift_to_node"] == cds_node_name)
            & (mapping_df["lift_from_node"] == n)
        ]
        # This is for a case where more than one properties in ccdi from
        # the SAME NODE mapped to one property in a cds node
        for index, row in n_mapping.iterrows():
            row_property_from = row["lift_from_property"]
            row_property_to = row["lift_to_property"]
            # if column row_property_to has no value assigned to it
            if len(template_n_df[row_property_to].dropna()) == 0:
                template_n_df[row_property_to] = manifest_n_df[row_property_from]
            else:
                template_n_df[row_property_to] = (
                    template_n_df[row_property_to]
                    + ";"
                    + manifest_n_df[row_property_from].astype(str)
                )
                # strip off any ";"
                template_n_df[row_property_to] = template_n_df[
                    row_property_to
                ].str.strip(";")
                logger.warning(
                    f"Property {row_property_to} in template node {cds_node_name} contains concatenated values from multiple properties from the same node in manifest"
                )
            # remove any row with all missing value
        template_n_df.dropna(axis=0, how="all", inplace=True)
        # add value to the type node
        template_n_df["type"] = cds_node_name
        # append template_n_df to the concatenate_df
        concatenate_df = pd.concat([concatenate_df, template_n_df], axis=0)

    # writing out output
    logger.info(f"Writing CDS node {cds_node_name} tsv: {out_tsv_path}")
    concatenate_df.to_csv(out_tsv_path, sep="\t", index=False)

    return None


@flow(name="Lift values from CCDI to CDS", log_prints=True)
def liftover_to_cds_template(
    mapping_file: str,
    manifest_file: str,
    template_folderpath: str,
    template_version: str,
):
    """Lift the content of manifest to the template based off mappping fil

    The function returns a collection of  and a log file
    """
    logger = get_logger(loggername=f"ccdi_liftover_workflow", log_level="info")
    log_name = "ccdi_to_cds_liftover_workflow_" + get_date() + ".log"

    # create output folder for cds tsvs
    tsv_folder_name = (
        os.path.basename(manifest_file).rsplit(".", 1)[0]
        + f"_liftover_CDS_v{template_version}_"
        + get_date()
    )
    tsv_folder_path = os.path.join(os.getcwd(), tsv_folder_name)
    Path(tsv_folder_path).mkdir(parents=True, exist_ok=True)

    # find nonempty node in ccdi manifest
    manifest_object = CheckCCDI(ccdi_manifest=manifest_file)
    nonempty_nodes_manifest = find_nonempty_nodes(checkccdi_object=manifest_object)
    logger.info(
        f"Nonempty nodes in the manifest {manifest_file}: {*nonempty_nodes_manifest,}"
    )
    print(
        f"Nonempty nodes in the manifest {manifest_file}: {*nonempty_nodes_manifest,}"
    )

    # report any property in ccdi manifest that are not empty and won't be lifted in CDS
    unlifted_properties = find_unlifted_properties(
        checkccdi_object=manifest_object,
        nonempty_nodes=nonempty_nodes_manifest,
        mapping_file=mapping_file,
    )
    unlifted_properties_str = unlifted_properties.to_markdown(
        index=False, tablefmt="rounded_grid"
    )
    if unlifted_properties.shape[0] > 0:
        logger.warning(
            f"There are properties with values in the manifest that won't be lifted because of mapping absence:\n{unlifted_properties_str}\n"
        )
        print(
            f"There are properties with values in the manifest that won't be lifted because of mapping absence:\n{unlifted_properties_str}\n"
        )
    else:
        logger.info(
            "Unmapped properties in the manifest are found empty. No vlaue is lost during liftover."
        )
        print(
            "Unmapped properties in the manifest are found empty. No vlaue is lost during liftover."
        )

    # slim down mapping df caring only nonempty nodes
    mapping_df = pd.read_csv(mapping_file, sep="\t")
    # filter mapping df based on nonempty nodes in manifest
    # we only need to look at the sheet that are not empty
    mapping_df = mapping_df[mapping_df["lift_from_node"].isin(nonempty_nodes_manifest)]

    # how many unique lift_to_node found in filtered mapping df
    # these are the nodes we need to be filled with info
    template_nodes_required = mapping_df["lift_to_node"].dropna().unique().tolist()
    logger.info(
        f"Nodes in cds template that will have lifted value: {*template_nodes_required,}"
    )
    print(
        f"Nodes in cds template file that will have lifted value: {*template_nodes_required,}"
    )

    # check if template folderpath has all the template files that needed
    cds_node_file_mapping = extract_cds_node_type(
        cds_template_folderpath=template_folderpath
    )
    template_nodes_unfound = [
        i for i in template_nodes_required if i not in cds_node_file_mapping.keys()
    ]
    if len(template_nodes_unfound) > 0:
        logger.error(
            f"The liftover requires some node that is not found in the CDS template folder you provided: {*template_nodes_unfound,}"
        )
        print(
            f"The liftover requires some node that is not found in the CDS template folder you provided: {*template_nodes_unfound,}"
        )
        return tsv_folder_name, log_name
    else:
        pass

    # writing CDS tsv files
    for node in template_nodes_required:
        try:
            node_file = cds_node_file_mapping[node]
            mapping_df_node = mapping_df[mapping_df["lift_to_node"] == node]
            logger.info(f"Creating CDS tsv for node: {node}")
            single_cds_node_liftover(
                mapping_df=mapping_df_node,
                cds_node_file=node_file,
                cds_node_name=node,
                tsv_output_folder=tsv_folder_path,
                ccdi_maniest=manifest_object,
                logger=logger,
            )

        except Exception as err:
            logger.error(
                f"Error occurred while creating CDS tsv for node {node}: {repr(err)}"
            )

    return tsv_folder_name, log_name
