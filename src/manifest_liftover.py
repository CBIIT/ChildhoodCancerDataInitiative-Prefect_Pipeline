from prefect import flow, task, get_run_logger
from src.utils import CheckCCDI, get_date
from enum import Enum
from typing import TypeVar
import pandas as pd

DataFrame = TypeVar("DataFrame")


@task(name="version validation", log_prints=True)
def tags_validation(manifest_path: str, tag: str, logger) -> bool:
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
            f"More than one lift from versions were found in mapping file: {*lift_to,}"
        )
        raise ValueError(
            f"More than one lift from versions were found in mapping file: {*lift_to,}"
        )
    else:
        pass
    return lift_from[0], lift_to[0]


@task(name="mapping file coverage check")
def mapping_coverage(
    mapping_df: DataFrame, node_colname: str, prop_colname: str, manifest_object
) -> str:
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
def evaludate_mapping_props(mapping_df: DataFrame, mapping_col_dict: dict) -> tuple:
    mapping_from_cols = mapping_col_dict["lift_from"]
    mapping_to_cols = mapping_col_dict["lift_to"]

    # find props not mapped to
    empty_in_template_rows = mapping_df[mapping_to_cols].isna().all(axis=1)
    manifest_unmapped_df = mapping_df.loc[
        empty_in_template_rows,
        ["lift_from_version", "lift_from_node", "lift_from_property"],
    ]

    # find props not mapped from
    empty_in_manifest_rows = mapping_df[mapping_from_cols].isna().all(axis=1)
    template_unmapped_df = mapping_df.loc[
        empty_in_manifest_rows, ["lift_to_version", "lift_to_node", "lift_to_property"]
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


def multiple_mapping_summary_cleanup(df: DataFrame, manifest_version: str, template_version: str) -> DataFrame:
    df.rename(
            columns={
                "lift_from_node": f"{manifest_version}_node",
                "lift_from_property": f"{manifest_version}_property",
                "lift_to_node": f"{template_version}_node",
                "lift_to_property": f"{template_version}_property"
            },
            inplace=True,
    )
    df.drop(columns=["lift_from_version","lift_to_version"], inplace=True)
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
    ) = evaludate_mapping_props(
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
            f"Properties in {manifest_version} model that are unmapped in the {template_version} model\nUnmapped propreties would be lifted over\n\n"
        )
        report_file.write(
            manifest_unmapped_df.to_markdown(index=False, tablefmt="rounded_grid")
            + "\n\n"
        )
        report_file.write(
            f"Properties in {template_version} model that are unmapped in the {manifest_version} model\nUnmapped propreties would be lifted over\n\n"
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
            template_version = template_version
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
            template_version=template_version
        )
        report_file.write(
            template_props_multiple_summary.to_markdown(
                index=False, tablefmt="rounded_grid"
            )
            + "\n\n"
        )
    return mapping_report
