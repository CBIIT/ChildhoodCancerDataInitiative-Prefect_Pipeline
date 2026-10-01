"""Script to compare the COG CRF delivered forms and fields between different tranches
Use aggregated TSV files as output from JSON2TSV, e.g. COG_JSON_table_conversion_*.tsv
"""

from asyncio.log import logger
import os
import pandas as pd
from prefect import flow, task, get_run_logger
from src.utils import get_time, file_dl, folder_ul
import seaborn as sns
import matplotlib.pyplot as plt


# function to read in TSV file
# ignore rows that are missing upi
@task(name="Read TSV", log_prints=True)
def read_tsv(file_path):
    df = pd.read_csv(file_path, sep="\t", low_memory=False)
    if "upi" in df.columns:
        df = df[~df["upi"].isna()]
    return df


def completeness_of_prop(df, prop):
    """Calculate the completeness of a given property/field, grouped by FINAL_DIAGNOSIS.PRIMDXDSCAT, round to 2 decimal places and return as a float"""
    return (
        df.groupby("FINAL_DIAGNOSIS.PRIMDXDSCAT")[prop]
        .apply(lambda x: round(1 - x.isna().sum() / len(x), 2))
        .reset_index(name="completeness")
    )


def completeness_of_prop_total(df, prop):
    """Calculate the completeness of a given property/field, total across all rows, round to 2 decimal places and return as a float"""
    if len(df) == 0:
        return float("nan")  # or 0, or skip — your call
    return round(1 - df[prop].isna().sum() / len(df[prop]), 2)


# fucntion to compare two dataframes and return the differences
# i.e.
# new properties/fields and removed properties/fields
# new upis and removed upis
# new forms (e.g. split header by "." and take first element) and removed forms
# completeness of props, e.g. how many non-null and null values for each prop, grouped by field FINAL_DIAGNOSIS.PRIMDXDSCAT
@task(name="Compare Dataframes", log_prints=True)
def compare_dataframes(old_tranche_df, new_tranche_df):
    # get upis
    upis1 = set(old_tranche_df["upi"].unique())
    upis2 = set(new_tranche_df["upi"].unique())

    new_upis = upis2 - upis1
    removed_upis = upis1 - upis2

    # get properties/fields
    old_props_set = set(old_tranche_df.columns)
    new_props_set = set(new_tranche_df.columns)
    new_props = new_props_set - old_props_set
    removed_props = old_props_set - new_props_set

    # get forms
    old_forms_set = set([col.split(".")[0] for col in old_tranche_df.columns])
    new_forms_set = set([col.split(".")[0] for col in new_tranche_df.columns])
    new_forms = new_forms_set - old_forms_set
    removed_forms = old_forms_set - new_forms_set

    # get completeness of props, grouped by field FINAL_DIAGNOSIS.PRIMDXDSCAT and total across all groups
    # then group props into groups of 0-25% completeness, 25-50% completeness, 50-75% completeness and 75-100% completeness

    # convert values "" or "NA" to NaN for the purpose of calculating completeness
    old_tranche_df = old_tranche_df.replace(["", "NA"], pd.NA)
    new_tranche_df = new_tranche_df.replace(["", "NA"], pd.NA)

    completeness_df = pd.DataFrame(
        columns=[
            "prop",
            "FINAL_DIAGNOSIS.PRIMDXDSCAT",
            "completeness_old_tranche",
            "completeness_new_tranche",
        ]
    )
    for prop in old_props_set.intersection(new_props_set):
        completeness_old = completeness_of_prop(old_tranche_df, prop)
        completeness_new = completeness_of_prop(new_tranche_df, prop)
        completeness_temp = pd.merge(
            completeness_old,
            completeness_new,
            on="FINAL_DIAGNOSIS.PRIMDXDSCAT",
            suffixes=("_old", "_new"),
        )
        completeness_temp["prop"] = prop
        completeness_temp = completeness_temp[
            [
                "prop",
                "FINAL_DIAGNOSIS.PRIMDXDSCAT",
                "completeness_old",
                "completeness_new",
            ]
        ].rename(
            columns={
                "completeness_old": "completeness_old_tranche",
                "completeness_new": "completeness_new_tranche",
                "FINAL_DIAGNOSIS.PRIMDXDSCAT": "MCI_substudy",
            }
        )
        completeness_df = pd.concat(
            [completeness_df, completeness_temp], ignore_index=True
        )

    # total completeness across all groups for each prop
    for prop in old_props_set.intersection(new_props_set):
        completeness_old_total = completeness_of_prop_total(old_tranche_df, prop)
        completeness_new_total = completeness_of_prop_total(new_tranche_df, prop)
        completeness_total_temp = pd.DataFrame(
            {
                "prop": [prop],
                "FINAL_DIAGNOSIS.PRIMDXDSCAT": ["TOTAL"],
                "completeness_old_tranche": [completeness_old_total],
                "completeness_new_tranche": [completeness_new_total],
            }
        )
        completeness_total_temp = completeness_total_temp[
            [
                "prop",
                "FINAL_DIAGNOSIS.PRIMDXDSCAT",
                "completeness_old_tranche",
                "completeness_new_tranche",
            ]
        ].rename(columns={"FINAL_DIAGNOSIS.PRIMDXDSCAT": "MCI_substudy"})
        completeness_df = pd.concat(
            [completeness_df, completeness_total_temp], ignore_index=True
        )

    # new props in new tranch
    for prop in new_props:
        completeness_temp = completeness_of_prop(new_tranche_df, prop)
        completeness_temp["prop"] = prop
        completeness_temp["completeness_old_tranche"] = 0
        completeness_temp["completeness_new_tranche"] = completeness_temp[
            "completeness"
        ]
        completeness_temp = completeness_temp[
            [
                "prop",
                "FINAL_DIAGNOSIS.PRIMDXDSCAT",
                "completeness_old_tranche",
                "completeness_new_tranche",
            ]
        ].rename(columns={"FINAL_DIAGNOSIS.PRIMDXDSCAT": "MCI_substudy"})
        completeness_df = pd.concat(
            [completeness_df, completeness_temp], ignore_index=True
        )

    for prop in new_props:
        completeness_new_total = completeness_of_prop_total(new_tranche_df, prop)
        completeness_total_temp = pd.DataFrame(
            {
                "prop": [prop],
                "FINAL_DIAGNOSIS.PRIMDXDSCAT": ["TOTAL"],
                "completeness_old_tranche": [0],
                "completeness_new_tranche": [completeness_new_total],
            }
        )
        completeness_total_temp = completeness_total_temp[
            [
                "prop",
                "FINAL_DIAGNOSIS.PRIMDXDSCAT",
                "completeness_old_tranche",
                "completeness_new_tranche",
            ]
        ].rename(columns={"FINAL_DIAGNOSIS.PRIMDXDSCAT": "MCI_substudy"})
        completeness_df = pd.concat(
            [completeness_df, completeness_total_temp], ignore_index=True
        )

    # sort completeness_df on prop and MCI_substudy
    completeness_df = completeness_df.sort_values(by=["prop", "MCI_substudy"])

    # add col called "completeness_change" to indicate if the completeness has increased, decreased or stayed the same between the two tranches
    def completeness_change(row):
        if row["completeness_new_tranche"] > row["completeness_old_tranche"]:
            return "increased"
        elif row["completeness_new_tranche"] < row["completeness_old_tranche"]:
            return "decreased"
        else:
            return "same"

    completeness_df["completeness_change"] = completeness_df.apply(
        completeness_change, axis=1
    )

    # group props into groups of 0-25% completeness, 25-50% completeness, 50-75% completeness and 75-100% completeness for both old and new tranche
    def completeness_group(completeness):
        if completeness <= 0.25:
            return "0-25%"
        elif completeness <= 0.5:
            return "25-50%"
        elif completeness <= 0.75:
            return "50-75%"
        else:
            return "75-100%"

    completeness_df["completeness_group_old_tranche"] = completeness_df[
        "completeness_old_tranche"
    ].apply(completeness_group)
    completeness_df["completeness_group_new_tranche"] = completeness_df[
        "completeness_new_tranche"
    ].apply(completeness_group)

    return {
        "total_new_upis": len(upis2),
        "total_old_upis": len(upis1),
        "new_upis": new_upis,
        "removed_upis": removed_upis,
        "total_new_props": len(new_tranche_df.columns),
        "total_old_props": len(old_tranche_df.columns),
        "new_props": new_props,
        "removed_props": removed_props,
        "new_forms": new_forms,
        "removed_forms": removed_forms,
        "old_total_values": ((old_tranche_df.notna()) & (old_tranche_df != ""))
        .sum()
        .sum(),
        "new_total_values": ((new_tranche_df.notna()) & (new_tranche_df != ""))
        .sum()
        .sum(),
        "completeness_df": completeness_df,
    }


# completeness parsing
def completeness_parser(completeness_df, output_path, dt):

    # Well populated properties across TOTAL MCI substudies, >65% completion
    well_pop = completeness_df[(completeness_df["completeness_new_tranche"] >= 0.65)]
    well_pop.to_csv(
        os.path.join(output_path, f"well_populated_properties_{dt}.csv"), index=False
    )


def frontline_treatment_image(
    old_tranche_df, new_tranche_df, old_tranche_date, new_tranche_date, output_path, dt
):
    df_treatment_new = new_tranche_df[
        new_tranche_df["FOLLOW_UP.FSTLNTXINIDXADM"] == "Yes"
    ][
        [
            "upi",
            "FOLLOW_UP.FSTLNTXINIDXADMCAT_A1",
            "FOLLOW_UP.FSTLNTXINIDXADMCAT_A2",
            "FOLLOW_UP.FSTLNTXINIDXADMCAT_A3",
            "FOLLOW_UP.FSTLNTXINIDXADMCAT_A4",
            "FOLLOW_UP.FSTLNTXINIDXADMCAT_A5",
            "FOLLOW_UP.FSTLNTXINIDXADMCAT_A6",
        ]
    ].drop_duplicates()
    df_treatment_old = old_tranche_df[
        old_tranche_df["FOLLOW_UP.FSTLNTXINIDXADM"] == "Yes"
    ][
        [
            "upi",
            "FOLLOW_UP.FSTLNTXINIDXADMCAT_A1",
            "FOLLOW_UP.FSTLNTXINIDXADMCAT_A2",
            "FOLLOW_UP.FSTLNTXINIDXADMCAT_A3",
            "FOLLOW_UP.FSTLNTXINIDXADMCAT_A4",
            "FOLLOW_UP.FSTLNTXINIDXADMCAT_A5",
            "FOLLOW_UP.FSTLNTXINIDXADMCAT_A6",
        ]
    ].drop_duplicates()

    df_treatment_new.columns = [
        "upi",
        "Chemo/Immunotherapy",
        "Radiation Therapy",
        "Stem Cell Transplant",
        "Surgery",
        "Cellular Therapy",
        "Other",
    ]
    df_treatment_old.columns = [
        "upi",
        "Chemo/Immunotherapy",
        "Radiation Therapy",
        "Stem Cell Transplant",
        "Surgery",
        "Cellular Therapy",
        "Other",
    ]

    treat_summ_new = []
    for col in df_treatment_new.columns[1:]:
        temp = df_treatment_new[df_treatment_new[col].notnull()]
        count = len(temp)
        treat_summ_new.append([col, count])
    treat_summ_new_df = pd.DataFrame(treat_summ_new, columns=["Treatment", "Count"])
    treat_summ_old = []
    for col in df_treatment_old.columns[1:]:
        temp = df_treatment_old[df_treatment_old[col].notnull()]
        count = len(temp)
        treat_summ_old.append([col, count])
    treat_summ_old_df = pd.DataFrame(treat_summ_old, columns=["Treatment", "Count"])

    df_merged = treat_summ_old_df.merge(
        treat_summ_new_df,
        on="Treatment",
        how="outer",
        suffixes=(f"_{old_tranche_date}", f"_{new_tranche_date}"),
    ).sort_values(by="Count_" + new_tranche_date, ascending=False)

    plt.figure(figsize=(11, 6))
    # Example: df with a column "category"
    ax = sns.barplot(
        data=df_merged.melt(
            id_vars="Treatment",
            value_vars=[f"Count_{old_tranche_date}", f"Count_{new_tranche_date}"],
            var_name="Tranche",
            value_name="Count",
        ),
        x="Treatment",
        y="Count",
        hue="Tranche",
        palette="Set2",
    )

    for container in ax.containers:
        ax.bar_label(container)

    # Place caption ABOVE the plot area
    ax.text(
        0.5,
        1.02,  # x centered, y slightly above the axes (1.0 = top of axes)
        f"{len(df_treatment_new)}/{new_tranche_df.upi.unique().shape[0]} ({round(len(df_treatment_new)/new_tranche_df.upi.unique().shape[0]*100, 1)}%) MCI Participants in COG delivery from tranche {new_tranche_date}\n received at least one "
        f"frontline treatment for their initial diagnosis.\n"
        f"FOLLOW_UP.FSTLNTXINIDXADM == Yes | FOLLOW_UP.FSTLNTXINIDXADMCAT_A1-6",
        transform=ax.transAxes,
        ha="center",
        va="bottom",
        fontsize=11,
    )

    ax.legend(loc="upper right")
    plt.xticks(rotation=0, ha="center", fontsize=8)
    plt.savefig(
        os.path.join(output_path, f"FollowUp_FirstLine_TX_{new_tranche_date}_{dt}.png"),
        dpi=300,
    )
    plt.close()


def enroll_clin_trial_image(
    old_tranche_df, new_tranche_df, old_tranche_date, new_tranche_date, output_path, dt
):
    df_seq_treat_old = (
        old_tranche_df[old_tranche_df["FOLLOW_UP.FSTLNTXINIDXADM"] == "Yes"][
            ["upi", "NCI_MCI_FUP.PTNTENRLSEQELIGTREATASGNIND"]
        ]
        .fillna("Not Applicable")
        .drop_duplicates()
    )
    df_seq_treat_new = (
        new_tranche_df[new_tranche_df["FOLLOW_UP.FSTLNTXINIDXADM"] == "Yes"][
            ["upi", "NCI_MCI_FUP.PTNTENRLSEQELIGTREATASGNIND"]
        ]
        .fillna("Not Applicable")
        .drop_duplicates()
    )
    df_seq_treat_summ_old = (
        df_seq_treat_old.groupby("NCI_MCI_FUP.PTNTENRLSEQELIGTREATASGNIND")
        .size()
        .reset_index(name="Count")
    )
    df_seq_treat_summ_new = (
        df_seq_treat_new.groupby("NCI_MCI_FUP.PTNTENRLSEQELIGTREATASGNIND")
        .size()
        .reset_index(name="Count")
    )

    df_merged = df_seq_treat_summ_old.merge(
        df_seq_treat_summ_new,
        on="NCI_MCI_FUP.PTNTENRLSEQELIGTREATASGNIND",
        how="outer",
        suffixes=(f"_{old_tranche_date}", f"_{new_tranche_date}"),
    ).sort_values(by="Count_" + new_tranche_date, ascending=False)
    df_merged = df_merged.rename(
        columns={"NCI_MCI_FUP.PTNTENRLSEQELIGTREATASGNIND": "Value"}
    )

    plt.figure(figsize=(11, 6))
    ax = sns.barplot(
        data=df_merged.melt(
            id_vars="Value",
            value_vars=[f"Count_{old_tranche_date}", f"Count_{new_tranche_date}"],
            var_name="Tranche",
            value_name="Count",
        ),
        x="Value",
        y="Count",
        hue="Tranche",
        palette="Set2",
    )

    for container in ax.containers:
        ax.bar_label(container)

    ax.text(
        0.5,
        1.02,  # x centered, y slightly above the axes (1.0 = top of axes)
        f"Of the {len(df_seq_treat_new)}/{new_tranche_df.upi.unique().shape[0]} ({round(len(df_seq_treat_new)/new_tranche_df.upi.unique().shape[0]*100, 1)}%) MCI Participants with follow up information: did the patient enroll on a therapeutic clinical trial \n that utilized these sequencing results for eligibility or treatment assignment? \n NCI_MCI_FUP.PTNTENRLSEQELIGTREATASGNIND",
        transform=ax.transAxes,
        ha="center",
        va="bottom",
        fontsize=11,
    )

    ax.legend(loc="upper right")
    plt.xticks(rotation=0, ha="center", fontsize=8)
    plt.savefig(
        os.path.join(output_path, f"Enroll_Clin_Trial_{new_tranche_date}_{dt}.png"),
        dpi=300,
    )
    plt.close()


def match_therapy_molec_image(
    df_old, df_new, old_tranche_date, new_tranche_date, output_path, dt
):
    df_seq_treat_old = (
        df_old[df_old["FOLLOW_UP.FSTLNTXINIDXADM"] == "Yes"][
            ["upi", "NCI_MCI_FUP.PTNTMOLSEQVARINDMCHTXTRLENR"]
        ]
        .fillna("Not Applicable")
        .drop_duplicates()
    )
    df_seq_treat_new = (
        df_new[df_new["FOLLOW_UP.FSTLNTXINIDXADM"] == "Yes"][
            ["upi", "NCI_MCI_FUP.PTNTMOLSEQVARINDMCHTXTRLENR"]
        ]
        .fillna("Not Applicable")
        .drop_duplicates()
    )
    df_seq_treat_summ_old = (
        df_seq_treat_old.groupby("NCI_MCI_FUP.PTNTMOLSEQVARINDMCHTXTRLENR")
        .size()
        .reset_index(name="Count")
    )
    df_seq_treat_summ_new = (
        df_seq_treat_new.groupby("NCI_MCI_FUP.PTNTMOLSEQVARINDMCHTXTRLENR")
        .size()
        .reset_index(name="Count")
    )

    df_merged = df_seq_treat_summ_old.merge(
        df_seq_treat_summ_new,
        on="NCI_MCI_FUP.PTNTMOLSEQVARINDMCHTXTRLENR",
        how="outer",
        suffixes=(f"_{old_tranche_date}", f"_{new_tranche_date}"),
    ).sort_values(by="Count_" + new_tranche_date, ascending=False)
    df_merged = df_merged.rename(
        columns={"NCI_MCI_FUP.PTNTMOLSEQVARINDMCHTXTRLENR": "Value"}
    )
    # shorten vals
    val_map = {
        "Yes, on a compassionate use basis / single patient IND": "Yes, on a compassionate use basis/\nsingle patient IND",
        "Yes, using a commercially available therapy": "Yes, using a commercially\navailable therapy",
    }
    df_merged["Value"] = df_merged["Value"].replace(val_map)

    plt.figure(figsize=(11, 6))
    ax = sns.barplot(
        data=df_merged.melt(
            id_vars="Value",
            value_vars=[f"Count_{old_tranche_date}", f"Count_{new_tranche_date}"],
            var_name="Tranche",
            value_name="Count",
        ),
        x="Value",
        y="Count",
        hue="Tranche",
        palette="Set2",
    )

    for container in ax.containers:
        ax.bar_label(container)

    ax.text(
        0.5,
        1.02,  # x centered, y slightly above the axes (1.0 = top of axes)
        f"Of the {len(df_seq_treat_new)}/{len(df_new.upi.unique())} ({round(len(df_seq_treat_new)/len(df_new.upi.unique())*100, 1)}%) MCI Participants with follow up information: If patient was not enrolled in clinical trial, \ndid the patient get a therapy matched to a molecular alteration identified by this sequencing independent of clinical trial enrollment? \n NCI_MCI_FUP.PTNTMOLSEQVARINDMCHTXTRLENR",
        transform=ax.transAxes,
        ha="center",
        va="bottom",
        fontsize=11,
    )

    ax.legend(loc="upper right")
    plt.xticks(rotation=0, ha="center", fontsize=8)
    plt.savefig(
        os.path.join(
            output_path, f"Matched_Therapy_Molec_Alt_{new_tranche_date}_{dt}.png"
        ),
        dpi=300,
    )
    plt.close()


# main function to read in two TSV files, compare them and write out the differences to a log file
@flow(
    name="COG Comparer",
    log_prints=True,
    flow_run_name="cog_comparer-" + f"{get_time()}",
)
def cog_comparer(
    bucket: str,
    old_tranche_path: str,
    new_tranche_path: str,
    old_tranche_date: str,
    new_tranche_date: str,
    sas_labels_path: str,
    runner: str,
):
    logger = get_run_logger()
    dt = get_time()

    output_path = f"./cog_compare_{dt}"
    if not os.path.exists(output_path):
        os.mkdir(output_path)

    # download the TSV files
    file_dl(bucket, old_tranche_path)
    file_dl(bucket, new_tranche_path)

    old_tranche = os.path.basename(old_tranche_path)
    new_tranche = os.path.basename(new_tranche_path)

    logger.info(f"Reading TSV files: {old_tranche} and {new_tranche}")
    old_tranche_df = read_tsv(old_tranche)
    new_tranche_df = read_tsv(new_tranche)

    logger.info("Comparing dataframes")
    comparison_results = compare_dataframes(old_tranche_df, new_tranche_df)

    # write out the differences to a log file
    log_file_path = os.path.join(output_path, f"cog_comparer_log_{dt}.txt")
    with open(log_file_path, "w+") as log_file:
        log_file.write(f"Comparison of COG CRF forms and fields between tranches:\n\n")
        log_file.write(f"Old Tranche: {old_tranche}\n")
        log_file.write(f"New Tranche: {new_tranche}\n\n")
        log_file.write(f"New UPIs Count: {len(comparison_results['new_upis'])}\n")
        log_file.write(
            f"Removed UPIs Count: {len(comparison_results['removed_upis'])}\n"
        )
        log_file.write(f"New Forms Count: {len(comparison_results['new_forms'])}\n")
        log_file.write(
            f"Removed Forms Count: {len(comparison_results['removed_forms'])}\n"
        )
        log_file.write(
            f"New Properties/Fields Count: {len(comparison_results['new_props'])}\n"
        )
        log_file.write(
            f"Removed Properties/Fields Count: {len(comparison_results['removed_props'])}\n\n"
        )
        log_file.write(
            f"Total Old Properties/Fields Count: {comparison_results['total_old_props']}\n"
        )
        log_file.write(
            f"Total New Properties/Fields Count: {comparison_results['total_new_props']}\n"
        )
        log_file.write(
            f"Total Old Values Count: {comparison_results['old_total_values']}\n"
        )
        log_file.write(
            f"Total New Values Count: {comparison_results['new_total_values']}\n\n"
        )
        log_file.write("\n\n---------- Lists of Updated Data ----------\n\n ")
        log_file.write(f"New UPIs: {sorted(comparison_results['new_upis'])}\n\n")
        log_file.write(
            f"Removed UPIs: {sorted(comparison_results['removed_upis'])}\n\n"
        )
        log_file.write(
            f"New Forms: {len(comparison_results['new_forms'])} - {sorted(comparison_results['new_forms'])}\n\n"
        )
        log_file.write(
            f"Removed Forms: {len(comparison_results['removed_forms'])} - {sorted(comparison_results['removed_forms'])}\n\n"
        )
        log_file.write(
            f"New Properties/Fields: {len(comparison_results['new_props'])} - {sorted(comparison_results['new_props'])}\n\n"
        )
        log_file.write(
            f"Removed Properties/Fields: {len(comparison_results['removed_props'])} - {sorted(comparison_results['removed_props'])}\n\n"
        )
    log_file.close()

    # map to completeness comparison the SaS Labels
    file_dl(bucket, sas_labels_path)
    sas_labels_df = read_tsv(os.path.basename(sas_labels_path))
    sas_labels_df.columns = ["prop", "label", "cde"]
    completeness_df = comparison_results["completeness_df"].merge(
        sas_labels_df, how="left", left_on="prop", right_on="prop"
    )
    logger.info("Mapping SAS Labels to completeness comparison")
    # save the completeness dataframe to a TSV file and upload to output path
    completeness_file_path = os.path.join(
        output_path, f"completeness_comparison_{dt}.xlsx"
    )
    completeness_df.to_excel(completeness_file_path, index=False)

    completeness_parser(completeness_df, output_path, dt)

    logger.info("Generating Images")
    frontline_treatment_image(
        old_tranche_df,
        new_tranche_df,
        old_tranche_date,
        new_tranche_date,
        output_path,
        dt,
    )
    enroll_clin_trial_image(
        old_tranche_df,
        new_tranche_df,
        old_tranche_date,
        new_tranche_date,
        output_path,
        dt,
    )
    match_therapy_molec_image(
        old_tranche_df,
        new_tranche_df,
        old_tranche_date,
        new_tranche_date,
        output_path,
        dt,
    )

    # upload folder
    folder_ul(
        local_folder=f"{output_path}",
        bucket=bucket,
        destination=runner,
        sub_folder="",
    )
