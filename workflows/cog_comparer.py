"""Script to compare the COG CRF delivered forms and fields between different tranches
Use aggregated TSV files as output from JSON2TSV, e.g. COG_JSON_table_conversion_*.tsv"""

import os
import pandas as pd
from prefect import flow, task, get_run_logger
from src.utils import get_time, file_dl, file_ul


# function to read in TSV file
# ignore rows that are missing upi
@task(name="Read TSV", log_prints=True)
def read_tsv(file_path):
    df = pd.read_csv(file_path, sep="\t", low_memory=False)
    df = df[~df["upi"].isna()]
    return df


def sparsity_of_prop(df, prop):
    """Calculate the sparsity of a given property/field, grouped by FINAL_DIAGNOSIS.PRIMDXDSCAT"""
    return df.groupby("FINAL_DIAGNOSIS.PRIMDXDSCAT")[prop].apply(lambda x: x.isna().sum() / len(x)).reset_index(name="sparsity")

# fucntion to compare two dataframes and return the differences
# i.e. 
# new properties/fields and removed properties/fields
# new upis and removed upis
# new forms (e.g. split header by "." and take first element) and removed forms
# sparsity of props, e.g. how many non-null and null values for each prop, grouped by field FINAL_DIAGNOSIS.PRIMDXDSCAT
@task(name="Compare Dataframes", log_prints=True)
def compare_dataframes(df1, df2):
    # get upis
    upis1 = set(df1["upi"].unique())
    upis2 = set(df2["upi"].unique())
    new_upis = upis2 - upis1
    removed_upis = upis1 - upis2

    # get properties/fields
    props1 = set(df1.columns)
    props2 = set(df2.columns)
    new_props = props2 - props1
    removed_props = props1 - props2

    # get forms
    forms1 = set([col.split(".")[0] for col in df1.columns])
    forms2 = set([col.split(".")[0] for col in df2.columns])
    new_forms = forms2 - forms1
    removed_forms = forms1 - forms2

    # get sparsity of props, grouped by field FINAL_DIAGNOSIS.PRIMDXDSCAT
    # then group props into groups of 0-25% sparsity, 25-50% sparsity, 50-75% sparsity and 75-100% sparsity
    
    # convert values "" or "NA" to NaN for the purpose of calculating sparsity
    df1 = df1.replace(["", "NA"], pd.NA)
    df2 = df2.replace(["", "NA"], pd.NA)
    
    sparsity_df = pd.DataFrame(columns=["prop", "FINAL_DIAGNOSIS.PRIMDXDSCAT",  "sparsity_old_tranche", "sparsity_new_tranche"])
    for prop in props1.intersection(props2):
        sparsity_old = sparsity_of_prop(df1, prop)
        sparsity_new = sparsity_of_prop(df2, prop)
        sparsity_temp = pd.merge(sparsity_old, sparsity_new, on="FINAL_DIAGNOSIS.PRIMDXDSCAT", suffixes=("_old", "_new"))
        sparsity_temp["prop"] = prop
        sparsity_temp = sparsity_temp[["prop", "FINAL_DIAGNOSIS.PRIMDXDSCAT", "sparsity_old", "sparsity_new"]].rename(columns={"sparsity_old": "sparsity_old_tranche", "sparsity_new": "sparsity_new_tranche", "FINAL_DIAGNOSIS.PRIMDXDSCAT": "MCI_substudy"})
        sparsity_df = pd.concat([sparsity_df, sparsity_temp], ignore_index=True)
        
    # add col called "sparsity_change" to indicate if the sparsity has increased, decreased or stayed the same between the two tranches
    def sparsity_change(row):
        if row["sparsity_new_tranche"] > row["sparsity_old_tranche"]:
            return "increased"
        elif row["sparsity_new_tranche"] < row["sparsity_old_tranche"]:
            return "decreased"
        else:
            return "same"
    sparsity_df["sparsity_change"] = sparsity_df.apply(sparsity_change, axis=1)
    
    # group props into groups of 0-25% sparsity, 25-50% sparsity, 50-75% sparsity and 75-100% sparsity for both old and new tranche
    def sparsity_group(sparsity):
        if sparsity <= 0.25:
            return "0-25%"
        elif sparsity <= 0.5:
            return "25-50%"
        elif sparsity <= 0.75:
            return "50-75%"
        else:
            return "75-100%"
    sparsity_df["sparsity_group_old_tranche"] = sparsity_df["sparsity_old_tranche"].apply(sparsity_group)
    sparsity_df["sparsity_group_new_tranche"] = sparsity_df["sparsity_new_tranche"].apply(sparsity_group)


    return {
        "new_upis": new_upis,
        "removed_upis": removed_upis,
        "new_props": new_props,
        "removed_props": removed_props,
        "new_forms": new_forms,
        "removed_forms": removed_forms,
        "sparsity_df": sparsity_df,
    }



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
    output_path: str,
):
    logger = get_run_logger()
    dt = get_time()

    # download the TSV files 
    file_dl(bucket, old_tranche_path)
    file_dl(bucket, new_tranche_path)
    
    old_tranche = os.path.basename(old_tranche_path)
    new_tranche = os.path.basename(new_tranche_path)
    
    logger.info(f"Reading TSV files: {old_tranche} and {new_tranche}")
    df1 = read_tsv(old_tranche)
    df2 = read_tsv(new_tranche)

    logger.info("Comparing dataframes")
    comparison_results = compare_dataframes(df1, df2)

    # write out the differences to a log file
    log_file_path = os.path.join(f"cog_comparer_log_{dt}.txt")
    with open(log_file_path, "w+") as log_file:
        log_file.write(f"Comparison of COG CRF forms and fields between tranches:\n")
        log_file.write(f"Old Tranche: {old_tranche}\n")
        log_file.write(f"New Tranche: {new_tranche}\n\n")       
        log_file.write(f"New UPIs Count: {len(comparison_results['new_upis'])}\n")
        log_file.write(f"Removed UPIs Count: {len(comparison_results['removed_upis'])}\n")
        log_file.write(f"New Forms Count: {len(comparison_results['new_forms'])}\n")
        log_file.write(f"Removed Forms Count: {len(comparison_results['removed_forms'])}\n")
        log_file.write(f"New Properties/Fields Count: {len(comparison_results['new_props'])}\n")
        log_file.write(f"Removed Properties/Fields Count: {len(comparison_results['removed_props'])}\n\n")
        log_file.write(f"New UPIs: {sorted(comparison_results['new_upis'])}\n\n")
        log_file.write(f"Removed UPIs: {sorted(comparison_results['removed_upis'])}\n\n")
        log_file.write(f"New Forms: {len(comparison_results['new_forms'])} - {sorted(comparison_results['new_forms'])}\n\n")
        log_file.write(f"Removed Forms: {len(comparison_results['removed_forms'])} - {sorted(comparison_results['removed_forms'])}\n\n")
        log_file.write(f"New Properties/Fields: {len(comparison_results['new_props'])} - {sorted(comparison_results['new_props'])}\n\n")
        log_file.write(f"Removed Properties/Fields: {len(comparison_results['removed_props'])} - {sorted(comparison_results['removed_props'])}\n\n")
    log_file.close()
    
    # save the sparsity dataframe to a TSV file and upload to output path
    sparsity_file_path = os.path.join(f"sparsity_comparison_{dt}.xlsx")
    comparison_results['sparsity_df'].to_excel(sparsity_file_path, index=False)
        
    # upload file
    file_ul(bucket, output_path, f"cog_compare_{dt}", log_file_path)
    file_ul(bucket, output_path, f"cog_compare_{dt}", sparsity_file_path)