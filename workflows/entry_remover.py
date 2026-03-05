#!/usr/bin/env python3
"""
entry_remover.py

Remove specified entries (and any linked “child” entries) from a CCDI metadata manifest.
"""

import os
import datetime
import pandas as pd
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from prefect import get_run_logger, task, flow
from yaml import warnings
from src.utils import file_dl, dl_ccdi_template, get_time, file_ul

@task(name="Drop empty rows/columns")
def drop_empty(df, axis):
    """
    Remove rows (axis=0) or columns (axis=1) that are entirely NA/blank.
    """
    return df.dropna(how="all", axis=axis)

@flow(name="Remover")
def main(file: str, template: str, entry: str):

    logger = get_run_logger()
    # Absolute paths & output names
    manifest_path = file
    entry_path    = entry

    base       = os.path.splitext(os.path.basename(manifest_path))[0]
    today      = get_time()
    out_xlsx   = f"{base}_EntRemove{today}.xlsx"
    log_txt    = f"{base}_EntRemove{today}_log.txt"

    ##############
    #
    # Pull Dictionary Page to create node pulls
    #
    ##############

    logger.info("Reading CCDI template file")

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
                "n/a",
                "nan",
                "null",
            ],
        )

        # Remove leading and trailing whitespace from all cells
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        return df

    # create workbook
    xlsx_model = pd.ExcelFile(manifest_path)

    # create dictionary for dfs
    model_dfs = {}

    # read in dfs and apply to dictionary
    for sheet_name in xlsx_model.sheet_names:
        model_dfs[sheet_name] = read_xlsx(xlsx_model, sheet_name)

    # Read in data
    logger.info("Reading CCDI manifest file")

    # create workbook
    xlsx_data = pd.ExcelFile(manifest_path)

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

    # Go through each tab and remove completely empty tabs
    logger.info("Removing empty tabs from the manifest file")

    for node in dict_nodes:
        # see if the tab contain any data
        test_df = meta_dfs[node]
        test_df = test_df.drop("type", axis=1)
        test_df = test_df.dropna(how="all").dropna(how="all", axis=1)
        # if there is no data, drop the node/tab
        if test_df.empty:
            del meta_dfs[node]

    # determine nodes again
    node_list = set(list(meta_dfs.keys()))


    # 2) Load each node sheet
    NA_bank = ["NA","na","N/A","n/a"]
    workbook_list = {}
    for node in node_list:
        try:
            df = pd.read_excel(
                manifest_path,
                sheet_name=node,
                dtype=str,
                na_values=NA_bank,
                keep_default_na=False,
                engine="openpyxl"
            )
        except ValueError:
            continue

        # --- FIXED here: separate df2 assignment steps ---
        df2 = drop_empty(df, axis=1)  # drop empty cols
        df2 = drop_empty(df2, axis=0)  # drop empty rows

        # require ≥1 “real” column (not just linking cols like “foo.bar_id”)
        real_cols = [c for c in df2.columns if "." not in c]
        if len(df2) > 0 and real_cols:
            workbook_list[node] = df.copy()

    if not workbook_list:
        raise RuntimeError("No valid node sheets found in manifest.")

    # 3) Read entries to remove
    entries_df = pd.read_csv(entry_path, sep="\t", header=None, names=['X1'], dtype=str)
    pending    = entries_df['X1'].dropna().tolist()

    deleted = {node: [] for node in workbook_list.keys()}

    # 4) Iterative removal + logging
    with open(log_txt, "w") as log:
        log.write("Entries to remove (and discovered children):\n")
        log.write("\n".join(pending) + "\n\n")

        while pending:
            curr = pending.pop(0)
            log.write(f"Removing: {curr}\n")
            for node, df in workbook_list.items():
                node_id_col = f"{node}_id"
                # direct hits
                if node_id_col in df.columns:
                    hits = df[df[node_id_col] == curr].index.tolist()
                    if hits:
                        deleted[node].append(curr)
                        log.write(f"  - {curr} dropped from {node}.{node_id_col}\n")
                        df.drop(index=hits, inplace=True)

                # discover child entries
                link_cols = [c for c in df.columns if "." in c and c.endswith("_id")]
                for lc in link_cols:
                    matches = df[df[lc] == curr]
                    for _, row in matches.iterrows():
                        child = row[f"{node}_id"]
                        if child not in deleted[node] and child not in pending:
                            pending.append(child)
                            log.write(f"    => discovered child {child} in {node}.{lc}\n")

        # summary
        log.write("\nSummary of deletions by sheet:\n")
        for node, items in deleted.items():
            log.write(f" {node}: {items}\n")

    # 5) Write out with openpyxl in-place
    wb = load_workbook(manifest_path)
    for node, df in workbook_list.items():
        if node in wb.sheetnames:
            ws = wb[node]
            ws.delete_rows(1, ws.max_row)
        else:
            ws = wb.create_sheet(title=node)
        for r in dataframe_to_rows(df, index=False, header=True):
            ws.append(r)

    # ensure at least one sheet visible
    if not wb.sheetnames:
        wb.create_sheet(title="Sheet1")
    wb.save(out_xlsx)

    print(f"\n✅ Done. Log written to {log_txt}\n   Cleaned workbook: {out_xlsx}\n")

    return out_xlsx, log_txt




@flow(name="CCDI Manifest Entry Remover", flow_run_name="{runner}_" + f"{get_time()}")
def entry_remover(bucket:str,runner:str,file_path: str, entry_removal_file_path:str)-> None:

    logger = get_run_logger()

    output_folder = runner + "/entry_remover_" + get_time()

    # download manifest
    file_dl(filename=file_path, bucket=bucket)
    file_path = os.path.basename(file_path)

    # download tsv of entries to remove
    file_dl(filename=entry_removal_file_path, bucket=bucket)
    entry_removal_file = os.path.basename(entry_removal_file_path)

    out_xlsx, log_txt = main(file=file_path, entry=entry_removal_file_path)

    file_ul (filename=out_xlsx, bucket=bucket, dest_folder=output_folder)
    file_ul (filename=log_txt, bucket=bucket, dest_folder=output_folder)
