import pandas as pd
import os
from datetime import date
import warnings
from src.utils import get_logger, get_date, get_time
from prefect import flow


@flow(
    name="CCDI_to_Index",
    flow_run_name="CCDI_to_Index_" + f"{get_time()}",
)
def CCDI_to_IndexeRy(manifest_path: str) -> tuple:
    # pull in args as variables
    file_path = manifest_path

    # get logger
    logger = get_logger(loggername="CCDI_to_Index", log_level="info")
    logger.info("The CCDI to Index conversion has begun")

    ##############
    #
    # File name rework
    #
    ##############

    # Determine file ext and abs path
    file_name = os.path.splitext(os.path.split(os.path.relpath(file_path))[1])[0]
    file_ext = os.path.splitext(file_path)[1]
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
    output_file = file_name + "_Index" + todays_date

    ##############
    #
    # Pull Dictionary Page to create node pulls
    #
    ##############

    def read_xlsx(file_path: str, sheet: str):
        # Read in excel file
        warnings.simplefilter(action="ignore", category=UserWarning)
        return pd.read_excel(file_path, sheet, dtype=str)

    index_df = pd.DataFrame()

    ##############
    #
    # Read in data
    #
    ##############

    # create workbook
    ccdi_data = pd.ExcelFile(file_path)

    # create dictionary for dfs
    ccdi_dfs = {}

    # read in dfs and apply to dictionary
    for sheet_name in ccdi_data.sheet_names:
        ccdi_dfs[sheet_name] = read_xlsx(ccdi_data, sheet_name)

    # close ccdi_data object
    ccdi_data.close()

    # ccdi nodes minus information nodes
    info_nodes = ["README and INSTRUCTIONS", "Dictionary", "Terms and Value Sets"]
    ccdi_nodes = [item for item in ccdi_data.sheet_names if item not in info_nodes]

    ## Go through each tab and remove completely empty tabs
    nodes_removed = []

    for node in ccdi_nodes:
        if node in ccdi_dfs:
            # see if the tab contain any data
            test_df = ccdi_dfs[node]
            test_df = test_df.drop("type", axis=1)
            test_df = test_df.dropna(how="all").dropna(how="all", axis=1)
            # if there is no data, drop the node/tab
            if test_df.empty:
                nodes_removed.append(node)
            else:
                pass
        else:
            nodes_removed.append(node)

    logger.info(f"{nodes_removed} tabs are empty")
    ccdi_dfs = {key: ccdi_dfs[key] for key in ccdi_dfs if key not in nodes_removed}

    if "cell_line" not in nodes_removed or "pdx" not in nodes_removed:
        logger.warning(
            "This submission contains 'cell_line' or 'pdx' entries in the submission.\n\tTHIS MAY CAUSE ERRORS IN THE RUN OR RETURNED SUBMISSION FILE.\n\t\tDOUBLE CHECK THAT ALL DATA IS PRESENT IN THE OUTPUT"
        )

    # This was removed as the nodes required for CDS destroys paths that need to be walked to obtain the full data.
    ccdi_to_cds_nodes = [node for node in ccdi_nodes if node not in nodes_removed]

    ### MERGING OF ALL DATA
    # The variable names will be the initials of the node as they are added
    # This will show the addition order in hopes to keep the graph walking logic correct and consistent.
    # We are opting for this more structured hard coded approach as more fuzzy walking logics tend to fail or eat up memory.

    # function to make dropping of columns easy and customizable.
    def drop_type_id_others(data_frame, others_list=[]):
        if "type" in data_frame.columns:
            data_frame = data_frame.drop(["type"], axis=1)
        if "id" in data_frame.columns:
            data_frame = data_frame.drop(["id"], axis=1)
        if any(data_frame.columns.str.contains("\.id")):
            dot_id_cols = data_frame.columns[
                data_frame.columns.str.contains("\.id")
            ].tolist()
            for dot_id_col in dot_id_cols:
                data_frame = data_frame.drop([dot_id_col], axis=1)
        if others_list:
            for other in others_list:
                if other in data_frame.columns:
                    data_frame = data_frame.drop([other], axis=1)
        return data_frame

    # start with study
    df_all = drop_type_id_others(ccdi_dfs["study"])

    # make note of columns to remap, so that joins are easier
    col_remap = {
        "study.study_id": "study_id",
        "participant.participant_id": "participant_id",
        "sample.sample_id": "sample_id",
        "pdx.pdx_id": "pdx_id",
        "cell_line.cell_line_id": "cell_line_id",
    }

    # add study_admin
    if "study_admin" in ccdi_to_cds_nodes:
        df_node = drop_type_id_others(ccdi_dfs["study_admin"])
        df_node.rename(columns=col_remap, inplace=True)
        df_all = pd.merge(df_all, df_node, how="left", on="study_id")

    # add study_personnel
    if "study_personnel" in ccdi_to_cds_nodes:
        df_node = drop_type_id_others(ccdi_dfs["study_personnel"])
        df_node.rename(columns=col_remap, inplace=True)
        df_all = pd.merge(df_all, df_node, how="left", on="study_id")

    # pull out df for study
    df_study_level = df_all

    # add participant
    if "participant" in ccdi_to_cds_nodes:
        df_node = drop_type_id_others(ccdi_dfs["participant"])
        df_node.rename(columns=col_remap, inplace=True)
        df_all = pd.merge(df_all, df_node, how="left", on="study_id")

    # add diagnosis
    if "diagnosis" in ccdi_to_cds_nodes:
        df_node = drop_type_id_others(ccdi_dfs["diagnosis"])
        df_node.rename(columns=col_remap, inplace=True)
        df_all = pd.merge(df_all, df_node, how="left", on="participant_id")

    # pull out df for diagnosis
    df_participant_level = df_all

    # ALL [node]_file nodes will need to be concatenated first so there are no conflicts on common column names:
    # file_name, file_type, dcf_indexd_guid, etc etc etc

    df_file = pd.DataFrame()

    if "radiology_file" in ccdi_to_cds_nodes:
        df_file = pd.concat([df_file, ccdi_dfs["radiology_file"]], ignore_index=True)

    if "sequencing_file" in ccdi_to_cds_nodes:
        df_file = pd.concat([df_file, ccdi_dfs["sequencing_file"]], ignore_index=True)

    if "methylation_array_file" in ccdi_to_cds_nodes:
        df_file = pd.concat(
            [df_file, ccdi_dfs["methylation_array_file"]], ignore_index=True
        )

    if "cytogenomic_file" in ccdi_to_cds_nodes:
        df_file = pd.concat([df_file, ccdi_dfs["cytogenomic_file"]], ignore_index=True)

    if "pathology_file" in ccdi_to_cds_nodes:
        df_file = pd.concat([df_file, ccdi_dfs["pathology_file"]], ignore_index=True)

    if "single_cell_sequencing_file" in ccdi_to_cds_nodes:
        df_file = pd.concat(
            [df_file, ccdi_dfs["single_cell_sequencing_file"]], ignore_index=True
        )

    if "clinical_measure_file" in ccdi_to_cds_nodes:
        df_file = pd.concat(
            [df_file, ccdi_dfs["clinical_measure_file"]], ignore_index=True
        )

    # rename the columns based on the col_remap dictionary made earlier
    df_file.rename(columns=col_remap, inplace=True)

    # drop off all the extra properties that are not required for transformation into a flattened data frame
    df_file = drop_type_id_others(df_file)

    # Check df_file to see if there are any rows
    # abort the script if no rows are left.
    if df_file.shape[0] == 0:
        logger.error(
            "No files were found in the submission template. Please add files or ignore the output from this step."
        )
        output_file_path = "(EMPTY)_" + output_file + ".xlsx"
        logger_file_name = "CCDI_to_Index_" + get_date() + ".log"
        return (output_file_path, logger_file_name)
    else:
        pass

    # START WITH FILES INSTEAD AND WALK EACH LINE BACK?
    # Based on each connection possible for the files walk it back manually by starting with the most likely connections

    def join_node(parent_df, child_df, join_by):
        # get rid of book-keeping columns and rename linking columns to node level
        parent_df = drop_type_id_others(parent_df)
        parent_df.rename(columns=col_remap, inplace=True)
        child_df = drop_type_id_others(child_df)
        child_df.rename(columns=col_remap, inplace=True)
        # get rid of rows that are empty for the join by key
        child_df = child_df.dropna(subset=[join_by])
        # merge data frames
        df_joined = pd.merge(parent_df, child_df, how="left", on=join_by)
        return df_joined

    def join_file_node_cleaner(node_df):
        # clean up data frame if there are _x and _y columns
        # always giving preference to _x data frame which should be the parent data frame
        for col in node_df.columns.tolist():
            if col.endswith("_x"):
                col_x = col
                col_base = col[:-2]
                col_y = col_base + "_y"
                node_df[col_base] = node_df[col_x].combine_first(node_df[col_y])
                node_df.drop(columns=[col_x, col_y], inplace=True)

        # clean up the data frame, drop empty columns, rows that don't have files, reset index and remove duplicates.
        if "file_url_in_cds" in node_df.columns:
            node_df = node_df.dropna(subset=["file_url_in_cds"])
        node_df = node_df.dropna(axis=1, how="all")
        node_df = node_df.reset_index(drop=True)
        node_df = node_df.drop_duplicates()
        return node_df

    # Do an empty check on the parent nodes before trying to join them
    # Since the final concatenation needs to have an object, each data frame will be made before the check to ensure it exists,
    # it will be removed from the final addition if it is an empty data frame.

    # file --> sample
    sample_file = pd.DataFrame()
    if "sample" in ccdi_to_cds_nodes:
        if "sample_id" in df_file.columns:
            sample_file = join_node(ccdi_dfs["sample"], df_file, "sample_id")
            sample_file = join_file_node_cleaner(sample_file)

    # file --> pdx
    pdx_file = pd.DataFrame()
    if "pdx" in ccdi_to_cds_nodes:
        if "pdx_id" in df_file.columns:
            pdx_file = join_node(ccdi_dfs["pdx"], df_file, "pdx_id")
            pdx_file = join_file_node_cleaner(pdx_file)

    # file --> cell_line
    cell_line_file = pd.DataFrame()
    if "cell_line" in ccdi_to_cds_nodes:
        if "cell_line_id" in df_file.columns:
            cell_line_file = join_node(ccdi_dfs["cell_line"], df_file, "cell_line_id")
            cell_line_file = join_file_node_cleaner(cell_line_file)

    # file --> participant
    participant_file = pd.DataFrame()
    if not df_participant_level.empty:
        if "participant_id" in df_file.columns:
            participant_file = join_node(
                df_participant_level, df_file, "participant_id"
            )
            participant_file = join_file_node_cleaner(participant_file)

    # file --> study
    study_file = pd.DataFrame()
    if not df_study_level.empty:
        if "study_id" in df_file.columns:
            study_file = join_node(df_study_level, df_file, "study_id")
            study_file = join_file_node_cleaner(study_file)

    # Then for the nodes that don't end on either sample, participant or study, walk again and check.
    # This is likely to be deprecated later as having all files go to sample is the preferred method.

    # pdx_file --> sample
    sample_pdx_file = pd.DataFrame()
    if not pdx_file.empty:
        if "sample_id" in pdx_file.columns:
            sample_pdx_file = join_node(ccdi_dfs["sample"], pdx_file, "sample_id")
            sample_pdx_file = join_file_node_cleaner(sample_pdx_file)

    # pdx_file --> study
    study_pdx_file = pd.DataFrame()
    if not pdx_file.empty:
        if "study_id" in pdx_file.columns:
            study_pdx_file = join_node(df_study_level, pdx_file, "study_id")
            study_pdx_file = join_file_node_cleaner(study_pdx_file)

    # cell_line_file --> sample
    sample_cell_line_file = pd.DataFrame()
    if not cell_line_file.empty:
        if "sample_id" in cell_line_file.columns:
            sample_cell_line_file = join_node(
                ccdi_dfs["sample"], cell_line_file, "sample_id"
            )
            sample_cell_line_file = join_file_node_cleaner(sample_cell_line_file)

    # cell_line_file --> participant
    participant_cell_line_file = pd.DataFrame()
    if not cell_line_file.empty:
        if "participant_id" in cell_line_file.columns:
            participant_cell_line_file = join_node(
                df_participant_level, cell_line_file, "participant_id"
            )
            participant_cell_line_file = join_file_node_cleaner(
                participant_cell_line_file
            )

    # cell_line_file --> study
    study_cell_line_file = pd.DataFrame()
    if not cell_line_file.empty:
        if "study_id" in cell_line_file.columns:
            study_cell_line_file = join_node(df_study_level, cell_line_file, "study_id")
            study_cell_line_file = join_file_node_cleaner(study_cell_line_file)

    # If not all samples can link to a participant, then handle those exceptions

    # sample_file --> pdx
    pdx_sample_file = pd.DataFrame()
    if not sample_file.empty:
        if "pdx_id" in sample_file.columns:
            pdx_sample_file = join_node(ccdi_dfs["pdx"], sample_file, "pdx_id")
            pdx_sample_file = join_file_node_cleaner(pdx_sample_file)

    # sample_file--> cell_line
    cell_line_sample_file = pd.DataFrame()
    if not sample_file.empty:
        if "cell_line_id" in sample_file.columns:
            cell_line_sample_file = join_node(
                ccdi_dfs["cell_line"], sample_file, "cell_line_id"
            )
            cell_line_sample_file = join_file_node_cleaner(cell_line_sample_file)

    # pdx_sample_file --> sample
    sample_pdx_sample_file = pd.DataFrame()
    # For entries that go from file-->sample-->[pdx/cell_line]-->sample
    # We have to remove the previous sample_ids in the file as they are confusing the join

    if not pdx_sample_file.empty:
        if "sample_id" in pdx_sample_file.columns:
            sample_pdx_sample_file = join_node(
                ccdi_dfs["sample"], pdx_sample_file, "sample_id"
            )
            sample_pdx_sample_file = join_file_node_cleaner(sample_pdx_sample_file)

    # pdx_sample_file --> study
    study_pdx_sample_file = pd.DataFrame()
    if not pdx_file.empty:
        if "study_id" in pdx_sample_file.columns:
            study_pdx_sample_file = join_node(
                df_study_level, pdx_sample_file, "study_id"
            )
            study_pdx_sample_file = join_file_node_cleaner(study_pdx_sample_file)

    # cell_line_sample_file --> sample
    sample_cell_line_sample_file = pd.DataFrame()
    if not cell_line_sample_file.empty:
        if "sample_id" in cell_line_sample_file.columns:
            sample_cell_line_sample_file = join_node(
                ccdi_dfs["sample"], cell_line_sample_file, "sample_id"
            )
            sample_cell_line_sample_file = join_file_node_cleaner(
                sample_cell_line_sample_file
            )

    # cell_line_sample_file --> participant
    participant_cell_line_sample_file = pd.DataFrame()
    if not cell_line_sample_file.empty:
        if "participant_id" in cell_line_sample_file.columns:
            participant_cell_line_sample_file = join_node(
                df_participant_level, cell_line_sample_file, "participant_id"
            )
            participant_cell_line_sample_file = join_file_node_cleaner(
                participant_cell_line_sample_file
            )

    # cell_line_sample_file --> study
    study_cell_line_sample_file = pd.DataFrame()
    if not cell_line_sample_file.empty:
        if "study_id" in cell_line_sample_file.columns:
            study_cell_line_sample_file = join_node(
                df_study_level, cell_line_sample_file, "study_id"
            )
            study_cell_line_sample_file = join_file_node_cleaner(
                study_cell_line_sample_file
            )

    # It is not likely to loop more than once as that would mean a pdx or cell_line was made from a sample of a pdx or cell_line.

    # Then handle the concatenation of the three main node end points:
    # Get everything up to sample, participant or study.
    # For all samples, we will join them with participant, which inherently has study information.
    # Then we will concatenate all the dfs into one large df, clean it up and we should get the same number of unique files.
    # Flattening files might cause duplicate rows, but this duplication is likely to leave when we pull out elements that are not usually multiple elements.

    ### INFO ###
    # List of Outputs from the different joins
    # # file --> sample
    # sample_file
    # # file --> pdx
    # pdx_file
    # # file --> cell_line
    # cell_line_file
    # # file --> participant
    # participant_file
    # # file --> study
    # study_file
    # # pdx_file --> sample
    # sample_pdx_file
    # # pdx_file --> study
    # study_pdx_file
    # # cell_line_file --> sample
    # sample_cell_line_file
    # # cell_line_file --> participant
    # participant_cell_line_file
    # # cell_line_file --> study
    # study_cell_line_file
    # # sample_file --> pdx
    # pdx_sample_file
    # # sample_file--> cell_line
    # cell_line_sample_file
    # # pdx_sample_file --> sample
    # sample_pdx_sample_file
    # # pdx_sample_file --> study
    # study_pdx_sample_file
    # # cell_line_sample_file --> sample
    # sample_cell_line_sample_file
    # # cell_line_sample_file --> participant
    # participant_cell_line_sample_file
    # # cell_line_sample_file --> study
    # study_cell_line_sample_file
    ### INFO ###

    ### INFO ###
    # List of outputs from the different joins sorted by end points

    # #PDX
    # # file --> pdx
    # pdx_file
    # # sample_file --> pdx
    # pdx_sample_file

    # #Cell_line
    # # file --> cell_line
    # cell_line_file
    # # sample_file--> cell_line
    # cell_line_sample_file

    # #Sample
    # # file --> sample
    # sample_file
    # # pdx_file --> sample
    # sample_pdx_file
    # # cell_line_file --> sample
    # sample_cell_line_file
    # # pdx_sample_file --> sample
    # sample_pdx_sample_file
    # # cell_line_sample_file --> sample
    # sample_cell_line_sample_file

    # #Participant
    # # file --> participant
    # participant_file
    # # cell_line_file --> participant
    # participant_cell_line_file
    # # cell_line_sample_file --> participant
    # participant_cell_line_sample_file

    # #Study
    # # file --> study
    # study_file
    # # pdx_file --> study
    # study_pdx_file
    # # cell_line_file --> study
    # study_cell_line_file
    # # pdx_sample_file --> study
    # study_pdx_sample_file
    # # cell_line_sample_file --> study
    # study_cell_line_sample_file
    ### INFO ###

    # The PDX and Cell_line outputs can be ignored as they must go to sample, participant or study.
    # The participant and study can be set aside as they should be able to be concatenated at the end.

    # Take the samples and attach to df_participant_level or df_study_level

    # Due to sample_type being a property that CCDI does not have and instead CCDI anatomic_site is used instead
    # we run into an issue where there are two anatomic_site properties that are being used, one in sample and the other in diagnosis.
    # In this context it makes more sense to use the samples.anatomic_site, which is counter to how the script handles most other conflicts.
    # The script treats the parent node's properties as the value to overwrite with, but in this one case we have to switch this to be more inline
    # with CDS, and hopefully prevent duplicate values.

    # Thus for the following node joins before the join_file_node_cleaner, part of that code will be used to do the reverse clean up ONLY for anatomic_site
    # and only when it is linking to the participant level.

    # sample_file --> participant
    participant_sample_file = pd.DataFrame()
    if not sample_file.empty:
        if "participant_id" in sample_file.columns:
            participant_sample_file = join_node(
                df_participant_level, sample_file, "participant_id"
            )

            # Handle the reverse situation of anatomic_site
            if "anatomic_site_x" in participant_sample_file.columns:
                col_x = "anatomic_site_x"
                col_base = col_x[:-2]
                col_y = col_base + "_y"
                participant_sample_file[col_base] = participant_sample_file[
                    col_y
                ].combine_first(participant_sample_file[col_x])
                participant_sample_file.drop(columns=[col_x, col_y], inplace=True)

            participant_sample_file = join_file_node_cleaner(participant_sample_file)

    # sample_pdx_file --> participant
    participant_sample_pdx_file = pd.DataFrame()
    if not sample_pdx_file.empty:
        if "participant_id" in sample_pdx_file.columns:
            participant_sample_pdx_file = join_node(
                df_participant_level, sample_pdx_file, "participant_id"
            )

            # Handle the reverse situation of anatomic_site
            if "anatomic_site_x" in participant_sample_pdx_file.columns:
                col_x = "anatomic_site_x"
                col_base = col_x[:-2]
                col_y = col_base + "_y"
                participant_sample_pdx_file[col_base] = participant_sample_pdx_file[
                    col_y
                ].combine_first(participant_sample_pdx_file[col_x])
                participant_sample_pdx_file.drop(columns=[col_x, col_y], inplace=True)

            participant_sample_pdx_file = join_file_node_cleaner(
                participant_sample_pdx_file
            )

    # sample_cell_line_file --> participant
    participant_sample_cell_line_file = pd.DataFrame()
    if not sample_cell_line_file.empty:
        if "participant_id" in sample_cell_line_file.columns:
            participant_sample_cell_line_file = join_node(
                df_participant_level, sample_cell_line_file, "participant_id"
            )

            # Handle the reverse situation of anatomic_site
            if "anatomic_site_x" in participant_sample_cell_line_file.columns:
                col_x = "anatomic_site_x"
                col_base = col_x[:-2]
                col_y = col_base + "_y"
                participant_sample_cell_line_file[col_base] = (
                    participant_sample_cell_line_file[col_y].combine_first(
                        participant_sample_cell_line_file[col_x]
                    )
                )
                participant_sample_cell_line_file.drop(
                    columns=[col_x, col_y], inplace=True
                )

            participant_sample_cell_line_file = join_file_node_cleaner(
                participant_sample_cell_line_file
            )

    # sample_pdx_sample_file --> participant
    participant_sample_pdx_sample_file = pd.DataFrame()
    if not sample_pdx_sample_file.empty:
        if "participant_id" in sample_pdx_sample_file.columns:
            participant_sample_pdx_sample_file = join_node(
                df_participant_level, sample_pdx_sample_file, "participant_id"
            )

            # Handle the reverse situation of anatomic_site
            if "anatomic_site_x" in participant_sample_pdx_sample_file.columns:
                col_x = "anatomic_site_x"
                col_base = col_x[:-2]
                col_y = col_base + "_y"
                participant_sample_pdx_sample_file[col_base] = (
                    participant_sample_pdx_sample_file[col_y].combine_first(
                        participant_sample_pdx_sample_file[col_x]
                    )
                )
                participant_sample_pdx_sample_file.drop(
                    columns=[col_x, col_y], inplace=True
                )

            participant_sample_pdx_sample_file = join_file_node_cleaner(
                participant_sample_pdx_sample_file
            )

    # sample_cell_line_sample_file --> participant
    participant_sample_cell_line_sample_file = pd.DataFrame()
    if not sample_cell_line_sample_file.empty:
        if "participant_id" in sample_cell_line_sample_file.columns:
            participant_sample_cell_line_sample_file = join_node(
                df_participant_level, sample_cell_line_sample_file, "participant_id"
            )

            # Handle the reverse situation of anatomic_site
            if "anatomic_site_x" in participant_sample_cell_line_sample_file.columns:
                col_x = "anatomic_site_x"
                col_base = col_x[:-2]
                col_y = col_base + "_y"
                participant_sample_cell_line_sample_file[col_base] = (
                    participant_sample_cell_line_sample_file[col_y].combine_first(
                        participant_sample_cell_line_sample_file[col_x]
                    )
                )
                participant_sample_cell_line_sample_file.drop(
                    columns=[col_x, col_y], inplace=True
                )

            participant_sample_cell_line_sample_file = join_file_node_cleaner(
                participant_sample_cell_line_sample_file
            )

    # sample_pdx_sample_file --> study
    study_sample_pdx_sample_file = pd.DataFrame()
    if not sample_pdx_sample_file.empty:
        if "study_id" in sample_pdx_sample_file.columns:
            study_sample_pdx_sample_file = join_node(
                df_study_level, sample_pdx_sample_file, "study_id"
            )
            study_sample_pdx_sample_file = join_file_node_cleaner(
                study_sample_pdx_sample_file
            )

    # List of all paths that can be derived from files that either end at study or participant (which has study information)
    all_paths = [
        participant_file,
        participant_cell_line_file,
        participant_cell_line_sample_file,
        participant_sample_file,
        participant_sample_pdx_file,
        participant_sample_cell_line_file,
        participant_sample_pdx_sample_file,
        participant_sample_cell_line_sample_file,
        study_file,
        study_pdx_file,
        study_cell_line_file,
        study_pdx_sample_file,
        study_cell_line_sample_file,
        study_sample_pdx_sample_file,
    ]

    df_join_all = pd.DataFrame()

    for node_path in all_paths:
        if not node_path.empty:
            df_join_all = pd.concat([df_join_all, node_path], axis=0)

    # To reduce complexity in the conversion, only lines where the personnel type is PI will be used in the CDS template end file. Otherwise use Co-PI or just pass if nothing else or too complex.
    if "PI" in df_join_all["personnel_type"].unique().tolist():
        df_join_all = df_join_all[df_join_all["personnel_type"] == "PI"]
    elif "Co-PI" in df_join_all["personnel_type"].unique().tolist():
        df_join_all = df_join_all[df_join_all["personnel_type"] == "Co-PI"]
    else:
        pass

    # Drop the current index as it is causing issues
    df_join_all = df_join_all.reset_index(drop=True)

    # To try and preserve synonym links this section will join the synonyms if the data exists.
    if "synonym" in ccdi_to_cds_nodes:
        synonym_df = ccdi_dfs["synonym"]
        synonym_df.rename(columns=col_remap, inplace=True)

        if "participant_id" in synonym_df.columns:
            df_join_all = join_node(df_join_all, synonym_df, "participant_id")
            df_join_all = join_file_node_cleaner(df_join_all)
            # now we need to move participant_ids that are related to dbGaP over to the property `dbGaP_subject_id`
            # if statment because we are not asssured that links are correctly made
            if "repository_of_synonym_id" in df_join_all.columns:
                df_join_all["dbGaP_subject_id"] = df_join_all.loc[
                    df_join_all["repository_of_synonym_id"] == "dbGaP", "synonym_id"
                ]

        if "synonym_id" in df_join_all.columns:
            df_join_all = df_join_all.drop("synonym_id", axis=1)
        if "repository_of_synonym_id" in df_join_all.columns:
            df_join_all = df_join_all.drop("repository_of_synonym_id", axis=1)

        if "sample_id" in synonym_df.columns:
            df_join_all = join_node(df_join_all, synonym_df, "sample_id")
            df_join_all = join_file_node_cleaner(df_join_all)
            # now we need to move sample_ids that are related to BioSample over to the property `dbGaP_subject_id`
            # if statment because we are not asssured that links are correctly made
            if "repository_of_synonym_id" in df_join_all.columns:
                df_join_all["biosample_accession"] = df_join_all.loc[
                    df_join_all["repository_of_synonym_id"] == "BioSample", "synonym_id"
                ]

    # Drop the current index as it is causing issues
    df_join_all = df_join_all.reset_index(drop=True)

    ###############
    #
    # CCDI to CDS Indexing required columns hard coding
    #
    ###############

    # From the df_join_all data frame, make either 1:1 mappings, rework mappings for different property names
    # or transform the data to match the new mapping setups.

    # for simple 1:1 mappings even if the property names are different, a simple function to handle it:
    def simple_add(index_prop, ccdi_prop):
        if ccdi_prop in df_join_all:
            index_df[index_prop] = df_join_all[ccdi_prop]

        return

    # add the subset of columns needed for DCF indexing
    simple_add("guid", "dcf_indexd_guid")
    simple_add("md5", "md5sum")
    simple_add("size", "file_size")
    simple_add("acl", "acl")

    if "authz" in df_join_all.columns:
        index_df["authz"] = df_join_all["authz"]
    else:
        authz = df_join_all["acl"].dropna().unique().tolist()[0]
        authz = "['/programs/" + authz[2:]
        index_df["authz"] = authz

    simple_add("url", "file_url_in_cds")

    # add information for CGC
    # study information
    simple_add("phs_accession", "phs_accession")

    if "study_name" in df_join_all.columns:
        index_df["study_name"] = df_join_all["study_name"]
        # if there isn't a study_name
    else:
        index_df["study_name"] = df_join_all["study_short_title"]
    simple_add("study_acronym", "study_acronym")
    simple_add(
        "experimental_strategy_and_data_subtype",
        "experimental_strategy_and_data_subtype",
    )
    simple_add("organism", "organism_species")

    # participant information
    simple_add("participant_id", "participant_id")
    simple_add("dbGaP_subject_id", "dbGaP_subject_id")

    if "gender" in df_join_all:
        simple_add("gender", "gender")
    elif "sex_at_birth" in df_join_all:
        simple_add("gender", "sex_at_birth")

    simple_add("race", "race")
    simple_add("ethnicity", "ethnicity")

    # sample information
    simple_add("sample_id", "sample_id")
    simple_add("biosample_accession", "biosample_accession")
    simple_add("sample_tumor_status", "sample_tumor_status")
    simple_add("age_at_collection", "participant_age_at_collection")
    simple_add("sample_anatomic_site", "anatomic_site")
    simple_add("sample_description", "sample_description")

    # file information
    simple_add("file_name", "file_name")
    simple_add("file_type", "file_type")
    simple_add("file_size", "file_size")
    simple_add("md5sum", "md5sum")
    simple_add("file_url_in_cds", "file_url_in_cds")

    # library information
    simple_add("library_id", "library_id")
    simple_add("library_strategy", "library_strategy")
    simple_add("library_source", "library_source")
    simple_add("library_selection", "library_selection")
    simple_add("library_layout", "library_layout")
    simple_add("platform", "platform")
    simple_add("instrument_model", "instrument_model")
    simple_add("design_description", "design_description")
    simple_add("reference_genome_assembly", "reference_genome_assembly")
    simple_add("bases", "number_of_bp")
    simple_add("number_of_reads", "number_of_reads")
    simple_add("coverage", "coverage")
    simple_add("avg_read_length", "avg_read_length")
    simple_add("sequence_alignment_software", "sequence_alignment_software")

    simple_add("methylation_platform", "methylation_platform")
    simple_add("reporter_label", "reporter_label")

    # diagnosis information
    simple_add("diagnosis_id", "diagnosis_id")
    # Not a perfect match, some logic required depending on version of CCDI
    # Also need to deconstruct the setup from `code : term` ----> `term`
    if "diagnosis_icd_o" in df_join_all.columns:
        index_df["primary_diagnosis"] = df_join_all["diagnosis_icd_o"]
    elif "diagnosis_classification" in df_join_all.columns:
        index_df["primary_diagnosis"] = df_join_all["diagnosis_classification"]
        # further diagnosis handling for "see diagnosis_comment" copying
        if "diagnosis_comment" in df_join_all:
            comment_true = index_df["primary_diagnosis"] == "see diagnosis_comment"
            index_df.loc[comment_true, "primary_diagnosis"] = df_join_all.loc[
                comment_true, "diagnosis_comment"
            ]
    else:
        logger.error("No 'primary_diagnosis' was transfered")

    simple_add("tumor_grade", "tumor_grade")
    simple_add("tumor_stage_clinical_t", "tumor_stage_clinical_t")
    simple_add("tumor_stage_clinical_n", "tumor_stage_clinical_n")
    simple_add("tumor_stage_clinical_m", "tumor_stage_clinical_m")

    # Remove any rows where there is not a file associated with the entry
    index_df = index_df.dropna(subset=["file_url_in_cds"])

    index_df = index_df.drop_duplicates()

    ##############
    #
    # Roll-up
    #
    ##############

    # Group by "GUID" and aggregate other columns
    index_df = index_df.groupby("GUID").agg(lambda x: list(x))

    # Reset index to make 'GUID' a column again
    index_df = index_df.reset_index()

    # Double check for duplicates again.
    index_df = index_df.drop_duplicates()

    ##############
    #
    # Stat check
    #
    ##############

    # Quick stats to check the conversion, make sure things are working as we think they should be.
    file_expected = len(
        df_file.loc[:, ["md5sum", "file_name", "file_url_in_cds"]].drop_duplicates()
    )
    file_returned = len(
        index_df.loc[:, ["md5sum", "file_name", "file_url_in_cds"]].drop_duplicates()
    )
    logger.info(
        f"For the following conversion:\n\tUnique files expected: {file_expected}\n\tUnique files returned: {file_returned}"
    )

    if file_expected == file_returned:
        logger.info(
            "The conversion was likely a success, please double check your files"
        )
    elif file_expected != file_returned:
        logger.warning(
            "The conversion was likely NOT successful, as it did not return the same number of unique files"
        )

    ##############
    #
    # Write out
    #
    ##############

    logger.info("Writing out the Index TSV file")

    # save out tsv
    index_df.to_csv(f"{file_dir_path}/{output_file}.tsv", sep="\t", index=False)

    logger.info(
        f"Process Complete. The output file can be found here: {file_dir_path}/{output_file}"
    )

    output_file_path = output_file + ".tsv"
    logger_file_name = "CCDI_to_Index_" + get_date() + ".log"

    return (output_file_path, logger_file_name)
