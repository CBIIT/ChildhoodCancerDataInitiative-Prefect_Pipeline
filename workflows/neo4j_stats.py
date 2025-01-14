import pandas as pd
import os
from prefect import flow, get_run_logger
from src.utils import get_time, file_ul, file_dl
from src.neo4j_data_tools import (
    StatsNeo4jCypherQuery,
    stats_pull_graph_data_study,
    stats_pull_graph_data_nodes,
    cypher_query_parameters,
)


@flow(
    name="Neo4j Stats",
    log_prints=True,
    flow_run_name="neo4j-stats-{runner}-" + f"{get_time()}",
)
def pull_neo4j_stats(
    bucket: str,
    runner: str,
    uri_parameter: str = "uri",
    username_parameter: str = "username",
    password_parameter: str = "password",
    additional_info_file: str = "",
):
    """Pipeline that pulls specific stats from ingested studies from a Neo4j database

    Args:
        bucket (str): Bucket name of where output goes to
        runner (str): Unique runner name
        uri_parameter (str, optional): uri parameter. Defaults to "uri".
        username_parameter (str, optional): username parameter. Defaults to "username".
        password_parameter (str, optional): password parameter. Defaults to "password".
        additional_info_file (str, optional): An extra information tsv file with one line per study in a study_id column.
    """

    logger = get_run_logger()

    bucket_folder = runner + "/db_stats_outputs_" + get_time()

    output_file = f"db_stats_{get_time()}.tsv"

    logger.info("Getting uri, username and password parameter from AWS")
    # get uri, username, and password value
    uri, username, password = cypher_query_parameters(
        uri_parameter=uri_parameter,
        username_parameter=username_parameter,
        password_parameter=password_parameter,
        logger=logger,
    )

    # Run the queries
    pi_df = stats_pull_graph_data_study(
        uri,
        username,
        password,
        StatsNeo4jCypherQuery.stats_get_pi_query,
        "PI",
    )

    institution_df = stats_pull_graph_data_study(
        uri,
        username,
        password,
        StatsNeo4jCypherQuery.stats_get_institution_query,
        "institution",
    )

    bucket_df = stats_pull_graph_data_study(
        uri,
        username,
        password,
        StatsNeo4jCypherQuery.stats_get_study_buckets,
        "bucket",
    )

    study_size_df = stats_pull_graph_data_study(
        uri,
        username,
        password,
        StatsNeo4jCypherQuery.stats_get_study_file_size,
        "file_size(Tb)",
    )

    node_count_df = stats_pull_graph_data_nodes(
        uri,
        username,
        password,
        StatsNeo4jCypherQuery.stats_get_study_node_counts,
        "node_count",
    )

    file_size_df = stats_pull_graph_data_nodes(
        uri,
        username,
        password,
        StatsNeo4jCypherQuery.stats_get_study_node_file_size,
        "file_size(Tb)",
    )

    library_strategy_df = stats_pull_graph_data_nodes(
        uri,
        username,
        password,
        StatsNeo4jCypherQuery.stats_get_study_library_strategy,
        "library_strategy",
    )

    library_strategy_count_df = stats_pull_graph_data_nodes(
        uri,
        username,
        password,
        StatsNeo4jCypherQuery.stats_get_study_library_strategy_count,
        "file_count",
    )

    library_strategy_size_df = stats_pull_graph_data_nodes(
        uri,
        username,
        password,
        StatsNeo4jCypherQuery.stats_get_study_library_strategy_size,
        "file_size(Tb)",
    )

    study_clinical_df = stats_pull_graph_data_study(
        uri,
        username,
        password,
        StatsNeo4jCypherQuery.stats_get_study_clinical,
        "data_exist",
    )

    study_methylation_array_df = stats_pull_graph_data_study(
        uri,
        username,
        password,
        StatsNeo4jCypherQuery.stats_get_study_methylation_array,
        "data_exist",
    )

    study_cytogenomic_df = stats_pull_graph_data_study(
        uri,
        username,
        password,
        StatsNeo4jCypherQuery.stats_get_study_cytogenomic,
        "data_exist",
    )

    study_pathology_df = stats_pull_graph_data_study(
        uri,
        username,
        password,
        StatsNeo4jCypherQuery.stats_get_study_pathology,
        "data_exist",
    )

    study_radiology_df = stats_pull_graph_data_study(
        uri,
        username,
        password,
        StatsNeo4jCypherQuery.stats_get_study_radiology,
        "data_exist",
    )

    study_file_count_df = stats_pull_graph_data_study(
        uri,
        username,
        password,
        StatsNeo4jCypherQuery.stats_get_study_file_count,
        "count",
    )

    # Concatenate the queries into a data frame   
    build_df = pd.DataFrame(columns=["study_id", "column_name", "value"])

    build_df = pd.concat(
        [
            pi_df,
            institution_df,
            bucket_df,
            study_size_df,
            node_count_df,
            file_size_df,
            library_strategy_df,
            library_strategy_count_df,
            library_strategy_size_df,
            study_clinical_df,
            study_methylation_array_df,
            study_cytogenomic_df,
            study_pathology_df,
            study_radiology_df,
            study_file_count_df,
        ],
        axis=0,
        ignore_index=True,
    )

    build_df = build_df.drop_duplicates()

    # Count duplicates based on 'study_id' and 'column_name'
    duplicate_counts = build_df.groupby(["study_id", "column_name"]).size()
    
    # Filter for cases where there are more than one entry
    duplicates = duplicate_counts[duplicate_counts > 1]
    
    # Print results
    if not duplicates.empty:
        logger.error("Duplicate entries found:")
        logger.error(duplicates)
    else:
        logger.info("No duplicate entries found.")

    # Pivot the DataFrame
    df_wide = build_df.pivot(index="study_id", columns="column_name", values="value")

    # Reset index if you don't want 'study_id' to be the index
    df_wide = df_wide.reset_index()

    # List of column names that contain the string "data_exist"
    columns_with_data_exist = [col for col in df_wide.columns if "data_exist" in col]

    # Columns to move to the front in a specific order
    columns_to_move_to_front = [
        "study_id",
        "study_name",
        "participant_node_count",
        "sample_node_count",
        "study_level_file_count",
        "study_level_file_size(Tb)",
        "sequencing_file_library_strategy",
        "study_personnel_PI",
        "study_personnel_institution",
        "study_bucket",
    ]

    # Move data_exists column list to a desired position
    columns_to_move_to_front[10:10] = columns_with_data_exist

    # Get the remaining columns that are not in columns_to_move
    remaining_columns = [
        col for col in df_wide.columns if col not in columns_to_move_to_front
    ]

    # Identify columns containing the string "file_size(Tb)"
    columns_to_convert = [col for col in df_wide.columns if "file_size(Tb)" in col]

    # Convert the values from Bytes to Terabytes for the identified columns
    for col in columns_to_convert:
        df_wide[col] = df_wide[col].apply(
            lambda x: round(int(x) / 1e12, 3) if pd.notna(x) else x
        )

    # Reorder the DataFrame
    df_wide = df_wide[columns_to_move_to_front + remaining_columns]

    if additional_info_file:
        file_dl(bucket=bucket, filename=additional_info_file)
        extra_df = pd.read_csv(os.path.basename(additional_info_file), sep='\t')
        if 'study_id' not in extra_df.columns:
            logger.warning(f"The file supplied {additional_info_file}, does not contain a `study_id` column.")
        else:
            df_wide = pd.merge(extra_df, df_wide, on= "study_id", how="left")


    # write out
    df_wide.to_csv(output_file, sep="\t", index=False)

    # upload db pulled data csv files and converted tsv files to the bucket
    logger.info(f"Uploading {output_file} to the bucket {bucket} at {bucket_folder}")
    file_ul(
        bucket=bucket,
        output_folder=bucket_folder,
        sub_folder="",
        newfile=output_file,
    )
