import pandas as pd
import os
from prefect import flow, get_run_logger
from src.utils import get_time, file_ul, file_dl, get_secret_centralized_worker
from src.neo4j_data_tools import (
    StatsNeo4jCypherQuery,
    stats_pull_graph_data_study,
    stats_pull_graph_data_nodes,
    cypher_query_parameters,
)


@flow(
    name="DB Stats",
    log_prints=True,
    flow_run_name="db-stats-{runner}-" + f"{get_time()}",
)
def pull_db_stats(
    bucket: str,
    runner: str,
    database_account_id: str,
    database_secret_path: str,
    database_secret_key_ip: str,
    database_secret_key_username: str,
    database_secret_key_password: str,
    additional_info_file: str = "",
):
    """Pipeline that pulls specific stats from ingested studies from a Neo4j database

    Args:
        bucket (str): Bucket name of where output goes to
        runner (str): Unique runner name
        database_account_id (str): Account ID for the database
        database_secret_path (str): Secret path for the database
        database_secret_key_ip (str): Secret key for the IP of the database
        database_secret_key_username (str): Secret key for the username of the database
        database_secret_key_password (str): Secret key for the password of the database
        additional_info_file (str, optional): An extra information tsv file with one line per study in a study_id column.
    """

    logger = get_run_logger()

    bucket_folder = runner + "/db_stats_outputs_" + get_time()

    output_file = f"db_stats_{get_time()}.tsv"

    logger.info("Getting uri, username and password parameter from AWS")
    # get uri, username, and password value
    uri = get_secret_centralized_worker(
        secret_path_name=database_secret_path,
        secret_key_name=database_secret_key_ip,
        account=database_account_id,
    )
    username = get_secret_centralized_worker(
        secret_path_name=database_secret_path,
        secret_key_name=database_secret_key_username,
        account=database_account_id,
    )
    password = get_secret_centralized_worker(
        secret_path_name=database_secret_path,
        secret_key_name=database_secret_key_password,
        account=database_account_id,
    )

    # Run the queries
    logger.info("Pulling data from database")

    logger.info("Pulling PI data from database")
    pi_df = stats_pull_graph_data_study(
        uri,
        username,
        password,
        StatsNeo4jCypherQuery.stats_get_pi_query,
        "PI",
    )

    logger.info("Pulling curation status data from database")
    curation_status_df = stats_pull_graph_data_study(
        uri,
        username,
        password,
        StatsNeo4jCypherQuery.stats_get_curation_status_query,
        "curation_status",
    )

    logger.info("Pulling estimated size data from database")
    est_size_df = stats_pull_graph_data_study(
        uri,
        username,
        password,
        StatsNeo4jCypherQuery.stats_get_est_size_query,
        "estimated_size",
    )

    logger.info("Pulling institution data from database")
    institution_df = stats_pull_graph_data_study(
        uri,
        username,
        password,
        StatsNeo4jCypherQuery.stats_get_institution_query,
        "institution",
    )

    logger.info("Pulling bucket data from database")
    bucket_df = stats_pull_graph_data_study(
        uri,
        username,
        password,
        StatsNeo4jCypherQuery.stats_get_study_buckets,
        "bucket",
    )

    logger.info("Pulling study file size data from database")
    study_size_df = stats_pull_graph_data_study(
        uri,
        username,
        password,
        StatsNeo4jCypherQuery.stats_get_study_file_size,
        "file_size(Tb)",
    )

    logger.info("Pulling node count data from database")
    node_count_df = stats_pull_graph_data_nodes(
        uri,
        username,
        password,
        StatsNeo4jCypherQuery.stats_get_study_node_counts,
        "node_count",
    )

    logger.info("Pulling file size data from database")
    file_size_df = stats_pull_graph_data_nodes(
        uri,
        username,
        password,
        StatsNeo4jCypherQuery.stats_get_study_node_file_size,
        "file_size(Tb)",
    )

    logger.info("Pulling library strategy data from database")
    library_strategy_df = stats_pull_graph_data_nodes(
        uri,
        username,
        password,
        StatsNeo4jCypherQuery.stats_get_study_library_strategy,
        "library_strategy",
    )

    logger.info("Pulling library strategy file count data from database")
    library_strategy_count_df = stats_pull_graph_data_nodes(
        uri,
        username,
        password,
        StatsNeo4jCypherQuery.stats_get_study_library_strategy_count,
        "file_count",
    )

    logger.info("Pulling library strategy file size data from database")
    library_strategy_size_df = stats_pull_graph_data_nodes(
        uri,
        username,
        password,
        StatsNeo4jCypherQuery.stats_get_study_library_strategy_size,
        "file_size(Tb)",
    )

    logger.info("Pulling clinical data from database")
    study_clinical_df = stats_pull_graph_data_study(
        uri,
        username,
        password,
        StatsNeo4jCypherQuery.stats_get_study_clinical,
        "data_exist",
    )

    logger.info("Pulling methylation array data from database")
    study_methylation_array_df = stats_pull_graph_data_study(
        uri,
        username,
        password,
        StatsNeo4jCypherQuery.stats_get_study_methylation_array,
        "data_exist",
    )

    logger.info("Pulling cytogenomic data from database")
    study_cytogenomic_df = stats_pull_graph_data_study(
        uri,
        username,
        password,
        StatsNeo4jCypherQuery.stats_get_study_cytogenomic,
        "data_exist",
    )

    logger.info("Pulling pathology data from database")
    study_pathology_df = stats_pull_graph_data_study(
        uri,
        username,
        password,
        StatsNeo4jCypherQuery.stats_get_study_pathology,
        "data_exist",
    )

    logger.info("Pulling radiology data from database")
    study_radiology_df = stats_pull_graph_data_study(
        uri,
        username,
        password,
        StatsNeo4jCypherQuery.stats_get_study_radiology,
        "data_exist",
    )

    logger.info("Pulling study file count data from database")
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
            curation_status_df,
            est_size_df,
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
