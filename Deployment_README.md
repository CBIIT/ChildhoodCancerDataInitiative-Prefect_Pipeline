# Deployment Reference

This document provides high-level documentation for all deployments defined in `prefect.yaml`.

## Scope

- Source of truth: `prefect.yaml`
- Deployment count: 57
- Prefect project name: `s3-Prefect-Workflow`
- Prefect version in config: `3.2.14`

## How to use this guide

- Start with deployment name to find a flow in Prefect Cloud.
- Use tags to identify domain and environment intent (for example: `Production`, `DCC`, `V3`, `Centralized Worker`, `CPI`, `MCI`).
- Validate parameter values against your runtime context before executing.
- If this guide and `prefect.yaml` differ, treat `prefect.yaml` as authoritative.

## Deployments

### bucket-content-search-v3
- Flow entrypoint: `src/search_bucket_content.py:search_buckets_content`
- Tags: ['Production', 'V3']
- Work pool: `ccdi-dcc-8gb-prefect-3.4.19-python3.13`
- Key parameters: bucket
- Declared description: List contents of a bucket and output as a tsv file to S3

### bucket-reader-2gb-v3
- Flow entrypoint: `workflows/bucket_reader.py:reader`
- Tags: ['Production', 'V3', 'DCC', 'Centralized Worker']
- Work pool: `ccdi-dcc-2gb-prefect-3.4.19-python3.13`
- Key parameters: bucket_write_out, runner, write_out
- Declared description: Pipeline to read data and high level stats from an S3 bucket.

### ccdi-cog_igm-transform-V3
- Flow entrypoint: `workflows/cog_igm_transformer.py:cog_igm_transform`
- Tags: ['MCI', 'V3']
- Work pool: `ccdi-dcc-16gb-prefect-3.4.19-python3.13`
- Key parameters: bucket, file_path, form_parsing, manifest_path, runner
- Declared description: Transform from IGM format to CCDI for COG submission using the cog_and_igm form parsing method

### ccdi-cog_igm-transform-V3_v2
- Flow entrypoint: `workflows/cog_igm_transformer_v2.py:cog_igm_transform`
- Tags: ['PROD', 'MCI', 'V3']
- Work pool: `ccdi-dcc-16gb-prefect-3.4.19-python3.13`
- Key parameters: bucket, file_path, form_parsing, manifest_path, runner
- Declared description: Transform CCDI COG and IGM data to DCC format

### ccdi-cpi-api-fetch-v3
- Flow entrypoint: `workflows/cpi_api_return.py:get_associated_domains_ids`
- Tags: ['Production', 'V3', 'CPI']
- Work pool: `ccdi-dcc-8gb-prefect-3.4.19-python3.13`
- Key parameters: bucket, password_parameter, uri_parameter, username_parameter
- Declared description: Fetch associated ids of all CCDI participants using CPI API

### ccdi-cpi-query-v3
- Flow entrypoint: `workflows/ccdi_cpi_query.py:get_ccdi_cpi_ids`
- Tags: ['Production', 'V3', 'CPI', 'CCDI']
- Work pool: `ccdi-dcc-8gb-prefect-3.4.19-python3.13`
- Key parameters: bucket, password_parameter, uri_parameter, username_parameter
- Declared description: Fetch associated ids of all CCDI participants given a CCDI Manifest

### ccdi-data-catalog-counts
- Flow entrypoint: `workflows/data_catalog_stats.py:data_catalog_stats`
- Tags: ['V3']
- Work pool: `ccdi-dcc-16gb-prefect-3.4.19-python3.13`
- Key parameters: bucket, phs, upload_path, workbook_path
- Declared description: Generate counts for the data catalog using the CCDI-DCC model

### ccdi-data-curation-16gb-v3
- Flow entrypoint: `workflows/s3-Prefect-Pipeline.py:runner`
- Tags: ['Production', 'V3']
- Work pool: `ccdi-dcc-16gb-prefect-3.4.19-python3.13`
- Key parameters: bucket, file_path
- Declared description: Pipeline to curate CCDI data 16gb.

### ccdi-data-curation-2gb-v3
- Flow entrypoint: `workflows/s3-Prefect-Pipeline.py:runner`
- Tags: ['Production', 'V3']
- Work pool: `ccdi-dcc-2gb-prefect-3.4.19-python3.13`
- Key parameters: bucket, file_path
- Declared description: Pipeline to curate CCDI data 2gb.

### ccdi-data-curation-32gb-v3
- Flow entrypoint: `workflows/s3-Prefect-Pipeline.py:runner`
- Tags: ['Production', 'V3']
- Work pool: `ccdi-dcc-32gb-prefect-3.4.19-python3.13`
- Key parameters: bucket, file_path
- Declared description: Pipeline to curate CCDI data 32gb.

### ccdi-data-curation-60gb-v3
- Flow entrypoint: `workflows/s3-Prefect-Pipeline.py:runner`
- Tags: ['Production', 'V3']
- Work pool: `ccdi-dcc-60gb-prefect-3.4.19-python3.13`
- Key parameters: bucket, file_path
- Declared description: Pipeline to curate CCDI data 60gb.

### ccdi-data-curation-8gb-v3
- Flow entrypoint: `workflows/s3-Prefect-Pipeline.py:runner`
- Tags: ['Production', 'V3']
- Work pool: `ccdi-dcc-8gb-prefect-3.4.19-python3.13`
- Key parameters: bucket, file_path
- Declared description: Pipeline to curate CCDI data 8gb.

### ccdi-file-copier-v3
- Flow entrypoint: `workflows/File_Mover.py:file_mover`
- Tags: ['Production', 'V3']
- Work pool: `ccdi-dcc-60gb-prefect-3.4.19-python3.13`
- Key parameters: bucket
- Declared description: Pipeline to move files from one location to another.

### ccdi-gdc-file-upload-v3
- Flow entrypoint: `workflows/gdc_file_upload.py:runner`
- Tags: ['Production', 'V3']
- Work pool: `ccdi-dcc-32gb-prefect-3.4.19-python3.13`
- Key parameters: bucket, gdc_client_path, manifest_path, n_processes, node_type, process_type, project_id, runner, secret_key_name, secret_name_path, upload_part_size_mb
- Declared description: Upload files to GDC using the GDC client.

### ccdi-gdc-import-v3
- Flow entrypoint: `workflows/gdc_import.py:runner`
- Tags: ['Production', 'V3']
- Work pool: `ccdi-dcc-16gb-prefect-3.4.19-python3.13`
- Key parameters: bucket, check_for_updates, file_path, node_type, project_id, runner, secret_key_name, secret_name_path, sstr
- Declared description: Import metadata files to GDC using the GDC client.

### ccdi-template-liftover-v3
- Flow entrypoint: `workflows/ccdi_manifest_liftover.py:manifest_liftover`
- Tags: ['Production', 'V3']
- Work pool: `ccdi-dcc-32gb-prefect-3.4.19-python3.13`
- Key parameters: bucket, lift_to_tag, liftover_mapping_filepath
- Declared description: Pipeline to perform liftover of CCDI manifest files.

### ccdi-to-sra-v3
- Flow entrypoint: `workflows/ccdi_to_sra.py:run_ccdi_to_sra`
- Tags: ['TEST', 'V3']
- Work pool: `ccdi-dcc-60gb-prefect-3.4.19-python3.13`
- Key parameters: bucket
- Declared description: Specific pipeline to convert CCDI submission metadata to SRA metadata.

### COG_comparer
- Flow entrypoint: `workflows/cog_comparer.py:cog_comparer`
- Tags: ['PROD', 'DCC', 'CENTRAL WORKERS', 'V3']
- Work pool: `ccdi-dcc-8gb-prefect-3.4.19-python3.13`
- Key parameters: bucket, new_tranche_path, old_tranche_path, output_path
- Declared description: Compare CCDI COG data with DCC data

### compare-dataframes-v3
- Flow entrypoint: `workflows/compare_dataframes.py:compare_dataframes_runner`
- Tags: ['Production', 'V3']
- Work pool: `ccdi-dcc-2gb-prefect-3.4.19-python3.13`
- Key parameters: bucket, file_path_1, file_path_2, join_column_1, join_column_2, runner
- Declared description: A simple flow to compare two dataframes and output the differences between them.

### daap-db-index-creation-32gb-v3
- Flow entrypoint: `workflows/db_index_creation.py:db_index_creation_flow`
- Tags: ['Production', 'DaaP', 'V3', 'Centralized Worker']
- Work pool: `ccdi-dcc-60gb-prefect-3.4.19-python3.13`
- Key parameters: bucket, database_target_account_id, database_target_secret_key_ip, database_target_secret_key_password, database_target_secret_key_username, database_target_secret_path, file_path, runner
- Declared description: Create database indexes for Memgraph databases

### DB-props-uniq-report-v3
- Flow entrypoint: `src/neo4j_data_tools.py:report_unique_values_properties`
- Tags: ['Production', 'V3']
- Work pool: `ccdi-dcc-8gb-prefect-3.4.19-python3.13`
- Key parameters: bucket
- Declared description: Report unique values for properties in the database

### db-stats-v3
- Flow entrypoint: `workflows/db_stats.py:pull_db_stats`
- Tags: ['Production', 'DCC', 'Centralized Worker', 'V3']
- Work pool: `ccdi-dcc-60gb-prefect-3.4.19-python3.13`
- Key parameters: bucket
- Declared description: Pull database stats based on DCC model

### dcc-data-curation-16gb-v3
- Flow entrypoint: `workflows/s3-Prefect-Pipeline-dcc.py:runner_dcc`
- Tags: ['Production', 'DCC', 'V3']
- Work pool: `ccdi-dcc-16gb-prefect-3.4.19-python3.13`
- Key parameters: bucket, template_tag
- Declared description: Curatation pipeline to prepare CCDI data for DCC submission, using DCC metadata templates and validation rules

### dcc-data-curation-SRA-16gb-v3
- Flow entrypoint: `workflows/s3-Prefect-Pipeline-dcc.py:runner_dcc_w_SRA`
- Tags: ['Production', 'DCC', 'SRA', 'V3']
- Work pool: `ccdi-dcc-16gb-prefect-3.4.19-python3.13`
- Key parameters: bucket, template_tag
- Declared description: Curatation pipeline to prepare CCDI data with SRA submission added on still using DCC metadata templates and validation rules

### dcc-db_diff-v3
- Flow entrypoint: `workflows/db_diff_central_worker.py:db_diff`
- Tags: ['Production', 'DCC', 'Centralized Worker', 'V3']
- Work pool: `ccdi-dcc-16gb-prefect-3.4.19-python3.13`
- Key parameters: bucket, database_1_account_id, database_1_secret_key_ip, database_1_secret_key_password, database_1_secret_key_username, database_1_secret_path, database_2_account_id, database_2_secret_key_ip, database_2_secret_key_password, database_2_secret_key_username, database_2_secret_path, runner
- Declared description: Compare two databases and report differences

### dcc-entry-remover-16gb-v3
- Flow entrypoint: `workflows/entry_remover.py:entry_remover`
- Tags: ['Production', 'DCC', 'V3', 'Centralized Worker']
- Work pool: `ccdi-dcc-16gb-prefect-3.4.19-python3.13`
- Key parameters: bucket, directory_path, entry_removal_file_path, file_path, runner
- Declared description: Remove entries from CCDI-DCC metadata manifests based on an input file with entry identifiers to remove

### dcf-index-v3
- Flow entrypoint: `workflows/dcf_indexing.py:dcf_index_manifest`
- Tags: ['Production', 'V3']
- Work pool: `ccdi-dcc-8gb-prefect-3.4.19-python3.13`
- Key parameters: bucket
- Declared description: Pipeline to create a DCF indexing manifest file.

### fetch-size-md5sum-v3
- Flow entrypoint: `workflows/fetch_size_md5sum.py:get_size_md5sum`
- Tags: ['Production', 'V3']
- Work pool: `ccdi-dcc-8gb-prefect-3.4.19-python3.13`
- Key parameters: bucket
- Declared description: Pipeline to fetch the size and MD5 checksum of files in a specified location.

### file-mover-v3
- Flow entrypoint: `workflows/file_mover_delete.py:file_mover_delete`
- Tags: ['Production', 'V3']
- Work pool: `ccdi-dcc-32gb-prefect-3.4.19-python3.13`
- Key parameters: bucket
- Declared description: A file mover pipeline to move files between locations. This version includes deletion of original files after copying.

### file-remover-v3
- Flow entrypoint: `workflows/File_Remover.py:run_file_remover`
- Tags: ['Production', 'V3']
- Work pool: `ccdi-dcc-32gb-prefect-3.4.19-python3.13`
- Key parameters: None
- Declared description: Pipeline to remove files from a specified location.

### gc-liftover-helper
- Flow entrypoint: `workflows/gc_liftover_helper.py:generate_ids_flow`
- Tags: ['Production', 'Liftover', 'CCDI', 'DCC']
- Work pool: `ccdi-dcc-2gb-prefect-3.4.19-python3.13`
- Key parameters: bucket, runner, submission_path
- Declared description: Helper flow to perform post-liftover operations on CCDI data for GC submission preparation

### generic-file-mover-v3
- Flow entrypoint: `workflows/file_mover_delete.py:file_mover_delete_complete_input`
- Tags: ['Production', 'V3']
- Work pool: `ccdi-dcc-32gb-prefect-3.4.19-python3.13`
- Key parameters: bucket
- Declared description: Generic pipeline to move files between locations.

### get-parameter-v3
- Flow entrypoint: `workflows/get_aws_parameter.py:get_aws_parameter`
- Tags: ['Production', 'V3']
- Work pool: `ccdi-dcc-2gb-prefect-3.4.19-python3.13`
- Key parameters: parameter_name
- Declared description: Pipeline to retrieve an AWS parameter. To be deprecated in favor of AWS Secrets Manager for better security practices.

### hello-world-v3
- Flow entrypoint: `workflows/hello_world.py:hello_flow`
- Tags: ['Production', 'V3']
- Work pool: Not specified
- Key parameters: name
- Declared description: Hello world flow to test deployment configuration and flow execution

### join-tsv-to-manifest-v3
- Flow entrypoint: `workflows/db_tsv_to_manifest.py:join_tsv_to_manifest`
- Tags: ['Production', 'V3']
- Work pool: `ccdi-dcc-16gb-prefect-3.4.19-python3.13`
- Key parameters: bucket
- Declared description: Pipeline to join tsv files into a manifest file for CCDI submission.

### join-tsv-to-manifest-v3-dcc
- Flow entrypoint: `workflows/db_tsv_to_manifest.py:join_tsv_to_manifest`
- Tags: ['Production', 'DCC', 'Centralized Worker', 'V3']
- Work pool: `ccdi-dcc-16gb-prefect-3.4.19-python3.13`
- Key parameters: bucket, dcc_template_tag, runner, tsv_folder_path
- Declared description: Join the exported model TSV back to the manifest template for CCDI-DCC

### KF-Data-Sync-Manifest-Generator-v3
- Flow entrypoint: `workflows/kf_data_sync_manifest_generator.py:kf_data_sync_manifest_generator`
- Tags: ['Production', 'V3', 'DCC', 'Centralized Worker']
- Work pool: `ccdi-dcc-16gb-prefect-3.4.19-python3.13`
- Key parameters: bucket, file_path, kf_data_sync_bucket, runner
- Declared description: Not provided in prefect.yaml

### liftover-generic
- Flow entrypoint: `workflows/submission_liftover.py:submission_liftover`
- Tags: ['Production', 'V3', 'CCDI', 'CDS']
- Work pool: `ccdi-dcc-16gb-prefect-3.4.19-python3.13`
- Key parameters: bucket, lift_from_tag
- Declared description: Generic liftover for metadata between projects using a provided mapping file

### liftover-generic-CCDI-DCC
- Flow entrypoint: `workflows/submission_liftover.py:submission_liftover_ccdi_to_dcc`
- Tags: ['Production', 'V3', 'CCDI', 'DCC']
- Work pool: `ccdi-dcc-16gb-prefect-3.4.19-python3.13`
- Key parameters: bucket
- Declared description: Liftover pipeline to convert from CCDI-DCC model to another using a mapping file for guidance.

### mci-release-manifest-v3
- Flow entrypoint: `workflows/mci_release_run.py:mci_release_manifest`
- Tags: ['Production', 'V3']
- Work pool: `ccdi-dcc-8gb-prefect-3.4.19-python3.13`
- Key parameters: bucket, template_tag
- Declared description: Pipeline to create a release manifest for MCI release. This pipeline takes in a template manifest and fills in the file information based on the files present in the specified S3 bucket.

### mci_gdc_transformer-v3
- Flow entrypoint: `workflows/mci_gdc_transform.py:mci_gdc_transform`
- Tags: ['PROD', 'V3']
- Work pool: `ccdi-dcc-16gb-prefect-3.4.19-python3.13`
- Key parameters: bucket, data_cleanup, manifest_file, preservation_meth_platform_file, runner
- Declared description: Transform MCI data in DCC format to GDC format for submission to GDC

### memgraph_promotion_v3
- Flow entrypoint: `workflows/memgraph_transfer_promotion.py:memgraph_transfer_promotion`
- Tags: ['Production', 'V3', 'DCC', 'Centralized Worker', 'Database Promotion']
- Work pool: `ccdi-dcc-32gb-prefect-3.4.19-python3.13`
- Key parameters: bucket, chunk_size, database_source_account_id, database_source_account_name, database_source_secret_key_ip, database_source_secret_key_password, database_source_secret_key_username, database_source_secret_path, database_target_account_id, database_target_account_name, database_target_secret_key_ip, database_target_secret_key_password, database_target_secret_key_username, database_target_secret_path, file_path, mode, promotion_filter_node_label, promotion_filter_property, promotion_filter_value, runner, wipe_db
- Declared description: Promote Memgraph database from source to target environment database

### model-mapping-maker-8gb-v3
- Flow entrypoint: `workflows/model_mapping_maker.py:runner`
- Tags: ['Production', 'MDF', 'MEVAL', 'Centralized Worker', 'V3']
- Work pool: `ccdi-dcc-8gb-prefect-3.4.19-python3.13`
- Key parameters: base_mode, bucket, mapping_file, new_model_repository, new_model_version, old_model_repository, old_model_version, runner
- Declared description: Create a mapping file between two models based on user input and upload to S3

### model-to-submission-ccdi-dcc-v3
- Flow entrypoint: `workflows/model_to_submission_ccdi_dcc.py:create_submission_manifest`
- Tags: ['DCC', 'Prod', 'V3', 'Python3.13']
- Work pool: `ccdi-dcc-16gb-prefect-3.4.19-python3.13`
- Key parameters: bucket
- Declared description: Creates the CCDI-DCC xlsx manifest for submission using the CCDI-DCC model

### model-to-submission-v3
- Flow entrypoint: `workflows/model_to_submission.py:create_submission_manifest`
- Tags: ['Production', 'V3']
- Work pool: `ccdi-dcc-2gb-prefect-3.4.19-python3.13`
- Key parameters: bucket
- Declared description: Pipeline to convert a model output file to a submission manifest file.

### new-model-validation-ccdi-dcc-v3
- Flow entrypoint: `workflows/new_model_validation_ccdi_dcc.py:validate_new_dcc_model`
- Tags: ['DCC', 'Prod', 'V3', 'Python3.13']
- Work pool: `ccdi-dcc-16gb-prefect-3.4.19-python3.13`
- Key parameters: bucket
- Declared description: Creates a small manifest of fake data using the DCC metadata templates and validation rules as an example of how to use the templates and rules for submission preparation and validates a model using the DCC validation rules as an example of how to validate a model for submission preparation

### new-model-validation-v3
- Flow entrypoint: `workflows/new_model_validation.py:validate_new_model`
- Tags: ['Production', 'V3']
- Work pool: `ccdi-dcc-2gb-prefect-3.4.19-python3.13`
- Key parameters: bucket
- Declared description: Validation pipeline to validate new models.

### pull-db-data-32gb-v3
- Flow entrypoint: `workflows/pull_db_data.py:pull_db_data`
- Tags: ['Production', 'V3', 'DCC', 'Centralized Worker']
- Work pool: `ccdi-dcc-60gb-prefect-3.4.19-python3.13`
- Key parameters: bucket, database_account_id, database_secret_key_ip, database_secret_key_password, database_secret_key_username, database_secret_path, runner
- Declared description: Pull memgraph database data for export via centralized worker

### pull-neo4j-data-16gb-c3dc-v3
- Flow entrypoint: `workflows/pull_neo4j_data.py:pull_neo4j_data`
- Tags: ['Production', 'C3DC', 'DEV', 'V3']
- Work pool: `ccdi-c3dc-16gb-prefect-3.2.14-python3.9`
- Key parameters: bucket, password_parameter, uri_parameter, username_parameter
- Declared description: Pipeline to pull data from a Neo4j database.

### pull-neo4j-data-32gb-ccdi-v3
- Flow entrypoint: `workflows/pull_neo4j_data.py:pull_neo4j_data`
- Tags: ['Production', 'V3', 'CCDI', 'Sandbox', 'Centralized Worker']
- Work pool: `ccdi-dcc-32gb-prefect-3.4.19-python3.13`
- Key parameters: bucket, database_account_id, database_secret_key_ip, database_secret_key_password, database_secret_key_username, database_secret_path, runner
- Declared description: Pull Neo4j data for CCDI V3

### pull_n_join_manifest_single_study
- Flow entrypoint: `workflows/pull_n_join_manifest_single_study.py:pull_n_join_manifest_single_study`
- Tags: ['Production', 'V3']
- Work pool: `ccdi-dcc-16gb-prefect-3.4.19-python3.13`
- Key parameters: bucket, dcc_template_tag, study_id
- Declared description: Pull and join manifest for a single study using the DCC metadata templates and validation rules

### pull_n_join_manifest_single_study-dcc
- Flow entrypoint: `workflows/pull_n_join_manifest_single_study.py:pull_n_join_manifest_single_study`
- Tags: ['Production', 'DCC', 'Centralized Worker', 'V3']
- Work pool: `ccdi-dcc-16gb-prefect-3.4.19-python3.13`
- Key parameters: bucket, database_account_id, database_secret_key_ip, database_secret_key_password, database_secret_key_username, database_secret_path, dcc_template_tag, runner, study_id
- Declared description: Pull and join CCDI-DCC manifest for a single study with DCC template

### secret-test-ccdi-dcc-v3
- Flow entrypoint: `workflows/secret_test.py:secret_pipeline`
- Tags: ['PROD', 'DCC', 'CENTRAL WORKERS', 'V3']
- Work pool: `ccdi-dcc-2gb-prefect-3.4.19-python3.13`
- Key parameters: account, secret_key_name, secret_path_name
- Declared description: Test access to secrets in the Centralized Worker environments

### submission-cruncher-v3
- Flow entrypoint: `workflows/Submission_Cruncher.py:submission_cruncher`
- Tags: ['Production', 'V3']
- Work pool: `ccdi-dcc-8gb-prefect-3.4.19-python3.13`
- Key parameters: bucket
- Declared description: A pipeline to concatenate two CCDI manifest files into a final submission manifest file.

### template-exampler-ccdi-dcc-v3
- Flow entrypoint: `workflows/Template_Exampler_ccdi_dcc.py:run_template_exampler_dcc`
- Tags: ['DCC', 'Prod', 'V3', 'Python3.13']
- Work pool: `ccdi-dcc-16gb-prefect-3.4.19-python3.13`
- Key parameters: bucket, number_of_entries
- Declared description: Create a small manifest of fake data using the DCC metadata templates and validation rules as an example of how to use the templates and rules for submission preparation

### template-exampler-v3
- Flow entrypoint: `workflows/Template_Exampler.py:run_template_exampler`
- Tags: ['Production', 'V3']
- Work pool: `ccdi-dcc-2gb-prefect-3.4.19-python3.13`
- Key parameters: bucket, number_of_entries
- Declared description: Pipeline to generate example CCDI submission files of fake data.

### validate-db-data-32gb-v3
- Flow entrypoint: `workflows/validate_db_data.py:validate_db_data`
- Tags: ['Production', 'Centralized Worker', 'DCC', 'V3']
- Work pool: `ccdi-dcc-32gb-prefect-3.4.19-python3.13`
- Key parameters: bucket
- Declared description: Validate a memgraph database data

