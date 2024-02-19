import os
import sys
import traceback

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.create_submission import ModelEndpoint, GetCCDIModel, ManifestSheet
from src.utils import file_ul, get_time, dl_file_from_url
from prefect import flow, get_run_logger
from requests.exceptions import ConnectionError


@flow(
    name="Model to Submission",
    log_prints=True,
    flow_run_name="model-to-submission-{runner}-" + f"{get_time()}",
)
def create_submission_manifest(bucket: str, runner: str, release_title: str) -> str:
    # create a logging object
    runner_logger = get_run_logger()

    # download ccdi-model.yml
    try:
        model_file = dl_file_from_url(ModelEndpoint.model_file)
    except ConnectionError as e:
        runner_logger.error(e)
        return None
    except Exception as er:
        runner_logger.error(f"Downloading ccdi-model.yml failed unexpectedly. {er}")
        traceback.print_exc()
        return None

    # download ccdi-model-props.yml
    try:
        prop_file = dl_file_from_url(ModelEndpoint.prop_file)
    except ConnectionError as e:
        runner_logger.error(e)
        return None
    except Exception as er:
        runner_logger.error(f"Downloading ccdi-model-props.yml failed unexpectedly {er}")
        traceback.print_exc()
        return None

    # download terms.yaml
    try:
        term_file = dl_file_from_url(ModelEndpoint.term_file)
    except ConnectionError as e:
        runner_logger.error(e)
        return None
    except Exception as er:
        runner_logger.error(f"downloading terms.yaml failed unexpectedly. {er}")
        traceback.print_exc()
        return None

    runner_logger.info(
        f"Downloaded models files: {model_file}, {prop_file}, {term_file}"
    )

    getmodel = GetCCDIModel(
        model_file=model_file, prop_file=prop_file, term_file=term_file
    )

    # get model version
    try:
        model_version = getmodel.get_version()
        runner_logger.info(f"Model version captured in ccdi-model.yml is {model_version}")
    except KeyError as e:
        runner_logger.error("Can't find Version information in ccdi-model.yml")
        return None
    except Exception as er:
        runner_logger.error(f"Failed to collect Version from ccdi-model.yml. {er}")
        traceback.print_exc()
        return None

    # get dictionary dataframe which can be used for Dictionary sheet
    try:
        dict_df = getmodel.get_prop_dict_df()
    except KeyError as e:
        runner_logger.error(e)
        return None
    except Exception as er:
        runner_logger.error(f"Failed to generate the dataframe for Dictionay sheet. {er}")
        traceback.print_exc()
        return None

    # get terms dataframe which can be used for terms and values set sheet
    terms_df = getmodel.get_terms_df()

    # create ManifestSheet object
    manifest_wb = ManifestSheet()

    # create readme sheet
    manifest_wb.readme_sheet(model_version=model_version, release_title=release_title)
    runner_logger.info("Created README and INSTRUCTIONS sheet")

    # create dictionary sheet
    manifest_wb.dictionary_sheet(dict_df=dict_df)
    runner_logger.info("Created Dictionary sheet")

    # create term sheet
    manifest_wb.terms_value_sets_sheet(terms_df=terms_df)
    runner_logger.info("Created Terms and Value Sets sheet")

    # define names for the workbook
    manifest_wb.get_define_names(term_df=terms_df)
    runner_logger.info("Defined global name ranges for enum properties")

    # create node metadata sheets
    model_node = getmodel.get_model_nodes()
    parent_node_dict = getmodel.get_parent_node()
    manifest_wb.metadata_sheets(
        model_node=model_node,
        parent_node_dict=parent_node_dict,
        prop_dict_df=dict_df,
        logger=runner_logger,
    )

    # sort sheets order
    sorted_sheet_list = getmodel._get_sorted_node_list(node_list=model_node.keys())
    manifest_wb.sort_sheets(sorted_node_list=sorted_sheet_list)

    # save manifest submission file
    output_wb_name = "CCDI_Submission_Template_" + model_version + ".xlsx"
    manifest_wb.workbook.save(output_wb_name)
    runner_logger.info(f"Saving submission manifest as {output_wb_name}")

    # Upload file to bucket
    output_folder = os.path.join(runner, "model_to_submission_outputs_" + get_time())
    file_ul(
        bucket=bucket,
        output_folder=output_folder,
        sub_folder="",
        newfile=output_wb_name,
    )
    runner_logger.info(
        f"Uploaded submiassion manifest file {output_wb_name} to the bucket {bucket} at {output_folder}"
    )
    return output_folder


if __name__ == "__main__":
    bucket = "my-source-bucket"
    runner = "QL"
    release_title = "test release title"

    create_submission_manifest(
        bucket=bucket, runner=runner, release_title=release_title
    )
