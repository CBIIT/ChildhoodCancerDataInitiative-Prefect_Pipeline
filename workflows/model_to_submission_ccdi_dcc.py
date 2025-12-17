import os
import sys
import traceback
import logging
import io
import yaml
from collections import Counter
import requests

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.create_submission_ccdi_dcc import DCCModelEndpoint, GetDCCModel, ManifestSheet
from src.utils import file_ul, get_time, dl_file_from_url
from prefect import flow, task, get_run_logger
from requests.exceptions import ConnectionError


@task(name="examine mdf load errors", log_prints=True)
def examine_enum_load_errors(log_contents: str, logger: get_run_logger) -> list[str]:
    """Examine the log contents from MDFReader to find any enum file load errors

    Args:
        log_contents (str): The log contents captured from GetDCCModel which creates MDF model instance during initialization

    Returns:
        list[str]: A list of enum file names that failed to load
    """
    error_enum_files = []
    for line in log_contents.splitlines():
        # this only catches enum loading from reference file
        if line.startswith("Error loading enum from url"):
            file_url = line.strip("Error loading enum from url").strip(
                "'"
            )  # extract file url
            error_enum_files.append(file_url)
            logger.error("Error loading enum from url: " + file_url)
        elif line.startswith("Error"):
            logger.error(line)
    return error_enum_files

@task(name="check enum duplicates", log_prints=True)
def check_enum_duplicates(enum_url_list: list[str])->None:
    """Check of list of url for enum yaml and find any duplicates

    Args:
        enum_url_list (list[str]): a list of enum yaml urls to check for duplicates
    """
    for i in enum_url_list:
        try:
            response = requests.get(i)
            response.raise_for_status()
            yaml_text = response.text

            # parse yaml
            data = yaml.safe_load(yaml_text)
            if "PropDefinitions" not in data:
                print("Missing 'PropDefinitions' key in YAML data")
            else:
                prop_definitions = data["PropDefinitions"]
                if not isinstance(prop_definitions, dict):
                    print("Value under 'PropDefinitions' is not a dictionary")
                else:
                    prop_keys = list(prop_definitions.keys())
                    for key in prop_keys:
                        if not isinstance(prop_definitions[key], list):
                            print(f"Value for key '{key}' is not a list")
                        else:
                            counter = Counter(prop_definitions[key])
                            for item, count in counter.items():
                                if count > 1:
                                    print(f"Duplicate item '{item}' found {count} times under key '{key}'")
                                else:
                                    pass
        except requests.RequestException as re:
            print(f"Error fetching URL {i}: {re}")
        except yaml.YAMLError as e:
            print(f"Failed to parse YAML for URL {i}: {e}")

    return None
                         

@flow(
    name="DCC Model to Submission",
    log_prints=True,
    flow_run_name="dcc-model-to-submission-{runner}-" + f"{get_time()}",
)
def create_submission_manifest(bucket: str, runner: str, release_title: str) -> None:
    """Pipeline that creates a CCDI-DCC manifest using the model files in GitHub repo main branch

    Args:
        bucket (str): Bucket name that output goes to
        runner (str): Unique runner name
        release_title (str): Release title to use in the new manifest
    """    

    # create a logging object
    runner_logger = get_run_logger()

    # download ccdi-dcc-model.yml
    try:
        model_file = dl_file_from_url(DCCModelEndpoint.model_file)
        ## try with a release tag url first
        # model_file = dl_file_from_url(
        #    "https://raw.githubusercontent.com/CBIIT/ccdi-dcc-model/refs/tags/0.0.2/model-desc/ccdi-dcc-model.yml"
        # )
    except ConnectionError as e:
        runner_logger.error(f"Failed to download ccdi-dcc-model.yml due to ConnectionError: {e}")
        raise
    except Exception as er:
        runner_logger.error(f"Downloading ccdi-dcc-model.yml failed unexpectedly. {er}")
        traceback.print_exc()
        raise

    # download ccdi-dcc-model-props.yml
    try:

        prop_file = dl_file_from_url(DCCModelEndpoint.prop_file)
        ## try with a release tag url first
        # prop_file = dl_file_from_url(
        #    "https://raw.githubusercontent.com/CBIIT/ccdi-dcc-model/refs/tags/0.0.2/model-desc/ccdi-dcc-model-props.yml"
        # )
    except ConnectionError as e:
        runner_logger.error(f"Failed to download ccdi-dcc-model-props.yml due to ConnectionError: {e}")
        raise
    except Exception as er:
        runner_logger.error(f"Downloading ccdi-dcc-model-props.yml failed unexpectedly {er}")
        traceback.print_exc()
        raise

    # download terms.yml
    try:

        term_file = dl_file_from_url(DCCModelEndpoint.term_file)
        ## try with a released tag url first
        # term_file = dl_file_from_url(
        #    "https://raw.githubusercontent.com/CBIIT/ccdi-dcc-model/refs/tags/0.0.2/model-desc/terms.yml"
        # )
    except ConnectionError as e:
        runner_logger.error(f"Failed to download terms.yml due to ConnectionError: {e}")
        raise
    except Exception as er:
        runner_logger.error(f"downloading terms.yml failed unexpectedly. {er}")
        traceback.print_exc()
        raise

    runner_logger.info(
        f"Downloaded models files: {model_file}, {prop_file}, {term_file}"
    )

    # need to catpture the logging output from MDFReader
    log_stream = io.StringIO()
    handler = logging.StreamHandler(log_stream)
    handler.setLevel(logging.DEBUG)
    logger = logging.getLogger()
    logger.addHandler(handler)
    try:
        getmodel = GetDCCModel(
            model_file=model_file, prop_file=prop_file, term_file=term_file
        )
    finally:
        logger.removeHandler(handler)
        log_contents = log_stream.getvalue()
        runner_logger.info(f"MDFReader log output:\n{log_contents}")
        log_stream.close()
    
    # examine if any yaml enum file loaded failed
    error_enum_urls = examine_enum_load_errors(log_contents=log_contents, logger=runner_logger)
    if len(error_enum_urls) > 0:
        runner_logger.error(
            f"The following enum files failed to load, please check the URLs or the files themselves: {*error_enum_urls,}"
        )
        runner_logger.warning("Checking duplicates in the errored enum files...")
        # check enum duplicates among the error enum files
        check_enum_duplicates(enum_url_list=error_enum_urls)
        raise RuntimeError("Enum file loading errors detected, see logs for details.")
    else:
        pass

    # get model version
    try:
        model_version = getmodel.get_version()
        runner_logger.info(f"Model version captured in ccdi-dcc-model.yml is {model_version}")
    except KeyError as e:
        runner_logger.error("Can't find Version information in ccdi-dcc-model.yml")
        raise
    except Exception as er:
        runner_logger.error(f"Failed to collect Version from ccdi-dcc-model.yml. {er}")
        traceback.print_exc()
        raise

    # get dictionary dataframe which can be used for Dictionary sheet
    try:
        dict_df = getmodel.get_prop_dict_df()
    except KeyError as e:
        runner_logger.error(f"Failed to generate the dataframe for Dictionary sheet due to KeyError: {e}")
        traceback.print_exc()
        raise
    except Exception as er:
        runner_logger.error(f"Failed to generate the dataframe for Dictionay sheet. {er}")
        traceback.print_exc()
        raise

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
    parent_node_dict = getmodel.get_parent_nodes()
    manifest_wb.metadata_sheets(
        model_node=model_node,
        parent_node_dict=parent_node_dict,
        prop_dict_df=dict_df,
        logger=runner_logger,
    )

    # sort sheets order
    try:
        sorted_sheet_list = getmodel._get_sorted_node_list(node_list=model_node.keys())
        manifest_wb.sort_sheets(sorted_node_list=sorted_sheet_list)
    except ValueError as err:
        runner_logger.error(f"Failed to sort sheets in wb, likely due to outdated GetCCDIModel.node_preferred_order: {err}")
        traceback.print_exc()
        raise

    # save manifest submission file
    output_wb_name = "CCDI-DCC_Submission_Template_" + model_version + ".xlsx"
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
    return None


if __name__ == "__main__":
    bucket = "my-source-bucket"
    runner = "QL"
    release_title = "test release title"

    create_submission_manifest(
        bucket=bucket, runner=runner, release_title=release_title
    )
