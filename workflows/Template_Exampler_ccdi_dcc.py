from prefect import flow, task, get_run_logger
import os
import sys


parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.template_exampler_ccdi_dcc import make_template_example, make_template_exampler_md
from src.utils import CCDI_DCC_Tags, get_time, get_date, file_ul, file_dl


@flow(
    name="Template Exampler DCC",
    log_prints=True,
    flow_run_name="template-exampler-dcc-{runner}-" + f"{get_time()}",
)
def run_template_exampler_dcc(
    bucket: str,
    runner: str,
    number_of_entries: int,
    dcc_manifest_version: str = "default_to_latest",
) -> None:
    """Pipelinei that generates simulated data for CCDI-DCC manifest

    Args:
        bucket (str): Bucket name of where the output goes to
        runner (str): Unique runner name
        number_of_entries (int): Number of data enteries in each node
        dcc_manifest_version (str, optional): The tag name of CCDI-DCC manifest. Defaults to "default_to_latest".
    """    
    # create run logger
    runner_logger = get_run_logger()

    output_folder = os.path.join(runner, "template_exampler_outputs_" + get_time())

    # Check if manifest version is provided,
    # download the specified version if yes, and download the latest version if no
    dcc_tag = CCDI_DCC_Tags()
    if dcc_manifest_version != "default_to_latest":
        check_tag = dcc_tag.if_tag_exists(
            tag=dcc_manifest_version, logger=runner_logger
        )
        if not check_tag:
            available_tags = dcc_tag.get_tags_only()
            runner_logger.error(
                f"Version {dcc_manifest_version} is not found among any ccdi-dcc-model releases. Here is a list of available tags you can use {*available_tags,}"
            )
            return None
        else:
            manifest_version = dcc_manifest_version
            manifest_file = dcc_tag.download_tag_manifest(
                tag=ccdi_manifest_version, logger=runner_logger
            )
    else:
        available_tags = dcc_tag.get_tags_only()
        manifest_version = available_tags[0]
        runner_logger.info(f"The latest version of ccdi-dcc model is {manifest_version}.")
        manifest_file = dcc_tag.download_tag_manifest(
            tag=manifest_version, logger=runner_logger
        )

    if manifest_file is not None:
        try:
            output_exampler, output_log = make_template_example(
                manifest_path=manifest_file, entry_num=number_of_entries
            )
        except:
            output_exampler = None
            output_log = "template_exampler_" + get_date() + ".log"
            runner_logger.error(
                "Template exampler workflow failed unexpectedly and investiagtion is needed"
            )
        if output_exampler is not None:
            file_ul(
                bucket=bucket,
                output_folder=output_folder,
                sub_folder="",
                newfile=output_exampler,
            )
            runner_logger.info(
                f"Uploaded example file {output_exampler} to the bucket folder {output_folder} "
            )
        else:
            pass

        file_ul(
            bucket=bucket,
            output_folder=output_folder,
            sub_folder="",
            newfile=output_log,
        )
        runner_logger.info(
            f"Uploaded template exampler log {output_log} to the bucket folder {output_folder}"
        )
        file_ul(
            bucket=bucket,
            output_folder=output_folder,
            sub_folder="",
            newfile=manifest_file,
        )
        runner_logger.info(
            f"Uploaded ccdi-dcc manifest template {manifest_file} to the bucket folder {output_folder}"
        )
        make_template_exampler_md(
            source_bucket=bucket,
            runner=runner,
            output_folder=output_folder,
            manifest_version=manifest_version,
        )

    else:
        runner_logger.error(
            "Template exampler workflow failed to download the ccdi-dcc manifest and was aborted"
        )
        return None

    return None


@flow(
    name="Template Exampler DCC test",
    log_prints=True,
    flow_run_name="template-exampler-dcc-test-{runner}-" + f"{get_time()}",
)
def run_template_exampler_dcc_test(
    bucket: str,
    runner: str,
    number_of_entries: int,
    dcc_manifest_path: str,
) -> None:
    """Pipelinei that generates simulated data for CCDI-DCC manifest

    Args:
        bucket (str): Bucket name of where the output goes to
        runner (str): Unique runner name
        number_of_entries (int): Number of data enteries in each node
        dcc_manifest_path (str): file path to the CCDI-DCC manifest file in the given bucket
    """
    # create run logger
    runner_logger = get_run_logger()

    output_folder = os.path.join(runner, "template_exampler_outputs_" + get_time())

    # download the manifest file from the given bucket
    file_dl(bucket=bucket, filename=dcc_manifest_path)
    manifest_file = os.path.basename(dcc_manifest_path)

    if manifest_file is not None:
        try:
            output_exampler, output_log = make_template_example(
                manifest_path=manifest_file, entry_num=number_of_entries
            )
        except:
            output_exampler = None
            output_log = "template_exampler_" + get_date() + ".log"
            runner_logger.error(
                "Template exampler workflow failed unexpectedly and investiagtion is needed"
            )
        if output_exampler is not None:
            file_ul(
                bucket=bucket,
                output_folder=output_folder,
                sub_folder="",
                newfile=output_exampler,
            )
            runner_logger.info(
                f"Uploaded example file {output_exampler} to the bucket folder {output_folder} "
            )
        else:
            pass

        file_ul(
            bucket=bucket,
            output_folder=output_folder,
            sub_folder="",
            newfile=output_log,
        )
        runner_logger.info(
            f"Uploaded template exampler log {output_log} to the bucket folder {output_folder}"
        )
        file_ul(
            bucket=bucket,
            output_folder=output_folder,
            sub_folder="",
            newfile=manifest_file,
        )
        runner_logger.info(
            f"Uploaded ccdi-dcc manifest template {manifest_file} to the bucket folder {output_folder}"
        )

    else:
        runner_logger.error(
            "Template exampler workflow failed to download the ccdi-dcc manifest and was aborted"
        )
        return None

    return None


if __name__ == "__main__":
    bucket = "my-source-bucket"
    runner = "QL"
    number_of_entries = 20
    ccdi_manifest_version="1.7.2"
    run_template_exampler(
        bucket=bucket, runner=runner, number_of_entries=number_of_entries, ccdi_manifest_version=ccdi_manifest_version
    )
