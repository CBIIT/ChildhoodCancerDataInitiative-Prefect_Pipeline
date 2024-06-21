from prefect import flow, task, get_run_logger
from utils import list_to_chunks, set_s3_resource, get_time, file_ul
from file_mover import copy_file_task, parse_file_url_in_cds
import pysam
import os
import pandas as pd
from botocore.exceptions import ClientError


@task(name="download object to local", log_prints=True)
def download_object_to_local(cds_url: str) -> None:
    """Download an aws object to local"""
    bucket, key = parse_file_url_in_cds(url=cds_url)
    # Set the s3 resource object for local or remote execution
    s3 = set_s3_resource()
    source = s3.Bucket(bucket)
    filename = os.path.basename(key)
    try:
        source.download_file(key, filename)
        return filename
    except ClientError as ex:
        ex_code = ex.response["Error"]["Code"]
        ex_message = ex.response["Error"]["Message"]
        print(
            f"ClientError occurred while downloading file {filename} from bucket {bucket}:\n{ex_code}, {ex_message}"
        )
        raise


def extract_base_reads_readlength(filename: str) -> tuple:
    """Reads a stats file and extract Bases, Reads, and AvgReadLength"""
    file = open(filename, "r")
    lines = file.readlines()
    for line in lines:
        # remove new line
        line=line.strip()
        if "SN	raw total sequences:	" in line:
            line_list = line.split("\t")
            read_count = line_list[2]
        elif "SN	total length:	" in line:
            line_list = line.split("\t")
            bases = line_list[2]
        elif "SN	average length:	" in line:
            line_list = line.split("\t")
            avgreadlength = line_list[2]
        else:
            pass
    file.close()
    return read_count, bases, avgreadlength


def extract_coverage(filename: str) -> float:
    cov_df = pd.read_csv(filename, sep="\t")
    coverage_avg = round(cov_df["coverage"].mean(), 2)
    return coverage_avg


@task(name="get bam sra stats", log_prints=True)
def get_bam_stats(filename: str):
    """Get the bam stats from pysam stats"""
    filename_wo_ext = filename.rsplit(".", 1)[0]
    stat_filename = filename_wo_ext + "_stats.txt"
    stats = pysam.stats(filename)
    with open(stat_filename, "w") as stats_f:
        stats_f.write(stats)
    print(f"created stats file: {stat_filename}")

    reads, bases, avgreadlength = extract_base_reads_readlength(filename=stat_filename)
    os.remove(stat_filename)
    print(f"removed stats file: {stat_filename}")

    coverage = pysam.coverage(filename)
    coverage_filename = filename_wo_ext + "_coverage.txt"
    with open(coverage_filename, "w") as cov_f:
        cov_f.write(coverage)
    print(f"created coverage file: {coverage_filename}")
    coverage = extract_coverage(filename=coverage_filename)
    os.remove(coverage_filename)
    print(f"removed coverage file: {coverage_filename}")
    return bases, reads, coverage, avgreadlength


@flow(name="SRA metadata extraction", log_prints=True)
def get_sra_metadata(uri_list: list[str]) -> None:
    metadata_records = []
    print(f"Number of objects: {len(uri_list)}")
    count = 1
    for i in uri_list:
        # download the file
        filename = download_object_to_local(cds_url=i)

        # extract sra metadata of object i
        bases, reads, coverage, avgreadlength = get_bam_stats(filename=filename)
        record_dict = {
            "uri": i,
            "Bases": bases,
            "Reads": reads,
            "coverage": coverage,
            "AvgReadLength": avgreadlength,
        }
        print(record_dict)
        metadata_records.append(record_dict)
        os.remove(filename)
        print(f"Progress: {count}/{len(uri_list)}")
        count += 1
    metadata_df = pd.DataFrame.from_records(metadata_records)
    print(metadata_df)
    outputfile = "SRA_metadata_extraction_" + get_time() + ".tsv"
    metadata_df.to_csv(outputfile, sep="\t", index=False)
    del metadata_df
    return outputfile


@flow(name="SRA metadata flow")
def sra_metadata_extraction(bucket: str, runner: str, uri_list: list[str]) -> None:
    logger=get_run_logger()

    logger.info(f"bucket: {bucket}\nrunner: {runner}\nuri count: {len(uri_list)}")
    output_folder = os.path.join(
        runner, "sra_metadata_extraction_" + get_time()
    )

    # get the metadata report file
    output_file = get_sra_metadata(uri_list=uri_list)

    # upload the file to the bucket
    file_ul(bucket=bucket, output_folder=output_folder, sub_folder="", newfile=output_file)
    logger.info(f"output file {output_file} has been uploaded to bucket {bucket} path {output_folder}")

    return None
