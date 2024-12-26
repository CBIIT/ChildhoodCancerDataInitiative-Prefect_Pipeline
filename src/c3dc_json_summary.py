import json
from collections import defaultdict
import os
from prefect import flow, task

"""
def process_item(key_counts, key_sums, item): 
    if isinstance(item, list):
        for sub_item in item:
            process_item(key_counts=key_counts, key_sums=key_sums, item=sub_item)
    elif isinstance(item, dict):
        for key, value in item.items():
            key_counts[key][json.dumps(value, sort_keys=True)] += 1
            key_sums[key] += 1  # Increment count for the key
            process_item(key_counts=key_counts, key_sums=key_sums, item=value)
    return key_counts, key_sums    
"""

@flow(name="Count nodes and records per node", log_prints=True)
def count_values_per_key(json_file: str) -> tuple:
    """For each item, for each key, count the instances of that value and record the total number for each value and total in the key.

    Args:
        json_file (str): json file path contains deserialized JSON data.

    Returns:
        tuple: A tuple containing key_counts and key_sums
    """
    with open(json_file, "r") as file:
        data = json.load(file)
    key_counts = defaultdict(lambda: defaultdict(int))
    key_sums = defaultdict(int)

    def process_item(item):
        if isinstance(item, list):
            for sub_item in item:
                process_item(sub_item)
        elif isinstance(item, dict):
            for key, value in item.items():
                key_counts[key][json.dumps(value, sort_keys=True)] += 1
                key_sums[key] += 1  # Increment count for the key
                process_item(value)

    for record_type, records in data.items():
        for record in records:
            process_item(record)
    # for record_type, records in data.items():
    #    for record in records:
    #        process_item(key_counts=key_counts, key_sums=key_sums, item=record)

    return key_counts, key_sums

#@flow(name="Write summary of json file", log_prints=True)
def write_c3dc_json_summary(filepath: str, json_filepath: str, key_counts: dict, key_sums: dict) -> None:
    """Write a detailed summary of the JSON file to a text file.

    Args:
        filepath (str): summary output path of text file
        json_filepath (str): json file path if input json file 
        key_counts (dict): key counts dictionary object
        key_sums (dict): key sums dictionary object

    """    
    # Implementation for writing the summary to a file
    with open(filepath, "w") as output_file:
        output_file.write(
            f"The following is a summary for the file: {json_filepath}.\nThe keys for '_id' and 'age_at_' were removed for readability, as these values are often unique per entry and thus would list each entry in the study.\n\n"
        )
        for key, value_counts in key_counts.items():
            # skip all _id props
            if "_id" not in key:
                # skip all age_at props
                if "age_" not in key:
                    output_file.write(f"Key: {key}, Total Count: {key_sums[key]}\n")
                    for value, count in value_counts.items():
                        output_file.write(f"\t{value}: {count} occurrences\n")
                    output_file.write("\n")
    return None

@flow(name="Create json summaries for harmonized json files", log_prints=True)
def create_c3dc_json_summaries(folder_path: str, output_dir: str) -> None:
    """Create a summary of each JSON file.
    Walk through the folder path. The harmonized json file(s) are under subfolder of each study.

    Args:
        folder_path (str): folder path which contains harmonized json file(s) under subfolder of each study
        output_dir (str): output directory for the summary files
    """
    # identify subfolders under the folder path
    study_folders = [os.path.join(folder_path, subfolder) for subfolder in os.listdir(folder_path) if os.path.isdir(os.path.join(folder_path, subfolder))]
    # identify all the json files under each subfolder
    json_files = []
    for subfolder in study_folders:
        subfolder_files = [os.path.join(subfolder, file) for file in os.listdir(subfolder) if file.endswith(".json")]
        json_files.extend(subfolder_files)
    # process each json file
    for json_file in json_files:
        print(f"processing: {json_file}")
        print("calculating key counts and sums")
        key_counts, key_sums = count_values_per_key(json_file=json_file)
        #print(key_counts)
        #print(key_sums)
        output_file = os.path.join(output_dir, f"{os.path.basename(json_file).replace('.json', '_summary.txt')}")
        print(f"writing summary to: {output_file}")
        write_c3dc_json_summary(filepath=output_file, json_filepath=json_file, key_counts=key_counts, key_sums=key_sums)
    return None
