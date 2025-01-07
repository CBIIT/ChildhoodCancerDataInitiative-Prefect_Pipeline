from src.utils import get_time, set_s3_session_client
from src.read_buckets import paginate_parameter
from prefect import task, flow
import os
import json
from src.utils import get_time, file_ul

@task(name="Search bucket content with text list")
def search_bucket_content(bucket_path: str, search_text_list: list[str]) -> dict:
    """Search for a list of text in the bucket content

    Args:
        bucket_path (str): Bucket path to search
        search_text_list (list[str]): A list of text to search in object keys

    Returns:
        dict: A dictionar with search text as key and list of object keys as value
    """    
    s3 = set_s3_session_client()
    s3_paginator = s3.get_paginator("list_objects_v2")
    operation_parameters = paginate_parameter(bucket_path)
    bucket_name = operation_parameters["Bucket"]
    pages = s3_paginator.paginate(**operation_parameters)
    return_dict = {}
    for text in search_text_list:
        return_dict[text] = []
    for page in pages:
        if "Contents" in page.keys():
            for obj in page["Contents"]:
                obj_key = obj["Key"]
                for text in search_text_list:
                    if text in obj_key:
                        return_dict[text].append("s3://" + bucket_name + "/" + obj_key)
                    else:
                        pass
        else:
            pass
    return return_dict


@flow(name="Search a list of bucket content with text list", log_prints=True)
def search_buckets_content(bucket: str, runner: str, bucket_path_list: list[str], search_text_list: list[str]) -> dict:
    """Search for a list of text in the list of bucket path

    Args:
        bucket (str): Bucket of where the search result will be stored
        runner (str): unique runner name
        bucket_path_list (list[str]): List of bucket paths to search
        search_text_list (list[str]): A list of text to search in object keys

    Returns:
        dict: A dictionar with search text as key and list of object keys as value
    """
    return_dict = {}
    for bucket_path in bucket_path_list:
        search_dict = search_bucket_content(bucket_path, search_text_list)
        for key in search_dict.keys():
            if key in return_dict.keys():
                return_dict[key].extend(search_dict[key])
            else:
                return_dict[key] = search_dict[key]
    # write search result to a file
    file_name = "bucket_content_search_result.json"
    json_obj = json.dumps(return_dict, indent=4)
    with open(file_name, "w") as outfile:
        outfile.write(json_obj)

    output_folder = os.path.join(runner, "search_bucket_content_" + get_time())
    file_ul(bucket=bucket, output_folder=output_folder, sub_folder="", newfile=file_name)
    return None