import os
import sys
import pytest
import mock
from unittest.mock import MagicMock

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)

from src.utils import (
    CCDI_Tags,
    list_to_chunks,
    get_ccdi_latest_release,
    calculate_single_size_task,
)


# test for CCDI_Tags class
@pytest.fixture
def fake_tags_api_return():
    return_object = [
        {
            "name": "0.1.0",
            "zipball_url": "http://url/tags/0.1.0",
            "commit": {
                "sha": "fake_sha_1",
                "url": "https://api.github.com/repos/fake-repo/commits/12345efghi",
            },
            "node_id": "REF_FAKEnodeID1",
        },
        {
            "name": "0.2.0",
            "zipball_url": "http://url/tags/0.2.0",
            "commit": {
                "sha": "fake_sha_2",
                "url": "https://api.github.com/repos/fake-repo/commits/34567hijkl",
            },
            "node_id": "REF_FAKEnodeID2",
        },
    ]
    return return_object


@pytest.fixture
def my_ccdi_tags():
    return CCDI_Tags()


def test_CCDI_Tags_init(my_ccdi_tags):
    api_url = my_ccdi_tags.tags_api
    assert api_url == "https://api.github.com/repos/CBIIT/ccdi-model/tags"


@mock.patch("src.utils.requests", autospec=True)
def test_CCDI_Tags_get_tags(mock_requests, my_ccdi_tags, fake_tags_api_return):
    request_return = mock_requests.get.return_value
    request_return.json.return_value = fake_tags_api_return
    get_tags = my_ccdi_tags.get_tags()
    assert len(get_tags) == 2
    assert get_tags[0]["name"] == "0.1.0"
    assert get_tags[1]["node_id"] == "REF_FAKEnodeID2"


@mock.patch("src.utils.requests", autospec=True)
def test_CCDI_Tags_get_tags_only(mock_requests, my_ccdi_tags, fake_tags_api_return):
    request_return = mock_requests.get.return_value
    request_return.json.return_value = fake_tags_api_return
    get_tags_only = my_ccdi_tags.get_tags_only()
    assert get_tags_only == ["0.1.0", "0.2.0"]


@mock.patch("src.utils.requests", autospec=True)
def test_CCDI_Tags_if_tag_exists_true(
    mock_requests, my_ccdi_tags, fake_tags_api_return
):
    logger = MagicMock()
    request_return = mock_requests.get.return_value
    request_return.json.return_value = fake_tags_api_return
    check_if_tag = my_ccdi_tags.if_tag_exists(tag="0.1.0", logger=logger)
    assert check_if_tag == True


@mock.patch("src.utils.requests", autospec=True)
def test_CCDI_Tags_if_tag_exists_false(
    mock_requests, my_ccdi_tags, fake_tags_api_return
):
    logger = MagicMock()
    request_return = mock_requests.get.return_value
    request_return.json.return_value = fake_tags_api_return
    check_if_tag = my_ccdi_tags.if_tag_exists(tag="0.4.0", logger=logger)
    assert check_if_tag == False


@mock.patch("src.utils.requests", autospec=True)
def test_CCDI_Tags_get_tag_element(
    mock_requests, my_ccdi_tags, fake_tags_api_return
):
    request_return = mock_requests.get.return_value
    request_return.json.return_value = fake_tags_api_return
    tag_element = my_ccdi_tags.get_tag_element(tag="0.1.0")
    assert tag_element["zipball_url"] == "http://url/tags/0.1.0"



@mock.patch("src.utils.requests", autospec=True)
def test_get_ccdi_latest_release_valid(mock_requests):
    request_return = mock_requests.get.return_value
    request_return.json.return_value = {"tag_name": "1.8.2"}
    latest_release = get_ccdi_latest_release()
    assert latest_release == "1.8.2"


@mock.patch("src.utils.requests", autospec=True)
def test_get_ccdi_latest_release_invalid(mock_requests):
    request_return = mock_requests.get.return_value
    request_return.json.return_value = {"message": "failed api call"}
    latest_release = get_ccdi_latest_release()
    assert latest_release == "unknown"

    
def test_list_to_chunks():
    test_list = [1,2,3,4,5]
    chunked_list =  list_to_chunks(mylist=test_list, chunk_len=2)
    assert chunked_list[0] == [1,2]
    assert chunked_list[2] == [5]


@mock.patch("src.utils.requests", autospec=True)
def test_get_ccdi_latest_release(mock_requests):
    request_return = mock_requests.get.return_value
    request_return.json.return_value = {
        "tag_name": "9.9.9",
        "name": "v9.9.9: This is a test",
        "created_at": "2023-03-09T13:22:57Z",
        "published_at": "2023-03-09T13:22:50Z",
    }
    latest_tag =  get_ccdi_latest_release()
    assert latest_tag == "9.9.9"

@mock.patch("src.utils.set_s3_session_client", autospec=True)
def test_calculate_single_size_task(mock_client):
    s3_client = mock_client.return_value
    s3_client.get_object.return_value = {
        "LastModified": "fake_datetime",
        "ContentLength": 123,
        "ContentType": "string",
        "Metadata": {"string": "string"},
    }
    object_size = calculate_single_size_task.fn(s3uri="s3://test-bucket/test_folder/test_file.txt", s3_client=s3_client)
    assert object_size == "123"

