import os
import sys
import pytest
import mock
import re

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.template_exampler import GetFakeValue


# tests for GetFakeValue
@pytest.fixture
def my_getfakevalue():
    return GetFakeValue()


@pytest.fixture
def my_word_lib():
    wordlist = [
        b"egg",
        b"applesauce",
        b"orangejuice",
        b"tag",
        b"elephant",
        b"chicken",
        b"incredible",
        b"computer",
        b"pen",
        b"nose",
        b"laboratory",
    ]
    return wordlist


@pytest.fixture
def test_enum_list():
    test_list = ["option1", "option2", "option3", "option4", "option5"]
    return test_list


@mock.patch("src.template_exampler.urlopen", autospec=True)
def test_getfakevalue_get_word_lib(mock_urlopen, my_getfakevalue, my_word_lib):
    fake_urlopen = mock_urlopen.return_value
    fake_read = fake_urlopen.read.return_value
    fake_read.splitlines.return_value = my_word_lib
    test_word_lib = my_getfakevalue.get_word_lib()
    assert "applesauce" in test_word_lib
    assert not "egg" in test_word_lib
    assert not "orangejuice" in test_word_lib


def test_getfakevalue_get_fake_md5sum(my_getfakevalue):
    test_md5sum = my_getfakevalue.get_fake_md5sum()
    find_list = re.findall(r"([a-fA-F\d]{32})", test_md5sum)
    assert len(find_list) == 1


def test_getfakevalue_get_fake_uuid(my_getfakevalue):
    test_uuid = my_getfakevalue.get_fake_uuid()
    assert "dg.4DFC/" in test_uuid


@mock.patch("src.template_exampler.urlopen", autospec=True)
def test_getfakevalue_get_fake_str(mock_urlopen, my_getfakevalue, my_word_lib):
    fake_urlopen = mock_urlopen.return_value
    fake_read = fake_urlopen.read.return_value
    fake_read.splitlines.return_value = my_word_lib
    myfakestr = my_getfakevalue.get_fake_str()
    fake_list = myfakestr.split("_")
    assert len(fake_list) == 3
    assert int(fake_list[2]) <= 100
    assert len(fake_list[0]) > 5
    assert len(fake_list[1]) < 11


def test_getfakevalue_get_random_int(my_getfakevalue):
    random_int = my_getfakevalue.get_random_int()
    assert isinstance(random_int, int)
    assert random_int < 1000000


def test_getfakevalue_get_random_number(my_getfakevalue):
    random_float = my_getfakevalue.get_random_number()
    assert isinstance(random_float, float)
    assert random_float < 1000000


def test_getfakevalue_random_enum_single(my_getfakevalue, test_enum_list):
    random_enum_single = my_getfakevalue.get_random_enum_single(test_enum_list)
    assert random_enum_single in test_enum_list


def test_getfakevalue_random_enum_single_empty(my_getfakevalue):
    random_enum_single = my_getfakevalue.get_random_enum_single(enum_list=[])
    assert random_enum_single == ""


@mock.patch("src.template_exampler.urlopen", autospec=True)
def test_getfakevalue_random_str_list(mock_urlopen, my_getfakevalue, my_word_lib):
    fake_urlopen = mock_urlopen.return_value
    fake_read = fake_urlopen.read.return_value
    fake_read.splitlines.return_value = my_word_lib
    test_fake_str_list = my_getfakevalue.get_random_string_list()
    assert len(test_fake_str_list.split(";")) <= 3
    assert len(test_fake_str_list.split(";")[0].split("_")) == 3


def test_getfakevalue_get_random_enum_list(my_getfakevalue, test_enum_list):
    test_enum_list = my_getfakevalue.get_random_enum_list(test_enum_list)
    assert len(test_enum_list.split(";")) >= 1
    assert test_enum_list.split(";")[0] in test_enum_list


def test_getfakevalue_get_random_enum_list_empty(my_getfakevalue):
    test_enum_list = my_getfakevalue.get_random_enum_list(enum_list=[])
    assert test_enum_list == ""


@mock.patch("src.template_exampler.random", autospec=True)
def test_getfakevalue_get_enum_string_list(mock_random, my_getfakevalue, test_enum_list):
    return None

