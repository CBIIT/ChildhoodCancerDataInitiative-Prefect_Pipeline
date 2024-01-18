import os
import sys
from prefect import flow, task
from urllib.request import urlopen
from typing import List, Dict
import random
import string
import uuid
from src.utils import GithubAPTendpoint, CCDI_Tags, get_time


class GetFakeValue:
    def __init__(self) -> None:
        self.word_site = "https://www.mit.edu/~ecprice/wordlist.10000"

    @classmethod
    def get_word_lib(self):
        """Creates a word library with word length between 6 and 10"""
        response = urlopen(self.word_site)
        text = response.read()
        words = text.splitlines()
        filter_words = [i for i in words if len(i) > 5 and len(i) < 11]
        return filter_words

    @classmethod
    def get_fake_md5sum(self):
        """Generates fake md5sum value"""
        chars = string.ascii_lowercase + string.digits
        fake_md5sum = "".join(random.choice(chars) for _ in range(32))
        return fake_md5sum

    @classmethod
    def get_fake_uuid(self):
        """Generates fake uuid id"""
        fake_uuid = uuid.uuid4()
        return "dg.4DFC/" + str(fake_uuid)

    @classmethod
    def get_fake_str(self):
        """Generates a fake string with two words and a number"""
        word_pool = self.get_word_lib()
        two_random_index = [random.randrange(0, len(word_pool)) for _ in range(2)]
        two_random_words = [word_pool[i] for i in two_random_index]
        one_random_int = random.randrange(0, 100)
        fake_list = two_random_words.append(str(one_random_int))
        return "_".join(fake_list)

    @classmethod
    def get_fake_file_url_in_cds(self):
        """Generate fake s3 file url"""
        fake_str = self.get_fake_str()
        fake_url = "s3://" + fake_str
        return fake_url

    @classmethod
    def get_random_int(self):
        """Generates random int"""
        random_int = random.randint(1, 1000000)
        return random_int

    @classmethod
    def get_random_number(self):
        """Generates random float"""
        random_float = round(random.uniform(1, 1000000), 2)
        return random_float

    def get_random_enum_single(self, enum_list: List) -> str:
        """Generates fake value of enum type property"""
        if len(enum_list) == 0:
            return ""
        else:
            rand_enum = random.choice(enum_list)
            return rand_enum

    def get_random_string_list(self) -> str:
        """Generates fake value of array[string] type property"""
        rand_enum_len = random.randint(1, 2)
        string_list = [self.get_fake_str for _ in range(rand_enum_len)]
        return ";".join(string_list)

    def get_random_enum_list(self, enum_list: List) -> str:
        """Generates fake value of array[enum] type property"""
        if len(enum_list) <= 1:
            rand_enum_list = enum_list
        else:
            rand_enum_len = random.randint(1, 2)
            rand_enum_list = random.sample(enum_list, rand_enum_len)
        return ";".join(rand_enum_list)

    def get_random_enum_string_list(self, enum_list: List) -> str:
        """Generates fake value of array[string;enum] type property"""
        if len(enum_list) <= 1:
            rand_enum_list = enum_list
        else:
            rand_enum_len = random.randint(1, 2)
            rand_enum_list = random.sample(enum_list, rand_enum_len)

        add_str = random.randint(0, 1)
        if add_str == 1:
            rand_str = self.get_fake_str()
            rand_enum_list.append(rand_str)
        else:
            pass
        return ";".join(rand_enum_list)

    def get_random_str_or_enum(self, enum_list) -> str:
        """Generates fake value of string;enum type of property"""
        str_or_enum = random.randint(0, 1)
        if str_or_enum == 0:
            if len(enum_list) > 0:
                return_value = random.choice(enum_list)
            else:
                return_value = ""
        else:
            return_value = self.get_fake_str
        return return_value
            

@flow(
    name="make template example file",
    log_prints=True,
    flow_run_name="make-template-example" + f"{get_time()}",
)
def make_template_example(manifest_path: str, entry_num: int) -> None:
    return None
