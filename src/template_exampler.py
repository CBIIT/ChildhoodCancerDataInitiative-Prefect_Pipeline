import os
import sys
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from typing import List, Dict, TypeVar
import random
import string
import uuid
import pandas as pd
from shutil import copy
import re
from wonderwords import RandomWord
from src.utils import CheckCCDI, get_time, get_logger, get_date, view_all_s3_objects


GetFakeValue = TypeVar("GetFakeValue")
DataFrame = TypeVar("DataFrame")


def columns_to_populate(full_col_list: List) -> List:
    """Removes linking properties and indexing properties from column list"""
    # remove the "type" property
    full_col_list.remove("type")
    full_col_list.remove("id")
    newlist = [i for i in full_col_list if "." not in i]
    return newlist


def get_property_type(dict_df: DataFrame, property_name: str, sheet_name: str) -> str:
    """filter dict_df based on property name and sheet/node name
    there should be only one match
    """
    property_type = dict_df.loc[
        (dict_df["Node"] == sheet_name) & (dict_df["Property"] == property_name)
    ]["Type"].values[0]
    return property_type


def if_linking_prop(property_name: str) -> bool:
    if "." in property_name:
        name_list = property_name.split(".")
        if name_list[0] + "_id" == name_list[1]:
            return True
        else:
            return False
    else:
        return False


@task
def populate_exampler(
    file_path: str,
    fake_data_generater: GetFakeValue,
    skip_sheets: List[str],
    dict_df: DataFrame,
    term_dict: Dict,
    entry_count: int,
    logger,
) -> None:
    """
    Returns a dict that has keys of sheet names and df of populated fake values
    """
    excelfile = CheckCCDI(ccdi_manifest=file_path)
    sheetnames = excelfile.get_sheetnames()

    sheetnames_short = [i for i in sheetnames if i not in skip_sheets]

    # generate a word generater
    filter_words = fake_data_generater.get_word_lib()

    populated_dfs = {}
    for j in sheetnames_short:
        logger.info(f"Populating sheet {j}")
        j_df_template = excelfile.read_sheet(sheetname=j)
        j_columns = j_df_template.columns.tolist()
        j_populated_df = pd.DataFrame(columns=j_columns)
        j_populated_df["type"] = [j for k in range(entry_count)]
        j_columns_to_populate = columns_to_populate(full_col_list=j_columns)
        for m in j_columns_to_populate:
            if m == "md5sum":
                j_populated_df[m] = [
                    fake_data_generater.get_fake_md5sum() for k in range(entry_count)
                ]
            elif m == "file_url":
                j_populated_df[m] = [
                    fake_data_generater.get_fake_file_url(
                        random_words=filter_words
                    )
                    for k in range(entry_count)
                ]
            elif m == "dcf_indexd_guid":
                j_populated_df[m] = [
                    fake_data_generater.get_fake_uuid() for k in range(entry_count)
                ]
            elif "age_at" in m:
                j_populated_df[m] = [
                    fake_data_generater.get_random_age() for k in range(entry_count)
                ]
            else:
                m_type = get_property_type(
                    dict_df=dict_df, property_name=m, sheet_name=j
                )
                if m_type == "string":
                    j_populated_df[m] = [
                        fake_data_generater.get_fake_str(random_words=filter_words)
                        for k in range(entry_count)
                    ]
                elif m_type == "integer":
                    j_populated_df[m] = [
                        fake_data_generater.get_random_int() for k in range(entry_count)
                    ]
                elif m_type == "number":
                    j_populated_df[m] = [
                        fake_data_generater.get_random_number()
                        for k in range(entry_count)
                    ]
                elif m_type == "array[string]":
                    j_populated_df[m] = [
                        fake_data_generater.get_random_string_list(
                            random_words=filter_words
                        )
                        for k in range(entry_count)
                    ]
                elif m_type == "enum":
                    m_enum_list = term_dict[m]
                    j_populated_df[m] = [
                        fake_data_generater.get_random_enum_single(
                            enum_list=m_enum_list
                        )
                        for k in range(entry_count)
                    ]
                elif m_type == "array[enum]":
                    m_enum_list = term_dict[m]
                    j_populated_df[m] = [
                        fake_data_generater.get_random_enum_list(enum_list=m_enum_list)
                        for k in range(entry_count)
                    ]
                elif m_type == "string;enum":
                    m_enum_list = term_dict[m]
                    j_populated_df[m] = [
                        fake_data_generater.get_random_str_or_enum(
                            enum_list=m_enum_list, random_words=filter_words
                        )
                        for k in range(entry_count)
                    ]
                elif m_type == "array[string;enum]":
                    m_enum_list = term_dict[m]
                    j_populated_df[m] = [
                        fake_data_generater.get_random_enum_string_list(
                            enum_list=m_enum_list, random_words=filter_words
                        )
                        for k in range(entry_count)
                    ]
                else:
                    logger.error(
                        f"Property {m} in Node {j} has an unknown property type of {m_type}. Therefore this property is left empty in the output"
                    )
                    j_populated_df[m] = ["" for k in range(entry_count)]
        populated_dfs[j] = j_populated_df

    # limiting entry number for study, study_admin, study_arms, study_funding, study_personnel, and publications
    # 1 entry for study
    populated_dfs["study"] = populated_dfs["study"].head(1)
    # 1 entry for study_admin
    populated_dfs["study_admin"] = populated_dfs["study_admin"].head(1)
    # 3 entries for study_arm
    populated_dfs["study_arm"] = populated_dfs["study_arm"].head(3)
    # 5 entries for study_funding
    populated_dfs["study_funding"] = populated_dfs["study_funding"].head(5)
    # 5 entries for study_personnel
    populated_dfs["study_personnel"] = populated_dfs["study_personnel"].head(5)
    # 3 entries for publication
    populated_dfs["publication"] = populated_dfs["publication"].head(3)

    # fix acl value in file nodes, adding [ and ]
    for node in populated_dfs.keys():
        node_df =  populated_dfs[node]
        if 'acl' in node_df.columns:
            node_df['acl'] =  "['" + node_df["acl"].astype(str) + "']"
            populated_dfs[node] = node_df

    return populated_dfs


@task
def create_linkage(populated_dfs: Dict) -> Dict:
    study_id = populated_dfs["study"]["study_id"][0]
    # populate linking properties
    for key, key_df in populated_dfs.items():
        key_df_cols = key_df.columns
        linking_cols = [i for i in key_df_cols if if_linking_prop(i)]
        if len(linking_cols) > 0:
            for k in linking_cols:
                if k == "study.study_id":
                    key_df.loc[:, "study.study_id"] = study_id
                else:
                    sheet_name, sheet_name_id = k.split(".")
                    key_df[k] = populated_dfs[sheet_name][sheet_name_id]
            populated_dfs[key] = key_df
        else:
            pass
    # randomly remove linkage for node linked to multiple parent nodes
    for key, key_df in populated_dfs.items():
        key_df_cols = key_df.columns
        linking_cols = [i for i in key_df_cols if if_linking_prop(i)]
        if len(linking_cols) >= 2:
            for j in range(key_df.shape[0]):
                linking_cols_copy = linking_cols.copy()
                linking_col_keep = random.choice(linking_cols_copy)
                linking_cols_copy.remove(linking_col_keep)
                for k in linking_cols_copy:
                    key_df.iloc[j, key_df.columns.get_loc(k)] = ""
            populated_dfs[key] = key_df
        else:
            pass
    return populated_dfs


class GetFakeValue:
    def __init__(self) -> None:
        return None

    @classmethod
    def get_word_lib(self):
        """Creates a word generator with adjective words of length longer than 7"""
        rand_words = RandomWord()
        return rand_words

    @classmethod
    def get_fake_md5sum(self):
        """Generates fake md5sum value"""
        chars = string.ascii_lowercase[:6] + string.digits
        fake_md5sum = "".join(random.choice(chars) for _ in range(32))
        return fake_md5sum

    @classmethod
    def get_fake_uuid(self):
        """Generates fake uuid id"""
        fake_uuid = uuid.uuid4()
        return "dg.4DFC/" + str(fake_uuid)

    def get_fake_str(self, random_words) -> str:
        """Generates a fake string with two words and a number"""
        two_random_words = random_words.random_words(
            amount=2, include_categories=["adjectives"], word_min_length=8
        )
        one_random_int = random.randrange(0, 100)
        two_random_words.append(str(one_random_int))
        return "_".join(two_random_words)

    def get_fake_file_url(self, random_words):
        """Generate fake s3 file url"""
        fake_str = self.get_fake_str(random_words=random_words)
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

    @classmethod
    def get_random_age(self):
        """Generates a random age value"""
        random_age = random.randint(0, 32849)
        return random_age

    def get_random_enum_single(self, enum_list: List) -> str:
        """Generates fake value of enum type property"""
        if len(enum_list) == 0:
            return ""
        else:
            rand_enum = random.choice(enum_list)
            return rand_enum

    def get_random_string_list(self, random_words) -> str:
        """Generates fake value of array[string] type property"""
        rand_enum_len = random.randint(2, 3)
        string_list = [
            self.get_fake_str(random_words=random_words) for _ in range(rand_enum_len)
        ]
        return ";".join(string_list)

    def get_random_enum_list(self, enum_list: List) -> str:
        """Generates fake value of array[enum] type property"""
        if len(enum_list) <= 1:
            rand_enum_list = enum_list
        else:
            rand_enum_len = random.randint(1, 2)
            rand_enum_list = random.sample(enum_list, rand_enum_len)
        return ";".join(rand_enum_list)

    def get_random_enum_string_list(self, enum_list: List, random_words) -> str:
        """Generates fake value of array[string;enum] type property"""
        if len(enum_list) <= 1:
            rand_enum_list = enum_list
        else:
            rand_enum_len = random.randint(1, 2)
            rand_enum_list = random.sample(enum_list, rand_enum_len)

        add_str = random.randint(0, 1)
        if add_str == 1:
            rand_str = self.get_fake_str(random_words=random_words)
            rand_enum_list.append(rand_str)
        else:
            pass
        return ";".join(rand_enum_list)

    def get_random_str_or_enum(self, enum_list: List, random_words) -> str:
        """Generates fake value of string;enum type of property"""
        str_or_enum = random.randint(0, 1)
        if str_or_enum == 0:
            if len(enum_list) > 0:
                return_value = random.choice(enum_list)
            else:
                return_value = ""
        else:
            return_value = self.get_fake_str(random_words)
        return return_value


@task
def make_template_exampler_md(
    source_bucket: str,
    runner: str,
    output_folder: str,
    manifest_version: str,
):
    """
    Creates markdown file summary of template updater flow run
    """
    source_file_list = view_all_s3_objects(source_bucket=source_bucket)
    manifest_template = [
        i
        for i in source_file_list
        if re.search(
            r"_Template_v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)\.xlsx$",
            i,
        )
        and output_folder in i
    ]

    exampler_log = [
        j
        for j in source_file_list
        if re.search(
            r"template_exampler_[0-9]{4}-[0-9]{2}-[0-9]{2}\.log$",
            j,
        )
        and output_folder in j
    ]

    exampler = [
        j
        for j in source_file_list
        if re.search(
            r"Exampler\.xlsx$",
            j,
        )
        and output_folder in j
    ]
    if len(exampler) == 0:
        exampler.append("")
    else:
        pass

    markdown_report = f"""# CCDI Template Updater Workflow Summary

### Source Bucket

{source_bucket}

### Runner

{runner}

### Output Folder

{output_folder}

### CCDI Manifest Template

- File: {os.path.basename(manifest_template[0])}

- Version: {manifest_version}

### CCDI Template Exampler Output

- File: {os.path.basename(exampler[0])}

### Template Exampler Log

- Log: {os.path.basename(exampler_log[0])}

"""
    create_markdown_artifact(
        key=f"{runner.lower().replace('_','-').replace(' ','-').replace('.','-').replace('/','-')}-template-exampler-summary",
        markdown=markdown_report,
        description=f"{runner} template exampler worklfow summary",
    )


@flow(
    name="make template example file",
    log_prints=True,
    flow_run_name="make-template-example" + f"{get_time()}",
)
def make_template_example(manifest_path: str, entry_num: int) -> tuple:
    # create a logger
    logger = get_logger(loggername="template_exampler", log_level="info")
    logger_filename = "template_exampler" + "_" + get_date() + ".log"

    # copy the manifest to the output path
    manifest_basename = os.path.basename(manifest_path)
    manifest_filename_wo_ext = manifest_basename.rsplit(".", 1)[0]
    manifest_dir = os.path.dirname(manifest_path)
    output_filename = manifest_filename_wo_ext + "_" + str(entry_num) + "Exampler.xlsx"
    output_path = os.path.join(manifest_dir, output_filename)
    copy(manifest_path, output_path)
    logger.info(f"Template path: {manifest_path}")

    checkccdi = CheckCCDI(ccdi_manifest=manifest_path)
    template_dict_df = checkccdi.get_dict_df()
    template_term_dict = checkccdi.get_terms_value_sets()

    # Populate the df of each node with fake data
    fake_data_generater = GetFakeValue()
    logger.info("Start generating example file using ccdi manifes template")
    populated_dfs = populate_exampler(
        file_path=output_path,
        fake_data_generater=fake_data_generater,
        skip_sheets=["README and INSTRUCTIONS", "Dictionary", "Terms and Value Sets"],
        dict_df=template_dict_df,
        term_dict=template_term_dict,
        entry_count=entry_num,
        logger=logger,
    )

    # Create links between nodes
    populated_dfs = create_linkage(populated_dfs=populated_dfs)

    # write outputs
    logger.info(f"Start writing output file {output_path}")
    with pd.ExcelWriter(
        output_path, mode="a", engine="openpyxl", if_sheet_exists="overlay"
    ) as writer:
        # for each sheet df
        for sheet_name in populated_dfs.keys():
            sheet_df = populated_dfs[sheet_name]
            sheet_df.to_excel(writer, sheet_name=sheet_name, index=False, header=False, startrow=1)

    return output_path, logger_filename
