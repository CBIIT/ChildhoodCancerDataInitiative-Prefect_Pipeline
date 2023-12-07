import requests
from dataclasses import dataclass, field
from prefect import flow, task
import warnings
import pandas as pd


@dataclass
class GithubURL:
    """DataClass"""
    ccdi_model_recent_release: str = field(default="https://api.github.com/repos/CBIIT/ccdi-model/releases/latest")
    sra_template: str = field(default="https://raw.githubusercontent.com/CBIIT/ChildhoodCancerDataInitiative-CCDI_to_SRAy/main/doc/example_inputs/phsXXXXXX.xlsx")


def get_ccdi_latest_release() -> str:
    latest_url = GithubURL.ccdi_model_recent_release
    response =  requests.get(latest_url)
    tag_name  =  response.json()["tag_name"]
    return tag_name


def dl_sra_template() -> None:
    sra_filename="phsXXXXXX.xlsx"
    r = requests.get(GithubURL.sra_template)
    f = open(sra_filename, "wb")
    f.write(r.content)


def check_ccdi_version(ccdi_manifest: str) -> str:
    warnings.simplefilter(action="ignore", category=UserWarning)
    ccdi_dict = {}
    ccdi_excel = pd.ExcelFile(ccdi_manifest)
    ccdi_dict["instruction"] = pd.read_excel(
        ccdi_excel, sheet_name="README and INSTRUCTIONS", header=0
    )
    manifest_version =  ccdi_dict["instruction"].columns[2][1:]
    ccdi_excel.close()
    return manifest_version

#def dl_ccdi_template() -> None:
    
