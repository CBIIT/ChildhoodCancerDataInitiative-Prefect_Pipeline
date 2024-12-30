import sys
import os
from prefect.testing.utilities import prefect_test_harness
from pathlib import Path
import pytest

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.c3dc_json_summary import count_values_per_key

FIXTURE_DIR = Path(__file__).parent.resolve() / "test_files"
ALL_FILES = pytest.mark.datafiles(
    FIXTURE_DIR / "test-c3dc-harmonization.json",
)

@ALL_FILES
def test_count_values_per_key(datafiles):
    """test for count_values_per_key()"""
    for file in datafiles.iterdir():
        if "json" in file.name:
            json_file = str(file)
    with prefect_test_harness():
        key_counts, key_sums = count_values_per_key(json_file=json_file)
    print(key_sums.keys())
    assert len(key_counts) == 8
    assert key_sums["race"] == 2
