"""test age transform"""

import pandas as pd
import numpy as np
import sys
from datetime import datetime
from src.utils import get_time
from prefect import flow, get_run_logger


@flow(
    name="COG Transformer",
    log_prints=True,
    flow_run_name="cog-transformer-" + f"{get_time()}",
)
def cog_transformer(df_reshape_file_name: str, output_dir: str): 

    # Data Reshape/mutate

    runner_logger = get_run_logger()

    # Load the data
    df_reshape = pd.read_csv(df_reshape_file_name, sep="\t", low_memory=False)

    df_reshape.replace('.', '', inplace=True) 

    direct_columns = [
        "upi",
        "COG_UPR_DX.DX_DT",
        "DEMOGRAPHY.DM_BRTHDAT",
        "FOLLOW_UP.PT_FU_END_DT",
    ]


    # EQUATIONS

    age_data = df_mutation[df_mutation["DEMOGRAPHY.DM_BRTHDAT"].notna() & df_mutation["COG_UPR_DX.DX_DT"].notna() & df_mutation["FOLLOW_UP.PT_FU_END_DT"].notna()]
    no_age_data = df_mutation[df_mutation["DEMOGRAPHY.DM_BRTHDAT"].isna() | df_mutation["COG_UPR_DX.DX_DT"].isna() | df_mutation["FOLLOW_UP.PT_FU_END_DT"].isna()]

    age_data["age_at_diagnosis"] = abs(
        age_data["DEMOGRAPHY.DM_BRTHDAT"].astype(float)
    ) + abs(age_data["COG_UPR_DX.DX_DT"].astype(float))
    age_data["age_at_follow_up"] = abs(
        age_data["DEMOGRAPHY.DM_BRTHDAT"].astype(float)
    ) + abs(age_data["FOLLOW_UP.PT_FU_END_DT"].astype(float))


    #rejoin the dataframes
    df_mutation = pd.concat([age_data, no_age_data], ignore_index=True)

    df_mutation.to_csv(f"{output_dir}/COG_CCDI_submission_{get_time()}.tsv", sep="\t", index=False)