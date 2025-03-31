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
        "COG_UPR_DX.DATE_DIA",
        "DEMOGRAPHY.DM_BRTHDAT",
        "FOLLOW_UP.PT_FU_END_DT",
    ]

    df_mutation = df_reshape[direct_columns]

    # EQUATIONS

    """age_do = df_mutation[df_mutation["DEMOGRAPHY.DM_BRTHDAT"].notna() & df_mutation["COG_UPR_DX.DATE_DIA"].notna() & df_mutation["FOLLOW_UP.PT_FU_END_DT"].notna()]
    no_age_data = df_mutation[df_mutation["DEMOGRAPHY.DM_BRTHDAT"].isna() | df_mutation["COG_UPR_DX.DATE_DIA"].isna() | df_mutation["FOLLOW_UP.PT_FU_END_DT"].isna()].drop(
        ["COG_UPR_DX.DATE_DIA",
        "DEMOGRAPHY.DM_BRTHDAT",
        "FOLLOW_UP.PT_FU_END_DT",], axis=1
    )

    no_age_data["age_at_diagnosis"] = np.nan
    no_age_data["age_at_follow_up"] = np.nan

    age_data["age_at_diagnosis"] = abs(
        age_data["DEMOGRAPHY.DM_BRTHDAT"].astype(float)
    ) + abs(age_data["COG_UPR_DX.DATE_DIA"].astype(float))
    age_data["age_at_follow_up"] = abs(
        age_data["DEMOGRAPHY.DM_BRTHDAT"].astype(float)
    ) + abs(age_data["FOLLOW_UP.PT_FU_END_DT"].astype(float))"""

    df_mutation["age_at_diagnosis"] = abs(df_mutation["DEMOGRAPHY.DM_BRTHDAT"]) + abs(df_mutation["COG_UPR_DX.DATE_DIA"])
    df_mutation["age_at_follow_up"] = abs(df_mutation["DEMOGRAPHY.DM_BRTHDAT"]) + abs(df_mutation["FOLLOW_UP.PT_FU_END_DT"])

    df_mutation.to_csv(f"{output_dir}/COG_CCDI_submission_{get_time()}.tsv", sep="\t", index=False)