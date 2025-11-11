from itertools import product
import requests
import pandas as pd
from datetime import date, datetime

from cloud_functions.utils.commons import fetch_ecb_data_for_ytd, transform_to_fact_table

def etl_fx_function(start_date, end_date):
    """
    Main ETL function to extract, transform FX data from ECB for given date range.
    """

    result_e = fetch_ecb_data_for_ytd(start_date, end_date)
    
    if isinstance(result_e, tuple):
        return result_e

    df_base_rates = result_e

    df_fact = transform_to_fact_table(df_base_rates)

    return df_fact
