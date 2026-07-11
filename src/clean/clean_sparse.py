from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col

from src.clean.clean_dates import clean_dates

NUMERIC_SPARSE_COLUMNS = [
    "annual_inc_joint",
    "dti_joint",
    "hardship_amount",
    "hardship_dpd",
    "hardship_payoff_balance_amount",
    "hardship_last_payment_amount",
    "orig_projected_additional_accrued_interest",
    "deferral_term",
    "settlement_amount",
    "settlement_percentage",
    "settlement_term",
]

DATE_SPARSE_COLUMNS = [
    "sec_app_earliest_cr_line",
    "debt_settlement_flag_date",
    "settlement_date",
    "hardship_start_date",
    "hardship_end_date",
    "payment_plan_start_date",
]



def cast_sparse_numerics(df: DataFrame) -> DataFrame:
    return df.withColumns({
        col_name:col(col_name).cast(DoubleType())
        for col_name in NUMERIC_SPARSE_COLUMNS
    })

def clean_sparse(df: DataFrame) -> DataFrame:
    df= cast_sparse_numerics(df)
    df=clean_dates(df, columns=DATE_SPARSE_COLUMNS)#this is where the date columns are cleaned
    return df