

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

HARDSHIP_COLUMNS = (
    "hardship_flag",
    "hardship_type",
    "hardship_reason",
    "hardship_status",
    "deferral_term",
    "hardship_amount",
    "hardship_start_date",
    "hardship_end_date",
    "payment_plan_start_date",
    "hardship_length",
    "hardship_dpd",
    "hardship_loan_status",
    "orig_projected_additional_accrued_interest",
    "hardship_payoff_balance_amount",
    "hardship_last_payment_amount",
)

SETTLEMENT_COLUMNS = (
    "debt_settlement_flag",
    "debt_settlement_flag_date",
    "settlement_status",
    "settlement_date",
    "settlement_amount",
    "settlement_percentage",
    "settlement_term",
)

SEC_APP_COLUMNS = (
    "sec_app_fico_range_low",
    "sec_app_fico_range_high",
    "sec_app_earliest_cr_line",
    "sec_app_inq_last_6mths",
    "sec_app_mort_acc",
    "sec_app_open_acc",
    "sec_app_revol_util",
    "sec_app_open_act_il",
    "sec_app_num_rev_accts",
    "sec_app_chargeoff_within_12_mths",
    "sec_app_collections_12_mths_ex_med",
    "sec_app_mths_since_last_major_derog",
)

JOINT_COLUMNS = (
    "annual_inc_joint",
    "dti_joint",
    "verification_status_joint",
    "revol_bal_joint",
)

SPARSE_COLUMN_BLOCKS = {
    "hardship": HARDSHIP_COLUMNS,
    "settlement": SETTLEMENT_COLUMNS,
    "sec_app": SEC_APP_COLUMNS,
    "joint": JOINT_COLUMNS,
}

ALL_SPARSE_COLUMNS = HARDSHIP_COLUMNS + SETTLEMENT_COLUMNS + SEC_APP_COLUMNS + JOINT_COLUMNS


def profile_null_rates(df: DataFrame, columns: tuple) -> DataFrame:
   
    missing_columns = [col for col in columns if col not in df.columns]
    if missing_columns:
        raise ValueError(
            f"profile_null_rates: missing expected column(s): {missing_columns}"
        )

    non_null_exprs = [F.count(F.col(c)).alias(c) for c in columns] #
    total_expr = F.count(F.lit(1)).alias("__total_rows")
    agg_row = df.agg(total_expr, *non_null_exprs).collect()[0]

    total_rows = agg_row["__total_rows"]

    rows = []
    for column in columns:
        non_null_count = agg_row[column]
        null_count = total_rows - non_null_count
        fill_rate = (non_null_count / total_rows) if total_rows > 0 else 0.0
        rows.append((column, total_rows, non_null_count, null_count, fill_rate))

    result_schema = ["column", "total_rows", "non_null_count", "null_count", "fill_rate"]
    return df.sparkSession.createDataFrame(rows, result_schema).orderBy(F.desc("fill_rate"))


def profile_all_sparse_blocks(df: DataFrame) -> DataFrame:
    """Profile null/fill rates across all four sparse column blocks."""

    return profile_null_rates(df, ALL_SPARSE_COLUMNS)