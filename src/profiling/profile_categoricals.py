"""Profile categorical columns to inform normalization rules for Step 5.

Chains the full pipeline from raw CSV through Week 2.5 Step 4, then prints
distinct value counts for `purpose` and `addr_state`, ordered by frequency
descending, so rare/long-tail categories are visible before deciding on
collapsing or casing rules.
"""

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from src.clean.load_and_drop import get_spark_session, load_raw_data, drop_member_id
from src.clean.clean_term import clean_term
from src.clean.clean_dates import clean_dates
from src.clean.clean_sparse import clean_sparse
from src.clean.label_target import label_target
from src.clean.add_vintage_year import add_vintage_year
from src.clean.add_loan_age import add_loan_age
from src.clean.add_dti_bucket import add_dti_bucket
from src.clean.add_delinquency_flag import add_delinquency_flag

GCS_PATH = "gs://credit-risk-batch-pipeline/raw/accepted_2007_to_2018Q4.csv.gz"


def profile_column(df: DataFrame, column: str) -> DataFrame:
    """Return distinct values and counts for a column, ordered by count descending."""
    return (
        df.groupBy(column)
        .agg(F.count(F.lit(1)).alias("row_count"))
        .orderBy(F.desc("row_count"))
    )


def profile_categoricals(df: DataFrame, columns: list[str]) -> None:
    """Print distinct value profiles for each column in `columns`."""
    for column in columns:
        print(f"\n=== Distinct values: {column} ===")
        profile_column(df, column).show(n=60, truncate=False)


def main() -> None:
    spark = get_spark_session("profile_categoricals")
    df = load_raw_data(spark, GCS_PATH)
    df = drop_member_id(df)
    df = clean_term(df)
    df = clean_dates(df)
    df = clean_sparse(df)
    df = label_target(df)
    df = add_vintage_year(df)
    df = add_loan_age(df)
    df = add_dti_bucket(df)
    df = add_delinquency_flag(df)

    profile_categoricals(df, columns=["purpose", "addr_state"])

    spark.stop()


if __name__ == "__main__":
    main()
