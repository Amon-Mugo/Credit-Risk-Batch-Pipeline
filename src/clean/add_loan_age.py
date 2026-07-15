from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_loan_age(df:DataFrame) -> DataFrame:
    return  df.withColumn("loan_age_months",F.months_between(F.col("last_pymnt_d"),F.col("issue_d")),)