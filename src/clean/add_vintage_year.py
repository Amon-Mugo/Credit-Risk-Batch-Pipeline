from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_vintage_year(df:DataFrame) -> DataFrame:
    return  df.withColumn("vintage_year",F.year(F.col("issue_d" )))