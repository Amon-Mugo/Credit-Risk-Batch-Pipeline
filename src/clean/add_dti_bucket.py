#dti is date to income

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_dti_bucket(df: DataFrame) -> DataFrame:
    df = df.withColumn("dti", F.expr("try_cast(dti as double)"))

    return df.withColumn(
        "dti_bucket",
        F.when(F.col("dti").isNull(), "Unknown")
        .when(F.col("dti") < 20, "Low")
        .when(F.col("dti") < 40, "Moderate")
        .otherwise("High"),
    )