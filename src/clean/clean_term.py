from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import  IntegerType


def clean_term(df: DataFrame) -> DataFrame:

    return (
        df.withColumn(
            "term_months",
            F.when(F.regexp_extract(F.col("term"), r"(\d+)", 1) == "", None).otherwise(F.regexp_extract(F.col("term"), r"(\d+)", 1).cast(IntegerType())),
        )
        .drop("term")
    )