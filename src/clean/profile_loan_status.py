from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def profile_loan_status(df: DataFrame) -> DataFrame:
    total = df.count()
    return (
        df.groupBy("loan_status").agg(F.count("*").alias("count"))
        .withColumn("pct", F.round(F.col("count") / F.lit(total) * 100, 2))
        .orderBy(F.col("count").desc())
    )