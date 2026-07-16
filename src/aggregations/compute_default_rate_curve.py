from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def compute_default_rate_curve(df: DataFrame) -> DataFrame:

   return (
        df.withColumn("loan_age_months", F.floor(F.col("loan_age_months")))
        .groupBy("vintage_year", "loan_age_months")
        .agg(
            F.sum("is_default").alias("default_count"),
            F.count("*").alias("total_count"),
        )
        .withColumn("npl_ratio", F.col("default_count") / F.col("total_count"))
        .select("vintage_year", "loan_age_months", "default_count", "total_count", "npl_ratio")
    )
    