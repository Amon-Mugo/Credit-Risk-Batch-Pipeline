from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def compute_npl_ratio(df:DataFrame,dimension:str) -> DataFrame:

    return(
        df.groupBy(F.col(dimension).alias("dimension_value"))
        .agg(
            F.sum("is_default").alias("default_count"),
            F.count("*").alias("total_count"),

        )
        .withColumn("npl_ratio", F.col("default_count") / F.col("total_count"))
        .withColumn("dimension",F.lit(dimension))
        .select("dimension","dimension_value","default_count","total_count","npl_ratio")
    )