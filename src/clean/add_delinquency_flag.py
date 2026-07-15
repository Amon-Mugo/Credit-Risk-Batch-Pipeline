from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def add_delinquency_flag(df: DataFrame) -> DataFrame:

    delinq_col = F.expr("try_cast(delinq_2yrs as double)")

    return df.withColumn(
        "has_prior_delinquency",
        F.when(delinq_col.isNull(), F.lit(None).cast("boolean"))
        .when(delinq_col > 0, F.lit(True))
        .otherwise(F.lit(False)),
    )