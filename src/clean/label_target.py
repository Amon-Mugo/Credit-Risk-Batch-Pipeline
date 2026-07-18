# src/clean/label_target.py
# Purpose: filters the dataset to only resolved loan outcomes (Fully Paid,
# Charged Off) and derives the binary target column used for credit risk
# modeling. Excludes in-progress statuses (Current, Late, etc.), NULLs, and
# any malformed rows as a natural side effect of the isin() filter.

# src/clean/label_target.py
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

RESOLVED_STATUSES = ("Fully Paid", "Charged Off")


def label_target(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("loan_status").isin(*RESOLVED_STATUSES))
        .withColumn(
            "is_default",
            F.when(F.col("loan_status") == "Charged Off", 1).otherwise(0),
        )
    )