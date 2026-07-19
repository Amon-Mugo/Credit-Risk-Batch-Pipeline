from pyspark.sql import DataFrame
import pyspark.sql.functions as F

# Value-count proportions per categorical column, used as the baseline
# snapshot for future-rerun drift comparison (2a). Not a time-series check —
# this dataset is static — it's a "does a rerun still look like this run"
# tripwire for future pipeline executions.

def get_distribution_snapshot(df: DataFrame, categorical_columns: list) -> dict:
    total = df.count()
    snapshot = {}

    for column in categorical_columns:
        counts = (
            df.groupBy(column)
            .agg(F.count("*").alias("count"))
            .collect()
        )

        proportions = {
            str(row[column]): round((row["count"] / total) * 100, 2) if total else None
            for row in counts
        }
        snapshot[column] = proportions

    return snapshot