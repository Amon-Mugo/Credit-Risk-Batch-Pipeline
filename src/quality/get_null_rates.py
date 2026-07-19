from pyspark.sql import DataFrame
import pyspark.sql.functions as F

# Null % for the columns downstream logic actually depends on, computed in a
# single wide .agg() pass rather than one .count() per column — same pattern
# as Week 2.5's profile_sparse.py, avoiding N full scans for N columns.

def get_null_rates(df: DataFrame, columns: list) -> dict:
    total = df.count()

    agg_exprs = [
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in columns
    ]
    null_counts = df.agg(*agg_exprs).collect()[0]

    return {
        c: round((null_counts[c] / total) * 100, 2) if total else None
        for c in columns
    }