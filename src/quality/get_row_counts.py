from pyspark.sql import DataFrame

def get_row_counts(raw_df:DataFrame, checkpoint_df:DataFrame) -> dict:

    raw_count=raw_df.count()
    checkpoint_count=checkpoint_df.count()
    delta=checkpoint_count-raw_count
    delta_pct=delta/raw_count*100 if raw_count else None

    return {
        "raw_row_count":raw_count,
        "checkpoint_row_count":checkpoint_count,
        "row_count_delta":delta,
        "row_count_delta_pct":round(delta_pct,2) if delta_pct is not None else None,
    }