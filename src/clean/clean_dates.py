from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

# Columns in the raw dataset stored as "MMM-yyyy" strings (e.g. "Dec-2015").
DATE_COLUMNS = (
    "issue_d",
    "earliest_cr_line",
    "last_pymnt_d",
    "next_pymnt_d",
    "last_credit_pull_d",
)

DATE_FORMAT = "MMM-yyyy"

# Matches a 3-letter month abbreviation, a dash, and a 4-digit year.
VALID_DATE_PATTERN = r"^[A-Za-z]{3}-\d{4}$"


def clean_dates(df: DataFrame, columns: tuple = DATE_COLUMNS) -> DataFrame:

    missing_columns = [col for col in columns if col not in df.columns]
    if missing_columns:
        raise ValueError(
            f"clean_dates: missing expected column(s): {missing_columns}"
        )

    for column in columns:
        trimmed = F.trim(F.col(column))
        df = df.withColumn(
            column,
            F.when(
                trimmed.rlike(VALID_DATE_PATTERN),
                F.to_date(trimmed, DATE_FORMAT),
            ).otherwise(F.lit(None).cast(DateType())),
        )

    return df