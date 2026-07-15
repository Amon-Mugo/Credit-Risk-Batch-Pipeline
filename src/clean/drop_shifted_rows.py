"""Drop rows affected by CSV column-shift corruption.

Some upstream rows contain unescaped quotes or commas inside free-text
fields (e.g. `desc`, `emp_title`), causing the CSV parser to misread field
boundaries and shift all subsequent column values by one or more positions.
This is the same failure mode as the single corrupted `emp_title` row
dropped in Week 2 Step 5 -- this module generalizes that handling.

`addr_state` is used as the detection signal because it has a narrow,
well-known valid-value set (2-letter US state/territory codes), making it
a reliable canary for row-level corruption that free-text columns cannot
provide on their own.
"""

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

# Matches exactly two uppercase letters, e.g. "CA", "NY", "DC"
VALID_STATE_REGEX = r"^[A-Z]{2}$"


def drop_shifted_rows(df: DataFrame) -> DataFrame:
    """Remove rows where addr_state fails to match a valid state code.

    A non-matching addr_state indicates the row's columns are misaligned
    due to upstream CSV parsing corruption, not a legitimate data value.
    """
    return df.filter(F.col("addr_state").rlike(VALID_STATE_REGEX))
