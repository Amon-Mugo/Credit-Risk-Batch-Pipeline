"""Validate categorical columns after column-shift rows have been dropped"""

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

# The 14 known LendingClub loan purpose categories, observed via
# src/profiling/profile_categoricals.py against the cleaned dataset.
VALID_PURPOSES = {
    "debt_consolidation",
    "credit_card",
    "home_improvement",
    "other",
    "major_purchase",
    "medical",
    "small_business",
    "car",
    "moving",
    "vacation",
    "house",
    "wedding",
    "renewable_energy",
    "educational",
}

# Matches exactly two uppercase letters, e.g. "CA", "NY", "DC"
VALID_STATE_REGEX = r"^[A-Z]{2}$"


def validate_categoricals(df: DataFrame) -> None:
    """Raise ValueError if purpose or addr_state contain unexpected values.

    Does not modify or return the DataFrame -- this is a pass/fail check
    intended to run after `drop_shifted_rows`, before downstream feature
    encoding work begins.
    """
    invalid_purpose_count = df.filter(
        ~F.col("purpose").isin(*VALID_PURPOSES)
    ).count()
    if invalid_purpose_count > 0:
        raise ValueError(
            f"Found {invalid_purpose_count} rows with unexpected purpose "
            f"values outside the known {len(VALID_PURPOSES)}-category set."
        )

    invalid_state_count = df.filter(
        ~F.col("addr_state").rlike(VALID_STATE_REGEX)
    ).count()
    if invalid_state_count > 0:
        raise ValueError(
            f"Found {invalid_state_count} rows with addr_state values "
            f"that do not match a valid 2-letter state/territory code."
        )