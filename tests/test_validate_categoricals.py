"""Tests for validate_categoricals: verifies clean data passes silently
and unexpected purpose/addr_state values raise ValueError."""

import pytest
from pyspark.sql import Row

from src.clean.validate_categoricals import validate_categoricals


@pytest.fixture(scope="module")
def spark():
    from src.clean.load_and_drop import get_spark_session

    return get_spark_session("test_validate_categoricals")


def test_all_valid_rows_pass_silently(spark):
    df = spark.createDataFrame(
        [
            Row(id=1, purpose="debt_consolidation", addr_state="CA"),
            Row(id=2, purpose="wedding", addr_state="NY"),
        ]
    )
    validate_categoricals(df)  # should not raise


def test_unexpected_purpose_raises(spark):
    df = spark.createDataFrame(
        [Row(id=1, purpose="not_a_real_purpose", addr_state="CA")]
    )
    with pytest.raises(ValueError, match="unexpected purpose"):
        validate_categoricals(df)


def test_invalid_addr_state_raises(spark):
    df = spark.createDataFrame(
        [Row(id=1, purpose="debt_consolidation", addr_state="California")]
    )
    with pytest.raises(ValueError, match="addr_state values"):
        validate_categoricals(df)


def test_lowercase_addr_state_raises(spark):
    df = spark.createDataFrame(
        [Row(id=1, purpose="debt_consolidation", addr_state="ca")]
    )
    with pytest.raises(ValueError, match="addr_state values"):
        validate_categoricals(df)


def test_purpose_check_runs_before_state_check(spark):
    # Both columns invalid -- purpose error should surface first
    df = spark.createDataFrame(
        [Row(id=1, purpose="bogus_purpose", addr_state="XX")]
    )
    with pytest.raises(ValueError, match="unexpected purpose"):
        validate_categoricals(df)