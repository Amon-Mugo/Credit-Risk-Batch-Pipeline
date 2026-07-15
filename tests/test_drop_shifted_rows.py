"""Tests for drop_shifted_rows: verifies valid state codes are kept and
shifted/corrupted rows are removed."""

import pytest
from pyspark.sql import Row

from src.clean.drop_shifted_rows import drop_shifted_rows


@pytest.fixture(scope="module")
def spark():
    from src.clean.load_and_drop import get_spark_session

    return get_spark_session("test_drop_shifted_rows")


def test_valid_state_code_is_kept(spark):
    df = spark.createDataFrame([Row(id=1, addr_state="CA")])
    result = drop_shifted_rows(df)
    assert result.count() == 1


def test_dc_is_kept(spark):
    df = spark.createDataFrame([Row(id=1, addr_state="DC")])
    result = drop_shifted_rows(df)
    assert result.count() == 1


def test_shifted_purpose_text_is_dropped(spark):
    df = spark.createDataFrame([Row(id=1, addr_state="debt_consolidation")])
    result = drop_shifted_rows(df)
    assert result.count() == 0


def test_shifted_zip_prefix_is_dropped(spark):
    df = spark.createDataFrame([Row(id=1, addr_state="941xx")])
    result = drop_shifted_rows(df)
    assert result.count() == 0


def test_lowercase_state_code_is_dropped(spark):
    df = spark.createDataFrame([Row(id=1, addr_state="ca")])
    result = drop_shifted_rows(df)
    assert result.count() == 0


def test_mixed_valid_and_shifted_rows(spark):
    df = spark.createDataFrame(
        [
            Row(id=1, addr_state="TX"),
            Row(id=2, addr_state="credit_card"),
            Row(id=3, addr_state="NY"),
        ]
    )
    result = drop_shifted_rows(df)
    assert result.count() == 2
    kept_ids = {row.id for row in result.collect()}
    assert kept_ids == {1, 3}
