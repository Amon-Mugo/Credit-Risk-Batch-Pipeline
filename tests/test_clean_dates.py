"""Tests for clean_dates.py"""
import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from src.clean.clean_dates import clean_dates, DATE_COLUMNS


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.appName("test-clean-dates").master("local[1]").getOrCreate()
    yield session
    session.stop()


def _single_column_df(spark, column: str, value):
    """Build a one-row DataFrame across all DATE_COLUMNS for a given column.

    All other DATE_COLUMNS are filled with a valid value so clean_dates'
    missing-column check passes while isolating the column under test.
    """
    schema = StructType([StructField(col, StringType(), True) for col in DATE_COLUMNS])
    row = tuple(value if col == column else "Jan-2015" for col in DATE_COLUMNS)
    return spark.createDataFrame([row], schema)


def test_clean_dates_parses_valid_value(spark):
    df = _single_column_df(spark, "issue_d", "Dec-2015")

    result = clean_dates(df)

    assert result.collect()[0]["issue_d"] == datetime.date(2015, 12, 1)


def test_clean_dates_parses_all_five_columns(spark):
    schema = StructType([StructField(col, StringType(), True) for col in DATE_COLUMNS])
    row = ("Dec-2015", "Nov-2001", "Jan-2019", "Feb-2019", "Mar-2019")
    df = spark.createDataFrame([row], schema)

    result = clean_dates(df)

    parsed = result.collect()[0]
    assert parsed["issue_d"] == datetime.date(2015, 12, 1)
    assert parsed["earliest_cr_line"] == datetime.date(2001, 11, 1)
    assert parsed["last_pymnt_d"] == datetime.date(2019, 1, 1)
    assert parsed["next_pymnt_d"] == datetime.date(2019, 2, 1)
    assert parsed["last_credit_pull_d"] == datetime.date(2019, 3, 1)


def test_clean_dates_handles_null(spark):
    df = _single_column_df(spark, "next_pymnt_d", None)

    result = clean_dates(df)

    assert result.collect()[0]["next_pymnt_d"] is None


def test_clean_dates_handles_empty_string(spark):
    df = _single_column_df(spark, "next_pymnt_d", "")

    result = clean_dates(df)

    assert result.collect()[0]["next_pymnt_d"] is None


def test_clean_dates_handles_malformed_value(spark):
    df = _single_column_df(spark, "issue_d", "not a date")

    result = clean_dates(df)

    assert result.collect()[0]["issue_d"] is None


def test_clean_dates_output_type_is_date(spark):
    df = _single_column_df(spark, "issue_d", "Dec-2015")

    result = clean_dates(df)

    field = result.schema["issue_d"]
    assert field.dataType.typeName() == "date"


def test_clean_dates_raises_on_missing_column(spark):
    df = spark.createDataFrame([("Dec-2015",)], ["issue_d"])

    with pytest.raises(ValueError):
        clean_dates(df)