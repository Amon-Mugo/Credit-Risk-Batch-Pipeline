# tests/test_profile_loan_status.py
# Purpose: verifies profile_loan_status() correctly aggregates loan_status
# counts and computes percentage of total, sorted descending by count.

import pytest
from pyspark.sql.types import StructType, StructField, StringType

from src.clean.load_and_drop import get_spark_session
from src.clean.profile_loan_status import profile_loan_status


@pytest.fixture(scope="module")
def spark():
    return get_spark_session()


def _schema():
    return StructType([
        StructField("id", StringType(), True),
        StructField("loan_status", StringType(), True),
    ])


def test_profile_loan_status_counts_correctly(spark):
    df = spark.createDataFrame(
        [
            ("1", "Fully Paid"),
            ("2", "Fully Paid"),
            ("3", "Charged Off"),
            ("4", "Current"),
        ],
        _schema(),
    )

    result = profile_loan_status(df)
    rows = {row["loan_status"]: row["count"] for row in result.collect()}

    assert rows["Fully Paid"] == 2
    assert rows["Charged Off"] == 1
    assert rows["Current"] == 1


def test_profile_loan_status_computes_correct_percentage(spark):
    df = spark.createDataFrame(
        [
            ("1", "Fully Paid"),
            ("2", "Fully Paid"),
            ("3", "Charged Off"),
            ("4", "Charged Off"),
        ],
        _schema(),
    )

    result = profile_loan_status(df)
    rows = {row["loan_status"]: row["pct"] for row in result.collect()}

    assert rows["Fully Paid"] == 50.0
    assert rows["Charged Off"] == 50.0


def test_profile_loan_status_sorted_descending_by_count(spark):
    df = spark.createDataFrame(
        [
            ("1", "Rare"),
            ("2", "Common"),
            ("3", "Common"),
            ("4", "Common"),
        ],
        _schema(),
    )

    result = profile_loan_status(df)
    ordered_statuses = [row["loan_status"] for row in result.collect()]

    assert ordered_statuses == ["Common", "Rare"]


def test_profile_loan_status_includes_null_as_category(spark):
    df = spark.createDataFrame(
        [
            ("1", "Fully Paid"),
            ("2", None),
        ],
        _schema(),
    )

    result = profile_loan_status(df)
    statuses = [row["loan_status"] for row in result.collect()]

    assert None in statuses


def test_profile_loan_status_returns_expected_columns(spark):
    df = spark.createDataFrame([("1", "Fully Paid")], _schema())

    result = profile_loan_status(df)

    assert set(result.columns) == {"loan_status", "count", "pct"}