# tests/test_label_target.py
# Purpose: verifies label_target() filters to only resolved loan outcomes
# (Fully Paid, Charged Off), correctly derives the binary target column,
# and excludes NULL/in-progress/malformed loan_status rows as a side
# effect of the isin() filter.

import pytest
from pyspark.sql.types import StructType, StructField, StringType

from src.clean.load_and_drop import get_spark_session
from src.clean.label_target import label_target, RESOLVED_STATUSES


@pytest.fixture(scope="module")
def spark():
    return get_spark_session()


def _schema():
    return StructType([
        StructField("id", StringType(), True),
        StructField("loan_status", StringType(), True),
    ])


def test_label_target_maps_charged_off_to_1(spark):
    df = spark.createDataFrame([("1", "Charged Off")], _schema())

    result = label_target(df)
    row = result.collect()[0]

    assert row["target"] == 1


def test_label_target_maps_fully_paid_to_0(spark):
    df = spark.createDataFrame([("1", "Fully Paid")], _schema())

    result = label_target(df)
    row = result.collect()[0]

    assert row["target"] == 0


def test_label_target_excludes_in_progress_statuses(spark):
    df = spark.createDataFrame(
        [
            ("1", "Current"),
            ("2", "Late (31-120 days)"),
            ("3", "In Grace Period"),
            ("4", "Default"),
        ],
        _schema(),
    )

    result = label_target(df)

    assert result.count() == 0


def test_label_target_excludes_null_loan_status(spark):
    df = spark.createDataFrame([("1", None)], _schema())

    result = label_target(df)

    assert result.count() == 0


def test_label_target_excludes_malformed_row(spark):
    df = spark.createDataFrame([("1", "Oct-2015")], _schema())

    result = label_target(df)

    assert result.count() == 0


def test_label_target_keeps_only_resolved_statuses(spark):
    df = spark.createDataFrame(
        [
            ("1", "Fully Paid"),
            ("2", "Charged Off"),
            ("3", "Current"),
            ("4", None),
        ],
        _schema(),
    )

    result = label_target(df)

    assert result.count() == 2
    statuses = {row["loan_status"] for row in result.collect()}
    assert statuses == set(RESOLVED_STATUSES)