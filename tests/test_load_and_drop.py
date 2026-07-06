# tests/test_load_and_drop.py
# Purpose: Unit tests for load_and_drop.py, verifying member_id is dropped
# when present and that the function is a no-op when it's already absent.
# Uses a local SparkSession fixture; no GCS access needed for these tests.

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from src.clean.load_and_drop import drop_member_id


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .appName("test_credit_risk_cleaning")
        .master("local[1]")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_drop_member_id_removes_column(spark):
    schema = StructType([
        StructField("loan_id", IntegerType(), True),
        StructField("member_id", StringType(), True),
    ])
    df = spark.createDataFrame(
        [(1, None), (2, None)],
        schema=schema,
    )
    result = drop_member_id(df)
    assert "member_id" not in result.columns
    assert "loan_id" in result.columns


def test_drop_member_id_noop_when_absent(spark):
    df = spark.createDataFrame(
        [(1,), (2,)],
        ["loan_id"],
    )
    result = drop_member_id(df)
    assert result.columns == ["loan_id"]