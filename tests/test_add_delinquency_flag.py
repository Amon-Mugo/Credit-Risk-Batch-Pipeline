# tests/test_add_delinquency_flag.py
# Purpose: verifies add_delinquency_flag correctly derives has_prior_delinquency
# from delinq_2yrs under ANSI mode, covering null, zero, positive, negative,
# and malformed-string inputs via try_cast.
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from src.clean.add_delinquency_flag import add_delinquency_flag


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .appName("test_add_delinquency_flag")
        .master("local[1]")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_null_delinq_yields_null_flag(spark):
    schema = StructType([StructField("delinq_2yrs", StringType(), True)])
    df = spark.createDataFrame([(None,)], schema)

    result = add_delinquency_flag(df).collect()[0]

    assert result["has_prior_delinquency"] is None


def test_zero_delinq_yields_false(spark):
    schema = StructType([StructField("delinq_2yrs", StringType(), True)])
    df = spark.createDataFrame([("0",)], schema)

    result = add_delinquency_flag(df).collect()[0]

    assert result["has_prior_delinquency"] is False


def test_positive_delinq_yields_true(spark):
    schema = StructType([StructField("delinq_2yrs", StringType(), True)])
    df = spark.createDataFrame([("3",)], schema)

    result = add_delinquency_flag(df).collect()[0]

    assert result["has_prior_delinquency"] is True


def test_negative_delinq_yields_false(spark):
    # Not a physically valid value, but try_cast succeeds, so it should
    # fall through to the "not > 0" branch rather than being treated as null.
    schema = StructType([StructField("delinq_2yrs", StringType(), True)])
    df = spark.createDataFrame([("-1",)], schema)

    result = add_delinquency_flag(df).collect()[0]

    assert result["has_prior_delinquency"] is False


def test_malformed_string_yields_null_flag(spark):
    # try_cast returns NULL on non-numeric input instead of throwing under ANSI mode.
    schema = StructType([StructField("delinq_2yrs", StringType(), True)])
    df = spark.createDataFrame([("N/A",)], schema)

    result = add_delinquency_flag(df).collect()[0]

    assert result["has_prior_delinquency"] is None