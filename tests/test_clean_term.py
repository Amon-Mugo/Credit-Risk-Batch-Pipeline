"""Tests for clean_term.py"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from src.clean.clean_term import clean_term


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.appName("test-clean-term").master("local[1]").getOrCreate()
    yield session
    session.stop()


def test_clean_term_extracts_month_count(spark):
    df = spark.createDataFrame(
        [(" 36 months",), (" 60 months",)],
        ["term"],
    )

    result = clean_term(df)

    rows = result.select("term_months").collect()
    assert rows[0]["term_months"] == 36
    assert rows[1]["term_months"] == 60


def test_clean_term_drops_original_column(spark):
    df = spark.createDataFrame([(" 36 months",)], ["term"])

    result = clean_term(df)

    assert "term" not in result.columns
    assert "term_months" in result.columns


def test_clean_term_handles_null(spark):
    schema = StructType([StructField("term", StringType(), True)])
    df = spark.createDataFrame([(None,)], schema)

    result = clean_term(df)

    assert result.collect()[0]["term_months"] is None


def test_clean_term_handles_malformed_value(spark):
    df = spark.createDataFrame([("not a term",)], ["term"])

    result = clean_term(df)

    assert result.collect()[0]["term_months"] is None