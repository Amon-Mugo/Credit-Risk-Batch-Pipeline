# tests/test_add_vintage_year.py
# Purpose: verifies add_vintage_year() correctly extracts the calendar year
# from issue_d, and stays nullable when issue_d is null.

import pytest
from pyspark.sql.types import StructType, StructField, DateType
from datetime import date

from src.clean.load_and_drop import get_spark_session
from src.clean.add_vintage_year import add_vintage_year


@pytest.fixture(scope="module")
def spark():
    return get_spark_session()


def _schema():
    return StructType([
        StructField("issue_d", DateType(), True),
    ])


def test_add_vintage_year_extracts_correct_year(spark):
    df = spark.createDataFrame([(date(2015, 12, 1),)], _schema())

    result = add_vintage_year(df)
    row = result.collect()[0]

    assert row["vintage_year"] == 2015


def test_add_vintage_year_handles_different_years(spark):
    df = spark.createDataFrame(
        [
            (date(2007, 6, 1),),
            (date(2018, 1, 1),),
        ],
        _schema(),
    )

    result = add_vintage_year(df)
    years = sorted(row["vintage_year"] for row in result.collect())

    assert years == [2007, 2018]


def test_add_vintage_year_stays_nullable_when_issue_d_null(spark):
    df = spark.createDataFrame([(None,)], _schema())

    result = add_vintage_year(df)
    row = result.collect()[0]

    assert row["vintage_year"] is None


def test_add_vintage_year_output_type_is_int(spark):
    df = spark.createDataFrame([(date(2015, 12, 1),)], _schema())

    result = add_vintage_year(df)

    assert result.schema["vintage_year"].dataType.typeName() == "integer"