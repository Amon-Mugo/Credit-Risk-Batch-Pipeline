# tests/test_add_loan_age.py
# Purpose: verifies add_loan_age() correctly computes months_between(last_pymnt_d,
# issue_d), and stays nullable when either date is missing.

import pytest
from pyspark.sql.types import StructType, StructField, DateType
from datetime import date

from src.clean.load_and_drop import get_spark_session
from src.clean.add_loan_age import add_loan_age


@pytest.fixture(scope="module")
def spark():
    return get_spark_session()


def _schema():
    return StructType([
        StructField("issue_d", DateType(), True),
        StructField("last_pymnt_d", DateType(), True),
    ])


def test_add_loan_age_computes_correct_months(spark):
    df = spark.createDataFrame(
        [(date(2015, 1, 1), date(2017, 1, 1))], _schema()
    )

    result = add_loan_age(df)
    row = result.collect()[0]

    assert row["loan_age_months"] == pytest.approx(24.0, abs=0.01)


def test_add_loan_age_handles_partial_month(spark):
    df = spark.createDataFrame(
        [(date(2015, 1, 1), date(2015, 2, 15))], _schema()
    )

    result = add_loan_age(df)
    row = result.collect()[0]

    assert row["loan_age_months"] == pytest.approx(1.45, abs=0.05)


def test_add_loan_age_null_when_last_pymnt_d_missing(spark):
    df = spark.createDataFrame(
        [(date(2015, 1, 1), None)], _schema()
    )

    result = add_loan_age(df)
    row = result.collect()[0]

    assert row["loan_age_months"] is None


def test_add_loan_age_null_when_issue_d_missing(spark):
    df = spark.createDataFrame(
        [(None, date(2015, 1, 1))], _schema()
    )

    result = add_loan_age(df)
    row = result.collect()[0]

    assert row["loan_age_months"] is None


def test_add_loan_age_output_type_is_double(spark):
    df = spark.createDataFrame(
        [(date(2015, 1, 1), date(2017, 1, 1))], _schema()
    )

    result = add_loan_age(df)

    assert result.schema["loan_age_months"].dataType.typeName() == "double"