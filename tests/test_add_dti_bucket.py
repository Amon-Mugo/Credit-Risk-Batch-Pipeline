# tests/test_add_dti_bucket.py
# Purpose: verifies add_dti_bucket() correctly casts dti to double and
# assigns the right risk band (Low/Moderate/High/Unknown) at each boundary.

import pytest
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.types import DoubleType

from src.clean.load_and_drop import get_spark_session
from src.clean.add_dti_bucket import add_dti_bucket


@pytest.fixture(scope="module")
def spark():
    return get_spark_session()


def _schema():
    return StructType([
        StructField("dti", StringType(), True),
    ])


def test_add_dti_bucket_low(spark):
    df = spark.createDataFrame([("15.5",)], _schema())

    result = add_dti_bucket(df)
    row = result.collect()[0]

    assert row["dti_bucket"] == "Low"


def test_add_dti_bucket_moderate(spark):
    df = spark.createDataFrame([("25.0",)], _schema())

    result = add_dti_bucket(df)
    row = result.collect()[0]

    assert row["dti_bucket"] == "Moderate"


def test_add_dti_bucket_high(spark):
    df = spark.createDataFrame([("45.0",)], _schema())

    result = add_dti_bucket(df)
    row = result.collect()[0]

    assert row["dti_bucket"] == "High"


def test_add_dti_bucket_boundary_at_20(spark):
    df = spark.createDataFrame([("20.0",)], _schema())

    result = add_dti_bucket(df)
    row = result.collect()[0]

    assert row["dti_bucket"] == "Moderate"


def test_add_dti_bucket_boundary_at_40(spark):
    df = spark.createDataFrame([("40.0",)], _schema())

    result = add_dti_bucket(df)
    row = result.collect()[0]

    assert row["dti_bucket"] == "High"


def test_add_dti_bucket_unknown_when_null(spark):
    df = spark.createDataFrame([(None,)], _schema())

    result = add_dti_bucket(df)
    row = result.collect()[0]

    assert row["dti_bucket"] == "Unknown"


def test_add_dti_bucket_unknown_when_malformed(spark):
    df = spark.createDataFrame([("not-a-number",)], _schema())

    result = add_dti_bucket(df)
    row = result.collect()[0]

    assert row["dti_bucket"] == "Unknown"


def test_add_dti_bucket_dti_column_cast_to_double(spark):
    df = spark.createDataFrame([("25.0",)], _schema())

    result = add_dti_bucket(df)

    assert result.schema["dti"].dataType == DoubleType()