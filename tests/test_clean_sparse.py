# tests/test_clean_sparse.py
# Purpose: verifies cast_sparse_numerics() correctly casts the 11 numeric string
# columns to DoubleType (nulling malformed input) and clean_sparse() correctly
# delegates the 6 sparse date columns to clean_dates(), leaving flag/status
# columns untouched. All schemas include the full NUMERIC_SPARSE_COLUMNS set
# since withColumns() resolves against the entire list in one projection.

import pytest
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    DateType,
)
from datetime import date

from src.clean.load_and_drop import get_spark_session
from src.clean.clean_sparse import (
    cast_sparse_numerics,
    clean_sparse,
    NUMERIC_SPARSE_COLUMNS,
    DATE_SPARSE_COLUMNS,
)


@pytest.fixture(scope="module")
def spark():
    return get_spark_session()


def _numeric_schema(extra_fields=None):
    fields = [StructField(c, StringType(), True) for c in NUMERIC_SPARSE_COLUMNS]
    if extra_fields:
        fields += extra_fields
    return StructType(fields)


def test_cast_sparse_numerics_valid(spark):
    schema = _numeric_schema()
    data = [tuple("1.5" for _ in NUMERIC_SPARSE_COLUMNS)]
    df = spark.createDataFrame(data, schema)

    result = cast_sparse_numerics(df)
    row = result.collect()[0]

    for col_name in NUMERIC_SPARSE_COLUMNS:
        assert result.schema[col_name].dataType == DoubleType()
        assert row[col_name] == 1.5


def test_cast_sparse_numerics_null(spark):
    schema = _numeric_schema()
    data = [tuple(None for _ in NUMERIC_SPARSE_COLUMNS)]
    df = spark.createDataFrame(data, schema)

    result = cast_sparse_numerics(df)
    row = result.collect()[0]

    for col_name in NUMERIC_SPARSE_COLUMNS:
        assert row[col_name] is None


def test_clean_sparse_casts_numerics_and_dates(spark):
    schema = _numeric_schema(extra_fields=[
        StructField(c, StringType(), True) for c in DATE_SPARSE_COLUMNS
    ] + [
        StructField("hardship_status", StringType(), True),  # untouched flag col
    ])
    date_values = tuple(
        "Mar-2019" if c == "hardship_start_date" else None
        for c in DATE_SPARSE_COLUMNS
    )
    data = [
        tuple("100.0" for _ in NUMERIC_SPARSE_COLUMNS) + date_values + ("ACTIVE",)
    ]
    df = spark.createDataFrame(data, schema)

    result = clean_sparse(df)
    row = result.collect()[0]

    assert result.schema["hardship_amount"].dataType == DoubleType()
    assert result.schema["hardship_start_date"].dataType == DateType()
    assert result.schema["hardship_status"].dataType == StringType()
    assert row["hardship_amount"] == 100.0
    assert row["hardship_start_date"] == date(2019, 3, 1)
    assert row["hardship_status"] == "ACTIVE"


def test_clean_sparse_nulls_malformed_date(spark):
    schema = _numeric_schema(extra_fields=[
        StructField(c, StringType(), True) for c in DATE_SPARSE_COLUMNS
    ])
    date_values = tuple(
        "not-a-date" if c == "settlement_date" else None
        for c in DATE_SPARSE_COLUMNS
    )
    data = [tuple("1.0" for _ in NUMERIC_SPARSE_COLUMNS) + date_values]
    df = spark.createDataFrame(data, schema)

    result = clean_sparse(df)
    row = result.collect()[0]

    assert row["settlement_date"] is None
