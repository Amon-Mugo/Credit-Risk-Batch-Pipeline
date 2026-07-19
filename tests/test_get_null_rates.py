import pytest
from src.clean.load_and_drop import get_spark_session
from src.quality.get_null_rates import get_null_rates


@pytest.fixture(scope="module")
def spark():
    return get_spark_session(app_name="test_get_null_rates")


def test_computes_null_pct_for_single_column(spark):
    df = spark.createDataFrame(
        [(1,), (None,), (3,), (None,)], ["grade"]
    )

    result = get_null_rates(df, ["grade"])

    assert result["grade"] == 50.0


def test_computes_null_pct_for_multiple_columns(spark):
    df = spark.createDataFrame(
        [
            (1, "A"),
            (None, "B"),
            (3, None),
            (4, "D"),
        ],
        ["loan_age_months", "purpose"],
    )

    result = get_null_rates(df, ["loan_age_months", "purpose"])

    assert result["loan_age_months"] == 25.0
    assert result["purpose"] == 25.0


def test_no_nulls_returns_zero(spark):
    df = spark.createDataFrame([(1,), (2,), (3,)], ["is_default"])

    result = get_null_rates(df, ["is_default"])

    assert result["is_default"] == 0.0


def test_all_nulls_returns_hundred(spark):
    schema = "vintage_year INT"
    df = spark.createDataFrame([(None,), (None,)], schema)

    result = get_null_rates(df, ["vintage_year"])

    assert result["vintage_year"] == 100.0


def test_empty_dataframe_returns_none(spark):
    schema = "grade STRING"
    df = spark.createDataFrame([], schema)

    result = get_null_rates(df, ["grade"])

    assert result["grade"] is None