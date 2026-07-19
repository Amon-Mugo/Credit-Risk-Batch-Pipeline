import pytest
from src.clean.load_and_drop import get_spark_session
from src.quality.get_distribution_snapshot import get_distribution_snapshot


@pytest.fixture(scope="module")
def spark():
    return get_spark_session(app_name="test_get_distribution_snapshot")


def test_computes_proportions_for_single_column(spark):
    df = spark.createDataFrame(
        [("A",), ("A",), ("B",), ("B",), ("B",), ("B",)], ["grade"]
    )

    result = get_distribution_snapshot(df, ["grade"])

    assert result["grade"]["A"] == pytest.approx(33.33, abs=0.01)
    assert result["grade"]["B"] == pytest.approx(66.67, abs=0.01)


def test_handles_multiple_columns(spark):
    df = spark.createDataFrame(
        [
            ("A", "Low"),
            ("A", "High"),
            ("B", "Low"),
            ("B", "Low"),
        ],
        ["grade", "dti_bucket"],
    )

    result = get_distribution_snapshot(df, ["grade", "dti_bucket"])

    assert set(result.keys()) == {"grade", "dti_bucket"}
    assert result["grade"]["A"] == 50.0
    assert result["dti_bucket"]["Low"] == 75.0


def test_null_values_bucketed_as_string_none(spark):
    schema = "grade STRING"
    df = spark.createDataFrame([("A",), (None,), ("A",), (None,)], schema)

    result = get_distribution_snapshot(df, ["grade"])

    assert result["grade"]["None"] == 50.0
    assert result["grade"]["A"] == 50.0


def test_single_category_gives_hundred_pct(spark):
    df = spark.createDataFrame([("A",), ("A",), ("A",)], ["grade"])

    result = get_distribution_snapshot(df, ["grade"])

    assert result["grade"]["A"] == 100.0


def test_empty_dataframe_returns_empty_proportions(spark):
    schema = "grade STRING"
    df = spark.createDataFrame([], schema)

    result = get_distribution_snapshot(df, ["grade"])

    assert result["grade"] == {}