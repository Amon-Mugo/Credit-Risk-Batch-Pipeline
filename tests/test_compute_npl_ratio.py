import pytest
from pyspark.sql import Row

from src.aggregations.compute_npl_ratio import compute_npl_ratio
from src.clean.load_and_drop import get_spark_session


@pytest.fixture(scope="module")
def spark():
    return get_spark_session()


@pytest.fixture(scope="module")
def sample_df(spark):
    return spark.createDataFrame(
        [
            Row(grade="A", is_default=0),
            Row(grade="A", is_default=0),
            Row(grade="A", is_default=0),
            Row(grade="A", is_default=1),
            Row(grade="B", is_default=1),
            Row(grade="B", is_default=1),
            Row(grade="C", is_default=0),
            Row(grade="C", is_default=0),
        ]
    )


def test_returns_expected_columns(sample_df):
    result = compute_npl_ratio(sample_df, "grade")
    assert result.columns == [
        "dimension",
        "dimension_value",
        "default_count",
        "total_count",
        "npl_ratio",
    ]


def test_one_row_per_distinct_dimension_value(sample_df):
    result = compute_npl_ratio(sample_df, "grade")
    assert result.count() == 3


def test_dimension_column_holds_dimension_name(sample_df):
    result = compute_npl_ratio(sample_df, "grade")
    distinct_dimensions = [row["dimension"] for row in result.collect()]
    assert set(distinct_dimensions) == {"grade"}


def test_correct_counts_and_ratio_for_mixed_group(sample_df):
    result = compute_npl_ratio(sample_df, "grade")
    grade_a = result.filter(result.dimension_value == "A").collect()[0]
    assert grade_a["total_count"] == 4
    assert grade_a["default_count"] == 1
    assert grade_a["npl_ratio"] == pytest.approx(0.25)


def test_group_with_all_defaults(sample_df):
    result = compute_npl_ratio(sample_df, "grade")
    grade_b = result.filter(result.dimension_value == "B").collect()[0]
    assert grade_b["total_count"] == 2
    assert grade_b["default_count"] == 2
    assert grade_b["npl_ratio"] == pytest.approx(1.0)


def test_group_with_zero_defaults(sample_df):
    result = compute_npl_ratio(sample_df, "grade")
    grade_c = result.filter(result.dimension_value == "C").collect()[0]
    assert grade_c["total_count"] == 2
    assert grade_c["default_count"] == 0
    assert grade_c["npl_ratio"] == pytest.approx(0.0)


def test_works_with_different_dimension_column(spark):
    df = spark.createDataFrame(
        [
            Row(addr_state="CA", is_default=0),
            Row(addr_state="CA", is_default=1),
            Row(addr_state="NY", is_default=1),
        ]
    )
    result = compute_npl_ratio(df, "addr_state")
    distinct_dimensions = [row["dimension"] for row in result.collect()]
    assert set(distinct_dimensions) == {"addr_state"}
    assert result.count() == 2
