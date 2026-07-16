import pytest
from pyspark.sql import Row

from src.aggregations.compute_default_rate_curve import compute_default_rate_curve
from src.clean.load_and_drop import get_spark_session


@pytest.fixture(scope="module")
def spark():
    return get_spark_session()


@pytest.fixture(scope="module")
def sample_df(spark):
    return spark.createDataFrame(
        [
            Row(vintage_year=2022, loan_age_months=1.2, is_default=0),
            Row(vintage_year=2022, loan_age_months=1.8, is_default=1),
            Row(vintage_year=2022, loan_age_months=2.1, is_default=0),
            Row(vintage_year=2022, loan_age_months=2.9, is_default=0),
            Row(vintage_year=2023, loan_age_months=1.0, is_default=0),
            Row(vintage_year=2023, loan_age_months=1.4, is_default=1),
        ]
    )


def test_returns_expected_columns(sample_df):
    result = compute_default_rate_curve(sample_df)
    assert result.columns == [
        "vintage_year",
        "loan_age_months",
        "default_count",
        "total_count",
        "npl_ratio",
    ]


def test_fractional_months_are_floored(sample_df):
    result = compute_default_rate_curve(sample_df)
    distinct_ages = sorted(
        row["loan_age_months"] for row in result.select("loan_age_months").distinct().collect()
    )
    # 1.2 and 1.8 both floor to 1; 2.1 and 2.9 both floor to 2
    assert distinct_ages == [1, 2]


def test_groups_by_vintage_and_age_together(sample_df):
    result = compute_default_rate_curve(sample_df)
    assert result.count() == 3  # (2022,1), (2022,2), (2023,1)


def test_correct_counts_and_ratio_for_2022_age_1(sample_df):
    result = compute_default_rate_curve(sample_df)
    row = result.filter(
        (result.vintage_year == 2022) & (result.loan_age_months == 1)
    ).collect()[0]
    assert row["total_count"] == 2
    assert row["default_count"] == 1
    assert row["npl_ratio"] == pytest.approx(0.5)


def test_correct_counts_and_ratio_for_2022_age_2(sample_df):
    result = compute_default_rate_curve(sample_df)
    row = result.filter(
        (result.vintage_year == 2022) & (result.loan_age_months == 2)
    ).collect()[0]
    assert row["total_count"] == 2
    assert row["default_count"] == 0
    assert row["npl_ratio"] == pytest.approx(0.0)


def test_different_vintages_kept_separate(sample_df):
    result = compute_default_rate_curve(sample_df)
    row = result.filter(
        (result.vintage_year == 2023) & (result.loan_age_months == 1)
    ).collect()[0]
    assert row["total_count"] == 2
    assert row["default_count"] == 1
    assert row["npl_ratio"] == pytest.approx(0.5)
