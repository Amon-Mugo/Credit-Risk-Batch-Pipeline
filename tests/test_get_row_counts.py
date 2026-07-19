import pytest
from src.clean.load_and_drop import get_spark_session
from src.quality.get_row_counts import get_row_counts


@pytest.fixture(scope="module")
def spark():
    return get_spark_session(app_name="test_get_row_counts")


def test_counts_match_dataframe_sizes(spark):
    raw_df = spark.createDataFrame([(1,), (2,), (3,), (4,)], ["id"])
    checkpoint_df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])

    result = get_row_counts(raw_df, checkpoint_df)

    assert result["raw_row_count"] == 4
    assert result["checkpoint_row_count"] == 3


def test_delta_is_negative_when_rows_dropped(spark):
    raw_df = spark.createDataFrame([(1,), (2,), (3,), (4,)], ["id"])
    checkpoint_df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])

    result = get_row_counts(raw_df, checkpoint_df)

    assert result["row_count_delta"] == -1


def test_delta_pct_computed_correctly(spark):
    raw_df = spark.createDataFrame([(i,) for i in range(10)], ["id"])
    checkpoint_df = spark.createDataFrame([(i,) for i in range(8)], ["id"])

    result = get_row_counts(raw_df, checkpoint_df)

    assert result["row_count_delta_pct"] == -20.0


def test_equal_counts_gives_zero_delta(spark):
    raw_df = spark.createDataFrame([(1,), (2,)], ["id"])
    checkpoint_df = spark.createDataFrame([(1,), (2,)], ["id"])

    result = get_row_counts(raw_df, checkpoint_df)

    assert result["row_count_delta"] == 0
    assert result["row_count_delta_pct"] == 0.0


def test_empty_raw_df_returns_none_pct(spark):
    schema = "id INT"
    raw_df = spark.createDataFrame([], schema)
    checkpoint_df = spark.createDataFrame([], schema)

    result = get_row_counts(raw_df, checkpoint_df)

    assert result["raw_row_count"] == 0
    assert result["row_count_delta_pct"] is None