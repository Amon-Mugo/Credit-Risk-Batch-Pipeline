from unittest.mock import MagicMock
from src.quality.get_bq_row_counts import get_bq_row_counts


def _mock_client_with_counts(counts_by_table: dict) -> MagicMock:
    client = MagicMock()

    def query_side_effect(sql):
        for table_id, count in counts_by_table.items():
            if table_id in sql:
                result_row = {"row_count": count}
                mock_result = MagicMock()
                mock_result.__iter__.return_value = iter([result_row])
                mock_query_job = MagicMock()
                mock_query_job.result.return_value = mock_result
                return mock_query_job
        raise ValueError(f"No mock configured for query: {sql}")

    client.query.side_effect = query_side_effect
    return client


def test_returns_row_count_per_table():
    table_ids = {
        "npl_ratio": "proj.dataset.npl_ratio_by_dimension",
        "vintage_curve": "proj.dataset.default_rate_vintage_curve",
    }
    client = _mock_client_with_counts({
        "proj.dataset.npl_ratio_by_dimension": 24,
        "proj.dataset.default_rate_vintage_curve": 613,
    })

    result = get_bq_row_counts(client, table_ids)

    assert result["npl_ratio_row_count"] == 24
    assert result["vintage_curve_row_count"] == 613


def test_keys_are_label_suffixed():
    table_ids = {"only_table": "proj.dataset.only_table"}
    client = _mock_client_with_counts({"proj.dataset.only_table": 5})

    result = get_bq_row_counts(client, table_ids)

    assert "only_table_row_count" in result
    assert len(result) == 1


def test_empty_table_ids_returns_empty_dict():
    client = MagicMock()

    result = get_bq_row_counts(client, {})

    assert result == {}