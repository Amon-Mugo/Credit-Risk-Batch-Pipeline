from src.quality.render_report import render_report


def _sample_row_counts():
    return {
        "raw_row_count": 2260701,
        "checkpoint_row_count": 1345309,
        "row_count_delta": -915392,
        "row_count_delta_pct": -40.49,
    }


def _sample_bq_row_counts():
    return {"npl_ratio_row_count": 24, "vintage_curve_row_count": 613}


def _sample_null_rates():
    return {"is_default": 0.0, "grade": 0.0, "dti_bucket": 1.2}


def _sample_distribution():
    return {"grade": {"A": 20.0, "B": 30.0, "C": 50.0}}


def test_report_contains_row_counts():
    report = render_report(
        _sample_row_counts(), _sample_bq_row_counts(), _sample_null_rates(), _sample_distribution()
    )

    assert "2,260,701" in report
    assert "1,345,309" in report


def test_report_contains_bq_counts():
    report = render_report(
        _sample_row_counts(), _sample_bq_row_counts(), _sample_null_rates(), _sample_distribution()
    )

    assert "npl_ratio_row_count: 24" in report
    assert "vintage_curve_row_count: 613" in report


def test_report_contains_null_rate_table():
    report = render_report(
        _sample_row_counts(), _sample_bq_row_counts(), _sample_null_rates(), _sample_distribution()
    )

    assert "| is_default | 0.0% |" in report
    assert "| dti_bucket | 1.2% |" in report


def test_report_contains_distribution_table():
    report = render_report(
        _sample_row_counts(), _sample_bq_row_counts(), _sample_null_rates(), _sample_distribution()
    )

    assert "### grade" in report
    assert "| C | 50.0% |" in report


def test_report_no_baseline_message_when_none_passed():
    report = render_report(
        _sample_row_counts(), _sample_bq_row_counts(), _sample_null_rates(), _sample_distribution(),
        baseline_comparison=None,
    )

    assert "No baseline available" in report


def test_report_shows_flagged_drift():
    baseline_comparison = {
        "grade": {
            "A": {"current_pct": 55.0, "baseline_pct": 50.0, "shift_pct": 5.0, "flagged": True},
            "B": {"current_pct": 45.0, "baseline_pct": 50.0, "shift_pct": -5.0, "flagged": True},
        }
    }

    report = render_report(
        _sample_row_counts(), _sample_bq_row_counts(), _sample_null_rates(), _sample_distribution(),
        baseline_comparison=baseline_comparison,
    )

    assert "### grade" in report
    assert "+5.0%" in report


def test_report_shows_no_drift_message_when_nothing_flagged():
    baseline_comparison = {
        "grade": {
            "A": {"current_pct": 50.0, "baseline_pct": 50.0, "shift_pct": 0.0, "flagged": False},
        }
    }

    report = render_report(
        _sample_row_counts(), _sample_bq_row_counts(), _sample_null_rates(), _sample_distribution(),
        baseline_comparison=baseline_comparison,
    )

    assert "No columns shifted beyond the flag threshold" in report