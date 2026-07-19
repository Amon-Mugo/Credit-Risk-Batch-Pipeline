from src.quality.compare_to_baseline import compare_to_baseline


def test_no_shift_when_snapshots_match():
    current = {"grade": {"A": 50.0, "B": 50.0}}
    baseline = {"grade": {"A": 50.0, "B": 50.0}}

    result = compare_to_baseline(current, baseline)

    assert result["grade"]["A"]["shift_pct"] == 0.0
    assert result["grade"]["A"]["flagged"] is False


def test_flags_shift_beyond_default_threshold():
    current = {"grade": {"A": 55.0, "B": 45.0}}
    baseline = {"grade": {"A": 50.0, "B": 50.0}}

    result = compare_to_baseline(current, baseline)

    assert result["grade"]["A"]["shift_pct"] == 5.0
    assert result["grade"]["A"]["flagged"] is True


def test_does_not_flag_shift_within_threshold():
    current = {"grade": {"A": 51.0, "B": 49.0}}
    baseline = {"grade": {"A": 50.0, "B": 50.0}}

    result = compare_to_baseline(current, baseline)

    assert result["grade"]["A"]["flagged"] is False


def test_respects_custom_threshold():
    current = {"grade": {"A": 51.0}}
    baseline = {"grade": {"A": 50.0}}

    result = compare_to_baseline(current, baseline, threshold_pct=0.5)

    assert result["grade"]["A"]["flagged"] is True


def test_new_category_missing_from_baseline_flagged():
    current = {"grade": {"A": 50.0, "G": 10.0}}
    baseline = {"grade": {"A": 50.0}}

    result = compare_to_baseline(current, baseline)

    assert result["grade"]["G"]["baseline_pct"] == 0.0
    assert result["grade"]["G"]["current_pct"] == 10.0
    assert result["grade"]["G"]["flagged"] is True


def test_category_disappearing_from_current_flagged():
    current = {"grade": {"A": 50.0}}
    baseline = {"grade": {"A": 50.0, "G": 10.0}}

    result = compare_to_baseline(current, baseline)

    assert result["grade"]["G"]["current_pct"] == 0.0
    assert result["grade"]["G"]["flagged"] is True


def test_column_present_only_in_current():
    current = {"grade": {"A": 100.0}, "purpose": {"debt_consolidation": 100.0}}
    baseline = {"grade": {"A": 100.0}}

    result = compare_to_baseline(current, baseline)

    assert "purpose" in result
    assert result["purpose"]["debt_consolidation"]["baseline_pct"] == 0.0