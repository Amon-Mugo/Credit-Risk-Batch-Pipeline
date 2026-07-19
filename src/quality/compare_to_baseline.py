# Diffs a current distribution snapshot against a saved baseline, flagging
# any category whose share moved beyond a threshold. This is the actual
# "drift" check (2a) — it only fires meaningfully on reruns, since the first
# run has no baseline to compare against yet.

DEFAULT_THRESHOLD_PCT = 2.0  # percentage-point shift considered a flag


def compare_to_baseline(
    current_snapshot: dict,
    baseline_snapshot: dict,
    threshold_pct: float = DEFAULT_THRESHOLD_PCT,
) -> dict:
    comparison = {}

    all_columns = set(current_snapshot) | set(baseline_snapshot)

    for column in all_columns:
        current_values = current_snapshot.get(column, {})
        baseline_values = baseline_snapshot.get(column, {})

        all_keys = set(current_values) | set(baseline_values)
        column_diffs = {}

        for key in all_keys:
            current_pct = current_values.get(key, 0.0)
            baseline_pct = baseline_values.get(key, 0.0)
            shift = round(current_pct - baseline_pct, 2)

            column_diffs[key] = {
                "current_pct": current_pct,
                "baseline_pct": baseline_pct,
                "shift_pct": shift,
                "flagged": abs(shift) >= threshold_pct,
            }

        comparison[column] = column_diffs

    return comparison