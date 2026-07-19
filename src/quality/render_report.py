from datetime import datetime, timezone

# Assembles all data quality check results into a single markdown report.
# Pure string-formatting function — no Spark, no I/O — so it's easy to test
# and easy to reuse if the output format ever needs to change (e.g. HTML later).

def render_report(
    row_counts: dict,
    bq_row_counts: dict,
    null_rates: dict,
    distribution_snapshot: dict,
    baseline_comparison: dict | None = None,
) -> str:
    lines = []
    lines.append("# Credit Risk Batch Pipeline — Data Quality Report")
    lines.append(f"\nGenerated: {datetime.now(timezone.utc).isoformat()}")

    # --- Row counts ---
    lines.append("\n## Row Counts")
    lines.append(f"- Raw dataset: {row_counts['raw_row_count']:,}")
    lines.append(f"- Checkpoint (labeled/feature-engineered): {row_counts['checkpoint_row_count']:,}")
    lines.append(f"- Delta: {row_counts['row_count_delta']:,} ({row_counts['row_count_delta_pct']}%)")

    for label, count in bq_row_counts.items():
        lines.append(f"- {label}: {count:,}")

    # --- Null rates ---
    lines.append("\n## Null Rates (checkpoint dataset)")
    lines.append("| Column | Null % |")
    lines.append("|---|---|")
    for column, pct in null_rates.items():
        lines.append(f"| {column} | {pct}% |")

    # --- Distribution snapshot ---
    lines.append("\n## Distribution Snapshot")
    for column, values in distribution_snapshot.items():
        lines.append(f"\n### {column}")
        lines.append("| Value | % |")
        lines.append("|---|---|")
        for value, pct in sorted(values.items(), key=lambda item: item[1], reverse=True):
            lines.append(f"| {value} | {pct}% |")

    # --- Baseline comparison (drift) ---
    lines.append("\n## Drift vs Baseline")
    if baseline_comparison is None:
        lines.append("\nNo baseline available — this run has been saved as the new baseline.")
    else:
        flagged_any = False
        for column, values in baseline_comparison.items():
            flagged_rows = [
                (value, stats) for value, stats in values.items() if stats["flagged"]
            ]
            if flagged_rows:
                flagged_any = True
                lines.append(f"\n### {column}")
                lines.append("| Value | Baseline % | Current % | Shift |")
                lines.append("|---|---|---|---|")
                for value, stats in flagged_rows:
                    lines.append(
                        f"| {value} | {stats['baseline_pct']}% | {stats['current_pct']}% | {stats['shift_pct']:+}% |"
                    )
        if not flagged_any:
            lines.append("\nNo columns shifted beyond the flag threshold.")

    return "\n".join(lines)