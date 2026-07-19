import json

from google.cloud import bigquery, storage

from src.clean.load_and_drop import get_spark_session, load_raw_data
from src.quality.get_row_counts import get_row_counts
from src.quality.get_bq_row_counts import get_bq_row_counts
from src.quality.get_null_rates import get_null_rates
from src.quality.get_distribution_snapshot import get_distribution_snapshot
from src.quality.compare_to_baseline import compare_to_baseline
from src.quality.render_report import render_report

RAW_GCS_PATH = "gs://credit-risk-batch-pipeline/raw/accepted_2007_to_2018Q4.csv.gz"
CHECKPOINT_PATH = "gs://credit-risk-batch-pipeline/checkpoints/labeled_dataset/"

BQ_PROJECT = "gcp-de-learning-498109"
BQ_DATASET = "credit_risk_batch_pipeline"
BQ_TABLE_IDS = {
    "npl_ratio": f"{BQ_PROJECT}.{BQ_DATASET}.npl_ratio_by_dimension",
    "vintage_curve": f"{BQ_PROJECT}.{BQ_DATASET}.default_rate_vintage_curve",
}

NULL_CHECK_COLUMNS = [
    "is_default",
    "grade",
    "addr_state",
    "purpose",
    "vintage_year",
    "loan_age_months",
    "dti_bucket",
    "has_prior_delinquency",
]

DISTRIBUTION_COLUMNS = [
    "grade",
    "dti_bucket",
    "purpose",
    "vintage_year",
    "has_prior_delinquency",
]

BASELINE_BUCKET = "credit-risk-batch-pipeline"
BASELINE_BLOB_PATH = "quality/baseline_snapshot.json"

REPORT_OUTPUT_PATH = "docs/data_quality/data_quality_report.md"


def load_baseline_snapshot() -> dict | None:
    client = storage.Client()
    bucket = client.bucket(BASELINE_BUCKET)
    blob = bucket.blob(BASELINE_BLOB_PATH)

    if not blob.exists():
        return None

    return json.loads(blob.download_as_text())


def save_baseline_snapshot(snapshot: dict) -> None:
    client = storage.Client()
    bucket = client.bucket(BASELINE_BUCKET)
    blob = bucket.blob(BASELINE_BLOB_PATH)
    blob.upload_from_string(json.dumps(snapshot, indent=2))


def main() -> None:
    spark = get_spark_session(app_name="credit_risk_data_quality_report")

    raw_df = load_raw_data(spark, RAW_GCS_PATH)
    checkpoint_df = spark.read.parquet(CHECKPOINT_PATH)

    row_counts = get_row_counts(raw_df, checkpoint_df)

    bq_client = bigquery.Client()
    bq_row_counts = get_bq_row_counts(bq_client, BQ_TABLE_IDS)

    null_rates = get_null_rates(checkpoint_df, NULL_CHECK_COLUMNS)

    distribution_snapshot = get_distribution_snapshot(checkpoint_df, DISTRIBUTION_COLUMNS)

    baseline_snapshot = load_baseline_snapshot()
    baseline_comparison = None
    if baseline_snapshot is not None:
        baseline_comparison = compare_to_baseline(distribution_snapshot, baseline_snapshot)
    else:
        save_baseline_snapshot(distribution_snapshot)

    report = render_report(
        row_counts,
        bq_row_counts,
        null_rates,
        distribution_snapshot,
        baseline_comparison=baseline_comparison,
    )

    with open(REPORT_OUTPUT_PATH, "w") as f:
        f.write(report)

    spark.stop()


if __name__ == "__main__":
    main()
