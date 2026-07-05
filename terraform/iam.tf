resource "google_service_account" "credit_risk_batch_pipeline" {
  account_id   = "credit-risk-batch-pipeline"
  display_name = "Credit Risk Batch Pipeline Service Account"
  project      = var.project_id
  description  = "Used by Dataproc Serverless batches to read raw data from GCS and write curated aggregates to BigQuery"
}

resource "google_storage_bucket_iam_member" "raw_bucket_access" {
  bucket = google_storage_bucket.credit_risk_batch_pipeline.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.credit_risk_batch_pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "curated_dataset_access" {
  dataset_id = google_bigquery_dataset.credit_risk_batch_pipeline.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.credit_risk_batch_pipeline.email}"
}

resource "google_project_iam_member" "bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.credit_risk_batch_pipeline.email}"
}

resource "google_project_iam_member" "dataproc_worker" {
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.credit_risk_batch_pipeline.email}"
}
