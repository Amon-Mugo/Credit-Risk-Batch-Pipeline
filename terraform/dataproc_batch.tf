# terraform/dataproc_batch.tf

resource "google_storage_bucket_object" "pipeline_entrypoint" {
  name   = "code/pipeline_entrypoint.py"
  bucket = google_storage_bucket.credit_risk_batch_pipeline.name
  source = "${path.module}/../src/pipeline_entrypoint.py"
}

resource "google_storage_bucket_object" "pipeline_src_zip" {
  name   = "code/src.zip"
  bucket = google_storage_bucket.credit_risk_batch_pipeline.name
  source = "${path.module}/../src.zip"
}

resource "google_dataproc_batch" "credit_risk_aggregation" {
  batch_id = "credit-risk-npl-aggregation-${var.batch_run_id}"
  location = var.region
  project  = var.project_id

  runtime_config {
    version = "2.1"
  }

  pyspark_batch {
    main_python_file_uri = "gs://${google_storage_bucket.credit_risk_batch_pipeline.name}/${google_storage_bucket_object.pipeline_entrypoint.name}"
    python_file_uris = [
      "gs://${google_storage_bucket.credit_risk_batch_pipeline.name}/${google_storage_bucket_object.pipeline_src_zip.name}"
    ]
  }

  environment_config {
    execution_config {
      service_account = google_service_account.credit_risk_batch_pipeline.email
      subnetwork_uri  = google_compute_subnetwork.credit_risk_batch_pipeline.id
    }
  }
}