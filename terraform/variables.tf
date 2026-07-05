variable "project_id" {
    description = "GCP project ID for the credit risk batch pipeline"
    type        = string
    default     = "gcp-de-learning-498109"
}

variable "region" {
    description = "GCP region for regional resources (GCS bucket, BigQuery dataset, Dataproc Serverless batches)"
    type        = string
    default     = "us-central1"
} 