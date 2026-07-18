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
variable "batch_run_id" {
  description = "Unique suffix for the Dataproc batch_id, to avoid collisions with prior failed/succeeded runs. Pass explicitly at apply time."
  type        = string
}
