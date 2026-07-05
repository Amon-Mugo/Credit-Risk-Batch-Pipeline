resource "google_storage_bucket" "credit_risk_batch_pipeline" {
  name     = "credit-risk-batch-pipeline"
  location = var.region
  project  = var.project_id

  force_destroy               = false
  uniform_bucket_level_access = true

  versioning {
    enabled = false
  }

  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type = "Delete"
    }
  }

  labels = {
    project = "credit-risk-batch-pipeline"
    layer   = "raw"
  }
}