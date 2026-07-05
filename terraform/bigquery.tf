resource "google_bigquery_dataset" "credit_risk_batch_pipeline" {
    dataset_id = "credit_risk_batch_pipeline"
    project    = var.project_id
    location   = var.region
    description = "Curated credit risk aggregates from Lending Club accepted loans data"
    labels = {
        project = "credit-risk-batch-pipeline"
        layer   = "curated"
    }
}