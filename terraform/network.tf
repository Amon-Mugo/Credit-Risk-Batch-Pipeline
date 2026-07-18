# terraform/network.tf
#
# Dedicated VPC and subnet for Dataproc Serverless batches. Private Google
# Access is required so batches can reach GCS/BigQuery without a public IP.
# The internal firewall rule allows driver-executor communication within
# the subnet, which GCP blocks by default in a custom (non-default) VPC.

resource "google_compute_network" "credit_risk_batch_pipeline" {
  name                    = "credit-risk-batch-pipeline"
  project                 = var.project_id
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "credit_risk_batch_pipeline" {
  name                     = "credit-risk-batch-pipeline"
  project                  = var.project_id
  region                   = var.region
  network                  = google_compute_network.credit_risk_batch_pipeline.id
  ip_cidr_range            = "10.10.0.0/24"
  private_ip_google_access = true
}

resource "google_compute_firewall" "allow_internal_dataproc" {
  name    = "credit-risk-batch-pipeline-allow-internal"
  project = var.project_id
  network = google_compute_network.credit_risk_batch_pipeline.id

  direction = "INGRESS"
  source_ranges = ["10.10.0.0/24"]

  allow {
    protocol = "tcp"
    ports    = ["0-65535"]
  }

  allow {
    protocol = "udp"
    ports    = ["0-65535"]
  }

  allow {
    protocol = "icmp"
  }
}