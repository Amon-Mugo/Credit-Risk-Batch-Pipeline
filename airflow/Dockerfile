# credit-risk-batch-pipeline/airflow/Dockerfile
#
# Extends the official Airflow image with the Google Cloud SDK so DAG tasks
# can shell out to `gcloud dataproc batches submit` via BashOperator.
# Base image/version must match docker-compose.yml's airflow image tag exactly.

FROM apache/airflow:2.10.5-python3.11

USER root

# Install gcloud CLI via the official apt repo (Debian-based image).
RUN apt-get update && apt-get install -y --no-install-recommends \
        apt-transport-https \
        ca-certificates \
        gnupg \
        curl \
    && curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
        | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
        > /etc/apt/sources.list.d/google-cloud-sdk.list \
    && apt-get update && apt-get install -y --no-install-recommends google-cloud-cli \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
