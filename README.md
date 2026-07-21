# Credit Risk Batch Pipeline

A batch data engineering pipeline that ingests the Lending Club loan dataset, cleans and enriches it with credit-risk features, computes non-performing loan (NPL) metrics at scale, and surfaces the results in an interactive dashboard — orchestrated end-to-end on Google Cloud Platform.

## Overview

This project simulates a real-world credit risk analytics pipeline for a lending institution. It answers two core business questions:

1. **Which borrower segments carry the highest default risk?** (by loan grade, state, loan purpose, and vintage year)
2. **How does default risk evolve over the life of a loan?** (default rate as a function of loan age, segmented by vintage year)

The pipeline processes the full Lending Club historical dataset (~2.26M loan records, 151 columns) through cleaning, feature engineering, and aggregation stages, then loads the results into BigQuery for visualization in Looker Studio. The entire pipeline is orchestrated via Airflow running in Docker, with infrastructure provisioned through Terraform.

**Tech stack:** GCS, PySpark, Dataproc Serverless, BigQuery, Looker Studio, Airflow, Terraform, Docker

![imagealt](https://github.com/Amon-Mugo/Credit-Risk-Batch-Pipeline/blob/38d6ae07dffb9d28d0355c3c5588c3200b38699d/dashboard-1.png)

## Architecture

The pipeline follows a batch ELT pattern with orchestrated, immutable execution stages:

\```mermaid
flowchart LR
    A[Lending Club CSV<br/>2.26M rows] -->|upload| B[(GCS<br/>raw bucket)]
    B --> C[PySpark Cleaning<br/>local + tested]
    C --> D[Feature Engineering<br/>vintage, DTI bucket,<br/>delinquency flag, loan age]
    D --> E[(GCS<br/>Parquet checkpoint)]
    E --> F[Dataproc Serverless<br/>Aggregations]
    F --> G[(BigQuery<br/>npl_ratio + default_rate_curve)]
    G --> H[Looker Studio<br/>Dashboard]

    I[Airflow DAG<br/>Docker/LocalExecutor] -.orchestrates.-> F
    I -.orchestrates.-> G
    J[Terraform<br/>VPC, GCS, BigQuery, IAM] -.provisions.-> B
    J -.provisions.-> F
    J -.provisions.-> G
\```

**Design notes:**

- **Cleaning and feature engineering run locally** (not on Dataproc) since the dataset comfortably fits in memory on a single machine during development; only the aggregation stage — the compute-heavy join/group-by work — runs on Dataproc Serverless.
- **Dataproc Serverless batches are immutable, one-shot executions**, a poor fit for Terraform's reconciliation model. Terraform therefore provisions only static infrastructure (VPC, subnet, firewall, GCS, BigQuery, IAM); Airflow owns all recurring pipeline execution via `BashOperator` calls to `gcloud dataproc batches submit`.
- **Airflow runs self-hosted in Docker** (not Cloud Composer) to keep the project cost-free and fully reproducible locally, using `network_mode: host` to work around a Pop!_OS-specific Docker bridge networking issue.
- **Failure alerting uses an explicit `on_failure_callback`** rather than Airflow's built-in `email_on_failure`, which silently no-op'd in this environment despite valid SMTP configuration.

## Data Dictionary

Key columns produced or transformed by the pipeline (source Lending Club columns are omitted here — see the raw schema in `schema/` for the full 151-column reference).

### Engineered Features

| Column | Type | Description |
|---|---|---|
| `is_default` | boolean (0/1) | Target label. 1 = Charged Off, 0 = Fully Paid. Derived from `loan_status`, filtered to resolved statuses only. |
| `vintage_year` | integer | Year the loan originated, extracted from `issue_d`. Used to segment cohorts by origination period. |
| `loan_age_months` | integer | Months between loan issue date and last payment/status date. Deterministic on this static dataset. |
| `dti_bucket` | string | Categorical bucket of debt-to-income ratio: `Low`, `Moderate`, `High`, or `Unknown` (null/unparseable DTI). |
| `delinquency_flag` | boolean / null | Three-way flag: `True` (has delinquency history), `False` (no history), or `null` (data not available). |

### Output Tables (BigQuery)

**`npl_ratio_by_dimension`** — long-format NPL ratio table, unioned across four dimensions:

| Column | Type | Description |
|---|---|---|
| `dimension` | string | One of `grade`, `addr_state`, `purpose`, `vintage_year`. |
| `dimension_value` | string | The specific value within that dimension (e.g. `A`, `CA`, `debt_consolidation`, `2015`). |
| `default_count` | integer | Count of defaulted loans within this dimension value. |
| `total_count` | integer | Total loan count within this dimension value. |
| `npl_ratio` | float | `default_count / total_count`. |

**`default_rate_curve`** — default rate by loan age, segmented by vintage year:

| Column | Type | Description |
|---|---|---|
| `vintage_year` | integer | Origination year cohort. |
| `loan_age_months` | integer | Loan age in whole months since origination. |
| `npl_ratio` | float | Default rate at this age/vintage combination. |
| `total_count` | integer | Loan count at this age/vintage combination (relevant for interpreting reliability — counts thin out past ~60 months). |

## Setup & Running the Pipeline

### Prerequisites

- GCP project with billing enabled
- `gcloud` CLI authenticated (ADC — no service account keys, per org policy)
- Docker + Docker Compose v2 (`docker compose`, not `docker-compose`)
- Terraform >= 1.x

### 1. Provision infrastructure

```bash
cd terraform
terraform init
terraform apply
```

This provisions the GCS bucket, BigQuery dataset, VPC/subnet/firewall for Dataproc Serverless, and IAM service account.

### 2. Configure Airflow

Set the Dataproc batch config as an Airflow Variable (`credit_risk_dataproc_config`) and add SMTP credentials to `airflow/.env` (gitignored) for failure email alerts.

### 3. Run the pipeline

```bash
cd airflow
./run_pipeline.sh
```

This single entry point brings up the Airflow stack via Docker Compose, waits for the scheduler to be ready, triggers the `credit_risk_dataproc_batch` DAG, and polls until the run succeeds or fails — exiting `0` or `1` accordingly.

### 4. View results

Query `npl_ratio_by_dimension` and `default_rate_curve` in BigQuery directly, or view the [Looker Studio dashboard](#dashboard) above.


## Key Design Decisions

**Why `try_cast` everywhere?**
PySpark 4.1.2 defaults to `spark.sql.ansi.enabled=true`, meaning a plain `.cast()` throws on malformed strings instead of returning null. Every type conversion in this pipeline uses `F.expr("try_cast(col as type)")` to fail gracefully on the Lending Club dataset's inconsistent formatting.

**Why does Terraform not manage the Dataproc batch itself?**
Dataproc Serverless batches are immutable, one-shot executions — each run needs a unique `batch_run_id` or it 409s on retry. That execution model doesn't fit Terraform's declarative reconciliation loop, which expects a resource to converge to a stable state. Terraform therefore owns only the static infrastructure (VPC, subnet, firewall, GCS, BigQuery, IAM), while Airflow owns recurring execution via `gcloud dataproc batches submit`.

**Why self-hosted Airflow in Docker instead of Cloud Composer?**
Keeps the project fully reproducible and cost-free to run locally, at the expense of some production-grade features (e.g. no managed scaling). `network_mode: host` works around a Pop!_OS-specific Docker bridge networking issue (missing `br_netfilter`, broken `docker0` bridge).

**Why `on_failure_callback` instead of `email_on_failure`?**
Airflow's built-in `email_on_failure` silently no-op'd in this environment — no error, no log line, nothing sent — despite a working SMTP configuration verified independently. An explicit `on_failure_callback` that wraps `send_email()` in a try/except and logs its own invocation guarantees visibility into whether the alert fired, and is now the standing pattern for failure alerting on this project.

**Why is the data quality report a separate local script rather than part of the Dataproc pipeline?**
The quality checks (row counts, null rates, distribution drift vs. baseline) are a lightweight diagnostic step best run locally against a Parquet checkpoint, rather than adding overhead to the Dataproc Serverless aggregation job.

