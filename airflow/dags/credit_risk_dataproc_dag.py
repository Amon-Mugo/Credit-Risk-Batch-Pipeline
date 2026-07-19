# credit-risk-batch-pipeline/airflow/dags/credit_risk_dataproc_dag.py
#
# Manually-triggered DAG orchestrating the Credit Risk Batch Pipeline's
# Dataproc Serverless aggregation job. Submits the batch, waits for a
# terminal state, then verifies curated BigQuery output landed correctly.
# GCP-specific config (project, region, bucket, service account, subnet)
# lives in the Airflow Variable `credit_risk_dataproc_config` rather than
# being hardcoded here, so the DAG stays portable across environments.

import json
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

CONFIG = Variable.get("credit_risk_dataproc_config", deserialize_json=True)

# Same expression rendered independently in submit and wait tasks; since both
# render within the same DAG run context, they resolve to the identical batch
# ID without needing XCom to pass it between tasks.
BATCH_ID_EXPR = (
    "credit-risk-batch-"
    "{{ ts_nodash | lower }}"
)

default_args = {
    "owner": "amon",
    "retries": 0,
}

with DAG(
    dag_id="credit_risk_dataproc_batch",
    description="Submit and verify the Credit Risk Batch Pipeline's Dataproc Serverless aggregation job",
    default_args=default_args,
    schedule=None,  # manual trigger only — static dataset, portfolio orchestration demo
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["credit-risk", "dataproc", "portfolio"],
) as dag:

    submit_batch = BashOperator(
        task_id="submit_batch",
        bash_command=f"""
        gcloud dataproc batches submit pyspark \
          gs://{CONFIG['bucket']}/code/pipeline_entrypoint.py \
          --batch={BATCH_ID_EXPR} \
          --project={CONFIG['project_id']} \
          --region={CONFIG['region']} \
          --py-files=gs://{CONFIG['bucket']}/code/src.zip \
          --service-account={CONFIG['service_account']} \
          --subnet={CONFIG['subnetwork']} \
          --version=2.1 \
          --async
        """,
    )

    wait_for_batch = BashOperator(
        task_id="wait_for_batch",
        bash_command=f"""
        gcloud dataproc batches wait {BATCH_ID_EXPR} \
          --project={CONFIG['project_id']} \
          --region={CONFIG['region']}
        """,
    )

    verify_output = BashOperator(
        task_id="verify_output",
        bash_command=f"""
        set -e

        NPL_COUNT=$(bq query --use_legacy_sql=false --format=csv \
          'SELECT COUNT(*) FROM `{CONFIG['project_id']}.credit_risk_batch_pipeline.npl_ratio_by_dimension`' \
          | tail -n 1)

        VINTAGE_COUNT=$(bq query --use_legacy_sql=false --format=csv \
          'SELECT COUNT(*) FROM `{CONFIG['project_id']}.credit_risk_batch_pipeline.default_rate_vintage_curve`' \
          | tail -n 1)

        echo "npl_ratio_by_dimension row count: $NPL_COUNT"
        echo "default_rate_vintage_curve row count: $VINTAGE_COUNT"

        if [ "$NPL_COUNT" -eq 0 ] || [ "$VINTAGE_COUNT" -eq 0 ]; then
          echo "ERROR: one or both curated tables are empty after batch completion."
          exit 1
        fi
        """,
    )

    submit_batch >> wait_for_batch >> verify_output