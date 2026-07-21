# credit-risk-batch-pipeline/airflow/dags/credit_risk_dataproc_dag.py
#
# Manually-triggered DAG orchestrating the Credit Risk Batch Pipeline's
# Dataproc Serverless aggregation job. Submits the batch, waits for a
# terminal state, then verifies curated BigQuery output landed correctly.
# GCP-specific config (project, region, bucket, service account, subnet)
# lives in the Airflow Variable `credit_risk_dataproc_config` rather than
# being hardcoded here, so the DAG stays portable across environments.

import json
import logging
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.utils.email import send_email

CONFIG = Variable.get("credit_risk_dataproc_config", deserialize_json=True)

# Same expression rendered independently in submit and wait tasks; since both
# render within the same DAG run context, they resolve to the identical batch
# ID without needing XCom to pass it between tasks.
BATCH_ID_EXPR = (
    "credit-risk-batch-"
    "{{ ts_nodash | lower }}"
)


def notify_failure(context):
    """
    Sends a failure alert email via SMTP, invoked directly as an
    on_failure_callback rather than relying on default_args' built-in
    email_on_failure — which was found to silently no-op in this Airflow
    2.10.5 setup despite valid SMTP config. Explicit logging on entry and
    a try/except around the send confirm invocation and surface any SMTP
    exception directly in the task log, rather than failing silently.
    """
    logger = logging.getLogger("airflow.task")
    logger.info("notify_failure callback invoked")

    task_instance = context["task_instance"]
    exception = context.get("exception")

    try:
        send_email(
            to=["amonkariuki325@gmail.com"],
            subject=f"Airflow task failed: {task_instance.dag_id}.{task_instance.task_id}",
            html_content=f"""
            <p>Task <b>{task_instance.task_id}</b> in DAG <b>{task_instance.dag_id}</b> failed.</p>
            <p>Run ID: {context['run_id']}</p>
            <p>Execution date: {context['logical_date']}</p>
            <p>Exception: {exception}</p>
            <p>Log URL: {task_instance.log_url}</p>
            """,
        )
        logger.info("notify_failure: email sent successfully")
    except Exception as e:
        logger.error(f"notify_failure: failed to send email: {e}")


default_args = {
    "owner": "amon",
    "retries": 0,
    "on_failure_callback": notify_failure,
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
