#!/usr/bin/env bash
#
# run_pipeline.sh
# Single entry point for the Credit Risk Batch Pipeline: starts the Airflow
# stack, triggers the credit_risk_dataproc_batch DAG, and blocks until the
# run finishes, printing a pass/fail summary.
#
# Usage: ./run_pipeline.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

DAG_ID="credit_risk_dataproc_batch"
SCHEDULER_SERVICE="airflow-scheduler"
HEALTH_TIMEOUT_SECONDS=120
HEALTH_POLL_INTERVAL=5
RUN_TIMEOUT_SECONDS=1800
RUN_POLL_INTERVAL=15

log() {
    printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$1"
}

log "Starting Airflow stack (docker compose up -d)..."
docker compose up -d

log "Waiting for scheduler to be ready (timeout: ${HEALTH_TIMEOUT_SECONDS}s)..."
elapsed=0
until docker compose exec -T "$SCHEDULER_SERVICE" airflow db check >/dev/null 2>&1; do
    if [ "$elapsed" -ge "$HEALTH_TIMEOUT_SECONDS" ]; then
        log "ERROR: Scheduler did not become ready within ${HEALTH_TIMEOUT_SECONDS}s."
        exit 1
    fi
    sleep "$HEALTH_POLL_INTERVAL"
    elapsed=$((elapsed + HEALTH_POLL_INTERVAL))
done
log "Scheduler is ready."

RUN_ID="manual_$(date -u '+%Y%m%dT%H%M%S')"
log "Triggering DAG '${DAG_ID}' with run_id '${RUN_ID}'..."
docker compose exec -T "$SCHEDULER_SERVICE" airflow dags trigger -r "$RUN_ID" "$DAG_ID"

log "Polling for run completion (timeout: ${RUN_TIMEOUT_SECONDS}s)..."
elapsed=0
final_state=""
while [ "$elapsed" -lt "$RUN_TIMEOUT_SECONDS" ]; do
    raw_runs="$(docker compose exec -T "$SCHEDULER_SERVICE" airflow dags list-runs -d "$DAG_ID" --output json)"

    state="$(printf '%s' "$raw_runs" | python3 -c "
import json, sys
data = json.load(sys.stdin)
rid = '$RUN_ID'
states = [r.get('state') for r in data if r.get('run_id') == rid]
print(states[0] if states else '')
")"

    log "Current state: ${state:-<not found yet>}"

    case "$state" in
        success)
            final_state="success"
            break
            ;;
        failed)
            final_state="failed"
            break
            ;;
    esac

    sleep "$RUN_POLL_INTERVAL"
    elapsed=$((elapsed + RUN_POLL_INTERVAL))
done

if [ -z "$final_state" ]; then
    log "ERROR: Run '${RUN_ID}' did not reach a terminal state within ${RUN_TIMEOUT_SECONDS}s."
    exit 1
fi

if [ "$final_state" = "success" ]; then
    log "Pipeline run '${RUN_ID}' completed SUCCESSFULLY."
    exit 0
else
    log "Pipeline run '${RUN_ID}' FAILED. Check task logs in airflow/logs/ or the alert email."
    exit 1
fi
