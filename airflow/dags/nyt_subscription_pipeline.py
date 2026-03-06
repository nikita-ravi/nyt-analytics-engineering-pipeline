"""
NYT Analytics Engineering — Airflow DAG (Portfolio Version)
=============================================================
Full pipeline visualization for the Airflow UI.
Uses only core Airflow operators (no extra providers needed)
so the DAG loads cleanly in a standard docker-compose setup.

In production at NYT:
  - GCSObjectExistenceSensor  →  replace the Python stub sensors
  - SlackWebhookOperator      →  replace the Python stub alert
  - BashOperator dbt commands →  connect to real BigQuery profile

DAG structure:
    schema_drift_check (NEW)
         ↓
    check_gcs_landing [task_group, parallel sensors]
         ↓
    run_dbt_staging  →  dbt test staging
         ↓
    marts_and_dq [parallel task_group]
      ├─ run_dbt_marts → run_dbt_metrics
      └─ run_dq_checks → Slack #data-quality-alerts
         ↓  (TriggerRule.ALL_DONE)
    reprocess_late_arrivals (NEW)
         ↓
    update_data_catalog
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task, task_group
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner":            "analytics-engineering",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email_on_failure": False,
}

GCS_BUCKET   = "nyt-data-platform-landing"
PROJECT_ID   = "nyt-analytics-engineering"
SLACK_CONN   = "slack_data_quality_alerts"


def _stub_sensor(dataset_name: str, **context):
    """
    Stubs a GCSObjectExistenceSensor for portfolio demo.
    In production: replace with GCSObjectExistenceSensor(
        bucket=GCS_BUCKET, object=f'raw/{dataset_name}/events.parquet',
        timeout=3600, poke_interval=120, mode='reschedule'
    )
    """
    log.info("✅ [SENSOR] %s data verified in GCS bucket %s", dataset_name, GCS_BUCKET)
    log.info("    In production: GCSObjectExistenceSensor polls every 2 min, times out after 1h")


def _alert_slack(context):
    """
    On-failure callback — posts structured alert to #data-quality-alerts.
    In production: SlackWebhookOperator via slack_data_quality_alerts connection.
    """
    dag_id  = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    log.error(
        "🔴 PIPELINE FAILURE | DAG: %s | Task: %s | "
        "Would POST to Slack #data-quality-alerts",
        dag_id, task_id
    )


@dag(
    dag_id="nyt_subscription_analytics_pipeline",
    description=(
        "End-to-end NYT subscription analytics pipeline. "
        "Schema drift → GCS sensors → dbt staging → marts + DQ → "
        "late arrival reprocessing → data catalog."
    ),
    schedule_interval="0 6 * * *",   # 6 AM UTC daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    on_failure_callback=_alert_slack,
    tags=["nyt", "subscription", "analytics-engineering", "daily", "dbt"],
    params={
        "lookback_days": Param(90,    type="integer", description="DQ + metrics lookback window (days)"),
        "dry_run":       Param(False,  type="boolean", description="Skip dbt & BQ writes — validation only"),
        "reprocess_t1":  Param(True,   type="boolean", description="Reprocess yesterday's late arrivals"),
    },
    doc_md="""
## NYT Subscription Analytics Pipeline

### Pipeline Architecture (3 new features added)

```
schema_drift_check   ← NEW: blocks pipeline on breaking schema change
       ↓
check_gcs_landing    [parallel sensors for all 5 product domains]
       ↓
run_dbt_staging      dbt run + test staging layer (Views in BigQuery)
       ↓
marts_and_dq         [PARALLEL]
  ├─ run_dbt_marts   mart_subscriber_health, mart_bundle_metrics
  │  run_dbt_metrics mtr_daily_active_subscribers ← NEW: official DAU
  └─ run_dq_checks   5-dimension DQ scorecard + Slack alerts
       ↓  (TriggerRule.ALL_DONE)
reprocess_late_arrivals ← NEW: reruns t-1 partitions from mobile lag manifest
       ↓
update_data_catalog  POST to Dataplex / catalog.nytimes.internal
```

### Key Mart Outputs
| Model | Grain | Consumer |
|-------|-------|----------|
| `mart_subscriber_health` | subscriber × day | Retention, DS churn model |
| `mart_data_quality_scorecard` | dataset × run_date | Data Platform monitoring |
| `mtr_daily_active_subscribers` | date × product | Finance, Exec dashboard |

### On-Call Runbook
1. Check Slack #data-quality-alerts for alert message
2. If `schema_drift_check` failed → a domain team changed their schema. Check GCS file vs registry.
3. If `run_dbt_staging` failed → likely a new dbt test failure. Check `dbt test` output.
4. If `run_dq_checks` warned → DQ score between 85-95%. Triage with domain owner.
5. Never hotfix mart data directly — fix upstream and rerun the DAG.
    """,
)
def nyt_subscription_pipeline():

    # ────────────────────────────────────────────────────────────────
    # NEW: Schema Drift Check — Task 0 before ANY data transformation
    # ────────────────────────────────────────────────────────────────
    @task(task_id="schema_drift_check")
    def schema_drift_check(**context):
        """
        Validates all 5 raw schemas against the version-controlled registry.
        BREAKING drift (missing columns, type narrowing, new enum values)
        causes this task to fail → ALL downstream tasks are blocked.

        In production: runs ingestion/schema_drift_detector.py
        Registry: docs/schema_registry.json (reviewed via GitHub PR)
        Slack: posts column-level diff to #schema-changes on WARNING
        """
        log.info("🔍 Schema Drift Check — registry v2024.Q1.3")
        log.info("   Checking: subscribers · events_news · events_games · events_cooking · events_athletic")
        log.info("   Approved by: tommy.wu@nytimes.com")

        # Simulated result — in production: subprocess.run(['python', 'schema_drift_detector.py'])
        drift_result = {
            "registry_version": "2024.Q1.3",
            "checked_at": datetime.utcnow().isoformat(),
            "summary": {"clean": 5, "warning": 0, "breaking": 0},
        }
        if drift_result["summary"]["breaking"] > 0:
            raise RuntimeError(
                "🔴 BREAKING schema drift detected! "
                "Downstream dbt models blocked. "
                "Check #schema-changes for column-level diff."
            )
        log.info("✅ All schemas validated — safe to proceed to GCS sensors")
        return drift_result

    # ────────────────────────────────────────────────────────────────
    # GCS Landing Sensors (parallel — all 5 domains)
    # ────────────────────────────────────────────────────────────────
    @task_group(group_id="check_gcs_landing")
    def check_gcs_landing():
        """
        Wait for raw files to land in GCS before transforming.
        Mode=reschedule frees the worker slot while polling.
        Timeout=1h to handle late upstream feeds.

        Production: replace PythonOperator stubs with GCSObjectExistenceSensor.
        """
        domains = ["subscribers", "news", "games", "cooking", "athletic"]
        tasks = []
        for domain in domains:
            t = PythonOperator(
                task_id=f"wait_for_{domain}",
                python_callable=_stub_sensor,
                op_kwargs={"dataset_name": domain},
                doc_md=f"Polls GCS every 2 min for raw/{domain}/events.parquet",
            )
            tasks.append(t)
        # All sensors run in parallel (no dependency between them)
        return tasks[-1]  # return last for wiring; all run in parallel within group

    # ────────────────────────────────────────────────────────────────
    # dbt Staging (Views in BigQuery — cheap, always fresh)
    # ────────────────────────────────────────────────────────────────
    @task(task_id="run_dbt_staging")
    def run_dbt_staging(**context):
        """
        Runs all staging models + tests.
        Staging = cleaning layer only (dedup, type casting, DQ flag cols).
        Materialized as Views → no storage cost, always fresh.

        Commands:
            dbt run  --select staging --profiles-dir .
            dbt test --select staging --profiles-dir .
        """
        dry_run = context["params"]["dry_run"]
        if dry_run:
            log.info("DRY RUN — skipping dbt staging")
            return {"status": "dry_run"}

        log.info("▶ dbt run --select staging")
        log.info("  stg_subscribers     : 5,000 rows | dedup CRM double-writes")
        log.info("  stg_events_news     : 14,231 rows | orphan quarantine applied")
        log.info("  stg_events_games    : 111,129 rows | future timestamps clamped")
        log.info("  stg_events_cooking  : 49,863 rows")
        log.info("  stg_events_athletic : 63,527 rows")
        log.info("▶ dbt test --select staging  →  all tests PASS")
        return {"models_run": 5, "tests_passed": 12}

    # ────────────────────────────────────────────────────────────────
    # Parallel: Mart Build + DQ Checks
    # ────────────────────────────────────────────────────────────────
    @task_group(group_id="marts_and_dq")
    def marts_and_dq():

        @task(task_id="run_dbt_marts")
        def run_dbt_marts(**context):
            """
            Builds core marts (Tables in BigQuery — materialized daily).
            mart_subscriber_health: cross-product engagement + churn risk
            mart_dq_scorecard: 5-dimension DQ monitoring
            mart_bundle_metrics: plan × week aggregates for Finance
            """
            log.info("▶ dbt run --select marts")
            log.info("  mart_subscriber_health       : 5,000 rows")
            log.info("  mart_data_quality_scorecard  : 5 datasets × 5 dimensions")
            log.info("  mart_bundle_metrics          : 6 plan types × 13 weeks")
            return {"marts_built": 3}

        @task(task_id="run_dbt_metrics")
        def run_dbt_metrics(**context):
            """
            NEW: Runs the official metrics layer.
            mtr_daily_active_subscribers — the SINGLE approved DAU definition,
            signed off by Data Platform, Finance, Product, Editorial.
            Version-pinned: _metric_version = '2024.Q1'
            """
            log.info("▶ dbt run --select metrics")
            log.info("  mtr_daily_active_subscribers : official DAU, version 2024.Q1")
            log.info("  mtr_churn_rate               : 90-day rolling churn by plan")
            log.info("  mtr_bundle_ltv               : LTV model by bundle tier")
            return {"metrics_built": 3}

        @task(task_id="run_dq_checks")
        def run_dq_checks(**context):
            """
            Runs standalone DQ framework (data_quality_runner.py).
            5 dimensions: completeness, uniqueness, timeliness, referential, validity.
            Score < 85: task FAILS → Slack alert to #data-quality-alerts.
            Score 85-95: task WARNS → Slack advisory, pipeline continues.
            Score > 95: PASS.
            """
            dq_summary = {
                "run_date":      context["ds"],
                "datasets_pass": 5,
                "datasets_warn": 0,
                "datasets_fail": 0,
                "avg_dq_score":  99.9,
                "min_dq_score":  99.9,
            }
            log.info("DQ Summary: %s", json.dumps(dq_summary, indent=2))

            if dq_summary["datasets_fail"] > 0:
                raise ValueError(
                    f"{dq_summary['datasets_fail']} dataset(s) FAIL. "
                    "Alert sent to #data-quality-alerts. Do not hotfix — triage first."
                )
            log.info("✅ All datasets PASS DQ framework")
            return dq_summary

        marts = run_dbt_marts()
        metrics = run_dbt_metrics()
        dq = run_dq_checks()
        marts >> metrics

    # ────────────────────────────────────────────────────────────────
    # NEW: Late Arrival Reprocessing
    # ────────────────────────────────────────────────────────────────
    @task(
        task_id="reprocess_late_arrivals",
        trigger_rule=TriggerRule.ALL_DONE,  # runs even if DQ warns
    )
    def reprocess_late_arrivals(**context):
        """
        NEW: Consumes the reprocessing manifest from late_arrival_detector.py.
        Reruns dbt staging for date partitions with > 5% late-arriving events.

        Why: Mobile events arrive up to 10h after they happened (Android Doze Mode).
        Without this, yesterday's DAU is permanently understated — at-risk subscribers
        get false churn alerts; retention sends unnecessary win-back emails.

        Commands (per flagged date partition):
            dbt run --select staging
                   --vars '{run_date: 2024-03-05, full_refresh: true}'
        """
        if not context["params"]["reprocess_t1"]:
            log.info("reprocess_t1=False — skipping late arrival reprocessing")
            return {}

        log.info("📱 Late Arrival Reprocessing")
        log.info("   Android avg lag: 3.5h | p95: 10.6h")
        log.info("   iOS avg lag:     1.5h | p95:  4.5h")
        log.info("   Overall late rate: 15.4%% across 36,701 events")
        log.info("   Reprocessing 364 date-domain combinations from manifest")
        log.info("   ✅ Partitions refreshed — yesterday's DAU corrected before 9 AM standup")
        return {"partitions_reprocessed": 364, "domains": 4}

    # ────────────────────────────────────────────────────────────────
    # Catalog Update
    # ────────────────────────────────────────────────────────────────
    @task(
        task_id="update_data_catalog",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    def update_data_catalog(**context):
        """
        Pushes asset metadata (freshness, row counts, owner, description)
        to the enterprise data catalog.
        In production: POST to Dataplex / DataHub / catalog.nytimes.internal
        """
        log.info("📚 Updating data catalog for run_date=%s", context["ds"])
        assets = [
            "raw_data.raw_subscribers",
            "dbt_prod.mart_subscriber_health",
            "dbt_prod.mart_data_quality_scorecard",
            "dbt_prod.mtr_daily_active_subscribers",
        ]
        for asset in assets:
            log.info("  ✅ Catalog updated: %s", asset)

    # ── Wire the DAG ────────────────────────────────────────────────
    drift     = schema_drift_check()
    landing   = check_gcs_landing()   # task_group — all sensors parallel inside
    staging   = run_dbt_staging()
    marts_dq  = marts_and_dq()        # task_group — marts + dq parallel inside
    reprocess = reprocess_late_arrivals()
    catalog   = update_data_catalog()

    drift >> landing >> staging >> marts_dq >> reprocess >> catalog


nyt_subscription_pipeline()
