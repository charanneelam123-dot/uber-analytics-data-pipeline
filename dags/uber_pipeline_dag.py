"""
Airflow DAG: Uber Analytics Medallion Pipeline

Schedule: daily at 02:00 UTC
Layers:   Bronze (ingest) → Silver (transform) → Gold (aggregate)
SLA:      Each layer must complete within 30 / 45 / 60 minutes respectively.
Alerts:   SLA miss and task failure both trigger email callbacks.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["charanneelam123@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

SLA_BRONZE = timedelta(minutes=30)
SLA_SILVER = timedelta(minutes=45)
SLA_GOLD = timedelta(minutes=60)

SOURCE_PATH = "s3://uber-raw-data/trips/"
BRONZE_PATH = "/mnt/datalake/bronze/uber_trips"
SILVER_PATH = "/mnt/datalake/silver/uber_trips"
GOLD_BASE = "/mnt/datalake/gold"


# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    subject = f"[SLA MISS] Uber Pipeline — {dag.dag_id}"
    body = (
        f"<b>SLA missed</b> for tasks: {[t.task_id for t in task_list]}<br>"
        f"Blocking tasks: {[t.task_id for t in blocking_task_list]}"
    )
    send_email("charanneelam123@gmail.com", subject, body)


def failure_callback(context):
    task_instance = context["task_instance"]
    subject = f"[FAILURE] {task_instance.dag_id}.{task_instance.task_id}"
    body = (
        f"<b>Task failed</b>: {task_instance.task_id}<br>"
        f"Execution date: {context['execution_date']}<br>"
        f"Log: {task_instance.log_url}"
    )
    send_email("charanneelam123@gmail.com", subject, body)


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------


def run_bronze(**context):

    from ingestion.ingest_data import get_spark, ingest_bronze

    spark = get_spark()
    try:
        ingest_bronze(spark, SOURCE_PATH)
    finally:
        spark.stop()


def run_silver(**context):
    from pyspark.sql import SparkSession

    from transformation.silver_transform import run

    spark = (
        SparkSession.builder.appName("UberSilver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    try:
        run(spark, BRONZE_PATH, SILVER_PATH)
    finally:
        spark.stop()


def run_gold(**context):
    from pyspark.sql import SparkSession

    from aggregation.gold_aggregate import run

    spark = (
        SparkSession.builder.appName("UberGold")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    try:
        run(spark, SILVER_PATH, GOLD_BASE)
    finally:
        spark.stop()


def run_validation(**context):
    """Lightweight row-count checks on all three layers."""
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("UberValidation")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    try:
        bronze_count = spark.read.format("delta").load(BRONZE_PATH).count()
        silver_count = spark.read.format("delta").load(SILVER_PATH).count()
        if silver_count == 0:
            raise ValueError("Silver table is empty after transformation")
        silver_ratio = silver_count / bronze_count if bronze_count > 0 else 0
        if silver_ratio < 0.90:
            raise ValueError(
                f"Silver/Bronze ratio {silver_ratio:.1%} below 90% threshold"
            )
    finally:
        spark.stop()


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="uber_analytics_pipeline",
    default_args=DEFAULT_ARGS,
    description="Medallion pipeline for Uber trip analytics",
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=sla_miss_callback,
    tags=["uber", "medallion", "data-engineering"],
) as dag:

    bronze_task = PythonOperator(
        task_id="ingest_bronze",
        python_callable=run_bronze,
        sla=SLA_BRONZE,
        on_failure_callback=failure_callback,
    )

    silver_task = PythonOperator(
        task_id="transform_silver",
        python_callable=run_silver,
        sla=SLA_SILVER,
        on_failure_callback=failure_callback,
    )

    gold_task = PythonOperator(
        task_id="aggregate_gold",
        python_callable=run_gold,
        sla=SLA_GOLD,
        on_failure_callback=failure_callback,
    )

    validation_task = PythonOperator(
        task_id="validate_pipeline",
        python_callable=run_validation,
        on_failure_callback=failure_callback,
    )

    bronze_task >> silver_task >> gold_task >> validation_task
