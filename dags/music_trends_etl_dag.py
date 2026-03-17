"""
music_trends_etl_dag.py — DAG Apache Airflow
=================================================
Pipeline ETL + Streaming : Last.fm → Kafka → Flink → MinIO → PostgreSQL

Tâches :
  1. check_health          — Santé de Kafka, Flink, MinIO
  2. produce_to_kafka      — Last.fm + MusicBrainz → topics raw-*
  3. flink_transform       — raw-* → silver-* (Kafka) + MinIO Silver
  4. flink_aggregate       — silver-* → gold-kpis (Kafka) + PostgreSQL KPIs
  5. log_summary           — Résumé complet du pipeline
"""

import sys
import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/scripts")
log = logging.getLogger(__name__)

default_args = {
    "owner": "data-team", "depends_on_past": False,
    "email_on_failure": False, "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def task_check_health(**context):
    import requests
    from confluent_kafka.admin import AdminClient

    errors = []

    # Last.fm
    api_key = os.environ.get("LASTFM_API_KEY", "")
    try:
        r = requests.get("https://ws.audioscrobbler.com/2.0/",
                         params={"method": "chart.getTopArtists", "api_key": api_key,
                                 "format": "json", "limit": 1}, timeout=10)
        r.raise_for_status()
        log.info("[Health] Last.fm : OK")
    except Exception as e:
        errors.append(f"Last.fm : {e}")

    # Kafka
    try:
        admin = AdminClient({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")})
        meta  = admin.list_topics(timeout=10)
        log.info(f"[Health] Kafka : OK ({len(meta.topics)} topics)")
    except Exception as e:
        errors.append(f"Kafka : {e}")

    # Flink
    try:
        r = requests.get(os.environ.get("FLINK_JOBMANAGER_URL", "http://flink-jobmanager:8081") + "/overview",
                         timeout=10)
        r.raise_for_status()
        flink_info = r.json()
        log.info(f"[Health] Flink : OK — {flink_info.get('taskmanagers',0)} TM, "
                 f"{flink_info.get('slots-available',0)} slots disponibles")
    except Exception as e:
        errors.append(f"Flink : {e}")

    # MinIO
    import boto3
    from botocore.client import Config
    try:
        client = boto3.client("s3",
            endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
            aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
            config=Config(signature_version="s3v4"), region_name="us-east-1")
        bucket = os.environ.get("MINIO_BUCKET", "music-trends-etl")
        try:
            client.head_bucket(Bucket=bucket)
        except Exception:
            client.create_bucket(Bucket=bucket)
        log.info("[Health] MinIO : OK")
    except Exception as e:
        errors.append(f"MinIO : {e}")

    if errors:
        raise RuntimeError("Checks échoués :\n" + "\n".join(errors))
    return {"status": "ok", "checked_at": datetime.utcnow().isoformat()}


def task_produce_to_kafka(**context):
    from kafka_producer import run_producer
    run_date = context["data_interval_start"].date()
    summary  = run_producer(run_date=run_date)
    context["ti"].xcom_push(key="produce_summary", value=summary)
    return summary


def task_flink_transform(**context):
    from flink_job import run_flink_transform_job
    run_date = context["data_interval_start"].date().strftime("%Y-%m-%d")
    summary  = run_flink_transform_job(run_date=run_date, timeout_s=180)
    context["ti"].xcom_push(key="transform_summary", value=summary)
    return summary


def task_flink_aggregate(**context):
    from flink_job import run_flink_aggregation_job
    run_date = context["data_interval_start"].date().strftime("%Y-%m-%d")
    summary  = run_flink_aggregation_job(run_date=run_date, timeout_s=90)
    context["ti"].xcom_push(key="aggregate_summary", value=summary)
    return summary


def task_log_summary(**context):
    ti       = context["ti"]
    prod_s   = ti.xcom_pull(task_ids="produce_to_kafka",   key="produce_summary")   or {}
    trans_s  = ti.xcom_pull(task_ids="flink_transform",    key="transform_summary") or {}
    agg_s    = ti.xcom_pull(task_ids="flink_aggregate",    key="aggregate_summary") or {}
    run_date = context["data_interval_start"].strftime("%Y-%m-%d")

    log.info(f"""
╔═══════════════════════════════════════════════════════════════╗
║   PIPELINE ETL + STREAMING — TENDANCES MUSICALES              ║
║   Run date : {run_date:<49}                                   ║
╠═══════════════════════════════════════════════════════════════╣
║  KAFKA PRODUCER (Last.fm + MusicBrainz → raw-*)              ║
║    Artistes publiés  : {str(prod_s.get("artists",  "N/A")):<38} ║
║    Titres publiés    : {str(prod_s.get("tracks",   "N/A")):<38} ║
║    Évén. géo         : {str(prod_s.get("geo_events","N/A")):<38} ║
║    Total événements  : {str(prod_s.get("total_events","N/A")):<38} ║
╠═══════════════════════════════════════════════════════════════╣
║  FLINK TRANSFORM (raw-* → silver-* + MinIO Silver)           ║
║    Artistes traités  : {str(trans_s.get("artist_events","N/A")):<38} ║
║    Titres traités    : {str(trans_s.get("track_events", "N/A")):<38} ║
║    PostgreSQL artists: {str(trans_s.get("postgres_artists","N/A")):<38} ║
║    PostgreSQL charts : {str(trans_s.get("postgres_charts", "N/A")):<38} ║
╠═══════════════════════════════════════════════════════════════╣
║  FLINK AGGREGATION (silver-* → gold-kpis + PostgreSQL)       ║
║    Événements traités: {str(agg_s.get("events_processed","N/A")):<38} ║
║    KPIs calculés     : {str(agg_s.get("kpis_computed",  "N/A")):<38} ║
╚═══════════════════════════════════════════════════════════════╝
    """)
    return {"status": "success", "run_date": run_date}


with DAG(
    dag_id="music_trends_etl_dag",
    description="ETL+Streaming Last.fm → Kafka → Flink → MinIO → PostgreSQL",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "streaming", "kafka", "flink", "music", "lastfm"],
) as dag:

    t1 = PythonOperator(task_id="check_health",        python_callable=task_check_health)
    t2 = PythonOperator(task_id="produce_to_kafka",    python_callable=task_produce_to_kafka)
    t3 = PythonOperator(task_id="flink_transform",     python_callable=task_flink_transform)
    t4 = PythonOperator(task_id="flink_aggregate",     python_callable=task_flink_aggregate)
    t5 = PythonOperator(task_id="log_summary",         python_callable=task_log_summary)

    t1 >> t2 >> t3 >> t4 >> t5
