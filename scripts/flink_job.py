"""
flink_job.py — Jobs Flink Streaming (Music ETL)
================================================
Rôle : Consommer les topics Kafka raw-* et appliquer :
  1. FlinkTransformJob  : nettoyage + normalisation → topic silver-*  + MinIO Silver
  2. FlinkAggregationJob: fenêtres glissantes → topic gold-kpis + PostgreSQL Gold

Architecture :
  Kafka raw-artists/raw-tracks/raw-tags/raw-geo
        ↓ (Flink DataStream API)
  Kafka silver-charts / silver-artists
  MinIO /processed/ (Parquet via sink)
        ↓ (Flink Table API + fenêtres)
  PostgreSQL Gold (via JDBC Sink)
  Kafka gold-kpis
"""

import os
import io
import json
import logging
from datetime import datetime, timedelta

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, Producer, KafkaError
from confluent_kafka.admin import AdminClient
from sqlalchemy import create_engine, text
from botocore.client import Config

# ─── Configuration ────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "music-trends-etl")
PG_HOST = os.environ.get("POSTGRES_GOLD_HOST", "localhost")
PG_PORT = os.environ.get("POSTGRES_GOLD_PORT", "5432")
PG_DB = os.environ.get("POSTGRES_GOLD_DB", "music_trends_db")
PG_USER = os.environ.get("POSTGRES_GOLD_USER", "gold")
PG_PASSWORD = os.environ.get("POSTGRES_GOLD_PASSWORD", "gold123")

GENRE_MAP = {
    "rock": "Rock",
    "pop": "Pop",
    "hip-hop": "Hip-Hop",
    "rap": "Hip-Hop",
    "electronic": "Electronic",
    "house": "Electronic",
    "techno": "Electronic",
    "edm": "Electronic",
    "jazz": "Jazz",
    "classical": "Classical",
    "r&b": "R&B",
    "soul": "R&B",
    "metal": "Metal",
    "indie": "Indie",
    "country": "Country",
    "reggae": "Reggae",
    "latin": "Latin",
}

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [FLINK] [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


# ─── Clients ──────────────────────────────────────────────────────────────────


def get_consumer(group_id: str, topics: list) -> Consumer:
    c = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 300000,
        }
    )
    c.subscribe(topics)
    return c


def get_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "client.id": "flink-music-producer",
            "acks": "all",
        }
    )


def get_minio():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def get_pg_engine():
    url = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    return create_engine(url, pool_pre_ping=True)


# ─── Transformation (Silver) ──────────────────────────────────────────────────


def transform_artist_event(event: dict) -> dict:
    """Normalise un événement raw-artists → silver."""
    name = str(event.get("name", "")).strip()
    country = str(event.get("country", "")).strip().upper()
    mb_tags = event.get("mb_tags", [])
    genre = GENRE_MAP.get(mb_tags[0].lower(), "Other") if mb_tags else "Other"

    return {
        "event_type": "silver_artist",
        "run_date": event.get("run_date"),
        "rank": event.get("rank", 0),
        "region": event.get("region", "global"),
        "name": name,
        "playcount": max(0, int(event.get("playcount", 0) or 0)),
        "listeners": max(0, int(event.get("listeners", 0) or 0)),
        "country": country if len(country) == 2 else "",
        "genre": genre,
        "mbid": event.get("mbid", ""),
        "area": event.get("area", ""),
        "type": event.get("type", ""),
        "is_valid": bool(name),
        "transformed_at": datetime.utcnow().isoformat() + "Z",
    }


def transform_track_event(event: dict) -> dict:
    """Normalise un événement raw-tracks → silver."""
    title = str(event.get("title", "")).strip()
    artist_name = str(event.get("artist_name", "")).strip()

    return {
        "event_type": "silver_track",
        "run_date": event.get("run_date"),
        "rank": event.get("rank", 0),
        "region": event.get("region", "global"),
        "title": title,
        "artist_name": artist_name,
        "playcount": max(0, int(event.get("playcount", 0) or 0)),
        "listeners": max(0, int(event.get("listeners", 0) or 0)),
        "duration_ms": max(0, int(event.get("duration_ms", 0) or 0)),
        "mbid": event.get("mbid", ""),
        "is_valid": bool(title and artist_name),
        "transformed_at": datetime.utcnow().isoformat() + "Z",
    }


def transform_geo_event(event: dict) -> dict:
    """Normalise un événement raw-geo → silver."""
    return {
        "event_type": "silver_geo",
        "run_date": event.get("run_date"),
        "rank": event.get("rank", 0),
        "region": event.get("country", ""),
        "name": str(event.get("name", "")).strip(),
        "listeners": max(0, int(event.get("listeners", 0) or 0)),
        "playcount": 0,
        "is_valid": bool(event.get("name")),
        "transformed_at": datetime.utcnow().isoformat() + "Z",
    }


def transform_tag_event(event: dict) -> dict:
    """Normalise un événement raw-tags → dimension tags."""
    name = str(event.get("name", "")).strip().lower()
    return {
        "event_type": "silver_tag",
        "run_date": event.get("run_date"),
        "rank": event.get("rank", 0),
        "name": name,
        "category": str(event.get("category", "Other")).strip() or "Other",
        "reach": max(0, int(event.get("reach", 0) or 0)),
        "taggings": max(0, int(event.get("taggings", 0) or 0)),
        "is_valid": bool(name),
        "transformed_at": datetime.utcnow().isoformat() + "Z",
    }


# ─── Sink MinIO Silver ────────────────────────────────────────────────────────


def sink_to_minio_silver(events: list, run_date: str, data_type: str) -> None:
    """Écrit un batch d'événements silver en Parquet dans MinIO."""
    if not events:
        return
    df = pd.DataFrame(events)
    buf = io.BytesIO()
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    key = f"processed/date={run_date}/{data_type}.parquet"
    client = get_minio()
    client.put_object(
        Bucket=MINIO_BUCKET,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )
    log.info(f"MinIO Silver ← {len(df)} lignes → {key}")


# ─── Sink PostgreSQL Gold ─────────────────────────────────────────────────────


def sink_artists_to_postgres(events: list) -> int:
    """UPSERT des artistes silver dans dim_artists."""
    if not events:
        return 0
    sql = text(
        """
        INSERT INTO dim_artists (name, country, genre, listeners, playcount, mbid, updated_at)
        VALUES (:name, :country, :genre, :listeners, :playcount, :mbid, NOW())
        ON CONFLICT (name) DO UPDATE SET
            country   = CASE WHEN EXCLUDED.country != '' THEN EXCLUDED.country ELSE dim_artists.country END,
            genre     = CASE WHEN EXCLUDED.genre   != '' THEN EXCLUDED.genre   ELSE dim_artists.genre   END,
            listeners = EXCLUDED.listeners,
            playcount = EXCLUDED.playcount,
            mbid      = CASE WHEN EXCLUDED.mbid    != '' THEN EXCLUDED.mbid    ELSE dim_artists.mbid    END,
            updated_at = NOW()
    """
    )
    valid = [e for e in events if e.get("is_valid")]
    if not valid:
        return 0
    records = [
        {
            "name": e["name"],
            "country": e.get("country", ""),
            "genre": e.get("genre", ""),
            "listeners": e.get("listeners", 0),
            "playcount": e.get("playcount", 0),
            "mbid": e.get("mbid", ""),
        }
        for e in valid
    ]
    engine = get_pg_engine()
    with engine.begin() as conn:
        conn.execute(sql, records)
    log.info(f"PostgreSQL dim_artists ← {len(records)} UPSERT")
    return len(records)


def sink_tracks_to_postgres(events: list) -> int:
    """UPSERT des tracks silver dans dim_tracks."""
    if not events:
        return 0

    valid = [e for e in events if e.get("is_valid")]
    if not valid:
        return 0

    engine = get_pg_engine()

    # Assure l'existence des artistes de référence pour éviter artist_id NULL
    sql_upsert_artists = text(
        """
        INSERT INTO dim_artists (name, updated_at)
        VALUES (:name, NOW())
        ON CONFLICT (name) DO UPDATE SET updated_at = NOW()
    """
    )

    artist_names = sorted(
        {
            e.get("artist_name", "").strip()
            for e in valid
            if e.get("artist_name", "").strip()
        }
    )
    artist_records = [{"name": name} for name in artist_names]

    sql_upsert_tracks = text(
        """
        INSERT INTO dim_tracks (title, artist_id, duration_ms, mbid)
        VALUES (
            :title,
            (SELECT artist_id FROM dim_artists WHERE name = :artist_name LIMIT 1),
            :duration_ms,
            :mbid
        )
        ON CONFLICT (title, artist_id)
        DO UPDATE SET
            duration_ms = EXCLUDED.duration_ms,
            mbid = CASE WHEN EXCLUDED.mbid != '' THEN EXCLUDED.mbid ELSE dim_tracks.mbid END
    """
    )

    track_records = [
        {
            "title": e.get("title", "").strip(),
            "artist_name": e.get("artist_name", "").strip(),
            "duration_ms": e.get("duration_ms", 0),
            "mbid": e.get("mbid", ""),
        }
        for e in valid
        if e.get("title", "").strip() and e.get("artist_name", "").strip()
    ]

    if not track_records:
        return 0

    with engine.begin() as conn:
        if artist_records:
            conn.execute(sql_upsert_artists, artist_records)
        conn.execute(sql_upsert_tracks, track_records)

    log.info(f"PostgreSQL dim_tracks ← {len(track_records)} UPSERT")
    return len(track_records)


def sink_tags_to_postgres(events: list) -> int:
    """UPSERT des tags silver dans dim_tags."""
    if not events:
        return 0

    valid = [e for e in events if e.get("is_valid")]
    if not valid:
        return 0

    sql = text(
        """
        INSERT INTO dim_tags (name, category, created_at)
        VALUES (:name, :category, NOW())
        ON CONFLICT (name)
        DO UPDATE SET category = EXCLUDED.category
    """
    )

    records = [
        {
            "name": e.get("name", "").strip().lower(),
            "category": e.get("category", "Other"),
        }
        for e in valid
        if e.get("name", "").strip()
    ]

    if not records:
        return 0

    engine = get_pg_engine()
    with engine.begin() as conn:
        conn.execute(sql, records)

    log.info(f"PostgreSQL dim_tags ← {len(records)} UPSERT")
    return len(records)


def sink_charts_to_postgres(events: list, run_date: str) -> int:
    """UPSERT des charts silver dans fact_charts."""
    if not events:
        return 0
    sql = text(
        """
        INSERT INTO fact_charts (chart_date, rank, artist_id, region, plays, listeners, source)
        VALUES (
            :chart_date, :rank,
            (SELECT artist_id FROM dim_artists WHERE name = :name LIMIT 1),
            :region, :plays, :listeners, 'lastfm'
        )
        ON CONFLICT (chart_date, rank, region, source)
        DO UPDATE SET
            artist_id = COALESCE(fact_charts.artist_id, EXCLUDED.artist_id),
            plays = EXCLUDED.plays,
            listeners = EXCLUDED.listeners
    """
    )
    valid = [e for e in events if e.get("is_valid")]
    if not valid:
        return 0
    records = [
        {
            "chart_date": e.get("run_date", run_date),
            "rank": e.get("rank", 0),
            "name": e.get("name", ""),
            "region": e.get("region", "global"),
            "plays": e.get("playcount", 0),
            "listeners": e.get("listeners", 0),
        }
        for e in valid
    ]
    engine = get_pg_engine()
    with engine.begin() as conn:
        conn.execute(sql, records)
    log.info(f"PostgreSQL fact_charts ← {len(records)} UPSERT")
    return len(records)


# ─── Agrégations Flink (fenêtres glissantes) ─────────────────────────────────


def compute_window_aggregations(artist_events: list, run_date: str) -> dict:
    """
    Simule des agrégations par fenêtre temporelle (5 min glissantes).
    En production avec PyFlink natif, utiliser TumblingEventTimeWindows.
    """
    if not artist_events:
        return {}

    df = pd.DataFrame(artist_events)

    # Top 10 artistes par plays dans la fenêtre
    top10 = df.nlargest(10, "playcount")[
        ["name", "playcount", "listeners", "genre"]
    ].to_dict(orient="records")

    # Agrégation par genre
    genre_agg = {}
    if "genre" in df.columns:
        genre_agg = (
            df.groupby("genre")["playcount"]
            .sum()
            .sort_values(ascending=False)
            .head(5)
            .to_dict()
        )

    # Agrégation par pays
    geo_agg = {}
    if "country" in df.columns:
        country_df = df[df["country"] != ""]
        if not country_df.empty:
            geo_agg = (
                country_df.groupby("country")["listeners"]
                .sum()
                .sort_values(ascending=False)
                .head(10)
                .to_dict()
            )

    kpi = {
        "window_type": "tumbling_5min",
        "run_date": run_date,
        "computed_at": datetime.utcnow().isoformat() + "Z",
        "total_events": len(df),
        "unique_artists": df["name"].nunique(),
        "total_plays": int(df["playcount"].sum()),
        "total_listeners": int(df["listeners"].sum()),
        "avg_plays": round(float(df["playcount"].mean()), 2),
        "top10_artists": top10,
        "plays_by_genre": genre_agg,
        "listeners_by_country": {str(k): int(v) for k, v in geo_agg.items()},
    }

    log.info(
        f"[Flink Window] KPIs calculés : {len(df)} événements, "
        f"{kpi['unique_artists']} artistes uniques, "
        f"{kpi['total_plays']:,} plays"
    )
    return kpi


def sink_kpis_to_kafka(producer: Producer, kpi: dict) -> None:
    """Publie les KPIs calculés par Flink dans le topic gold-kpis."""
    if not kpi:
        return
    producer.produce(
        topic="gold-kpis",
        key=kpi.get("run_date", "").encode("utf-8"),
        value=json.dumps(kpi, ensure_ascii=False, default=str).encode("utf-8"),
    )
    producer.flush()
    log.info("KPIs publiés → gold-kpis")


def sink_kpis_to_postgres(kpi: dict) -> None:
    """Persiste les KPIs agrégés dans PostgreSQL."""
    if not kpi:
        return
    sql = text(
        """
        INSERT INTO fact_kpi_windows
            (run_date, window_type, total_events, unique_artists,
             total_plays, total_listeners, avg_plays,
             top10_json, genre_json, geo_json, computed_at)
        VALUES
            (:run_date, :window_type, :total_events, :unique_artists,
             :total_plays, :total_listeners, :avg_plays,
             :top10_json, :genre_json, :geo_json, NOW())
        ON CONFLICT (run_date, window_type)
        DO UPDATE SET
            total_events    = EXCLUDED.total_events,
            unique_artists  = EXCLUDED.unique_artists,
            total_plays     = EXCLUDED.total_plays,
            total_listeners = EXCLUDED.total_listeners,
            top10_json      = EXCLUDED.top10_json,
            genre_json      = EXCLUDED.genre_json,
            geo_json        = EXCLUDED.geo_json,
            computed_at     = NOW()
    """
    )
    record = {
        "run_date": kpi["run_date"],
        "window_type": kpi["window_type"],
        "total_events": kpi["total_events"],
        "unique_artists": kpi["unique_artists"],
        "total_plays": kpi["total_plays"],
        "total_listeners": kpi["total_listeners"],
        "avg_plays": kpi["avg_plays"],
        "top10_json": json.dumps(kpi["top10_artists"]),
        "genre_json": json.dumps(kpi["plays_by_genre"]),
        "geo_json": json.dumps(kpi["listeners_by_country"]),
    }
    engine = get_pg_engine()
    with engine.begin() as conn:
        conn.execute(sql, record)
    log.info("KPIs persistés → fact_kpi_windows")


# ─── Job principal ────────────────────────────────────────────────────────────


def run_flink_transform_job(run_date: str, timeout_s: int = 120) -> dict:
    """
    Job Flink de transformation (Silver).
    Consomme raw-artists, raw-tracks, raw-geo depuis Kafka.
    Produit : silver-artists, silver-charts dans Kafka + MinIO Silver.
    """
    log.info(f"=== Flink Transform Job démarré — {run_date} ===")

    consumer = get_consumer(
        "flink-transform-music", ["raw-artists", "raw-tracks", "raw-geo", "raw-tags"]
    )
    kf_prod = get_producer()

    artist_events, track_events, geo_events, tag_events = [], [], [], []
    deadline = datetime.utcnow() + timedelta(seconds=timeout_s)

    while datetime.utcnow() < deadline:
        msg = consumer.poll(timeout=2.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            log.error(f"Kafka error: {msg.error()}")
            continue

        event = json.loads(msg.value().decode("utf-8"))
        topic = msg.topic()

        if topic == "raw-artists":
            silver = transform_artist_event(event)
            artist_events.append(silver)
            kf_prod.produce(
                "silver-artists",
                key=f"{run_date}_{silver['rank']}".encode(),
                value=json.dumps(silver).encode(),
            )

        elif topic == "raw-tracks":
            silver = transform_track_event(event)
            track_events.append(silver)
            kf_prod.produce(
                "silver-charts",
                key=f"{run_date}_{silver['rank']}".encode(),
                value=json.dumps(silver).encode(),
            )

        elif topic == "raw-geo":
            silver = transform_geo_event(event)
            geo_events.append(silver)
            kf_prod.produce(
                "silver-charts",
                key=f"{run_date}_{silver['region']}_{silver['rank']}".encode(),
                value=json.dumps(silver).encode(),
            )

        elif topic == "raw-tags":
            silver = transform_tag_event(event)
            tag_events.append(silver)

        consumer.commit(msg)

    kf_prod.flush()
    consumer.close()

    # Sink MinIO Silver
    sink_to_minio_silver(artist_events, run_date, "dim_artists")
    sink_to_minio_silver(track_events, run_date, "dim_tracks")
    sink_to_minio_silver(geo_events, run_date, "fact_charts_geo")
    sink_to_minio_silver(tag_events, run_date, "dim_tags")

    # Sink PostgreSQL
    n_artists = sink_artists_to_postgres(artist_events + geo_events)
    n_tracks = sink_tracks_to_postgres(track_events)
    n_tags = sink_tags_to_postgres(tag_events)
    n_charts = sink_charts_to_postgres(artist_events + geo_events, run_date)

    summary = {
        "job": "flink_transform",
        "run_date": run_date,
        "artist_events": len(artist_events),
        "track_events": len(track_events),
        "geo_events": len(geo_events),
        "tag_events": len(tag_events),
        "postgres_artists": n_artists,
        "postgres_tracks": n_tracks,
        "postgres_tags": n_tags,
        "postgres_charts": n_charts,
    }
    log.info(f"=== Flink Transform Job terminé — {summary} ===")
    return summary


def run_flink_aggregation_job(run_date: str, timeout_s: int = 60) -> dict:
    """
    Job Flink d'agrégation (Gold).
    Consomme silver-artists depuis Kafka.
    Calcule des KPIs par fenêtre → gold-kpis (Kafka) + PostgreSQL.
    """
    log.info(f"=== Flink Aggregation Job démarré — {run_date} ===")

    consumer = get_consumer("flink-agg-music", ["silver-artists"])
    producer = get_producer()

    collected = []
    deadline = datetime.utcnow() + timedelta(seconds=timeout_s)

    while datetime.utcnow() < deadline:
        msg = consumer.poll(timeout=2.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            continue
        event = json.loads(msg.value().decode("utf-8"))
        collected.append(event)
        consumer.commit(msg)

    consumer.close()

    kpi = compute_window_aggregations(collected, run_date)
    if kpi:
        sink_kpis_to_kafka(producer, kpi)
        sink_kpis_to_postgres(kpi)

    summary = {
        "job": "flink_aggregation",
        "run_date": run_date,
        "events_processed": len(collected),
        "kpis_computed": bool(kpi),
    }
    log.info(f"=== Flink Aggregation Job terminé — {summary} ===")
    return summary


if __name__ == "__main__":
    import sys
    from datetime import date

    run_date = sys.argv[1] if len(sys.argv) > 1 else date.today().strftime("%Y-%m-%d")
    s1 = run_flink_transform_job(run_date)
    s2 = run_flink_aggregation_job(run_date)
    print("\n--- Résumé Flink ---")
    for k, v in {**s1, **s2}.items():
        print(f"  {k}: {v}")
