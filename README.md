# Pipeline ETL + Streaming — Tendances Musicales v2
**Kafka + Flink + Airflow + MinIO + PostgreSQL + Metabase**

## Architecture

```
[Last.fm API] + [MusicBrainz API]
        │
        ▼  kafka_producer.py (Airflow Task 2)
┌───────────────────────────────────────────┐
│  KAFKA TOPICS (source + bus inter-couches) │
│  raw-artists │ raw-tracks │ raw-tags │ raw-geo │
└───────────────────────────────────────────┘
        │
        ▼  flink_job.py — FlinkTransformJob (Airflow Task 3)
┌──────────────────────────────────────────────┐
│  KAFKA TOPICS Silver                          │
│  silver-artists │ silver-charts               │
│  + MinIO /processed/ (Parquet)               │
└──────────────────────────────────────────────┘
        │
        ▼  flink_job.py — FlinkAggregationJob (Airflow Task 4)
┌──────────────────────────────────────────────┐
│  KAFKA TOPIC gold-kpis                        │
│  + PostgreSQL Gold (Star Schema + KPI Windows)│
└──────────────────────────────────────────────┘
        │
        ▼
  [Metabase Dashboards]

  Orchestration globale : Apache Airflow DAG @daily
```

## Services Docker (12 au total)

| Service              | Image                          | Port(s)    | Rôle                          |
|----------------------|--------------------------------|------------|-------------------------------|
| zookeeper            | confluentinc/cp-zookeeper:7.5  | —          | Coordination Kafka            |
| kafka                | confluentinc/cp-kafka:7.5      | 9092/29092 | Broker de messages            |
| kafka-ui             | provectuslabs/kafka-ui         | 8090       | Monitoring des topics         |
| kafka-init           | confluentinc/cp-kafka:7.5      | —          | Création des topics           |
| flink-jobmanager     | flink:1.18                     | 8081       | Gestion des jobs Flink        |
| flink-taskmanager    | flink:1.18                     | —          | Exécution des tâches Flink    |
| minio                | minio/minio                    | 9000/9001  | Data Lake Bronze/Silver       |
| postgres-airflow     | postgres:15                    | 5433       | Métabase Airflow              |
| redis                | redis:7                        | 6379       | Broker Celery Airflow         |
| airflow-webserver    | apache/airflow:2.8             | 8080       | UI Airflow                    |
| postgres-gold        | postgres:15                    | 5432       | Entrepôt Gold                 |
| metabase             | metabase/metabase              | 3000       | Dashboards                    |

## Topics Kafka

| Topic           | Producteur        | Consommateur      | Contenu                          |
|-----------------|-------------------|-------------------|----------------------------------|
| raw-artists     | kafka_producer.py | flink_job.py      | Artistes bruts Last.fm + MB      |
| raw-tracks      | kafka_producer.py | flink_job.py      | Titres bruts Last.fm             |
| raw-tags        | kafka_producer.py | flink_job.py      | Tags/genres bruts                |
| raw-geo         | kafka_producer.py | flink_job.py      | Classements par pays             |
| silver-artists  | flink_job.py      | flink_job.py (agg)| Artistes normalisés              |
| silver-charts   | flink_job.py      | Consommateurs ext.| Charts normalisés                |
| gold-kpis       | flink_job.py      | Metabase / ext.   | KPIs agrégés par fenêtre         |

## Démarrage

```bash
cp .env.example .env
# Renseigner LASTFM_API_KEY

docker compose up -d
# Attendre ~3 minutes

# Déclencher le DAG
# Airflow UI → http://localhost:8080 → music_trends_etl_v2_dag → ▶ Trigger
```

## Interfaces

| Interface        | URL                       | Identifiants              |
|------------------|---------------------------|---------------------------|
| Airflow          | http://localhost:8080     | airflow / airflow         |
| Kafka UI         | http://localhost:8090     | (pas d'auth)              |
| Flink Dashboard  | http://localhost:8081     | (pas d'auth)              |
| MinIO Console    | http://localhost:9001     | minioadmin / minioadmin   |
| Metabase         | http://localhost:3000     | À configurer              |

## Connexion Metabase → PostgreSQL Gold
- Host : `postgres-gold` | Port : `5432`
- Database : `music_trends_db` | User : `gold` | Password : `gold123`

## Structure du projet

```
music_etl_v2/
├── docker-compose.yml
├── .env.example
├── requirements.txt
├── README.md
├── dags/
│   └── music_trends_etl_dag.py   ← DAG v2 (5 tâches)
├── scripts/
│   ├── kafka_producer.py          ← Last.fm → Kafka raw-*
│   ├── flink_job.py               ← Transform + Aggregation
│   └── init_schema.sql            ← Star Schema + fact_kpi_windows
├── flink-jobs/                    ← JARs Flink (si jobs Java/Scala)
└── logs/
```
