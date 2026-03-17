-- =============================================================================
-- Schéma Star Schema v2 — music_trends_db
-- Pipeline ETL + Streaming (Last.fm · Kafka · Flink)
-- =============================================================================

CREATE TABLE IF NOT EXISTS dim_artists (
    artist_id   SERIAL       PRIMARY KEY,
    name        VARCHAR(255) NOT NULL UNIQUE,
    country     VARCHAR(10),
    genre       VARCHAR(100),
    listeners   BIGINT       DEFAULT 0,
    playcount   BIGINT       DEFAULT 0,
    bio         TEXT,
    mbid        VARCHAR(36),
    created_at  TIMESTAMP    DEFAULT NOW(),
    updated_at  TIMESTAMP    DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_tracks (
    track_id    SERIAL       PRIMARY KEY,
    title       VARCHAR(255) NOT NULL,
    artist_id   INTEGER      REFERENCES dim_artists(artist_id),
    duration_ms INTEGER,
    mbid        VARCHAR(36),
    created_at  TIMESTAMP    DEFAULT NOW(),
    UNIQUE(title, artist_id)
);

CREATE TABLE IF NOT EXISTS dim_tags (
    tag_id      SERIAL       PRIMARY KEY,
    name        VARCHAR(100) NOT NULL UNIQUE,
    category    VARCHAR(100),
    created_at  TIMESTAMP    DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fact_charts (
    chart_id    SERIAL       PRIMARY KEY,
    chart_date  DATE         NOT NULL,
    rank        INTEGER      NOT NULL,
    artist_id   INTEGER      REFERENCES dim_artists(artist_id),
    track_id    INTEGER      REFERENCES dim_tracks(track_id),
    region      VARCHAR(100) DEFAULT 'global',
    plays       BIGINT       DEFAULT 0,
    listeners   BIGINT       DEFAULT 0,
    source      VARCHAR(50)  DEFAULT 'lastfm',
    created_at  TIMESTAMP    DEFAULT NOW(),
    UNIQUE(chart_date, rank, region, source)
);

-- Table Flink : KPIs calculés par fenêtre temporelle
CREATE TABLE IF NOT EXISTS fact_kpi_windows (
    kpi_id          SERIAL       PRIMARY KEY,
    run_date        DATE         NOT NULL,
    window_type     VARCHAR(50)  NOT NULL DEFAULT 'tumbling_5min',
    total_events    INTEGER      DEFAULT 0,
    unique_artists  INTEGER      DEFAULT 0,
    total_plays     BIGINT       DEFAULT 0,
    total_listeners BIGINT       DEFAULT 0,
    avg_plays       NUMERIC(12,2),
    top10_json      JSONB,
    genre_json      JSONB,
    geo_json        JSONB,
    computed_at     TIMESTAMP    DEFAULT NOW(),
    UNIQUE(run_date, window_type)
);

CREATE INDEX IF NOT EXISTS idx_fact_charts_date     ON fact_charts(chart_date);
CREATE INDEX IF NOT EXISTS idx_fact_charts_artist   ON fact_charts(artist_id);
CREATE INDEX IF NOT EXISTS idx_fact_charts_region   ON fact_charts(region);
CREATE INDEX IF NOT EXISTS idx_dim_artists_name     ON dim_artists(name);
CREATE INDEX IF NOT EXISTS idx_dim_artists_country  ON dim_artists(country);
CREATE INDEX IF NOT EXISTS idx_fact_kpi_run_date    ON fact_kpi_windows(run_date);

-- Vues analytiques
CREATE OR REPLACE VIEW v_top_artists_global AS
SELECT a.name, a.country, a.genre,
       SUM(f.plays) AS total_plays, SUM(f.listeners) AS total_listeners,
       MIN(f.rank) AS best_rank, AVG(f.rank)::NUMERIC(6,1) AS avg_rank
FROM fact_charts f JOIN dim_artists a ON a.artist_id = f.artist_id
WHERE f.region = 'global'
GROUP BY a.name, a.country, a.genre
ORDER BY total_plays DESC;

CREATE OR REPLACE VIEW v_latest_kpis AS
SELECT * FROM fact_kpi_windows
ORDER BY run_date DESC, computed_at DESC LIMIT 1;

COMMENT ON TABLE fact_kpi_windows IS 'KPIs calculés par Flink (fenêtres glissantes)';
