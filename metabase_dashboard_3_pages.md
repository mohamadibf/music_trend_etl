# Dashboard Metabase — 3 pages (prêt soutenance)

## Page 1 — Vue Exécutive

Objectif: donner une vision globale des volumes et des leaders.

### 1) KPI — Nombre d'artistes

```sql
SELECT COUNT(*) AS nb_artistes
FROM dim_artists;
```

### 2) KPI — Nombre de tracks

```sql
SELECT COUNT(*) AS nb_tracks
FROM dim_tracks;
```

### 3) KPI — Nombre de tags

```sql
SELECT COUNT(*) AS nb_tags
FROM dim_tags;
```

### 4) KPI — Total plays

```sql
SELECT COALESCE(SUM(plays), 0) AS total_plays
FROM fact_charts;
```

### 5) KPI — Total listeners

```sql
SELECT COALESCE(SUM(listeners), 0) AS total_listeners
FROM fact_charts;
```

### 6) Top 10 artistes (Histogramme)

```sql
SELECT a.name, SUM(f.plays) AS plays, SUM(f.listeners) AS listeners
FROM fact_charts f
JOIN dim_artists a ON a.artist_id = f.artist_id
GROUP BY a.name
ORDER BY plays DESC
LIMIT 10;
```

### 7) Top 10 tracks (Histogramme)

```sql
SELECT t.title, COALESCE(a.name, 'Unknown') AS artist, COUNT(*) AS apparitions
FROM dim_tracks t
LEFT JOIN dim_artists a ON a.artist_id = t.artist_id
GROUP BY t.title, a.name
ORDER BY apparitions DESC, t.title
LIMIT 10;
```

**Script oral (30s):**
"Cette page donne la vue macro de notre pipeline: volumes chargés et classement des artistes/titres les plus performants."

---

## Page 2 — Tendances & Segments

Objectif: montrer la dynamique temporelle et la segmentation métier.

### 1) Tendance journalière plays/listeners (Ligne)

```sql
SELECT chart_date, SUM(plays) AS plays, SUM(listeners) AS listeners
FROM fact_charts
GROUP BY chart_date
ORDER BY chart_date;
```

### 2) Répartition par genre (Camembert)

```sql
SELECT genre, COUNT(*) AS nb_artistes
FROM dim_artists
WHERE genre IS NOT NULL AND genre <> ''
GROUP BY genre
ORDER BY nb_artistes DESC;
```

### 3) Top pays par nombre d'artistes (Histogramme) (pas de résultat)

```sql
SELECT country, COUNT(*) AS nb_artistes
FROM dim_artists
WHERE country IS NOT NULL AND country <> ''
GROUP BY country
ORDER BY nb_artistes DESC
LIMIT 15;
```

### 4) Top régions des charts (Histogramme)

```sql
SELECT region, COUNT(*) AS nb_lignes_charts, SUM(plays) AS plays
FROM fact_charts
GROUP BY region
ORDER BY plays DESC
LIMIT 15;
```

**Script oral (30s):**
"Cette page explique où et comment la musique performe: évolution des plays/listeners, poids des genres, et zones géographiques dominantes."

---

## Page 3 — Qualité & Fiabilité Pipeline

Objectif: prouver la qualité des données et la robustesse ETL.

### 1) Couverture MBID artistes (Camembert ou Jauge)

```sql
SELECT
  SUM(CASE WHEN mbid IS NOT NULL AND mbid <> '' THEN 1 ELSE 0 END) AS avec_mbid,
  SUM(CASE WHEN mbid IS NULL OR mbid = '' THEN 1 ELSE 0 END) AS sans_mbid
FROM dim_artists;
```

### 2) Intégrité dim_tracks (Numérique) (0)

```sql
SELECT COUNT(*) AS tracks_sans_artist_id
FROM dim_tracks
WHERE artist_id IS NULL;
```

### 3) Intégrité fact_charts (Numérique)

```sql
SELECT COUNT(*) AS charts_sans_artist_id
FROM fact_charts
WHERE artist_id IS NULL;
```

### 4) Derniers KPI de fenêtres (Tableau)

```sql
SELECT run_date, window_type, total_events, unique_artists, total_plays, total_listeners, computed_at
FROM fact_kpi_windows
ORDER BY computed_at DESC
LIMIT 10;
```

### 5) Cohérence volumes (Tableau)

```sql
SELECT 'dim_artists' AS table_name, COUNT(*) AS rows FROM dim_artists
UNION ALL
SELECT 'dim_tracks', COUNT(*) FROM dim_tracks
UNION ALL
SELECT 'dim_tags', COUNT(*) FROM dim_tags
UNION ALL
SELECT 'fact_charts', COUNT(*) FROM fact_charts
UNION ALL
SELECT 'fact_kpi_windows', COUNT(*) FROM fact_kpi_windows;
```

**Script oral (30s):**
"Cette page démontre la fiabilité: qualité d'enrichissement, intégrité référentielle et cohérence des volumes alimentés par le pipeline."

---

## Filtres Dashboard à créer (communs)

- chart_date (Date, sur fact_charts.chart_date)
- country (Field filter sur dim_artists.country)
- genre (Field filter sur dim_artists.genre)
- region (Field filter sur fact_charts.region)

## Conseils layout (rapide)

- Page 1: 5 KPI en haut (Numérique), 2 Histogrammes en bas.
- Page 2: 1 Ligne + 1 Camembert + 2 Histogrammes.
- Page 3: 2 KPI qualité (Numérique) + 2 Tableaux + 1 visuel Camembert/Jauge.
