"""
kafka_producer.py — Producteur Kafka (Music ETL)
=================================================
Rôle : Appeler Last.fm + MusicBrainz APIs et publier
       les événements bruts dans les topics Kafka (couche SOURCE).

Topics publiés :
  - raw-artists  : top artistes mondiaux
  - raw-tracks   : top titres mondiaux
  - raw-tags     : top tags/genres
  - raw-geo      : classements par pays

Le producteur sert à la fois de SOURCE et de première
couche du BUS DE MESSAGES (Bronze → Kafka → Flink).
"""

import os
import json
import time
import logging
from datetime import date, datetime

import requests
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# ─── Configuration ────────────────────────────────────────────────────────────

LASTFM_API_KEY        = os.environ.get("LASTFM_API_KEY", "")
LASTFM_BASE_URL       = "https://ws.audioscrobbler.com/2.0/"
MB_BASE_URL           = "https://musicbrainz.org/ws/2/"
MB_USER_AGENT         = "MusicTrendsETL/2.0 (contact@example.com)"
KAFKA_BOOTSTRAP       = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TOPICS = [
    "raw-artists", "raw-tracks", "raw-tags",
    "raw-geo", "silver-charts", "silver-artists", "gold-kpis"
]

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# ─── Kafka helpers ────────────────────────────────────────────────────────────

def get_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id":         "music-etl-producer",
        "acks":              "all",
        "retries":           3,
        "linger.ms":         50,
        "batch.size":        16384,
    })


def delivery_report(err, msg):
    if err:
        log.error(f"Erreur livraison Kafka : {err}")
    else:
        log.debug(f"Message livré → {msg.topic()} [partition {msg.partition()}]")


def publish(producer: Producer, topic: str, key: str, value: dict) -> None:
    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=json.dumps(value, ensure_ascii=False, default=str).encode("utf-8"),
        callback=delivery_report,
    )


def ensure_topics(topics: list) -> None:
    """Crée les topics Kafka s'ils n'existent pas."""
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    new_topics = [NewTopic(t, num_partitions=3, replication_factor=1) for t in topics]
    fs = admin.create_topics(new_topics)
    for topic, f in fs.items():
        try:
            f.result()
            log.info(f"Topic '{topic}' créé")
        except Exception as e:
            if "already exists" in str(e).lower():
                log.debug(f"Topic '{topic}' déjà existant")
            else:
                log.warning(f"Erreur création topic '{topic}': {e}")


# ─── Last.fm API ──────────────────────────────────────────────────────────────

def lastfm_get(method: str, extra: dict = {}) -> dict:
    params = {"method": method, "api_key": LASTFM_API_KEY,
              "format": "json", "limit": 50, **extra}
    resp = requests.get(LASTFM_BASE_URL, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def musicbrainz_enrich(artist_name: str) -> dict:
    headers = {"User-Agent": MB_USER_AGENT}
    params  = {"query": f'artist:"{artist_name}"', "limit": 1, "fmt": "json"}
    try:
        resp = requests.get(f"{MB_BASE_URL}artist", params=params,
                            headers=headers, timeout=10)
        resp.raise_for_status()
        artists = resp.json().get("artists", [])
        if not artists:
            return {}
        a = artists[0]
        return {
            "mbid":       a.get("id", ""),
            "country":    a.get("country", ""),
            "type":       a.get("type", ""),
            "area":       a.get("area", {}).get("name", ""),
            "begin_date": a.get("life-span", {}).get("begin", ""),
            "mb_tags":    [t["name"] for t in a.get("tags", [])[:5]],
        }
    except Exception as e:
        log.warning(f"MusicBrainz échoué pour '{artist_name}': {e}")
        return {}
    finally:
        time.sleep(1.1)  # rate limit strict


# ─── Fonctions de production Kafka ────────────────────────────────────────────

def produce_top_artists(producer: Producer, run_date: str) -> int:
    """Last.fm chart.getTopArtists → topic raw-artists."""
    log.info("Producing : top artistes → raw-artists")
    data    = lastfm_get("chart.getTopArtists")
    artists = data.get("artists", {}).get("artist", [])

    for i, artist in enumerate(artists, start=1):
        name = artist.get("name", "")
        # Enrichissement MusicBrainz pour le top 20
        mb_data = musicbrainz_enrich(name) if i <= 20 else {}

        event = {
            "event_type":  "top_artist",
            "run_date":    run_date,
            "rank":        i,
            "region":      "global",
            "name":        name,
            "playcount":   int(artist.get("playcount", 0) or 0),
            "listeners":   int(artist.get("listeners", 0) or 0),
            "mbid_lastfm": artist.get("mbid", ""),
            "produced_at": datetime.utcnow().isoformat() + "Z",
            **mb_data,
        }
        publish(producer, "raw-artists", f"{run_date}_{i}", event)

    producer.flush()
    log.info(f"  → {len(artists)} artistes publiés dans raw-artists")
    return len(artists)


def produce_top_tracks(producer: Producer, run_date: str) -> int:
    """Last.fm chart.getTopTracks → topic raw-tracks."""
    log.info("Producing : top titres → raw-tracks")
    data   = lastfm_get("chart.getTopTracks")
    tracks = data.get("tracks", {}).get("track", [])

    for i, track in enumerate(tracks, start=1):
        artist = track.get("artist", {})
        event = {
            "event_type":   "top_track",
            "run_date":     run_date,
            "rank":         i,
            "region":       "global",
            "title":        track.get("name", ""),
            "artist_name":  artist.get("name", "") if isinstance(artist, dict) else str(artist),
            "playcount":    int(track.get("playcount", 0) or 0),
            "listeners":    int(track.get("listeners", 0) or 0),
            "duration_ms":  int(track.get("duration", 0) or 0),
            "mbid":         track.get("mbid", ""),
            "produced_at":  datetime.utcnow().isoformat() + "Z",
        }
        publish(producer, "raw-tracks", f"{run_date}_{i}", event)

    producer.flush()
    log.info(f"  → {len(tracks)} titres publiés dans raw-tracks")
    return len(tracks)


def produce_top_tags(producer: Producer, run_date: str) -> int:
    """Last.fm chart.getTopTags → topic raw-tags."""
    log.info("Producing : top tags → raw-tags")
    data = lastfm_get("chart.getTopTags")
    tags = data.get("tags", {}).get("tag", [])

    genre_map = {
        "rock": "Rock", "pop": "Pop", "hip-hop": "Hip-Hop", "rap": "Hip-Hop",
        "electronic": "Electronic", "house": "Electronic", "techno": "Electronic",
        "jazz": "Jazz", "classical": "Classical", "r&b": "R&B", "soul": "R&B",
        "metal": "Metal", "indie": "Indie", "country": "Country",
        "reggae": "Reggae", "latin": "Latin", "folk": "Folk",
    }

    for i, tag in enumerate(tags, start=1):
        name = tag.get("name", "").strip().lower()
        event = {
            "event_type":  "top_tag",
            "run_date":    run_date,
            "rank":        i,
            "name":        name,
            "reach":       int(tag.get("reach", 0) or 0),
            "taggings":    int(tag.get("taggings", 0) or 0),
            "category":    genre_map.get(name, "Other"),
            "produced_at": datetime.utcnow().isoformat() + "Z",
        }
        publish(producer, "raw-tags", f"{run_date}_{i}", event)

    producer.flush()
    log.info(f"  → {len(tags)} tags publiés dans raw-tags")
    return len(tags)


def produce_geo_artists(producer: Producer, run_date: str,
                        countries: list = None) -> int:
    """Last.fm geo.getTopArtists → topic raw-geo."""
    if countries is None:
        countries = ["France", "United Kingdom", "United States",
                     "Germany", "Brazil", "Japan", "South Korea", "Senegal"]

    log.info(f"Producing : top artistes par pays → raw-geo ({len(countries)} pays)")
    total = 0

    for country in countries:
        try:
            data    = lastfm_get("geo.getTopArtists", {"country": country, "limit": 20})
            artists = data.get("topartists", {}).get("artist", [])
            for i, artist in enumerate(artists, start=1):
                event = {
                    "event_type":  "geo_artist",
                    "run_date":    run_date,
                    "country":     country,
                    "rank":        i,
                    "name":        artist.get("name", ""),
                    "listeners":   int(artist.get("listeners", 0) or 0),
                    "produced_at": datetime.utcnow().isoformat() + "Z",
                }
                publish(producer, "raw-geo", f"{run_date}_{country}_{i}", event)
            total += len(artists)
            time.sleep(0.3)
        except Exception as e:
            log.warning(f"  geo échoué pour {country}: {e}")

    producer.flush()
    log.info(f"  → {total} événements géo publiés dans raw-geo")
    return total


# ─── Point d'entrée ───────────────────────────────────────────────────────────

def run_producer(run_date: date = None) -> dict:
    """Orchestre la production Kafka complète pour une date donnée."""
    if run_date is None:
        run_date = date.today()
    date_str = run_date.strftime("%Y-%m-%d")

    log.info(f"=== Début production Kafka — {date_str} ===")
    ensure_topics(TOPICS)

    producer = get_producer()

    n_artists = produce_top_artists(producer, date_str)
    n_tracks  = produce_top_tracks(producer, date_str)
    n_tags    = produce_top_tags(producer, date_str)
    n_geo     = produce_geo_artists(producer, date_str)

    summary = {
        "run_date":    date_str,
        "produced_at": datetime.utcnow().isoformat() + "Z",
        "artists":     n_artists,
        "tracks":      n_tracks,
        "tags":        n_tags,
        "geo_events":  n_geo,
        "total_events": n_artists + n_tracks + n_tags + n_geo,
    }
    log.info(f"=== Production Kafka terminée — {summary} ===")
    return summary


if __name__ == "__main__":
    summary = run_producer()
    print("\n--- Résumé production Kafka ---")
    for k, v in summary.items():
        print(f"  {k}: {v}")
