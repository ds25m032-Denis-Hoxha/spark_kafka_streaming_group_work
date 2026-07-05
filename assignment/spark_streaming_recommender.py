import json
import os
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict

import psycopg2
from pyspark.sql.functions import round
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    when,
    count,
    sum as spark_sum,
    avg,
    desc,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)


TOPIC_NAME = "springer_hoxha_music_events"
CONFIG_FILE = Path(__file__).with_name("kafka.config")

# --- Postgres connection details (read-only: track/artist/album catalog) ---
DB_HOST = "fhtw-big-data.postgres.database.azure.com"
DB_NAME = "music_store"
DB_USER = "student"
DB_PASSWORD = "reRZ2pjg1WxqlwjU"

# --- Shared file both this script and streamlit_app.py read/write ---
# Must live in the same directory as streamlit_app.py so both processes
# agree on its location.
RECOMMENDATIONS_FILE = Path(__file__).with_name("recommendations.json")

ACTION_SCORES = {"like": 3, "play": 1, "skip": -1, "dislike": -3}

# --- In-memory per-user state, rebuilt from Kafka history on restart ---
user_interaction_counts = defaultdict(int)
user_artist_scores = defaultdict(lambda: defaultdict(float))
user_seen_tracks = defaultdict(set)

# --- In-memory recommendations dict, persisted to RECOMMENDATIONS_FILE ---
recommendations_by_user = {}


def load_recommendations_file():
    if RECOMMENDATIONS_FILE.exists():
        try:
            with open(RECOMMENDATIONS_FILE, "r") as f:
                recommendations_by_user.update(json.load(f))
        except (json.JSONDecodeError, OSError):
            pass  # start fresh if the file is missing or corrupt


def write_recommendations_file():
    """Atomic write: write to a temp file, then rename over the real file,
    so streamlit_app.py never reads a half-written file."""
    tmp_path = RECOMMENDATIONS_FILE.with_suffix(".json.tmp")
    with open(tmp_path, "w") as f:
        json.dump(recommendations_by_user, f, indent=2)
    os.replace(tmp_path, RECOMMENDATIONS_FILE)


def read_kafka_config(config_file):
    config = {}

    with open(config_file, "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                key, value = line.split("=", 1)
                config[key.strip()] = value.strip()

    return config


def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )


def pick_recommendation(conn, user_id):
    """Pick the highest-priced (revenue-maximizing) track by the user's
    top-preferred artist that they haven't already interacted with."""
    artist_scores = user_artist_scores[user_id]
    if not artist_scores:
        return None

    for artist_name, score in sorted(
        artist_scores.items(), key=lambda kv: kv[1], reverse=True
    ):
        if score <= 0:
            break  # no more positively-preferred artists left

        with conn.cursor() as cur:
            cur.execute("""
                SELECT t.id, t.name, a.name, al.title, t.unit_price
                FROM public.tracks t
                JOIN public.albums al ON t.album_id = al.id
                JOIN public.artists a ON al.artist_id = a.id
                WHERE a.name = %s
                ORDER BY t.unit_price DESC
            """, (artist_name,))
            rows = cur.fetchall()

        for track_id, track_name, artist, album_title, unit_price in rows:
            if track_id not in user_seen_tracks[user_id]:
                return {
                    "track_id": track_id,
                    "track_name": track_name,
                    "artist_name": artist,
                    "album_title": album_title,
                    "unit_price": float(unit_price) if unit_price is not None else 0.0,
                    "score": score,
                }
    return None


def save_recommendation(user_id, rec):
    recommendations_by_user[user_id] = {
        **rec,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    write_recommendations_file()


def process_batch(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return

    conn = get_db_connection()
    try:
        for row in rows:
            user_id = row["user_id"]
            action_score = ACTION_SCORES.get(row["action_type"], 0)

            user_interaction_counts[user_id] += 1
            user_artist_scores[user_id][row["artist_name"]] += action_score
            user_seen_tracks[user_id].add(row["track_id"])

            if user_interaction_counts[user_id] >= 10:
                rec = pick_recommendation(conn, user_id)
                if rec:
                    save_recommendation(user_id, rec)
                    print(
                        f"[batch {batch_id}] Recommendation for {user_id}: "
                        f"{rec['track_name']} by {rec['artist_name']} "
                        f"(${rec['unit_price']:.2f})"
                    )
    finally:
        conn.close()


def main():
    kafka_config = read_kafka_config(CONFIG_FILE)

    spark = (
        SparkSession.builder
        .appName("MusicStreamingRecommender")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    load_recommendations_file()

    event_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("track_id", IntegerType(), True),
        StructField("track_name", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("album_title", StringType(), True),
        StructField("track_length_ms", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("action_type", StringType(), True),
        StructField("timestamp", StringType(), True),
    ])

    raw_events = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_config["bootstrap.servers"])
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .load()
    )

    events = (
        raw_events
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), event_schema).alias("event"))
        .select("event.*")
    )

    scored_events = events.withColumn(
        "action_score",
        when(col("action_type") == "like", 3)
        .when(col("action_type") == "play", 1)
        .when(col("action_type") == "skip", -1)
        .when(col("action_type") == "dislike", -3)
        .otherwise(0)
    )

    user_interactions = (
        scored_events
        .groupBy("user_id")
        .agg(
            count("*").alias("interaction_count"),
            spark_sum("action_score").alias("total_preference_score"),
        )
        .filter(col("interaction_count") >= 10)
    )

    artist_preferences = (
        scored_events
        .groupBy("user_id", "artist_name")
        .agg(
            count("*").alias("artist_interactions"),
            spark_sum("action_score").alias("artist_score"),
            round(avg("unit_price"), 2).alias("avg_price"),
        )
        .filter(col("artist_score") > 0)
        .orderBy(desc("artist_score"))
    )

    track_popularity = (
        scored_events
        .groupBy("track_id", "track_name", "artist_name", "album_title")
        .agg(
            count("*").alias("total_interactions"),
            spark_sum("action_score").alias("popularity_score"),
            round(avg("unit_price"), 2).alias("avg_price"),
        )
        .withColumn(
            "revenue_score",
            col("popularity_score") * col("avg_price")
        )
        .filter(col("popularity_score") > 0)
        .orderBy(desc("revenue_score"))
    )

    query_user = (
        user_interactions.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", False)
        .queryName("user_interactions")
        .start()
    )

    query_artist = (
        artist_preferences.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", False)
        .queryName("artist_preferences")
        .start()
    )

    query_tracks = (
        track_popularity.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", False)
        .queryName("track_popularity_revenue")
        .start()
    )

    # Row-level stream that maintains per-user state and writes
    # recommendations to a shared JSON file once a user has 10+ actions.
    query_recommendations = (
        scored_events.writeStream
        .outputMode("append")
        .foreachBatch(process_batch)
        .queryName("recommendations")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()