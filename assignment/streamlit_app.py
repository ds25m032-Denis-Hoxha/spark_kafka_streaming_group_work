from pathlib import Path
from datetime import datetime, timezone
import json

import psycopg2
import streamlit as st
from confluent_kafka import Producer

from utils import ccloud_lib


# Database connection details
HOST = "fhtw-big-data.postgres.database.azure.com"
DATABASE = "music_store"
USER = "student"
PASSWORD = "reRZ2pjg1WxqlwjU"

# Kafka details
CONFIG_FILE = Path(__file__).with_name("kafka.config")
TOPIC_NAME = "springer_hoxha_music_events"

# Shared file the Spark job writes recommendations to.
# Must live in the same directory as spark_streaming_recommender.py.
RECOMMENDATIONS_FILE = Path(__file__).with_name("recommendations.json")


@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host=HOST,
        dbname=DATABASE,
        user=USER,
        password=PASSWORD,
    )


@st.cache_resource
def get_kafka_config():
    return ccloud_lib.read_ccloud_config(str(CONFIG_FILE))


@st.cache_resource
def get_kafka_producer():
    kafka_config = get_kafka_config()
    producer_conf = ccloud_lib.pop_schema_registry_params_from_config(
        kafka_config.copy()
    )
    return Producer(producer_conf)


def delivery_report(err, msg):
    if err is not None:
        st.error(f"Kafka delivery failed: {err}")
    else:
        print(
            f"Produced event to {msg.topic()} "
            f"partition [{msg.partition()}] @ offset {msg.offset()}"
        )


def get_random_track(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                t.id AS track_id,
                t.name AS track_name,
                a.name AS artist_name,
                al.title AS album_title,
                t.milliseconds AS track_length_ms,
                t.unit_price AS unit_price
            FROM public.tracks t
            JOIN public.albums al ON t.album_id = al.id
            JOIN public.artists a ON al.artist_id = a.id
            ORDER BY random()
            LIMIT 1;
        """)
        row = cur.fetchone()

    if row is None:
        return None

    return {
        "track_id": row[0],
        "track_name": row[1],
        "artist_name": row[2],
        "album_title": row[3],
        "track_length_ms": row[4],
        "unit_price": float(row[5]) if row[5] is not None else None,
    }


def get_recommendation(user_id):
    """Read the latest recommendation the Spark job wrote for this user
    from the shared JSON file."""
    if not RECOMMENDATIONS_FILE.exists():
        return None

    try:
        with open(RECOMMENDATIONS_FILE, "r") as f:
            all_recs = json.load(f)
    except (json.JSONDecodeError, OSError):
        # File may be mid-write (rare, since writes are atomic) or missing;
        # just skip this render, it'll show up on the next rerun.
        return None

    return all_recs.get(user_id)


def create_event(user_id, track, action_type):
    return {
        "user_id": user_id,
        "track_id": track["track_id"],
        "track_name": track["track_name"],
        "artist_name": track["artist_name"],
        "album_title": track["album_title"],
        "track_length_ms": track["track_length_ms"],
        "unit_price": track["unit_price"],
        "action_type": action_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def send_event(producer, user_id, track, action_type):
    event = create_event(user_id, track, action_type)

    producer.produce(
        TOPIC_NAME,
        key=str(user_id),
        value=json.dumps(event),
        on_delivery=delivery_report,
    )
    producer.poll(0)

    return event


def load_new_track(conn):
    st.session_state.current_track = get_random_track(conn)


def main():
    st.title("Track Recommender")

    conn = get_connection()
    producer = get_kafka_producer()

    user_id = st.text_input("User ID", value="user_1")

    if "current_track" not in st.session_state:
        load_new_track(conn)

    track = st.session_state.current_track

    if track:
        st.header(f"Track: {track['track_name']}")
        st.subheader(f"Artist: {track['artist_name']}")
        st.caption(f"Album: {track['album_title']}")

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            if st.button("Play"):
                send_event(producer, user_id, track, "play")
                st.info("Play event sent to Kafka.")

        with col2:
            if st.button("Like"):
                send_event(producer, user_id, track, "like")
                st.success("Like event sent to Kafka.")

        with col3:
            if st.button("Dislike"):
                send_event(producer, user_id, track, "dislike")
                st.error("Dislike event sent to Kafka.")

        with col4:
            if st.button("Skip"):
                send_event(producer, user_id, track, "skip")
                st.warning("Skip event sent to Kafka.")
                load_new_track(conn)
                st.rerun()

        if st.button("Next Random Track"):
            load_new_track(conn)
            st.rerun()

        st.divider()
        st.caption(f"Kafka topic: `{TOPIC_NAME}`")

    else:
        st.error("No track found in the database.")

    st.divider()
    st.subheader("🎧 Your Recommendation")

    recommendation = get_recommendation(user_id)
    if recommendation:
        price = recommendation.get("unit_price")
        price_str = f"${price:.2f}" if price is not None else "N/A"
        st.success(
            f"**{recommendation['track_name']}** by {recommendation['artist_name']}  \n"
            f"Album: {recommendation['album_title']} · Price: {price_str}"
        )
        st.caption(f"Generated at {recommendation['generated_at']}")
    else:
        st.info(
            "Keep interacting (10+ Play/Like/Dislike/Skip actions) to unlock "
            "a personalized recommendation. Make sure the Spark streaming "
            "job is running."
        )


if __name__ == "__main__":
    main()