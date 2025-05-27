import streamlit as st
import psycopg2
import random

# Database connection details
HOST = "fhtw-big-data.postgres.database.azure.com"
DATABASE = "music_store"
USER = "student"
PASSWORD = "reRZ2pjg1WxqlwjU"

# Establish a connection to the database
@st.cache_resource
def get_connection():
    conn = psycopg2.connect(
        host=HOST,
        dbname=DATABASE,
        user=USER,
        password=PASSWORD
    )
    return conn

def get_random_track(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT t.name, a.name 
            FROM public.tracks t 
            JOIN public.albums al ON t.album_id = al.id
            JOIN public.artists a ON al.artist_id = a.id
            ORDER BY random()
            LIMIT 1;
        """)
        track = cur.fetchone()
    return track


# Streamlit UI
def main():
    conn = get_connection()
    track = None
    st.title("Track Recommender")
    track = get_random_track(conn)
    if track:
        st.header(f"Track: {track[0]}")
        st.subheader(f"Artist: {track[1]}")
        
        if st.button("Thumbs Up"):
            st.success("You liked the track!")
        if st.button("Thumbs Down"):
            st.error("You disliked the track!")
        if st.button("Play"):
            st.info("Playing the track!")

if __name__ == "__main__":
    main()