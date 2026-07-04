from pathlib import Path

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


def read_kafka_config(config_file):
    config = {}

    with open(config_file, "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                key, value = line.split("=", 1)
                config[key.strip()] = value.strip()

    return config


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

    album_preferences = (
        scored_events
        .groupBy("user_id", "album_title")
        .agg(
            count("*").alias("album_interactions"),
            spark_sum("action_score").alias("album_score"),
            round(avg("unit_price"), 2).alias("avg_price"),
        )
        .filter(col("album_score") > 0)
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

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()