# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *


class IngestData:
    """
    Class responsible for ingesting raw podcast-related data,
    joining datasets, and persisting the aggregated results
    as a Delta table.
    """

    def __init__(self):
        # Initialize configuration and commonly used attributes
        self.config = Config()
        self.catalog = self.config.catalog
        self.db_name = self.config.db_name
        self.raw_path = self.config.raw_path

    def object_creation(self):
        """
        Create catalog and schema if they do not already exist.
        """
        spark.sql(f"create catalog if not exists {self.catalog}")
        spark.sql(f"create schema if not exists {self.catalog}.{self.db_name}")

    def initial_ingest(self):
        """
        Perform the initial ingestion of raw CSV data, join related datasets,
        and write the aggregated result to a Delta table.
        """

        # Ensure required catalog and schema exist
        self.object_creation()

        # -----------------------------
        # Read listening events data
        # -----------------------------
        schema = "user_id string, episode_id string, listen_timestamp string"

        df_listen_events = (
            spark.read.format("csv")
            .option("header", "true")
            .schema(schema)
            .option("inferSchema", "true")
            .option("multiLine", "true")
            .load(f"{self.raw_path}/listening-events/")
        )

        # -----------------------------
        # Read episodes metadata
        # -----------------------------
        schema = (
            "episode_id string, podcast_id string, "
            "episode_title string, duration_seconds int"
        )

        df_episodes = (
            spark.read.format("csv")
            .option("header", "true")
            .schema(schema)
            .option("inferSchema", "true")
            .option("multiLine", "true")
            .load(f"{self.raw_path}/episodes/")
        )

        # -----------------------------
        # Read podcast metadata
        # -----------------------------
        schema = "podcast_id string, podcast_title string, category string"

        df_podcast = (
            spark.read.format("csv")
            .option("header", "true")
            .schema(schema)
            .option("inferSchema", "true")
            .option("multiLine", "true")
            .load(f"{self.raw_path}/podcast/")
        )

        # -----------------------------
        # Join episode and podcast data
        # -----------------------------
        joined_metadata = (
            df_episodes.alias("e")
            .join(df_podcast.alias("p"), on="podcast_id", how="left")
            .select(
                "e.episode_id",
                "e.podcast_id",
                "e.episode_title",
                "e.duration_seconds",
                "p.podcast_title",
                "p.category",
            )
        )

        # -----------------------------
        # Join listening events with metadata
        # -----------------------------
        joined_data = (
            df_listen_events.alias("e")
            .join(joined_metadata.alias("m"), on="episode_id", how="left")
            .drop("m.episode_id")
            .withColumn("listen_timestamp", to_date(col("listen_timestamp")))
            .withColumn(
                "duration_seconds", col("duration_seconds").cast(IntegerType())
            )
        )

        # -----------------------------
        # Write aggregated data to Delta table
        # -----------------------------
        output = (
            joined_data.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(
                f"{self.catalog}.{self.db_name}.listening_events_agg"
            )
        )

        return output


# COMMAND ----------

ingestion_start = IngestData()
ingestion_start.initial_ingest()