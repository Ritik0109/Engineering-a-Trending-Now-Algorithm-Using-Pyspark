# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

# -----------------------------------
# Load configuration settings
# -----------------------------------

# Initialize configuration object
config = Config()

# Extract commonly used configuration values
raw_path = config.raw_path
catalog = config.catalog
db_name = config.db_name

# COMMAND ----------

# MAGIC %md
# MAGIC Defining Base Dataset for analysis

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# -----------------------------------
# Read aggregated listening events
# -----------------------------------
df = (
    spark.read
    .table(f"{catalog}.{db_name}.listening_events_agg")
)

# -----------------------------------
# Create a time-series aggregation
# -----------------------------------
# Group by date and podcast metadata, then count listens per group
df_timeseries = (
    df.groupBy(
        "listen_timestamp",
        "podcast_id",
        "podcast_title",
        "category"
    )
    .agg(
        count(col("user_id")).alias("listen_count")
    )
)

# COMMAND ----------

# -----------------------------------
# Define window specifications
# -----------------------------------

# Window partitioned by podcast and ordered by date
podcast_window = (
    Window
    .partitionBy(col("podcast_id"))
    .orderBy(col("listen_timestamp"))
)

# Rolling 7-day window (current row + previous 6 rows)
rolling_window = podcast_window.rowsBetween(-6, 0)

# -----------------------------------
# Create rolling features
# -----------------------------------
features_df = (
    df_timeseries
    # Average listen count over the rolling 7-day window
    .withColumn(
        "popularity_score",
        avg("listen_count").over(rolling_window)
    )
    # Listen count from 7 days ago (default to 0 if missing)
    .withColumn(
        "listens_7_days_ago",
        lag("listen_count", 7, 0).over(podcast_window)
    )
)

# -----------------------------------
# Calculate momentum score
# -----------------------------------
features_df = (
    features_df
    .withColumn(
        "momentum_score",
        when(col("listens_7_days_ago") == 0, lit(1.5))
        .otherwise(
            100 * (col("listen_count") / col("listens_7_days_ago") - 1)
        )
    )
)

# -----------------------------------
# Calculate final trending score
# -----------------------------------
features_df = (
    features_df
    .withColumn(
        "trending_score",
        col("popularity_score") * 0.4 + col("momentum_score") * 0.6
    )
)

# -----------------------------------
# Get most recent date in the dataset
# -----------------------------------
max_date = (
    features_df
    .agg(max("listen_timestamp"))
    .collect()[0][0]
)

# -----------------------------------
# Filter to trending podcasts for the latest date
# -----------------------------------
trending_now = (
    features_df
    .where(col("listen_timestamp") == max_date)
    .orderBy(desc("trending_score"))
)

# -----------------------------------
# Rank podcasts within each category
# -----------------------------------
category_window = (
    Window
    .partitionBy(col("category"))
    .orderBy(col("trending_score").desc())
)

trending_per_category = (
    trending_now
    .withColumn("rank", rank().over(category_window))
    .filter("rank <= 5")
    .select(
        "category",
        "rank",
        "podcast_title",
        "trending_score"
    )
    .orderBy("category", "rank")
)

# -----------------------------------
# Display results
# -----------------------------------
display(trending_per_category.toPandas())


# COMMAND ----------

# ----------------------------------------------------
# Output saved at raw_path in ADLS location
# for extraction / external use
# ----------------------------------------------------

# Write overall trending podcasts (latest date)
trending_now.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{raw_path}/output/trending_now/")

# Write top trending podcasts per category
trending_per_category.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{raw_path}/output/trending_per_category/")

# Log output location
print(f"Files outputted at location {raw_path}/output/")