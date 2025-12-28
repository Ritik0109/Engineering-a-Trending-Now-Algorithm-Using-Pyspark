# Databricks notebook source
class Config:
    """
    Configuration class for managing shared application settings
    such as storage locations and database identifiers.
    """

    def __init__(self):
        # Retrieve raw data path from the external location metadata
        self.raw_path = (
            spark.sql("Describe external location `soundwave-landing`")
            .collect()[0][1]
        )

        # Define catalog and database (schema) names
        self.catalog = "soundWave_dev"
        self.db_name = "events"