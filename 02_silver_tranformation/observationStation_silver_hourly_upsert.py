# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# --- STAGE 1: Parameters & Setup ---
# Using widgets allows the Job to pass the catalog name dynamically
dbutils.widgets.text("catalog", "weather_oax")
CATALOG = dbutils.widgets.get("catalog")

# Define our table paths
bronze_table_path = f"{CATALOG}.bronze.observation_stations_raw"
silver_table_path = f"{CATALOG}.silver.observations_cleaned"

# Ensure the silver schema exists before we try to write to it
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")

# --- STAGE 2: Transformation Logic ---
# Read the raw data from Bronze
bronze_df = spark.table(bronze_table_path)

# Extract and cast fields from the Variant 'observation_data' column
# We use date_trunc to round DOWN to the start of the hour
silver_updates_df = bronze_df.select(
    F.col("station_id"),

    # Extracting the NWS timestamp and rounding down to the hour
    F.expr("observation_data:properties.timestamp").cast("timestamp").alias("observation_time"),
    F.date_trunc("hour", F.expr("observation_data:properties.timestamp").cast("timestamp")).alias("observation_hour"),
    
    # Extracting weather metrics
    F.expr("observation_data:properties.temperature.value").cast("float").alias("temp_c"),
    F.expr("observation_data:properties.dewpoint.value").cast("float").alias("dewpoint_c"),
    F.expr("observation_data:properties.relativeHumidity.value").cast("float").alias("humidity_pct"),
    F.expr("observation_data:properties.barometricPressure.value").cast("float").alias("pressure_pa"),
    F.expr("observation_data:properties.windSpeed.value").cast("float").alias("windspeed_kmph"),
    F.expr("observation_data:properties.windGust.value").cast("float").alias("windgust_kmph"),
    F.expr("observation_data:properties.windDirection.value").cast("float").alias("winddir_deg"),
    F.expr("observation_data:properties.precipitationLastHour.value").cast("float").alias("precip_mm"),
    F.expr("observation_data:properties.uvIndex.value").cast("float").alias("uv_index"),
    F.expr("observation_data:properties.visibility.value").cast("float").alias("visibility_km"),
    F.expr("observation_data:properties.windChill.value").cast("float").alias("windchill_c"),
    F.expr("observation_data:properties.textDescription").cast("string").alias("conditions_desc"),
    
    # Audit Metadata
    F.col("ingested_at"),
    F.col("source_api")
)

#display(silver_updates_df.limit(15))

# --- STAGE 3: Upsert (Merge) Logic ---
# Check if the table exists. If not, create it with the first batch of data.
if not spark.catalog.tableExists(silver_table_path):
    print(f"Creating new Silver table: {silver_table_path}")
    silver_updates_df.write.format("delta").mode("overwrite").saveAsTable(silver_table_path)
else:
    print(f"Performing Upsert (Merge) into: {silver_table_path}")
    
    # Initialize the Delta Table object for merging
    target_table = DeltaTable.forName(spark, silver_table_path)
    
    # Merge on Station ID and the exact Observation Timestamp
    # This prevents duplicates if the NWS hasn't updated their data between our runs
    (
    target_table.alias("target")
        .merge(
            silver_updates_df.alias("source"),
            "target.station_id = source.station_id AND target.observation_time = source.observation_time"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

print("Silver transformation complete.")