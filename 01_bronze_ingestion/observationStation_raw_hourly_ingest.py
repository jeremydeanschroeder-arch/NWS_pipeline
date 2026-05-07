# Databricks notebook source
import requests
from pyspark.sql.functions import lit, current_timestamp, parse_json
from pyspark.sql.types import StringType, StructField, StructType

# --- 1. SET UP PARAMETERS (WIDGETS) ---
dbutils.widgets.text("catalog", "weather_oax", "1. Catalog Name")
dbutils.widgets.text("schema", "bronze", "2. Schema Name")
dbutils.widgets.text("table", "observation_stations_raw", "3. Table Name")
dbutils.widgets.text("user_email", "jeremy.dean.schroeder@gmail.com", "4. Contact Email")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
TABLE = dbutils.widgets.get("table")
EMAIL = dbutils.widgets.get("user_email")

FULL_TABLE_PATH = f"{CATALOG}.{SCHEMA}.{TABLE}"

# --- 2. API CONFIGURATION ---
HEADERS = {"User-Agent": f"(Databricks Weather Pipeline, {EMAIL})"}

# --- 3. HELPER FUNCTIONS ---
def get_authorized_stations(wfo_id="OAX"):
    url = f"https://api.weather.gov/offices/{wfo_id}"
    headers = HEADERS
    try:
        resp = requests.get(url, headers=headers)
        data = resp.json()
        
        # Pull the URLs from approvedObservationStations and grab the last part of the path
        return [uri.split('/')[-1] for uri in data.get('approvedObservationStations', [])]
    
    except Exception as e:
        print(f"Discovery failed: {e}")
        return None


def get_latest_observation(station_id):
    url = f"https://api.weather.gov/stations/{station_id}/observations/latest"
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"FAILED to fetch {station_id}: {e}")
        return None

# --- 4. DATA INGESTION LOGIC ---

# Retrieve list of stations
STATIONS = get_authorized_stations()

print(f"Starting ingestion for {len(STATIONS)} stations...")
data_payload = []
for station in STATIONS:
    raw_json = get_latest_observation(station)
    if raw_json:
        data_payload.append((station, raw_json))

if not data_payload:
    raise Exception("No data was retrieved from the NWS API. Aborting.")

# --- 5. SPARK PROCESSING ---
# Stage 1: Load as text
bronze_schema = StructType([
    StructField("station_id", StringType(), True),
    StructField("raw_json_string", StringType(), True)
])

df = spark.createDataFrame(data_payload, schema=bronze_schema)

# Stage 2: Convert text to JSON Object (Variant) and add metadata
df_final = df.withColumn("observation_data", parse_json("raw_json_string")) \
             .withColumn("ingested_at", current_timestamp()) \
             .withColumn("source_api", lit("api.weather.gov")) \
             .drop("raw_json_string")

# --- 6. WRITE TO DELTA (UNITY CATALOG) ---
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

print(f"Writing data to {FULL_TABLE_PATH}...")

(df_final.write
  .format("delta")
  .mode("append")
  .option("mergeSchema", "true") 
  .saveAsTable(FULL_TABLE_PATH))

print("Ingestion Complete.")