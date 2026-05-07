# NWS_pipeline
Sample Databricks Pipeline Project using National Weather Service (NWS) API for Omaha/Valley(OAX).

## What I'm doing
I am using the NWS API because it well-documented and does not require and API Key.  

I am pulling observation data from all of the authorized observation stations linked to the Omaha/Valley(OAX) Weather Forecast Office (WFO).  Data from the API is stored into a raw bronze table.  Seprate tasks create the silver and gold tables.

## Plan
I intend to also pull information about the individual weather stations, weather forecast office, alert areas, and couny level information to create a proper database in the silver level, and then use the combined and enriched data to create gold tables.  

I don't know what I will do with the gold tables, perhaps a weather GUI via streamlit or other tool or maybe an analysis of forecast accuracy.  Very much a TODO task.

## Catalog
Data is stored in catalog called weather_oax.  There are schemas for bronze, silver, and gold.

## Pipeline
API data is collected and depoisted into bronze tables via python scripts utilizing the requests library, and pushed into datawarehouse using pyspark.

Pipeline is orchastrated via a databricks job.

## Bronze Table
API data is stored into a bronze table with 4 fields:
1. station_id: the 4-digit station code ::STRING
2. obsrvation_data: the complete JSON response ::VARIANT
3. ingested_at: UTC timestamp API was run  ::TIMESTAMP
4. source_api: api run to collect data ::STRING

## Silver Table
The bronze table is converted to silver table via python script 

