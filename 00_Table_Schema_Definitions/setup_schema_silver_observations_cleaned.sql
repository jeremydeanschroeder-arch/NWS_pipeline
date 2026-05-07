--Creates or Modifies table Schema for
--weather_oax.silver.observations_cleaned

-- One-time setup to define the Schema
CREATE OR REPLACE TABLE weather_oax.silver.observations_cleaned (
  station_id STRING NOT NULL,
  observation_time TIMESTAMP NOT NULL,
  observation_hour TIMESTAMP NOT NULL,
  temp_c FLOAT,
  dewpoint_c FLOAT,
  humidity_pct FLOAT,
  pressure_pa FLOAT,
  windspeed_kmph FLOAT,
  windgust_kmph FLOAT,
  winddir_deg FLOAT,
  precip_mm FLOAT,
  uv_index FLOAT,
  visibility_km FLOAT,
  windchill_c FLOAT,
  conditions_desc STRING,
  ingested_at TIMESTAMP NOT NULL,
  source_api STRING,
  
  -- Primary Key (Informational for optimization)
  CONSTRAINT observations_pk PRIMARY KEY(station_id, observation_time)
)
USING DELTA;

-- Enforce Data Quality (The Job will fail if the API sends impossible data)
ALTER TABLE weather_oax.silver.observations_cleaned 
ADD CONSTRAINT temp_check CHECK (temp_c IS NULL OR (temp_c > -90 AND temp_c < 60));

ALTER TABLE weather_oax.silver.observations_cleaned 
ADD CONSTRAINT humidity_check CHECK (humidity_pct IS NULL OR (humidity_pct >= 0 AND humidity_pct <= 100));

ALTER TABLE weather_oax.silver.observations_cleaned 
ADD CONSTRAINT wind_speed_check CHECK (windspeed_kmph IS NULL OR (windspeed_kmph >= 0 AND windspeed_kmph < 400));

ALTER TABLE weather_oax.silver.observations_cleaned 
ADD CONSTRAINT pressure_check CHECK (pressure_pa IS NULL OR (pressure_pa > 80000 AND pressure_pa < 110000));