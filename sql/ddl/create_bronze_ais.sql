-- DDL for Bronze Layer: Raw AIS Landing
-- This table is designed for high-throughput ingestion.
CREATE OR REPLACE TABLE `supply_chain_bronze.historical_backfill` (
    mmsi INT64,
    base_date_time STRING,
    longitude STRING,
    latitude STRING,
    sog STRING,
    cog STRING,
    heading STRING,
    vessel_name STRING,
    imo STRING,
    call_sign STRING,
    vessel_type STRING,
    status STRING,
    length STRING,
    width STRING,
    draft STRING,
    cargo STRING,
    transceiver STRING
)
OPTIONS(
    description="Raw landing table for NOAA AIS telemetry",
    labels=[("env", "dev"), ("layer", "bronze")]
);
