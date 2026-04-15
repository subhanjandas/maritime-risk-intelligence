-- DDL for Silver Layer: Enriched & Optimized Telemetry
-- Partitioned by day for cost-efficiency; Clustered by mmsi for query speed.
CREATE OR REPLACE TABLE `supply_chain_silver.enriched_vessel_telemetry` (
    mmsi INT64 NOT NULL,
    ship_name STRING,
    event_timestamp TIMESTAMP,
    lat FLOAT64,
    lon FLOAT64,
    ship_point GEOGRAPHY,
    vessel_type STRING,
    speed_over_ground FLOAT64,
    data_source STRING
)
PARTITION BY DATE(event_timestamp)
CLUSTER BY mmsi, vessel_type
OPTIONS(
    description="Partitioned and clustered vessel telemetry for analytics",
    labels=[("env", "prod"), ("layer", "silver")]
);
