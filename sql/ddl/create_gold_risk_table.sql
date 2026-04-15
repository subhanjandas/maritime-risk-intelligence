-- DDL for Gold Layer: Maritime Risk Intelligence
-- Purpose: Final analytical table for dashboard consumption.
-- Optimized with clustering on risk_status for high-performance filtering.
CREATE OR REPLACE TABLE `supply_chain_gold.maritime_risk_dashboard` (
    ship_name STRING,
    mmsi INT64,
    event_timestamp TIMESTAMP,
    lat FLOAT64,
    lon FLOAT64,
    ship_point GEOGRAPHY,
    risk_status STRING,
    dist_to_harbor_km FLOAT64,
    is_at_risk_flag INT64,
    vessel_type STRING,
    speed_over_ground FLOAT64
)
CLUSTER BY risk_status, mmsi
OPTIONS(
    description="Final Gold layer table for maritime risk reporting and visualization",
    labels=[("env", "prod"), ("layer", "gold")]
);
