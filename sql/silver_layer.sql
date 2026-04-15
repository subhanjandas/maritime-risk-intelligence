-- Silver Layer: Data Cleansing & Geospatial Enrichment
CREATE OR REPLACE TABLE `supply_chain_silver.enriched_vessel_telemetry` AS
SELECT 
    mmsi,
    TRIM(UPPER(vessel_name)) as ship_name,
    CAST(base_date_time AS TIMESTAMP) as event_timestamp,
    ST_GEOGPOINT(CAST(longitude AS FLOAT64), CAST(latitude AS FLOAT64)) as ship_point,
    vessel_type,
    CAST(sog AS FLOAT64) as speed_over_ground
FROM `supply_chain_bronze.historical_backfill`
WHERE latitude != 0 AND longitude != 0 AND latitude IS NOT NULL;
