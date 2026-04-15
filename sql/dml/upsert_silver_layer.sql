-- DML script to process Bronze to Silver
-- Implements cleaning, type casting, and spatial materialization.
INSERT INTO `supply_chain_silver.enriched_vessel_telemetry`
SELECT 
    mmsi,
    TRIM(UPPER(vessel_name)),
    CAST(base_date_time AS TIMESTAMP),
    CAST(latitude AS FLOAT64),
    CAST(longitude AS FLOAT64),
    ST_GEOGPOINT(CAST(longitude AS FLOAT64), CAST(latitude AS FLOAT64)),
    vessel_type,
    CAST(sog AS FLOAT64),
    'NOAA_2025_BATCH'
FROM `supply_chain_bronze.historical_backfill`
WHERE latitude IS NOT NULL 
  AND longitude IS NOT NULL
  AND CAST(latitude AS FLOAT64) != 0;
