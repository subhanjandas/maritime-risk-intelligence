-- Gold Layer: Risk Modeling via Point-in-Polygon
CREATE OR REPLACE TABLE `supply_chain_gold.maritime_risk_dashboard` AS
SELECT 
  *,
  CASE 
    WHEN ST_INTERSECTS(ship_point, ST_GEOGFROMTEXT('POLYGON((-70.30 41.63, -70.25 41.63, -70.25 41.66, -70.30 41.66, -70.30 41.63))')) 
    THEN 'CRITICAL: HARBOR ENTRY'
    ELSE 'SAFE'
  END as risk_status
FROM `supply_chain_silver.enriched_vessel_telemetry`;
