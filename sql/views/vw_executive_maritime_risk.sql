-- Business View: Executive Maritime Risk Overview
-- Purpose: Abstraction layer for BI tools to prevent dashboard breakage 
-- during schema migrations or logic updates.
CREATE OR REPLACE VIEW `supply_chain_gold.vw_executive_maritime_risk` AS
SELECT 
    ship_name,
    mmsi,
    event_timestamp,
    risk_status,
    dist_to_harbor_km,
    CASE 
        WHEN risk_status = 'CRITICAL: HARBOR ENTRY' THEN 1
        WHEN risk_status = 'WARNING: APPROACHING' THEN 2
        ELSE 3
    END as priority_level,
    ship_point
FROM `supply_chain_gold.maritime_risk_dashboard`
WHERE event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY);
