-- Sample query for demand_normalized table - READ-ONLY ACCESS
WITH demand_parsed AS (
  SELECT 
    pn,
    demand_all_time,
    demand_totals,
    
    -- Extract demand index from demand_totals JSON
    CAST(JSON_EXTRACT_SCALAR(demand_totals, '$.demand_totals[0].demand_index') AS FLOAT64) as demand_index,
    
    -- Extract additional demand metrics if available
    JSON_EXTRACT_ARRAY(demand_totals, '$.demand_totals') as demand_array
    
  FROM `datadojo.prod.demand_normalized`  -- READ-ONLY SOURCE
  WHERE 
    pn IS NOT NULL
    AND pn != ''
    AND demand_all_time >= 0
    AND demand_totals IS NOT NULL
)

SELECT 
  pn,
  demand_all_time,
  demand_totals,
  demand_index,
  ARRAY_LENGTH(demand_array) as demand_periods
FROM demand_parsed
WHERE demand_index IS NOT NULL
ORDER BY demand_all_time DESC
LIMIT {limit};