-- Incremental scoring for updated components only
WITH updated_components AS (
  SELECT DISTINCT pn
  FROM `datadojo.prod.panda`
  WHERE _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
  
  UNION DISTINCT
  
  SELECT DISTINCT pn
  FROM `datadojo.prod.demand_normalized`
  WHERE _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
),

components_to_score AS (
  SELECT p.*, d.demand_all_time, d.demand_index
  FROM `datadojo.prod.panda` p
  LEFT JOIN `datadojo.prod.demand_normalized` d ON p.pn = d.pn
  WHERE p.pn IN (SELECT pn FROM updated_components)
    AND p._PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
)

SELECT *
FROM components_to_score
ORDER BY demand_all_time DESC
LIMIT {limit};