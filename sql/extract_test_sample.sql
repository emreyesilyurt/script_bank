WITH recent_panda AS (
  SELECT *
  FROM `datadojo.prod.panda`
  WHERE pn IS NOT NULL 
    AND pn != ''
    AND inventory > 0
    AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
),

joined_parts AS (
  -- Join first before sampling
  SELECT DISTINCT p.pn
  FROM recent_panda p
  JOIN `datadojo.prod.demand_normalized` d ON p.pn = d.pn
  WHERE d.demand_all_time > 0
),

matched_parts AS (
  SELECT pn
  FROM joined_parts
  ORDER BY RAND()
  LIMIT 10000
),

panda_data AS (
  SELECT 
    p.pn,
    p.pn_clean,
    p.desc,
    p.category,
    p.manuf,
    p.inventory,
    CASE 
      WHEN p.pricing IS NOT NULL AND ARRAY_LENGTH(p.pricing) > 0 
      THEN SAFE_CAST(p.pricing[OFFSET(0)].price AS FLOAT64)
      ELSE NULL 
    END as first_price,
    CASE 
      WHEN REGEXP_CONTAINS(COALESCE(p.leadtime, ''), r'(\d+)\s*Week') 
      THEN CAST(REGEXP_EXTRACT(p.leadtime, r'(\d+)\s*Week') AS INT64)
      ELSE NULL 
    END as leadtime_weeks,
    p.moq,
    p.source_type,
    p.datasheet,
    ROW_NUMBER() OVER (PARTITION BY p.pn ORDER BY p.timestamp DESC) as rn
  FROM recent_panda p
  INNER JOIN matched_parts m ON p.pn = m.pn
),

demand_data AS (
  SELECT 
    d.pn,
    d.demand_all_time,
    CASE 
      WHEN ARRAY_LENGTH(d.demand_totals) > 0 THEN
        SAFE_CAST(d.demand_totals[OFFSET(0)].demand_index AS FLOAT64)
      ELSE NULL
    END AS demand_index
  FROM `datadojo.prod.demand_normalized` d
  INNER JOIN matched_parts m ON d.pn = m.pn
)

SELECT 
  p.pn,
  p.pn_clean,
  p.desc,
  p.category,
  p.manuf,
  p.inventory,
  p.first_price,
  p.leadtime_weeks,
  p.moq,
  p.source_type,
  p.datasheet,
  COALESCE(d.demand_all_time, 0) as demand_all_time,
  COALESCE(d.demand_index, 0.0) as demand_index
FROM panda_data p
LEFT JOIN demand_data d ON p.pn = d.pn
WHERE p.rn = 1
ORDER BY d.demand_all_time DESC, p.inventory DESC;