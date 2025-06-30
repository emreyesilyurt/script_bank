WITH recent_panda AS (
  SELECT 
    pn,
    pn_clean,
    desc,
    category,
    manuf,
    inventory,
    leadtime,
    moq,
    source_type,
    datasheet,
    timestamp,
    ROW_NUMBER() OVER (PARTITION BY pn ORDER BY timestamp DESC) as rn
  FROM `datadojo.prod.panda`
  WHERE pn IS NOT NULL 
    AND pn != ''
    AND inventory > 0
    AND DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)  -- Only recent data
),

-- Get parts that have demand data (join early to reduce data)
parts_with_demand AS (
  SELECT DISTINCT p.pn
  FROM recent_panda p
  WHERE p.rn = 1  -- Only latest record per part
    AND EXISTS (
      SELECT 1 FROM `datadojo.prod.demand_normalized` d 
      WHERE d.pn = p.pn AND d.demand_all_time > 0
    )
  ORDER BY RAND()
  LIMIT 1000  -- Limit early for cost control
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
      WHEN REGEXP_CONTAINS(COALESCE(p.leadtime, ''), r'(\d+)\s*Week') 
      THEN CAST(REGEXP_EXTRACT(p.leadtime, r'(\d+)\s*Week') AS INT64)
      ELSE NULL 
    END as leadtime_weeks,
    p.moq,
    p.source_type,
    p.datasheet
  FROM recent_panda p
  INNER JOIN parts_with_demand pwd ON p.pn = pwd.pn
  WHERE p.rn = 1
),

demand_data AS (
  SELECT 
    d.pn,
    d.demand_all_time,
    SAFE_CAST(JSON_EXTRACT_SCALAR(d.demand_totals, '$.demand_totals[0].demand_index') AS FLOAT64) as demand_index
  FROM `datadojo.prod.demand_normalized` d
  INNER JOIN parts_with_demand pwd ON d.pn = pwd.pn
)

SELECT 
  p.pn,
  p.pn_clean,
  p.desc,
  p.category,
  p.manuf,
  p.inventory,
  p.leadtime_weeks,
  p.moq,
  p.source_type,
  p.datasheet,
  COALESCE(d.demand_all_time, 0) as demand_all_time,
  COALESCE(d.demand_index, 0.0) as demand_index
FROM panda_data p
LEFT JOIN demand_data d ON p.pn = d.pn
ORDER BY d.demand_all_time DESC, p.inventory DESC;