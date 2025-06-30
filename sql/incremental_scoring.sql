-- Incremental scoring for updated components only (COST-EFFECTIVE)
WITH updated_components AS (
  -- Use date-based filtering instead of _PARTITIONTIME
  SELECT DISTINCT pn
  FROM `datadojo.prod.panda`
  WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  
  UNION DISTINCT
  
  SELECT DISTINCT pn
  FROM `datadojo.prod.demand_normalized`
  WHERE DATE(created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)  -- Adjust column name if different
),

components_to_score AS (
  SELECT 
    p.pn,
    p.pn_clean,
    p.desc,
    p.category,
    p.manuf,
    p.inventory,
    CASE 
      WHEN REGEXP_CONTAINS(p.leadtime, r'(\d+)\s*Week') 
      THEN CAST(REGEXP_EXTRACT(p.leadtime, r'(\d+)\s*Week') AS INT64)
      ELSE NULL 
    END as leadtime_weeks,
    p.moq,
    p.source_type,
    p.datasheet,
    COALESCE(d.demand_all_time, 0) as demand_all_time,
    COALESCE(SAFE_CAST(JSON_EXTRACT_SCALAR(d.demand_totals, '$.demand_totals[0].demand_index') AS FLOAT64), 0) as demand_index,
    ROW_NUMBER() OVER (PARTITION BY p.pn ORDER BY p.timestamp DESC) as rn
  FROM `datadojo.prod.panda` p
  LEFT JOIN `datadojo.prod.demand_normalized` d ON p.pn = d.pn
  WHERE p.pn IN (SELECT pn FROM updated_components)
    AND DATE(p.timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
)

SELECT * EXCEPT(rn)
FROM components_to_score
WHERE rn = 1  -- Only latest record per part
ORDER BY demand_all_time DESC
LIMIT {limit};