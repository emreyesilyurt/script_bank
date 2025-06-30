-- COST-OPTIMIZED: This should process much less data (under 10GB)
WITH recent_panda_sample AS (
  SELECT 
    pn,
    pn_clean,
    `desc`,  -- DESC is a reserved keyword, needs backticks
    category,
    manuf,
    SAFE_CAST(inventory AS INT64) as inventory,
    leadtime,
    SAFE_CAST(moq AS FLOAT64) as moq,
    source_type,
    datasheet,
    timestamp,
    -- Extract leadtime weeks
    CASE 
      WHEN REGEXP_CONTAINS(COALESCE(leadtime, ''), r'(\d+)\s*Week') 
      THEN CAST(REGEXP_EXTRACT(leadtime, r'(\d+)\s*Week') AS INT64)
      ELSE NULL 
    END as leadtime_weeks
  FROM `datadojo.prod.panda`
  WHERE 
    pn IS NOT NULL 
    AND pn != ''
    -- COST OPTIMIZATION: Only recent data (last 7 days)
    AND DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    -- COST OPTIMIZATION: Only parts with some inventory or reasonable MOQ
    AND (SAFE_CAST(inventory AS INT64) > 0 OR SAFE_CAST(moq AS FLOAT64) < 10000)
  -- COST OPTIMIZATION: Sample early, before JOIN
  ORDER BY RAND()
  LIMIT 150000  -- Get more to account for deduplication
),

-- Only get demand data for the sampled parts
demand_for_sample AS (
  SELECT 
    d.pn,
    d.demand_all_time,
    CASE 
      WHEN ARRAY_LENGTH(d.demand_totals) > 0 THEN d.demand_totals[OFFSET(0)].demand_index
      ELSE NULL 
    END as demand_index
  FROM `datadojo.prod.demand_normalized` d
  -- COST OPTIMIZATION: Only join with our sampled parts
  INNER JOIN (SELECT DISTINCT pn FROM recent_panda_sample) p ON d.pn = p.pn
),

-- Deduplicate and join
final_sample AS (
  SELECT 
    p.*,
    COALESCE(d.demand_all_time, 0) as demand_all_time,
    d.demand_index,
    ROW_NUMBER() OVER (PARTITION BY p.pn ORDER BY p.timestamp DESC) as rn
  FROM recent_panda_sample p
  LEFT JOIN demand_for_sample d ON p.pn = d.pn
)

SELECT * EXCEPT(rn, timestamp)
FROM final_sample
WHERE rn = 1  -- Only latest record per part
ORDER BY RAND()
LIMIT 100000;