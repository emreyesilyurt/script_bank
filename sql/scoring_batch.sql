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
    -- Extract leadtime weeks
    CASE 
      WHEN REGEXP_CONTAINS(leadtime, r'(\d+)\s*Week') 
      THEN CAST(REGEXP_EXTRACT(leadtime, r'(\d+)\s*Week') AS INT64)
      ELSE NULL 
    END as leadtime_weeks,
    ROW_NUMBER() OVER (PARTITION BY pn ORDER BY timestamp DESC) as rn
  FROM `datadojo.prod.panda`
  WHERE 
    pn IS NOT NULL
    AND DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)  -- Shorter window for cost
    AND {batch_filter}  -- Additional filtering from application
),

joined_data AS (
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
    COALESCE(SAFE_CAST(JSON_EXTRACT_SCALAR(d.demand_totals, '$.demand_totals[0].demand_index') AS FLOAT64), 0) as demand_index,
    
    -- Pre-calculate features for efficiency
    CASE WHEN p.inventory > 0 THEN 1 ELSE 0 END as in_stock,
    CASE WHEN p.leadtime_weeks = 0 THEN 1 ELSE 0 END as immediate_availability,
    CASE WHEN p.source_type = 'Authorized' THEN 1 ELSE 0 END as is_authorized,
    CASE WHEN p.datasheet IS NOT NULL THEN 1 ELSE 0 END as has_datasheet,
    
    -- Availability score components
    CASE WHEN p.inventory > 0 THEN 0.5 ELSE 0 END +
    CASE WHEN p.leadtime_weeks = 0 THEN 0.3 ELSE 0 END +
    COALESCE(SAFE_DIVIDE(p.inventory, NULLIF(p.moq, 0)), 0) * 0.2 as availability_score,
    
    -- Log transformations (safe)
    LN(1 + COALESCE(p.inventory, 0)) as log_inventory,
    LN(1 + COALESCE(p.moq, 1)) as log_moq,
    
    -- Inverse transformations (safe)
    SAFE_DIVIDE(1, (1 + COALESCE(p.leadtime_weeks, 0))) as inv_leadtime,
    SAFE_DIVIDE(1, (1 + COALESCE(p.moq, 1))) as inv_moq,
    
    CURRENT_TIMESTAMP() as processed_at
    
  FROM recent_panda p
  LEFT JOIN `datadojo.prod.demand_normalized` d ON p.pn = d.pn
  WHERE p.rn = 1  -- Only latest record per part
)

SELECT *
FROM joined_data
WHERE 
  NOT (inventory = 0 AND leadtime_weeks > 12)  -- Filter out unavailable items
ORDER BY 
  demand_all_time DESC,
  availability_score DESC
LIMIT {batch_size};