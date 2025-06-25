-- Optimized query for batch processing - ONLY READS FROM SOURCE TABLES
WITH joined_data AS (
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
    COALESCE(d.demand_index, 0) as demand_index,
    
    -- Pre-calculate some features for efficiency
    CASE WHEN p.inventory > 0 THEN 1 ELSE 0 END as in_stock,
    CASE WHEN p.leadtime_weeks = 0 THEN 1 ELSE 0 END as immediate_availability,
    CASE WHEN p.source_type = 'Authorized' THEN 1 ELSE 0 END as is_authorized,
    CASE WHEN p.datasheet IS NOT NULL THEN 1 ELSE 0 END as has_datasheet,
    
    -- Availability score components
    CASE WHEN p.inventory > 0 THEN 0.5 ELSE 0 END +
    CASE WHEN p.leadtime_weeks = 0 THEN 0.3 ELSE 0 END +
    COALESCE(p.inventory / NULLIF(p.moq, 0), 0) * 0.2 as availability_score,
    
    -- Log transformations
    LN(1 + COALESCE(p.inventory, 0)) as log_inventory,
    LN(1 + COALESCE(p.first_price, 0)) as log_price,
    LN(1 + COALESCE(p.moq, 1)) as log_moq,
    
    -- Inverse transformations
    1 / (1 + COALESCE(p.leadtime_weeks, 0)) as inv_leadtime,
    1 / (1 + COALESCE(p.first_price, 0)) as inv_price,
    1 / (1 + COALESCE(p.moq, 1)) as inv_moq,
    
    CURRENT_TIMESTAMP() as processed_at
    
  FROM `datadojo.prod.panda` p  -- READ-ONLY SOURCE
  LEFT JOIN `datadojo.prod.demand_normalized` d  -- READ-ONLY SOURCE
    ON p.pn = d.pn
  WHERE 
    p.pn IS NOT NULL
    AND p._PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    AND {batch_filter}
)

SELECT *
FROM joined_data
WHERE 
  NOT (inventory = 0 AND leadtime_weeks > 12)  -- Filter out unavailable items
ORDER BY 
  demand_all_time DESC,
  availability_score DESC
LIMIT {batch_size};
