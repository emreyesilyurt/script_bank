-- Sample query for panda table - READ-ONLY ACCESS
WITH panda_filtered AS (
  SELECT 
    pn,
    pn_clean,
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', timestamp) as timestamp,
    pricing,
    leadtime,
    desc,
    cpn,
    site,
    currency,
    datasheet,
    CAST(moq AS FLOAT64) as moq,
    packaging,
    category,
    source_url,
    site_url,
    ordered,
    taskid,
    jobId,
    extra_params,
    manuf,
    manuf_original,
    CAST(inventory AS INT64) as inventory,
    source,
    source_type,
    
    -- Extract first price from pricing JSON
    CAST(JSON_EXTRACT_SCALAR(pricing, '$.pricing[0].price') AS FLOAT64) as first_price,
    
    -- Extract leadtime weeks from leadtime string
    CASE 
      WHEN REGEXP_CONTAINS(leadtime, r'(\d+)\s*Week') 
      THEN CAST(REGEXP_EXTRACT(leadtime, r'(\d+)\s*Week') AS INT64)
      ELSE NULL 
    END as leadtime_weeks,
    
    -- Extract order count from ordered JSON
    ARRAY_LENGTH(JSON_EXTRACT_ARRAY(ordered, '$.ordered')) as order_count

  FROM `datadojo.prod.panda`  -- READ-ONLY SOURCE
  WHERE 
    pn IS NOT NULL
    AND pn != ''
    AND inventory >= 0
    AND (pricing IS NULL OR JSON_EXTRACT_SCALAR(pricing, '$.pricing[0].price') IS NOT NULL)
    AND _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY) -- Only recent data
),

-- Add ranking for better sampling
panda_ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY pn_clean 
      ORDER BY 
        CASE WHEN inventory > 0 THEN 1 ELSE 2 END,
        CASE WHEN source_type = 'Authorized' THEN 1 ELSE 2 END,
        timestamp DESC
    ) as rn
  FROM panda_filtered
)

SELECT 
  pn,
  pn_clean,
  timestamp,
  pricing,
  leadtime,
  desc,
  cpn,
  site,
  currency,
  datasheet,
  moq,
  packaging,
  category,
  source_url,
  site_url,
  ordered,
  taskid,
  jobId,
  extra_params,
  manuf,
  manuf_original,
  inventory,
  source,
  source_type,
  first_price,
  leadtime_weeks,
  order_count
FROM panda_ranked
WHERE rn = 1  -- Deduplicate by pn_clean
ORDER BY 
  CASE WHEN inventory > 0 THEN 1 ELSE 2 END,
  inventory DESC,
  first_price ASC
LIMIT {limit};