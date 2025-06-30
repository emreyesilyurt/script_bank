WITH panda_filtered AS (
  SELECT 
    pn,
    pn_clean,
    timestamp,
    leadtime,
    desc,
    category,
    manuf,
    CAST(inventory AS INT64) as inventory,
    CAST(moq AS FLOAT64) as moq,
    source_type,
    datasheet,
    
    -- Extract leadtime weeks from leadtime string
    CASE 
      WHEN REGEXP_CONTAINS(leadtime, r'(\d+)\s*Week') 
      THEN CAST(REGEXP_EXTRACT(leadtime, r'(\d+)\s*Week') AS INT64)
      ELSE NULL 
    END as leadtime_weeks

  FROM `datadojo.prod.panda`  -- READ-ONLY SOURCE
  WHERE 
    pn IS NOT NULL
    AND pn != ''
    AND inventory >= 0
    -- Use DATE() instead of _PARTITIONTIME for cost efficiency
    AND DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
),

-- Add ranking for better sampling and deduplication
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
  leadtime,
  desc,
  category,
  manuf,
  inventory,
  moq,
  source_type,
  datasheet,
  leadtime_weeks
FROM panda_ranked
WHERE rn = 1  -- Deduplicate by pn_clean
ORDER BY 
  CASE WHEN inventory > 0 THEN 1 ELSE 2 END,
  inventory DESC
LIMIT {limit};