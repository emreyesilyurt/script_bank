-- Analysis query for scoring results validation
WITH score_stats AS (
  SELECT 
    DATE(processed_at) as score_date,
    COUNT(*) as total_parts,
    COUNT(CASE WHEN priority_score > 0 THEN 1 END) as scored_parts,
    AVG(priority_score) as avg_score,
    STDDEV(priority_score) as score_stddev,
    MIN(priority_score) as min_score,
    MAX(priority_score) as max_score,
    
    -- Score distribution
    COUNT(CASE WHEN priority_score >= 90 THEN 1 END) as high_priority,
    COUNT(CASE WHEN priority_score >= 70 AND priority_score < 90 THEN 1 END) as medium_priority,
    COUNT(CASE WHEN priority_score >= 50 AND priority_score < 70 THEN 1 END) as low_priority,
    COUNT(CASE WHEN priority_score > 0 AND priority_score < 50 THEN 1 END) as very_low_priority,
    
    -- Quality metrics
    COUNT(CASE WHEN inventory > 0 THEN 1 END) as in_stock_count,
    COUNT(CASE WHEN leadtime_weeks = 0 THEN 1 END) as immediate_availability_count,
    COUNT(CASE WHEN source_type = 'Authorized' THEN 1 END) as authorized_count,
    COUNT(CASE WHEN has_datasheet = 1 THEN 1 END) as with_datasheet_count
    
  FROM `datadojo.part_priority_scoring.part_scores`  -- OUR OUTPUT TABLE
  WHERE processed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  GROUP BY DATE(processed_at)
)

SELECT 
  score_date,
  total_parts,
  scored_parts,
  ROUND(scored_parts * 100.0 / total_parts, 2) as scoring_rate_pct,
  ROUND(avg_score, 2) as avg_score,
  ROUND(score_stddev, 2) as score_stddev,
  min_score,
  max_score,
  high_priority,
  medium_priority,
  low_priority,
  very_low_priority,
  ROUND(in_stock_count * 100.0 / total_parts, 2) as in_stock_pct,
  ROUND(immediate_availability_count * 100.0 / total_parts, 2) as immediate_avail_pct,
  ROUND(authorized_count * 100.0 / total_parts, 2) as authorized_pct,
  ROUND(with_datasheet_count * 100.0 / total_parts, 2) as datasheet_pct
FROM score_stats
ORDER BY score_date DESC;