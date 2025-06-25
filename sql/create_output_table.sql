-- Create the output tables in our dedicated dataset

-- Main scoring results table
CREATE TABLE IF NOT EXISTS `datadojo.part_priority_scoring.part_scores` (
  pn STRING NOT NULL,
  pn_clean STRING,
  desc STRING,
  category STRING,
  manuf STRING,
  inventory INT64,
  first_price FLOAT64,
  leadtime_weeks INT64,
  moq FLOAT64,
  source_type STRING,
  demand_all_time INT64,
  demand_index FLOAT64,
  availability_score FLOAT64,
  is_authorized INT64,
  has_datasheet INT64,
  base_score FLOAT64,
  priority_score FLOAT64,
  score_percentile FLOAT64,
  processed_at TIMESTAMP,
  batch_id STRING,
  pipeline_version STRING
)
PARTITION BY DATE(processed_at)
CLUSTER BY category, source_type, is_authorized
OPTIONS (
  description = "Part priority scores for product enrichment - Main Results Table",
  labels = [("team", "data-engineering"), ("purpose", "scoring"), ("env", "production")]
);

-- Historical snapshots table  
CREATE TABLE IF NOT EXISTS `datadojo.part_priority_scoring.part_scores_history` (
  snapshot_date DATE,
  pn STRING,
  priority_score FLOAT64,
  score_rank INT64,
  category STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY snapshot_date
CLUSTER BY category, score_rank
OPTIONS (
  description = "Historical snapshots of part scores for trend analysis"
);

-- Pipeline metrics table
CREATE TABLE IF NOT EXISTS `datadojo.part_priority_scoring.scoring_metrics` (
  run_id STRING,
  run_timestamp TIMESTAMP,
  environment STRING,
  total_parts_processed INT64,
  total_parts_scored INT64,
  processing_duration_seconds FLOAT64,
  success_rate FLOAT64,
  avg_score FLOAT64,
  score_distribution STRUCT<
    very_high INT64,
    high INT64,
    medium INT64,
    low INT64,
    very_low INT64,
    zero INT64
  >,
  pipeline_version STRING,
  config_version STRING
)
PARTITION BY DATE(run_timestamp)
OPTIONS (
  description = "Pipeline execution metrics and performance data"
);

-- Data quality reports table
CREATE TABLE IF NOT EXISTS `datadojo.part_priority_scoring.data_quality_reports` (
  report_id STRING,
  report_timestamp TIMESTAMP,
  batch_id STRING,
  total_rows INT64,
  valid_rows INT64,
  quality_score FLOAT64,
  issues ARRAY<STRUCT<
    type STRING,
    severity STRING,
    message STRING,
    affected_rows INT64,
    field STRING
  >>,
  field_coverage ARRAY<STRUCT<
    field_name STRING,
    coverage_pct FLOAT64,
    null_count INT64,
    unique_values INT64
  >>
)
PARTITION BY DATE(report_timestamp)
OPTIONS (
  description = "Data quality validation reports for monitoring data health"
);

-- Create views for easy access
CREATE OR REPLACE VIEW `datadojo.part_priority_scoring.latest_scores` AS
SELECT *
FROM `datadojo.part_priority_scoring.part_scores`
WHERE DATE(processed_at) = (
  SELECT MAX(DATE(processed_at)) 
  FROM `datadojo.part_priority_scoring.part_scores`
);

CREATE OR REPLACE VIEW `datadojo.part_priority_scoring.top_parts` AS
SELECT 
  pn,
  pn_clean,
  desc,
  category,
  priority_score,
  score_percentile,
  inventory,
  source_type,
  processed_at
FROM `datadojo.part_priority_scoring.latest_scores`
WHERE priority_score > 0
ORDER BY priority_score DESC
LIMIT 1000;