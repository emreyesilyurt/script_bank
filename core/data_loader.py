"""Data loading utilities for BigQuery and other sources."""

import pandas as pd
import logging
from typing import Optional, Dict, Any
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

logger = logging.getLogger(__name__)

class DataLoader:
    """Load part and demand data from various sources."""
    
    def __init__(self, project_id: str = None, dataset: str = None):
        """Initialize data loader.
        
        Args:
            project_id: Google Cloud Project ID
            dataset: BigQuery dataset name for output tables
        """
        self.project_id = project_id
        self.dataset = dataset or 'datadojo.part_priority_scoring'
        self.source_dataset = 'datadojo.prod'
        
        if project_id:
            self.client = bigquery.Client(project=project_id)
        else:
            self.client = None
    
    def load_sample_data(self, limit: int = 10000) -> pd.DataFrame:
        """Load sample data from BigQuery.
        
        Args:
            limit: Maximum number of rows to load
            
        Returns:
            Merged dataframe with part and demand data
        """
        if not self.client:
            raise ValueError("BigQuery client not initialized. Provide project_id.")
        
        logger.info(f"Loading sample data with limit {limit}")
        
        # Query to join panda and demand data
        query = f"""
        WITH panda_sample AS (
          SELECT 
            pn,
            pn_clean,
            desc,
            category,
            manuf,
            CAST(inventory AS INT64) as inventory,
            CAST(JSON_EXTRACT_SCALAR(pricing, '$.pricing[0].price') AS FLOAT64) as first_price,
            CASE 
              WHEN REGEXP_CONTAINS(leadtime, r'(\\d+)\\s*Week') 
              THEN CAST(REGEXP_EXTRACT(leadtime, r'(\\d+)\\s*Week') AS INT64)
              ELSE NULL 
            END as leadtime_weeks,
            CAST(moq AS FLOAT64) as moq,
            source_type,
            datasheet
          FROM `{self.source_dataset}.panda`
          WHERE pn IS NOT NULL 
            AND inventory >= 0
          ORDER BY RAND()
          LIMIT {limit}
        ),
        
        demand_sample AS (
          SELECT 
            pn,
            demand_all_time,
            CAST(JSON_EXTRACT_SCALAR(demand_totals, '$.demand_totals[0].demand_index') AS FLOAT64) as demand_index
          FROM `{self.source_dataset}.demand_normalized`
          WHERE pn IS NOT NULL 
            AND demand_all_time >= 0
        )
        
        SELECT 
          p.*,
          COALESCE(d.demand_all_time, 0) as demand_all_time,
          COALESCE(d.demand_index, 0) as demand_index
        FROM panda_sample p
        LEFT JOIN demand_sample d ON p.pn = d.pn
        """
        
        try:
            result_df = self.client.query(query).to_dataframe()
            logger.info(f"Loaded {len(result_df)} rows")
            return result_df
        except GoogleCloudError as e:
            logger.error(f"BigQuery error: {e}")
            raise
    
    def save_results(self, df: pd.DataFrame, table_name: str = 'part_scores'):
        """Save scoring results to BigQuery.
        
        Args:
            df: Dataframe with scoring results
            table_name: Target table name
        """
        if not self.client:
            raise ValueError("BigQuery client not initialized. Provide project_id.")
        
        # Add metadata
        df = df.copy()
        df['processed_at'] = pd.Timestamp.now()
        df['pipeline_version'] = '1.0.0'
        
        # Configure job
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED"
        )
        
        # Get table reference
        table_id = f"{self.dataset}.{table_name}"
        
        try:
            job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()  # Wait for completion
            logger.info(f"Saved {len(df)} rows to {table_id}")
        except GoogleCloudError as e:
            logger.error(f"Error saving to BigQuery: {e}")
            raise
