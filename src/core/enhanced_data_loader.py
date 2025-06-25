"""Enhanced data loader with BigQuery optimization and error handling."""

import asyncio
import pandas as pd
import json
from typing import Dict, Optional, Tuple, AsyncGenerator, List
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError, NotFound
from pathlib import Path
import logging
from concurrent.futures import ThreadPoolExecutor
import time

logger = logging.getLogger(__name__)

class EnhancedDataLoader:
    """Production-ready data loader with BigQuery optimization."""
    
    def __init__(self, settings):
        self.settings = settings
        self.db_config = settings.database
        self.client = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Initialize BigQuery client with error handling."""
        try:
            self.client = bigquery.Client(
                project=self.db_config.project_id,
                default_query_job_config=bigquery.QueryJobConfig(
                    maximum_bytes_billed=self.db_config.max_bytes_billed,
                    use_query_cache=self.db_config.use_query_cache
                )
            )
            logger.info(f"BigQuery client initialized for project {self.db_config.project_id}")
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client: {e}")
            raise
    
    async def load_sample_data(self, limit: int = 10000) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Load sample data for development/testing."""
        logger.info(f"Loading sample data with limit {limit}")
        
        # Load SQL templates
        sql_dir = Path(__file__).parent.parent.parent / "sql"
        
        with open(sql_dir / "panda_sample.sql", "r") as f:
            panda_query = f.read().format(limit=limit)
        
        with open(sql_dir / "demand_sample.sql", "r") as f:
            demand_query = f.read().format(limit=limit)
        
        # Execute queries concurrently
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor(max_workers=2) as executor:
            panda_future = loop.run_in_executor(executor, self._execute_query, panda_query)
            demand_future = loop.run_in_executor(executor, self._execute_query, demand_query)
            
            panda_df, demand_df = await asyncio.gather(panda_future, demand_future)
        
        logger.info(f"Loaded {len(panda_df)} panda rows, {len(demand_df)} demand rows")
        return panda_df, demand_df
    
    async def load_batch_data(
        self, 
        batch_size: int = 100000,
        offset: int = 0
    ) -> AsyncGenerator[Tuple[pd.DataFrame, pd.DataFrame], None]:
        """Load data in batches for production processing."""
        
        sql_dir = Path(__file__).parent.parent.parent / "sql"
        
        with open(sql_dir / "scoring_batch.sql", "r") as f:
            batch_query = f.read()
        
        current_offset = offset
        
        while True:
            # Create batch filter
            batch_filter = f"MOD(ABS(FARM_FINGERPRINT(p.pn)), 1000000) BETWEEN {current_offset} AND {current_offset + batch_size - 1}"
            
            query = batch_query.format(
                batch_filter=batch_filter,
                batch_size=batch_size
            )
            
            try:
                logger.info(f"Loading batch at offset {current_offset}")
                
                loop = asyncio.get_event_loop()
                with ThreadPoolExecutor(max_workers=1) as executor:
                    batch_df = await loop.run_in_executor(executor, self._execute_query, query)
                
                if len(batch_df) == 0:
                    logger.info("No more data to process")
                    break
                
                # Split into panda and demand components for compatibility
                panda_cols = [col for col in batch_df.columns if not col.startswith('demand_')]
                demand_cols = ['pn', 'demand_all_time', 'demand_index']
                
                panda_df = batch_df[panda_cols]
                demand_df = batch_df[demand_cols]
                
                yield panda_df, demand_df
                
                current_offset += batch_size
                
                # Break if we got less than requested batch size
                if len(batch_df) < batch_size:
                    break
                    
            except Exception as e:
                logger.error(f"Error loading batch at offset {current_offset}: {e}")
                break
    
    def _execute_query(self, query: str) -> pd.DataFrame:
        """Execute BigQuery query with error handling and retries."""
        max_retries = 3
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                start_time = time.time()
                query_job = self.client.query(query)
                result_df = query_job.to_dataframe()
                
                execution_time = time.time() - start_time
                
                logger.info(f"Query executed successfully in {execution_time:.2f}s, "
                           f"returned {len(result_df)} rows, "
                           f"processed {query_job.total_bytes_processed} bytes")
                
                return result_df
                
            except GoogleCloudError as e:
                logger.warning(f"BigQuery error on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    raise
            except Exception as e:
                logger.error(f"Unexpected error executing query: {e}")
                raise
    
    def save_results(self, df: pd.DataFrame, table_name: str, write_mode: str = "WRITE_TRUNCATE"):
        """Save results to BigQuery with proper error handling."""
        try:
            # Add metadata columns
            df = df.copy()
            df['processed_at'] = pd.Timestamp.now()
            df['batch_id'] = f"batch_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Configure job
            job_config = bigquery.LoadJobConfig(
                write_disposition=write_mode,
                create_disposition="CREATE_IF_NEEDED",
                schema_update_options=["ALLOW_FIELD_ADDITION"]
            )
            
            # Get table reference
            table_ref = self.client.dataset(self.db_config.dataset_id.split('.')[-1]).table(table_name)
            
            # Load data
            job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            job.result()  # Wait for completion
            
            logger.info(f"Successfully saved {len(df)} rows to {table_name}")
            
        except Exception as e:
            logger.error(f"Error saving results to BigQuery: {e}")
            raise
