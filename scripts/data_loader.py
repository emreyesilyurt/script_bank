import pandas as pd
import json
import logging
from pathlib import Path
from typing import Dict, Optional, Tuple
from google.cloud import bigquery

logger = logging.getLogger(__name__)


class DataLoader:
    \"\"\"Handles data loading from CSV files or BigQuery\"\"\"
    
    def __init__(self, source: str = "csv", project_id: Optional[str] = None):
        self.source = source
        self.project_id = project_id
        
        if source == "bigquery" and project_id:
            self.bq_client = bigquery.Client(project=project_id)
    
    def load_data(self, 
                  panda_path: Optional[str] = None, 
                  demand_path: Optional[str] = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
        \"\"\"Load panda and demand data from specified source\"\"\"
        
        if self.source == "csv":
            return self._load_csv_data(panda_path, demand_path)
        elif self.source == "bigquery":
            return self._load_bigquery_data()
        else:
            raise ValueError(f"Unknown source: {self.source}")
    
    def _load_csv_data(self, panda_path: str, demand_path: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        \"\"\"Load data from CSV files\"\"\"
        logger.info(f"Loading CSV data from {panda_path} and {demand_path}")
        
        panda_df = pd.read_csv(panda_path)
        demand_df = pd.read_csv(demand_path)
        
        logger.info(f"Loaded {len(panda_df)} panda rows and {len(demand_df)} demand rows")
        return panda_df, demand_df
    
    def _load_bigquery_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        \"\"\"Load data from BigQuery\"\"\"
        logger.info("Loading data from BigQuery")
        
        # Load SQL queries
        sql_dir = Path(__file__).parent.parent / "sql"
        
        with open(sql_dir / "sample_panda.sql", "r") as f:
            panda_query = f.read()
        
        with open(sql_dir / "sample_demand.sql", "r") as f:
            demand_query = f.read()
        
        # Execute queries
        panda_df = self.bq_client.query(panda_query).to_dataframe()
        demand_df = self.bq_client.query(demand_query).to_dataframe()
        
        logger.info(f"Loaded {len(panda_df)} panda rows and {len(demand_df)} demand rows from BigQuery")
        return panda_df, demand_df
    
    @staticmethod
    def parse_json_fields(df: pd.DataFrame) -> pd.DataFrame:
        \"\"\"Parse JSON fields in the dataframe\"\"\"
        df = df.copy()
        
        # Parse pricing to extract first price
        if 'pricing' in df.columns:
            df['first_price'] = df['pricing'].apply(extract_first_price)
        
        # Parse leadtime to extract weeks
        if 'leadtime' in df.columns:
            df['leadtime_weeks'] = df['leadtime'].apply(extract_leadtime_weeks)
        
        # Parse ordered to get count
        if 'ordered' in df.columns:
            df['order_count'] = df['ordered'].apply(extract_order_count)
        
        return df


def extract_first_price(pricing_json: str) -> Optional[float]:
    \"\"\"Extract first price from pricing JSON\"\"\"
    try:
        pricing_data = json.loads(pricing_json)
        if pricing_data and 'pricing' in pricing_data:
            price_list = pricing_data['pricing']
            if price_list and len(price_list) > 0:
                return float(price_list[0]['price'])
    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
        pass
    return None


def extract_leadtime_weeks(leadtime_str: str) -> Optional[int]:
    \"\"\"Extract weeks from leadtime string\"\"\"
    if pd.isna(leadtime_str):
        return None
    
    import re
    match = re.search(r'(\d+)\s*Weeks', str(leadtime_str), re.IGNORECASE)
    return int(match.group(1)) if match else None


def extract_order_count(ordered_json: str) -> int:
    \"\"\"Extract order count from ordered JSON\"\"\"
    try:
        ordered_data = json.loads(ordered_json)
        if ordered_data and 'ordered' in ordered_data:
            return len(ordered_data['ordered'])
    except (json.JSONDecodeError, KeyError, TypeError):
        pass
    return 0


def extract_demand_index(demand_totals_json: str) -> Optional[float]:
    \"\"\"Extract demand index from demand totals JSON\"\"\"
    try:
        demand_data = json.loads(demand_totals_json)
        if demand_data and 'demand_totals' in demand_data:
            totals = demand_data['demand_totals']
            if totals and len(totals) > 0:
                return float(totals[0]['demand_index'])
    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
        pass
    return None