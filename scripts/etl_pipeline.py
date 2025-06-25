#!/usr/bin/env python3
\"\"\"
Main ETL pipeline for component priority scoring
\"\"\"

import click
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Optional

from data_loader import DataLoader, extract_demand_index
from feature_engineering import FeatureEngineer
from scoring import ComponentScorer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def merge_datasets(panda_df: pd.DataFrame, demand_df: pd.DataFrame) -> pd.DataFrame:
    \"\"\"Merge panda and demand datasets on 'pn'\"\"\"
    # Extract demand index from demand_totals
    if 'demand_totals' in demand_df.columns:
        demand_df['demand_index'] = demand_df['demand_totals'].apply(extract_demand_index)
    
    # Merge on pn
    merged_df = panda_df.merge(
        demand_df[['pn', 'demand_all_time', 'demand_index']], 
        on='pn', 
        how='left'
    )
    
    logger.info(f"Merged {len(merged_df)} rows")
    
    # Deduplicate on pn_clean if it exists
    if 'pn_clean' in merged_df.columns:
        before_dedup = len(merged_df)
        merged_df = merged_df.drop_duplicates(subset=['pn_clean'], keep='first')
        logger.info(f"Deduplicated {before_dedup - len(merged_df)} rows based on pn_clean")
    
    return merged_df


def save_results(df: pd.DataFrame, output_path: str, format: str = 'csv'):
    \"\"\"Save results to specified format\"\"\"
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    if format == 'csv':
        df.to_csv(output_path, index=False)
        logger.info(f"Saved {len(df)} rows to {output_path}")
    elif format == 'parquet':
        df.to_parquet(output_path, index=False)
        logger.info(f"Saved {len(df)} rows to {output_path}")
    else:
        raise ValueError(f"Unknown format: {format}")


@click.command()
@click.option('--source', type=click.Choice(['csv', 'bigquery']), default='csv', 
              help='Data source type')
@click.option('--input-path', type=str, default='data/input/',
              help='Path to input CSV files (for CSV source)')
@click.option('--output-path', type=str, default=None,
              help='Path for output file')
@click.option('--project-id', type=str, default=None,
              help='GCP project ID (for BigQuery source)')
@click.option('--output-format', type=click.Choice(['csv', 'parquet']), default='csv',
              help='Output file format')
def main(source: str, input_path: str, output_path: Optional[str], 
         project_id: Optional[str], output_format: str):
    \"\"\"Run the component scoring ETL pipeline\"\"\"
    
    logger.info(f"Starting ETL pipeline with source={source}")
    
    # Set default output path if not provided
    if output_path is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = f"data/output/component_scores_{timestamp}.{output_format}"
    
    try:
        # 1. Load data
        loader = DataLoader(source=source, project_id=project_id)
        
        if source == 'csv':
            input_path = Path(input_path)
            panda_df, demand_df = loader.load_data(
                panda_path=input_path / 'panda.csv',
                demand_path=input_path / 'demand_normalized.csv'
            )
        else:
            panda_df, demand_df = loader.load_data()
        
        # 2. Parse JSON fields
        logger.info("Parsing JSON fields...")
        panda_df = loader.parse_json_fields(panda_df)
        
        # 3. Merge datasets
        logger.info("Merging datasets...")
        merged_df = merge_datasets(panda_df, demand_df)
        
        # 4. Feature engineering
        logger.info("Engineering features...")
        config_path = Path(__file__).parent.parent / "config" / "feature_config.json"
        with open(config_path, 'r') as f:
            import json
            feature_config = json.load(f)
        
        engineer = FeatureEngineer(feature_config)
        featured_df = engineer.transform(merged_df)
        
        # 5. Calculate scores
        logger.info("Calculating priority scores...")
        scorer = ComponentScorer()
        scored_df = scorer.calculate_scores(featured_df)
        
        # 6. Select output columns
        output_columns = [
            'pn', 'pn_clean', 'manuf', 'desc', 'category',
            'inventory', 'leadtime_weeks', 'first_price', 'moq',
            'source_type', 'demand_all_time', 'demand_index',
            'availability_score', 'is_authorized', 'has_datasheet',
            'base_score', 'priority_score'
        ]
        
        # Only include columns that exist
        output_columns = [col for col in output_columns if col in scored_df.columns]
        final_df = scored_df[output_columns].sort_values('priority_score', ascending=False)
        
        # 7. Save results
        save_results(final_df, output_path, format=output_format)
        
        # Print summary statistics
        logger.info("\n=== Scoring Summary ===")
        logger.info(f"Total components scored: {len(final_df)}")
        logger.info(f"Components with score > 0: {(final_df['priority_score'] > 0).sum()}")
        logger.info(f"Top score: {final_df['priority_score'].max():.2f}")
        logger.info(f"Median score: {final_df['priority_score'].median():.2f}")
        logger.info(f"Score distribution:")
        logger.info(final_df['priority_score'].describe())
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()