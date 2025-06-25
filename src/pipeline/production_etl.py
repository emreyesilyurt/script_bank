import asyncio
import click
import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any
import traceback

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

from config.settings import Settings
from utils.logging_config import setup_logging
from utils.validators import DataValidator
from core.enhanced_data_loader import EnhancedDataLoader
from core.enhanced_scorer import EnhancedPartScorer

class ProductionETLPipeline:
    """Production ETL pipeline with comprehensive error handling and monitoring."""
    
    def __init__(self, environment: str = 'development'):
        self.environment = environment
        self.settings = Settings(env=environment)
        self.logger = setup_logging(level='INFO' if environment == 'production' else 'DEBUG')
        
        # Initialize components
        self.data_loader = EnhancedDataLoader(self.settings)
        self.scorer = EnhancedPartScorer(self.settings)
        self.validator = DataValidator(self.settings.feature_config)
        
        # Pipeline metrics
        self.pipeline_metrics = {
            'start_time': None,
            'end_time': None,
            'total_batches': 0,
            'successful_batches': 0,
            'failed_batches': 0,
            'total_parts': 0,
            'total_scored': 0
        }
    
    async def run_full_pipeline(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """Run the complete scoring pipeline."""
        self.pipeline_metrics['start_time'] = datetime.now()
        self.logger.info(f"Starting production ETL pipeline in {self.environment} environment")
        
        try:
            if self.settings.processing.sampling_enabled or limit:
                # Sample mode for development/testing
                sample_size = limit or self.settings.processing.sample_size
                return await self._run_sample_pipeline(sample_size)
            else:
                # Full production mode
                return await self._run_batch_pipeline()
                
        except Exception as e:
            self.logger.error(f"Pipeline failed with error: {e}")
            self.logger.error(traceback.format_exc())
            raise
        finally:
            self.pipeline_metrics['end_time'] = datetime.now()
            self._log_final_metrics()
    
    async def _run_sample_pipeline(self, sample_size: int) -> Dict[str, Any]:
        """Run pipeline with sample data."""
        self.logger.info(f"Running sample pipeline with {sample_size} components")
        
        # Load sample data
        panda_df, demand_df = await self.data_loader.load_sample_data(limit=sample_size)
        
        # Merge datasets
        merged_df = self._merge_datasets(panda_df, demand_df)
        
        # Validate data
        validation_result = self.validator.validate_batch(merged_df, batch_id="sample")
        
        if not validation_result.is_valid and self.environment == 'production':
            raise ValueError(f"Data validation failed: {validation_result.issues}")
        
        # Score components
        scored_df = self.scorer.calculate_scores(merged_df, batch_id="sample")
        
        # Save results
        output_table = f"component_scores_sample_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.data_loader.save_results(scored_df, output_table)
        
        # Update metrics
        self.pipeline_metrics.update({
            'total_batches': 1,
            'successful_batches': 1,
            'total_components': len(merged_df),
            'total_scored': (scored_df['priority_score'] > 0).sum()
        })
        
        return self._create_pipeline_summary(scored_df)
    
    async def _run_batch_pipeline(self) -> Dict[str, Any]:
        """Run pipeline with batch processing for production."""
        self.logger.info("Running production batch pipeline")
        
        batch_size = self.settings.processing.batch_size
        batch_number = 0
        all_results = []
        
        async for panda_df, demand_df in self.data_loader.load_batch_data(batch_size=batch_size):
            batch_number += 1
            batch_id = f"batch_{batch_number:04d}_{datetime.now().strftime('%H%M%S')}"
            
            try:
                self.logger.info(f"Processing {batch_id} with {len(panda_df)} components")
                
                # Merge datasets
                merged_df = self._merge_datasets(panda_df, demand_df)
                
                # Validate batch
                validation_result = self.validator.validate_batch(merged_df, batch_id=batch_id)
                
                if validation_result.quality_score < 50:
                    self.logger.warning(f"Low quality score for {batch_id}: {validation_result.quality_score}")
                
                # Score components
                scored_df = self.scorer.calculate_scores(merged_df, batch_id=batch_id)
                
                # Save batch results
                self.data_loader.save_results(scored_df, "component_scores", write_mode="WRITE_APPEND")
                
                # Track metrics
                self.pipeline_metrics['successful_batches'] += 1
                self.pipeline_metrics['total_components'] += len(merged_df)
                self.pipeline_metrics['total_scored'] += (scored_df['priority_score'] > 0).sum()
                
                all_results.append(scored_df)
                
            except Exception as e:
                self.logger.error(f"Error processing {batch_id}: {e}")
                self.pipeline_metrics['failed_batches'] += 1
                
                if self.environment == 'production' and self.pipeline_metrics['failed_batches'] > 5:
                    raise RuntimeError("Too many batch failures, stopping pipeline")
            
            finally:
                self.pipeline_metrics['total_batches'] += 1
                
                # Progress logging
                if batch_number % 10 == 0:
                    self._log_progress()
        
        # Combine all results for summary
        if all_results:
            import pandas as pd
            combined_df = pd.concat(all_results, ignore_index=True)
            return self._create_pipeline_summary(combined_df)
        else:
            return self._create_pipeline_summary(None)
    
    def _merge_datasets(self, panda_df, demand_df):
        """Merge panda and demand datasets."""
        self.logger.debug(f"Merging {len(panda_df)} panda rows with {len(demand_df)} demand rows")
        
        # Merge on part number
        merged_df = panda_df.merge(
            demand_df[['pn', 'demand_all_time', 'demand_index']], 
            on='pn', 
            how='left'
        )
        
        # Fill missing demand data
        merged_df['demand_all_time'] = merged_df['demand_all_time'].fillna(0)
        merged_df['demand_index'] = merged_df['demand_index'].fillna(0)
        
        # Deduplicate if needed
        if 'pn_clean' in merged_df.columns:
            before_dedup = len(merged_df)
            merged_df = merged_df.drop_duplicates(subset=['pn_clean'], keep='first')
            dedup_count = before_dedup - len(merged_df)
            if dedup_count > 0:
                self.logger.info(f"Deduplicated {dedup_count} rows based on pn_clean")
        
        return merged_df
    
    def _create_pipeline_summary(self, scored_df) -> Dict[str, Any]:
        """Create comprehensive pipeline summary."""
        summary = {
            'pipeline_metrics': self.pipeline_metrics.copy(),
            'scorer_metrics': self.scorer.get_metrics_summary(),
            'execution_summary': {}
        }
        
        if scored_df is not None and len(scored_df) > 0:
            scores = scored_df['priority_score']
            summary['execution_summary'] = {
                'total_components': len(scored_df),
                'scored_components': (scores > 0).sum(),
                'avg_score': float(scores.mean()),
                'median_score': float(scores.median()),
                'top_score': float(scores.max()),
                'score_distribution': {
                    'high_priority_count': int((scores >= 90).sum()),
                    'medium_priority_count': int(((scores >= 50) & (scores < 90)).sum()),
                    'low_priority_count': int(((scores > 0) & (scores < 50)).sum()),
                    'zero_score_count': int((scores == 0).sum())
                }
            }
        
        return summary
    
    def _log_progress(self):
        """Log pipeline progress."""
        metrics = self.pipeline_metrics
        success_rate = metrics['successful_batches'] / metrics['total_batches'] * 100
        
        self.logger.info(f"Pipeline Progress - "
                        f"Batches: {metrics['successful_batches']}/{metrics['total_batches']} "
                        f"({success_rate:.1f}% success), "
                        f"Components: {metrics['total_components']:,}, "
                        f"Scored: {metrics['total_scored']:,}")
    
    def _log_final_metrics(self):
        """Log final pipeline metrics."""
        metrics = self.pipeline_metrics
        
        if metrics['start_time'] and metrics['end_time']:
            duration = (metrics['end_time'] - metrics['start_time']).total_seconds()
            components_per_second = metrics['total_components'] / duration if duration > 0 else 0
            
            self.logger.info("=== PIPELINE COMPLETION SUMMARY ===")
            self.logger.info(f"Environment: {self.environment}")
            self.logger.info(f"Duration: {duration:.2f} seconds")
            self.logger.info(f"Batches processed: {metrics['successful_batches']}/{metrics['total_batches']}")
            self.logger.info(f"Components processed: {metrics['total_components']:,}")
            self.logger.info(f"Components scored: {metrics['total_scored']:,}")
            self.logger.info(f"Processing rate: {components_per_second:.2f} components/second")
            
            if metrics['failed_batches'] > 0:
                self.logger.warning(f"Failed batches: {metrics['failed_batches']}")

@click.command()
@click.option('--environment', '-e', 
              type=click.Choice(['development', 'staging', 'production']), 
              default='development',
              help='Environment to run in')
@click.option('--limit', '-l', type=int, default=None,
              help='Limit number of components (for testing)')
@click.option('--log-level', 
              type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR']),
              default='INFO',
              help='Logging level')
async def main(environment: str, limit: Optional[int], log_level: str):
    """Run the production component scoring pipeline."""
    
    # Setup logging
    logger = setup_logging(level=log_level)
    
    try:
        # Initialize and run pipeline
        pipeline = ProductionETLPipeline(environment=environment)
        summary = await pipeline.run_full_pipeline(limit=limit)
        
        # Log summary
        logger.info("Pipeline completed successfully!")
        logger.info(f"Summary: {summary}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        return 1

if __name__ == "__main__":
    import asyncio
    exit_code = asyncio.run(main())
    sys.exit(exit_code)