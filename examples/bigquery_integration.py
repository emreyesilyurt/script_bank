"""Example of integrating with BigQuery for production data."""

import os
from part_priority_scoring import DataLoader, PartScorer

def bigquery_example():
    """Example using BigQuery data source."""
    
    # Set up environment
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'your-project-id')
    
    # Initialize data loader
    loader = DataLoader(project_id=project_id)
    
    try:
        # Load sample data from BigQuery
        df = loader.load_sample_data(limit=1000)
        print(f"Loaded {len(df)} parts from BigQuery")
        
        # Score the parts
        scorer = PartScorer()
        scored_df = scorer.calculate_scores(df)
        
        # Save results back to BigQuery
        loader.save_results(scored_df, table_name='part_scores')
        print("Results saved to BigQuery")
        
        # Show top 10 parts
        print("\nTop 10 Scored Parts:")
        top_parts = scored_df[['pn', 'desc', 'priority_score', 'inventory', 'demand_all_time']].head(10)
        print(top_parts)
        
    except Exception as e:
        print(f"Error: {e}")
        print("Make sure GOOGLE_CLOUD_PROJECT is set and you have BigQuery access")

if __name__ == "__main__":
    bigquery_example()