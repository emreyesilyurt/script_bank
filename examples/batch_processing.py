# examples/batch_processing.py
"""Batch processing example with BigQuery."""

from part_priority_scoring import DataLoader, PartScorer
import os

# Set up BigQuery connection
os.environ['GOOGLE_CLOUD_PROJECT'] = 'your-project-id'

# Load data from BigQuery
loader = DataLoader(project_id='your-project-id')
df = loader.load_sample_data(limit=10000)

# Initialize scorer with custom config
config = {
    'weights': {
        'demand_score': 0.30,
        'availability_score': 0.25,
        'inv_price': 0.20,
        'inv_leadtime': 0.15,
        'is_authorized': 0.10
    }
}

scorer = PartScorer(config)
scored_df = scorer.calculate_scores(df)

# Save results
scored_df.to_csv('part_scores.csv', index=False)