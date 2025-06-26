# Part Priority Scoring Module

A Python module for scoring and prioritizing electronic component parts based on availability, demand, pricing, and other business factors.

## Features

- **Multi-factor scoring**: Combines inventory, demand, pricing, lead times, and vendor reliability
- **Configurable weights**: Easily adjust scoring formula for different business needs  
- **Feature engineering**: Automatic creation of log, inverse, binary, and composite features
- **Business rule boosts**: Apply multiplicative improvements for special conditions
- **BigQuery integration**: Native support for loading and saving data to `datadojo.part_priority_scoring`

## Quick Start

### Installation

```bash
# Install from source
git clone https://github.com/EETech-Group/part-priority-scoring.git
cd part-priority-scoring
pip install -e .

# Or install development dependencies
pip install -e ".[dev]"
```

### Basic Usage

```python
import pandas as pd
from part_priority_scoring import score_parts

# Your parts data
df = pd.DataFrame({
    'pn': ['PART001', 'PART002', 'PART003'],
    'inventory': [100, 0, 50],
    'leadtime_weeks': [0, 8, 2],
    'first_price': [1.50, 2.00, 0.75],
    'moq': [1, 100, 10],
    'demand_all_time': [500, 20, 200]
})

# Score parts
scored_df = score_parts(df)
print(scored_df[['pn', 'priority_score']].head())
```

### BigQuery Integration

```python
from part_priority_scoring import DataLoader
import os

# Set up BigQuery
os.environ['GOOGLE_CLOUD_PROJECT'] = 'your-project-id'

# Load data from your production tables
loader = DataLoader(project_id='your-project-id')
df = loader.load_sample_data(limit=10000)

# Score and save to datadojo.part_priority_scoring.part_scores
scored_df = score_parts(df)
loader.save_results(scored_df, 'part_scores')
```

### Custom Weights

```python
from part_priority_scoring import PartScorer

# Emphasize demand over availability
custom_weights = {
    'demand_score': 0.40,      # Increased demand importance
    'availability_score': 0.20, # Decreased availability
    'inv_first_price': 0.20,   # Price competitiveness
    'inv_leadtime_weeks': 0.20  # Delivery speed
}

scored_df = score_parts(df, weights_config=custom_weights)
```

## Scoring Methodology

The module uses a sophisticated three-tier approach:

1. **Feature Engineering**: Creates log, inverse, binary, and composite features
2. **Base Scoring**: Weighted sum of normalized features  
3. **Business Boosts**: Multiplicative improvements for exceptional conditions

### Default Weights

- `demand_score`: 25% - Historical demand importance
- `availability_score`: 25% - Stock and lead time combined
- `inv_leadtime_weeks`: 15% - Delivery speed (inverse)
- `inv_first_price`: 10% - Price competitiveness (inverse)
- `inv_moq`: 10% - Order flexibility (inverse)
- `is_authorized`: 10% - Source reliability
- `has_datasheet`: 5% - Documentation completeness

## Input Data Format

### Required Columns
- `pn`: Part number (string)

### Optional Columns  
- `inventory`: Stock quantity (int)
- `first_price`: Price per unit (float)  
- `leadtime_weeks`: Lead time in weeks (int)
- `moq`: Minimum order quantity (int)
- `demand_all_time`: Historical demand (int)
- `source_type`: Vendor type ('Authorized', 'Other')
- `datasheet`: Documentation URL (string)
- `category`: Part category (string)
- `desc`: Part description (string)

## Output Format

The module adds these columns to your dataframe:
- `priority_score`: Final score (0-100)
- `score_percentile`: Percentile ranking
- `base_score`: Score before boosts
- `boosted_score`: Score after business rule boosts
- Various engineered features (`log_*`, `inv_*`, etc.)

## Examples

### A/B Testing Different Strategies

```python
from part_priority_scoring import PartScorer

# Strategy A: Demand-focused
strategy_a = PartScorer({
    'weights': {
        'demand_score': 0.50,
        'availability_score': 0.30,
        'inv_first_price': 0.20
    }
})

# Strategy B: Availability-focused  
strategy_b = PartScorer({
    'weights': {
        'demand_score': 0.20,
        'availability_score': 0.50,
        'inv_leadtime_weeks': 0.30
    }
})

results_a = strategy_a.calculate_scores(df)
results_b = strategy_b.calculate_scores(df)
```

### Integration with Existing Code

```python
# Easy integration into existing workflow
from part_priority_scoring import score_parts

class PartEnrichmentPipeline:
    def __init__(self):
        self.scoring_enabled = True
    
    def process_parts(self, parts_df):
        """Add priority scoring to existing pipeline."""
        
        # Your existing processing
        processed_df = self.existing_processing(parts_df)
        
        # Add priority scoring
        if self.scoring_enabled:
            scored_df = score_parts(processed_df)
            
            # Use scores for prioritization
            high_priority = scored_df[scored_df['priority_score'] >= 80]
            return self.prioritize_enrichment(high_priority)
        
        return processed_df
```

## Testing

```bash
# Run tests
pytest tests/

# With coverage
pytest tests/ --cov=part_priority_scoring --cov-report=html

# Run specific test
pytest tests/test_scorer.py::TestPartScorer::test_basic_scoring -v
```

## Development

### Code Formatting

```bash
# Format code
black part_priority_scoring/
isort part_priority_scoring/

# Lint code  
flake8 part_priority_scoring/
```

### Adding Custom Features

```python
# Extend the FeatureEngineer class
from part_priority_scoring.core.feature_engineer import FeatureEngineer

class CustomFeatureEngineer(FeatureEngineer):
    def _create_custom_features(self, df):
        """Add your custom features here."""
        
        # Example: Price-to-demand ratio
        if all(col in df.columns for col in ['first_price', 'demand_all_time']):
            df['price_demand_ratio'] = df['first_price'] / (df['demand_all_time'] + 1)
        
        return df
    
    def transform(self, df):
        """Override to include custom features."""
        df = super().transform(df)
        df = self._create_custom_features(df)
        return df
```

## Configuration Files

The module uses YAML configuration files that you can customize:

### `config/weights.yaml`
```yaml
base_weights:
  demand_score: 0.25
  availability_score: 0.25
  inv_leadtime_weeks: 0.15
  inv_first_price: 0.10
  inv_moq: 0.10
  is_authorized: 0.10
  has_datasheet: 0.05
```

### `config/feature_config.yaml`
```yaml
log_transforms:
  - inventory
  - first_price  
  - moq

inverse_transforms:
  - leadtime_weeks
  - first_price
  - moq

binary_features:
  - is_authorized
  - has_datasheet
  - in_stock
  - immediate_availability
```

## Production Deployment

### Environment Variables

```bash
export GOOGLE_CLOUD_PROJECT="your-project-id"
export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account.json"
```

### BigQuery Setup

The module automatically creates these tables in `datadojo.part_priority_scoring`:

- `part_scores` - Main scoring results
- `part_scores_history` - Historical snapshots
- `scoring_metrics` - Pipeline performance data

### Performance Guidelines

| Dataset Size | Expected Time | Memory Usage | BigQuery Cost |
|--------------|---------------|--------------|---------------|
| 1K parts | 1-2 seconds | 100MB | $0.001 |
| 10K parts | 10-15 seconds | 500MB | $0.01 |
| 100K parts | 1-2 minutes | 2GB | $0.10 |
| 1M+ parts | 10-20 minutes | 8GB+ | $1.00+ |

### Batch Processing

```python
# For large datasets, process in chunks
def score_large_dataset(df, chunk_size=50000):
    results = []
    
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i+chunk_size]
        scored_chunk = score_parts(chunk)
        results.append(scored_chunk)
    
    return pd.concat(results, ignore_index=True)
```

## API Reference

### `score_parts(df, weights_config=None, feature_config=None)`

Convenience function to score parts dataframe.

**Parameters:**
- `df`: Input dataframe with part data
- `weights_config`: Optional custom weights dictionary  
- `feature_config`: Optional feature engineering configuration

**Returns:** DataFrame with `priority_score` column added

### `PartScorer(config=None)`

Main scoring class for advanced usage.

**Methods:**
- `calculate_scores(df, normalize=True)`: Calculate priority scores
- `_engineer_features(df)`: Create scoring features
- `_apply_boosts(df)`: Apply business rule boosts

### `DataLoader(project_id=None, dataset=None)`

BigQuery data loading utilities.

**Methods:**
- `load_sample_data(limit=10000)`: Load sample data from BigQuery
- `save_results(df, table_name='part_scores')`: Save results to BigQuery

### `FeatureEngineer(config=None)`

Feature engineering pipeline.

**Methods:**
- `transform(df)`: Apply all feature transformations
- `_create_log_features(df)`: Log transformations
- `_create_inverse_features(df)`: Inverse transformations
- `_create_binary_features(df)`: Binary indicators
- `_create_composite_features(df)`: Multi-signal features

## Troubleshooting

### Common Issues

1. **Import Error**: Make sure to install the package with `pip install -e .`

2. **BigQuery Authentication**: Set `GOOGLE_APPLICATION_CREDENTIALS` environment variable

3. **Missing Features**: The module gracefully handles missing columns by setting default values

4. **Performance Issues**: For large datasets, consider processing in smaller batches

### Getting Help

1. Check the examples in `examples/` directory
2. Run the test suite to verify installation: `pytest tests/`
3. Review configuration files in `part_priority_scoring/config/`

## Changelog

### v1.0.0
- Initial release
- Core scoring functionality
- BigQuery integration
- Comprehensive test suite

---