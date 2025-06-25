# Part Priority Scoring Module

A Python module for scoring and prioritizing electronic component parts based on availability, demand, pricing, and other business factors.

## Features

- **Multi-factor scoring**: Combines inventory, demand, pricing, lead times, and vendor reliability
- **Configurable weights**: Easily adjust scoring formula for different business needs
- **Feature engineering**: Automatic creation of log, inverse, binary, and composite features
- **Business rule boosts**: Apply multiplicative improvements for special conditions
- **BigQuery integration**: Native support for loading and saving data
- **Production ready**: Robust error handling and logging

## Quick Start

### Installation

```bash
pip install part-priority-scoring
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

### Custom Weights

```python
from part_priority_scoring import PartScorer

# Custom configuration
config = {
    'weights': {
        'demand_score': 0.40,      # Emphasize demand
        'availability_score': 0.25,
        'inv_first_price': 0.20,   # Price importance
        'inv_leadtime_weeks': 0.15
    }
}

scorer = PartScorer(config)
scored_df = scorer.calculate_scores(df)
```

### BigQuery Integration

```python
from part_priority_scoring import DataLoader
import os

# Set up BigQuery
os.environ['GOOGLE_CLOUD_PROJECT'] = 'your-project-id'

# Load data
loader = DataLoader(project_id='your-project-id')
df = loader.load_sample_data(limit=10000)

# Score and save
scored_df = score_parts(df)
loader.save_results(scored_df, 'part_scores')
```

## Scoring Methodology

The module uses a three-tier scoring approach:

1. **Feature Engineering**: Creates log, inverse, binary, and composite features
2. **Base Scoring**: Weighted sum of normalized features
3. **Business Boosts**: Multiplicative improvements for special conditions

### Default Weights

- `demand_score`: 25% - Historical demand importance
- `availability_score`: 25% - Stock and lead time
- `inv_leadtime_weeks`: 15% - Delivery speed (inverse)
- `inv_first_price`: 10% - Price competitiveness (inverse)
- `inv_moq`: 10% - Order flexibility (inverse)
- `is_authorized`: 10% - Source reliability
- `has_datasheet`: 5% - Documentation completeness


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

## Input Data Format

Required columns:
- `pn`: Part number (string)
- `inventory`: Stock quantity (int)
- `first_price`: Price per unit (float)

Optional columns:
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
- `boosted_score`: Score after boosts
- Various engineered features (`log_*`, `inv_*`, etc.)

## Configuration

### Custom Weights Example

```yaml
weights:
  demand_score: 0.30
  availability_score: 0.25
  inv_first_price: 0.20
  inv_leadtime_weeks: 0.15
  is_authorized: 0.10
```

### Feature Engineering Config

```yaml
features:
  log_transforms: ['inventory', 'first_price', 'moq']
  inverse_transforms: ['leadtime_weeks', 'first_price', 'moq']
  binary_features: ['is_authorized', 'has_datasheet']
```

### Running Tests

```bash
pytest tests/
```

### Code Formatting

```bash
black part_priority_scoring/
isort part_priority_scoring/
```

## Support

For issues and questions, please use the GitHub issue tracker.