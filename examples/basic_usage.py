"""Basic usage examples for part priority scoring module."""

import pandas as pd
from part_priority_scoring import score_parts, PartScorer

def simple_example():
    """Simple scoring example with sample data."""
    print("=== Simple Usage Example ===")
    
    # Create sample dataframe
    sample_data = pd.DataFrame({
        'pn': ['PART001', 'PART002', 'PART003', 'PART004'],
        'inventory': [100, 0, 50, 1000],
        'leadtime_weeks': [0, 8, 2, 1],
        'first_price': [1.50, 2.00, 0.75, 5.00],
        'moq': [1, 100, 10, 1],
        'demand_all_time': [500, 20, 200, 1000],
        'source_type': ['Authorized', 'Other', 'Authorized', 'Authorized'],
        'datasheet': ['url1', None, 'url2', 'url3']
    })
    
    # Score parts using convenience function
    scored_df = score_parts(sample_data)
    
    print("Scoring Results:")
    print(scored_df[['pn', 'priority_score', 'score_percentile', 'inventory', 'demand_all_time']].round(2))
    print()
    
    return scored_df

def custom_weights_example():
    """Example with custom weights."""
    print("=== Custom Weights Example ===")
    
    # Sample data
    sample_data = pd.DataFrame({
        'pn': ['PART001', 'PART002', 'PART003'],
        'inventory': [100, 0, 50],
        'leadtime_weeks': [0, 8, 2],
        'first_price': [1.50, 2.00, 0.75],
        'moq': [1, 100, 10],
        'demand_all_time': [500, 20, 200]
    })
    
    # Custom weights - emphasize demand more
    custom_weights = {
        'demand_score': 0.40,  # Increased from 0.25
        'availability_score': 0.20,  # Decreased from 0.25
        'inv_first_price': 0.15,  # Increased price importance
    }
    
    # Score with custom weights
    scored_df = score_parts(sample_data, weights_config=custom_weights)
    
    print("Custom Weights Results:")
    print(scored_df[['pn', 'priority_score', 'demand_all_time']].round(2))
    print()
    
    return scored_df

def advanced_usage_example():
    """Advanced usage with PartScorer class."""
    print("=== Advanced Usage Example ===")
    
    # Sample data
    sample_data = pd.DataFrame({
        'pn': ['PART001', 'PART002', 'PART003'],
        'inventory': [100, 0, 50],
        'leadtime_weeks': [0, 8, 2],
        'first_price': [1.50, 2.00, 0.75],
        'moq': [1, 100, 10],
        'demand_all_time': [500, 20, 200],
        'source_type': ['Authorized', 'Other', 'Authorized']
    })
    
    # Initialize scorer with custom config
    config = {
        'weights': {
            'demand_score': 0.30,
            'availability_score': 0.25,
            'inv_first_price': 0.20,
            'inv_leadtime_weeks': 0.15,
            'is_authorized': 0.10
        }
    }
    
    scorer = PartScorer(config)
    scored_df = scorer.calculate_scores(sample_data)
    
    print("Advanced Usage Results:")
    print(scored_df[['pn', 'base_score', 'boosted_score', 'priority_score']].round(2))
    print()
    
    return scored_df

if __name__ == "__main__":
    # Run examples
    simple_example()
    custom_weights_example()
    advanced_usage_example()
