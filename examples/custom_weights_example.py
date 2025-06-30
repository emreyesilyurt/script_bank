"""Example showing different weight configurations for A/B testing."""

import pandas as pd
from part_priority_scoring import PartScorer

def compare_weight_strategies():
    """Compare different weight strategies."""
    
    # Sample data with diverse characteristics
    sample_data = pd.DataFrame({
        'pn': ['HIGH_DEMAND', 'IMMEDIATE_SHIP', 'LOW_COST', 'AUTHORIZED_ONLY'],
        'inventory': [50, 1000, 10, 200],
        'leadtime_weeks': [4, 0, 12, 2], 
        'first_price': [10.0, 5.0, 0.50, 15.0],
        'moq': [100, 1, 1000, 10],
        'demand_all_time': [5000, 100, 50, 800],
        'source_type': ['Other', 'Other', 'Other', 'Authorized'],
        'datasheet': ['url1', 'url2', None, 'url4']
    })
    
    # Different weight strategies
    strategies = {
        'balanced': {
            'demand_score': 0.35,
            'availability_score': 0.35,
            'inv_leadtime_weeks': 0.15,
            'inv_moq': 0.10,
            'is_authorized': 0.05
        },
        'demand_focused': {
            'demand_score': 0.50,
            'availability_score': 0.25,
            'inv_leadtime_weeks': 0.15,
            'inv_moq': 0.05,
            'is_authorized': 0.05
        },
        'availability_focused': {
            'demand_score': 0.20,
            'availability_score': 0.50,
            'inv_leadtime_weeks': 0.20,
            'inv_moq': 0.05,
            'is_authorized': 0.05
        }
    }
    print("=== Weight Strategy Comparison ===")
    results = {}
    
    for strategy_name, weights in strategies.items():
        config = {'weights': weights}
        scorer = PartScorer(config)
        scored_df = scorer.calculate_scores(sample_data)
        
        results[strategy_name] = scored_df[['pn', 'priority_score']].set_index('pn')['priority_score']
        
        print(f"\n{strategy_name.upper()} Strategy:")
        for _, row in scored_df.iterrows():
            print(f"  {row['pn']}: {row['priority_score']:.1f}")
    
    # Show ranking differences
    print("\n=== Ranking Comparison ===")
    comparison_df = pd.DataFrame(results).round(1)
    comparison_df['avg_score'] = comparison_df.mean(axis=1)
    comparison_df = comparison_df.sort_values('avg_score', ascending=False)
    
    print(comparison_df)
    
    return comparison_df

if __name__ == "__main__":
    compare_weight_strategies()
