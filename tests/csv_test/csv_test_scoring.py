#!/usr/bin/env python3
"""
CSV Testing Script for Part Priority Scoring
============================================

Temporary testing script to validate scoring logic with manually exported CSV data.
This script is separate from the main repo and used only for development testing.

Usage:
    1. Run the cost-optimized BigQuery query 
    2. Export result as CSV (name it 'test_sample_data.csv')  
    3. Place CSV in same directory as this script
    4. Run: python csv_test_scoring.py

Requirements:
    pip install pandas numpy scikit-learn
"""

import pandas as pd
import numpy as np
import sys
from pathlib import Path
from sklearn.preprocessing import RobustScaler, MinMaxScaler
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

class CSVPartScorer:
    """Simplified part scorer for CSV testing"""
    
    def __init__(self, weights=None):
        """Initialize with custom weights"""
        self.weights = weights or {
            'demand_score': 0.35,          # Increased from 0.25
            'availability_score': 0.35,    # Increased from 0.25
            'inv_leadtime_weeks': 0.15,    # Unchanged
            'inv_moq': 0.10,              # Unchanged
            'is_authorized': 0.05          # Reduced from 0.10
            # REMOVED: 'inv_first_price': 0.10
            # REMOVED: 'has_datasheet': 0.05
        }
        
        self.robust_scaler = RobustScaler()
        self.final_scaler = MinMaxScaler(feature_range=(0, 100))
    
    def score_parts(self, df):
        """Score parts from CSV data."""
        logger.info(f"Scoring {len(df)} parts from CSV...")
        
        if len(df) == 0:
            logger.warning("Empty dataframe provided")
            return df
        
        # Make a copy
        result_df = df.copy()
        
        # Engineer features
        result_df = self._engineer_features(result_df)
        
        # Calculate base score
        result_df['base_score'] = self._calculate_base_score(result_df)
        
        # Apply business boosts
        result_df['boosted_score'] = self._apply_boosts(result_df)
        
        # Normalize to 0-100
        result_df['priority_score'] = self._normalize_scores(result_df['boosted_score'])
        
        # Add percentile
        result_df['score_percentile'] = result_df['priority_score'].rank(pct=True) * 100
        
        # Sort by score
        result_df = result_df.sort_values('priority_score', ascending=False)
        
        logger.info(f"Scoring complete. Average score: {result_df['priority_score'].mean():.2f}")
        
        return result_df
    
    def _engineer_features(self, df):
        """Create engineered features"""
        logger.info("Engineering features...")
        
        # Fill missing values
        df['inventory'] = df['inventory'].fillna(0)
        # REMOVED: df['first_price'] = df['first_price'].fillna(0)
        df['leadtime_weeks'] = df['leadtime_weeks'].fillna(0)
        df['moq'] = df['moq'].fillna(1)
        df['demand_all_time'] = df['demand_all_time'].fillna(0)
        
        # Log transformations (PRICING REMOVED)
        df['log_inventory'] = np.log1p(df['inventory'].clip(lower=0))
        # REMOVED: df['log_first_price'] = np.log1p(df['first_price'].clip(lower=0))
        df['log_moq'] = np.log1p(df['moq'].clip(lower=0))
        
        # Inverse transformations (PRICING REMOVED)
        df['inv_leadtime_weeks'] = 1 / (1 + df['leadtime_weeks'].clip(lower=0))
        # REMOVED: df['inv_first_price'] = 1 / (1 + df['first_price'].clip(lower=0))
        df['inv_moq'] = 1 / (1 + df['moq'].clip(lower=0))
        
        # Binary features
        df['is_authorized'] = (df['source_type'] == 'Authorized').astype(int)
        df['has_datasheet'] = df['datasheet'].notna().astype(int)
        df['in_stock'] = (df['inventory'] > 0).astype(int)
        df['immediate_availability'] = (df['leadtime_weeks'] == 0).astype(int)
        
        # Composite features
        # Availability score
        in_stock_score = df['in_stock'] * 0.5
        immediate_score = df['immediate_availability'] * 0.3
        inventory_ratio = (df['inventory'] / df['moq'].clip(lower=1)).clip(upper=10) * 0.2
        df['availability_score'] = (in_stock_score + immediate_score + inventory_ratio).clip(0, 2)
        
        # Demand score (normalized)
        df['demand_score'] = df['demand_all_time'].fillna(0)
        
        # Scale features (PRICING REMOVED)
        scale_features = [col for col in df.columns 
                         if col.startswith(('log_', 'inv_', 'availability_', 'demand_'))]
        scale_features = [f for f in scale_features if f in df.columns]
        
        if scale_features:
            df[scale_features] = df[scale_features].fillna(0)
            scaled_values = self.robust_scaler.fit_transform(df[scale_features])
            df[scale_features] = scaled_values
        
        return df
    
    def _calculate_base_score(self, df):
        """Calculate weighted base score."""
        base_score = pd.Series(0.0, index=df.index)
        
        for feature, weight in self.weights.items():
            if feature in df.columns:
                feature_values = df[feature].fillna(0)
                base_score += feature_values * weight
                logger.debug(f"Added {feature} with weight {weight}")
            else:
                logger.warning(f"Feature {feature} not found in dataframe")
        
        # Zero out completely unavailable items
        unavailable = (df['inventory'] == 0) & (df['leadtime_weeks'] > 12)
        base_score[unavailable] = 0
        
        return base_score
    
    def _apply_boosts(self, df):
        """Apply business rule boosts."""
        boosted_score = df['base_score'].copy()
        
        # Ample stock boost
        ample_stock = (df['inventory'] >= 10 * df['moq'])
        if ample_stock.sum() > 0:
            boosted_score[ample_stock] *= 1.1
            logger.info(f"Applied ample stock boost to {ample_stock.sum()} parts")
        
        # Immediate shipping boost
        immediate_ship = (df['leadtime_weeks'] == 0)
        if immediate_ship.sum() > 0:
            boosted_score[immediate_ship] *= 1.15
            logger.info(f"Applied immediate ship boost to {immediate_ship.sum()} parts")
        
        # Authorized source boost
        authorized = (df['source_type'] == 'Authorized')
        if authorized.sum() > 0:
            boosted_score[authorized] *= 1.05
            logger.info(f"Applied authorized source boost to {authorized.sum()} parts")
        
        # High demand boost
        high_demand = (df['demand_all_time'] > 100)
        if high_demand.sum() > 0:
            boosted_score[high_demand] *= 1.08
            logger.info(f"Applied high demand boost to {high_demand.sum()} parts")
        
        return boosted_score
    
    def _normalize_scores(self, scores):
        """Normalize scores to 0-100 range."""
        if len(scores) == 0:
            return pd.Series(dtype=float)
        
        if scores.max() == scores.min():
            return pd.Series(50.0, index=scores.index)
        
        # Min-max normalization to 0-100
        min_score = scores.min()
        max_score = scores.max()
        
        normalized = ((scores - min_score) / (max_score - min_score)) * 100
        return normalized.clip(lower=0).round(2)

def load_csv_data(filename='test_sample_data.csv'):
    """Load CSV data for testing."""
    filepath = Path(filename)
    
    if not filepath.exists():
        logger.error(f"CSV file not found: {filepath}")
        logger.info("Please:")
        logger.info("1. Run the cost-optimized BigQuery query")
        logger.info("2. Export result as CSV named 'test_sample_data.csv'")
        logger.info("3. Place CSV in same directory as this script")
        sys.exit(1)
    
    logger.info(f"Loading data from {filepath}")
    df = pd.read_csv(filepath)
    
    logger.info(f"Loaded {len(df)} records")
    logger.info(f"Columns: {list(df.columns)}")
    
    # Validate no pricing columns
    pricing_cols = [col for col in df.columns if 'price' in col.lower()]
    if pricing_cols:
        logger.warning(f"Found pricing columns in CSV: {pricing_cols}")
        logger.info("These will be ignored in scoring")
    
    return df

def run_scoring_tests():
    """Run scoring tests with updated strategies - PRICING REMOVED."""
    print("=" * 60)
    print("Part Priority Scoring - CSV Testing (Pricing Removed)")
    print("=" * 60)
    
    # Load data
    df = load_csv_data()
    
    print(f"Loaded {len(df)} parts for scoring")
    
    # Test different strategies (PRICING REMOVED)
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
    
    results = {}
    
    for strategy_name, weights in strategies.items():
        print(f"\nTesting {strategy_name} strategy...")
        
        # Validate weights sum to 1.0
        weights_sum = sum(weights.values())
        if abs(weights_sum - 1.0) > 0.01:
            logger.warning(f"Weights for {strategy_name} sum to {weights_sum}, not 1.0")
        
        scorer = CSVPartScorer(weights)
        scored_df = scorer.score_parts(df)
        
        # Keep only pn and score
        strategy_results = scored_df[['pn', 'priority_score']].copy()
        strategy_results = strategy_results.rename(columns={'priority_score': f'{strategy_name}_score'})
        
        results[strategy_name] = strategy_results
        
        print(f"  Average score: {scored_df['priority_score'].mean():.1f}")
        print(f"  Score range: {scored_df['priority_score'].min():.1f} - {scored_df['priority_score'].max():.1f}")
        print(f"  Parts with score > 80: {(scored_df['priority_score'] > 80).sum()}")
    
    # Combine all results
    final_results = results['balanced']
    for strategy_name in ['demand_focused', 'availability_focused']:
        final_results = final_results.merge(
            results[strategy_name], 
            on='pn', 
            how='outer'
        )
    
    # Sort by balanced score (renamed from default)
    final_results = final_results.sort_values('balanced_score', ascending=False)
    
    # Save simple results - just PN and scores
    output_file = 'part_scores_only.csv'
    final_results.to_csv(output_file, index=False)
    
    print(f"\n" + "="*50)
    print("Final Results Summary")
    print("="*50)
    print(f"Total parts scored: {len(final_results)}")
    print(f"Results saved to: {output_file}")
    
    # Show top 10 for each strategy
    print(f"\nTop 10 Parts by Strategy:")
    print("PN".ljust(15), "Balanced".ljust(10), "Demand".ljust(10), "Avail".ljust(10))
    print("-" * 55)
    
    for _, row in final_results.head(10).iterrows():
        print(f"{row['pn']:<15} {row['balanced_score']:<10.1f} {row['demand_focused_score']:<10.1f} {row['availability_focused_score']:<10.1f}")
    
    # Show strategy comparison stats
    print(f"\nStrategy Comparison:")
    print(f"Balanced avg:     {final_results['balanced_score'].mean():.1f}")
    print(f"Demand-focused:   {final_results['demand_focused_score'].mean():.1f}")
    print(f"Availability:     {final_results['availability_focused_score'].mean():.1f}")
    
    print(f"\n" + "="*60)
    print("CSV Testing Complete!")
    print("="*60)
    
    return final_results

if __name__ == "__main__":
    try:
        run_scoring_tests()
    except KeyboardInterrupt:
        print("\nTesting interrupted by user")
    except Exception as e:
        logger.error(f"Error during testing: {e}")
        sys.exit(1)