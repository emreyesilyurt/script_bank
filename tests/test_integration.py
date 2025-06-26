"""Integration tests for the complete scoring pipeline."""

import pytest
import pandas as pd
from part_priority_scoring import score_parts, PartScorer, DataLoader

class TestIntegration:
    
    @pytest.fixture
    def comprehensive_data(self):
        """Comprehensive test dataset."""
        return pd.DataFrame({
            'pn': ['PART001', 'PART002', 'PART003', 'PART004', 'PART005'],
            'inventory': [1000, 0, 50, 500, 10],
            'leadtime_weeks': [0, 16, 2, 1, 4],
            'first_price': [1.00, 10.00, 0.50, 5.00, 2.00],
            'moq': [1, 1000, 10, 100, 1],
            'demand_all_time': [5000, 10, 1000, 500, 2000],
            'source_type': ['Authorized', 'Other', 'Authorized', 'Other', 'Authorized'],
            'datasheet': ['url1', 'url2', None, 'url4', 'url5'],
            'category': ['IC', 'Connector', 'Resistor', 'Capacitor', 'IC'],
            'desc': ['Microcontroller', 'USB Connector', '1k Resistor', '100uF Cap', 'Memory']
        })
    
    def test_end_to_end_scoring(self, comprehensive_data):
        """Test complete end-to-end scoring process."""
        scored_df = score_parts(comprehensive_data)
        
        # Verify all expected columns are present
        expected_columns = [
            'pn', 'priority_score', 'score_percentile', 'base_score', 
            'boosted_score', 'availability_score', 'demand_score'
        ]
        
        for col in expected_columns:
            assert col in scored_df.columns, f"Missing column: {col}"
        
        # Verify score properties
        assert len(scored_df) == len(comprehensive_data)
        assert scored_df['priority_score'].min() >= 0
        assert scored_df['priority_score'].max() <= 100
        assert scored_df['priority_score'].is_monotonic_decreasing
    
    def test_different_weight_strategies(self, comprehensive_data):
        """Test that different weight strategies produce different results."""
        
        # Strategy 1: Demand-focused
        demand_weights = {
            'demand_score': 0.60,
            'availability_score': 0.20,
            'inv_first_price': 0.20
        }
        
        # Strategy 2: Availability-focused  
        availability_weights = {
            'demand_score': 0.20,
            'availability_score': 0.60,
            'inv_first_price': 0.20
        }
        
        demand_scores = score_parts(comprehensive_data, weights_config=demand_weights)
        availability_scores = score_parts(comprehensive_data, weights_config=availability_weights)
        
        # Results should be different
        assert not demand_scores['priority_score'].equals(availability_scores['priority_score'])
        
        # High demand part should rank higher in demand-focused strategy
        high_demand_part = comprehensive_data[comprehensive_data['demand_all_time'].idxmax()]['pn']
        
        demand_rank = demand_scores[demand_scores['pn'] == high_demand_part].index[0]
        availability_rank = availability_scores[availability_scores['pn'] == high_demand_part].index[0]
        
        # Lower index = higher rank (better position)
        assert demand_rank <= availability_rank
    
    def test_boost_effectiveness(self, comprehensive_data):
        """Test that business boosts actually improve scores."""
        scorer = PartScorer()
        scored_df = scorer.calculate_scores(comprehensive_data)
        
        # Find parts that should get boosts
        immediate_parts = scored_df[scored_df['leadtime_weeks'] == 0]
        authorized_parts = scored_df[scored_df['source_type'] == 'Authorized']
        
        # These parts should have boosted scores > base scores
        if len(immediate_parts) > 0:
            boosted_immediate = immediate_parts[immediate_parts['boosted_score'] > immediate_parts['base_score']]
            assert len(boosted_immediate) > 0, "Immediate availability boost not applied"
        
        if len(authorized_parts) > 0:
            boosted_authorized = authorized_parts[authorized_parts['boosted_score'] > authorized_parts['base_score']]
            assert len(boosted_authorized) > 0, "Authorized source boost not applied"
    
    def test_score_consistency(self, comprehensive_data):
        """Test that scoring is consistent across multiple runs."""
        
        # Run scoring multiple times
        results = []
        for _ in range(3):
            scored_df = score_parts(comprehensive_data)
            results.append(scored_df['priority_score'].values)
        
        # Results should be identical
        for i in range(1, len(results)):
            assert np.array_equal(results[0], results[i]), "Scoring results are not consistent"
    
    def test_realistic_score_distribution(self, comprehensive_data):
        """Test that score distribution is realistic."""
        scored_df = score_parts(comprehensive_data)
        scores = scored_df['priority_score']
        
        # Should have good score spread
        assert scores.std() > 10, "Score variance too low"
        
        # Should not have all scores at extremes
        mid_range_count = len(scores[(scores > 20) & (scores < 80)])
        assert mid_range_count > 0, "No scores in middle range"
        
        # Best part should have significantly higher score than worst
        score_range = scores.max() - scores.min()
        assert score_range > 30, "Score range too narrow"