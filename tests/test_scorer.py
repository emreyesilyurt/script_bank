"""Tests for part scorer functionality."""

import pytest
import pandas as pd
import numpy as np
from part_priority_scoring import PartScorer, score_parts

class TestPartScorer:
    
    @pytest.fixture
    def sample_data(self):
        """Sample data for testing."""
        return pd.DataFrame({
            'pn': ['PART001', 'PART002', 'PART003', 'PART004'],
            'inventory': [100, 0, 50, 1000],
            'leadtime_weeks': [0, 8, 2, 1],
            'first_price': [1.50, 2.00, 0.75, 5.00],
            'moq': [1, 100, 10, 1],
            'demand_all_time': [500, 20, 200, 1000],
            'source_type': ['Authorized', 'Other', 'Authorized', 'Authorized'],
            'datasheet': ['url1', None, 'url2', 'url3']
        })
    
    def test_basic_scoring(self, sample_data):
        """Test basic scoring functionality."""
        scored_df = score_parts(sample_data)
        
        # Check that scores are added
        assert 'priority_score' in scored_df.columns
        assert 'score_percentile' in scored_df.columns
        
        # Check score range
        assert scored_df['priority_score'].min() >= 0
        assert scored_df['priority_score'].max() <= 100
        
        # Check that results are sorted by score
        assert scored_df['priority_score'].is_monotonic_decreasing
    
    def test_custom_weights(self, sample_data):
        """Test scoring with custom weights."""
        custom_weights = {
            'demand_score': 0.50,
            'availability_score': 0.30,
            'inv_first_price': 0.20
        }
        
        scored_df = score_parts(sample_data, weights_config=custom_weights)
        
        assert 'priority_score' in scored_df.columns
        assert len(scored_df) == len(sample_data)
    
    def test_scorer_class(self, sample_data):
        """Test PartScorer class directly."""
        scorer = PartScorer()
        scored_df = scorer.calculate_scores(sample_data)
        
        # Check that intermediate scores are included
        assert 'base_score' in scored_df.columns
        assert 'boosted_score' in scored_df.columns
        assert 'priority_score' in scored_df.columns
    
    def test_feature_engineering(self, sample_data):
        """Test that features are properly engineered."""
        scorer = PartScorer()
        scored_df = scorer.calculate_scores(sample_data)
        
        # Check that engineered features exist
        expected_features = [
            'log_inventory', 'log_first_price', 'log_moq',
            'inv_leadtime_weeks', 'inv_first_price', 'inv_moq',
            'is_authorized', 'has_datasheet', 'in_stock',
            'availability_score', 'demand_score'
        ]
        
        for feature in expected_features:
            assert feature in scored_df.columns, f"Missing feature: {feature}"
    
    def test_boost_application(self, sample_data):
        """Test that business boosts are applied."""
        scorer = PartScorer()
        scored_df = scorer.calculate_scores(sample_data)
        
        # Check that some parts got boosted
        boosted_parts = scored_df[scored_df['boosted_score'] > scored_df['base_score']]
        assert len(boosted_parts) > 0, "No parts received boosts"
    
    def test_empty_dataframe(self):
        """Test handling of empty dataframe."""
        empty_df = pd.DataFrame()
        scored_df = score_parts(empty_df)
        
        assert len(scored_df) == 0
        assert 'priority_score' in scored_df.columns
    
    def test_missing_columns(self):
        """Test handling of missing columns."""
        minimal_df = pd.DataFrame({
            'pn': ['PART001', 'PART002'],
            'inventory': [100, 50]
        })
        
        # Should not crash with missing columns
        scored_df = score_parts(minimal_df)
        assert 'priority_score' in scored_df.columns
        assert len(scored_df) == 2