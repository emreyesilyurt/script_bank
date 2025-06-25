import pytest
import pandas as pd
import numpy as np
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from scripts.scoring import ComponentScorer


class TestScoring:
    
    @pytest.fixture
    def sample_scored_data(self):
        # Create sample data with all required features
        return pd.DataFrame({
            # Original features
            'inventory': [0, 100, 1000, 0],
            'leadtime_weeks': [20, 0, 4, 8],
            'moq': [1, 10, 100, 1000],
            'source_type': ['Authorized', 'Other', 'Authorized', 'Other'],
            
            # Engineered features (normalized)
            'demand_score': [0.5, 0.8, 1.0, 0.2],
            'availability_score': [0.0, 1.5, 1.2, 0.0],
            'inv_leadtime': [0.1, 1.0, 0.7, 0.5],
            'inv_price': [0.8, 0.6, 0.4, 0.2],
            'inv_moq': [1.0, 0.8, 0.5, 0.1],
            'is_authorized': [1, 0, 1, 0],
            'has_datasheet': [1, 1, 0, 0]
        })
    
    def test_tiered_filtering(self, sample_scored_data):
        # Test that unavailable items are filtered
        scorer = ComponentScorer()
        result = scorer._apply_tiered_filtering(sample_scored_data)
        
        # First row: inventory=0, leadtime=20 > 12, should be unavailable
        assert not result['is_available'].iloc[0]
        
        # Second row: inventory=100, leadtime=0, should be available
        assert result['is_available'].iloc[1]
        
        # Last row: inventory=0, leadtime=8 < 12, should be available
        assert result['is_available'].iloc[3]
    
    def test_base_score_calculation(self, sample_scored_data):
        # Test weighted base score calculation
        scorer = ComponentScorer()
        sample_scored_data = scorer._apply_tiered_filtering(sample_scored_data)
        base_scores = scorer._calculate_base_score(sample_scored_data)
        
        # First item should have score 0 (unavailable)
        assert base_scores.iloc[0] == 0
        
        # Others should have positive scores
        assert all(base_scores.iloc[1:] > 0)
        
        # Higher demand and availability should give higher scores
        assert base_scores.iloc[2] > base_scores.iloc[3]
    
    def test_boost_application(self, sample_scored_data):
        # Test business rule boosts
        scorer = ComponentScorer()
        
        # Add base scores
        sample_scored_data['base_score'] = [0.0, 0.5, 0.7, 0.3]
        
        # Second row should get immediate availability boost (leadtime=0)
        # Third row should get authorized source boost
        boosted = scorer._apply_boosts(sample_scored_data)
        
        assert boosted.iloc[1] > sample_scored_data['base_score'].iloc[1]  # Immediate boost
        assert boosted.iloc[2] > sample_scored_data['base_score'].iloc[2]  # Authorized boost
    
    def test_score_normalization(self):
        \"\"\"Test score normalization to 0-100\"\"\"
        scorer = ComponentScorer()
        
        raw_scores = pd.Series([0, 0.1, 0.5, 1.0, 2.0])
        normalized = scorer._normalize_scores(raw_scores)
        
        # Check range
        assert normalized.min() == 0
        assert normalized.max() == 100
        
        # Check ordering preserved
        assert all(normalized.diff()[1:] >= 0)
    
    def test_end_to_end_scoring(self, sample_scored_data):
        \"\"\"Test complete scoring pipeline\"\"\"
        scorer = ComponentScorer()
        result = scorer.calculate_scores(sample_scored_data)
        
        assert 'priority_score' in result.columns
        assert result['priority_score'].min() >= 0
        assert result['priority_score'].max() <= 100
        
        # Unavailable item should have score 0
        assert result['priority_score'].iloc[0] == 0